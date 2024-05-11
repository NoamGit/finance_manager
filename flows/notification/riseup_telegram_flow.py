import asyncio
import sys
from datetime import datetime
from typing import Dict, Any, Tuple

import pandas as pd
from prefect import flow, task

from flows.common.tasks.mysql_task import async_load_to_mysql
from src.core.common import async_get_db_secrets
from src.core.notification.categorization import THEME_CATEGORY, categorize_by_account_number, HighLevelCategories
from src.interface.common.utils import get_mysql_client, get_today
from src.interface.notification.telegram import send_monthly_progress_to_telegram, enrich_with_more_details

sys.path.append("../../src/core")
sys.path.append("../../src/interface")


async def get_flow_db_secrets() -> Dict[str, str]:
    cred = await async_get_db_secrets()
    cred.update({"mysql_table_name": "mart_mtd_expenses"})
    return cred


@task()
async def fetch_month_to_date_aggregated_snapshot(cred: Dict[str, Any], start_date: str,
                                                  table_name: str) -> pd.DataFrame:
    db = get_mysql_client(cred)
    query = f"""
        SELECT datetime, category, category_id, account_number, month_to_date_expense 
            FROM {table_name} yt
            WHERE (account_number, category, datetime) IN (
              SELECT account_number, category, MAX(datetime) as max_datetime
              FROM {table_name}
              WHERE datetime BETWEEN DATE_FORMAT(NOW(), '%%Y-%%m-01') AND '{start_date}'
              GROUP BY account_number, category
            )
            ORDER BY account_number, category, datetime;
    """
    df = pd.read_sql(query, db)
    df["datetime"] = start_date
    return df


@task()
async def fetch_month_to_date_raw_data(cred: Dict[str, Any], start_date: str, table_name: str) -> pd.DataFrame:
    db = get_mysql_client(cred)
    query = f"""
    SELECT date
        , charged
        , category
        , description
        , account_number
        , category_id
    FROM {table_name} yt
    WHERE date BETWEEN DATE_FORMAT(NOW(), '%%Y-%%m-01') AND '{start_date}'
    and transaction_type = 'expense'
    ORDER BY date desc, category, account_number;
    """
    df = pd.read_sql(query, db)
    return df


def add_high_level_category_to_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "category_id" not in df.columns or "account_number" not in df.columns:
        raise ValueError("category_id or account_number is not in the dataframe")
    df["category_id"] = df["category_id"].where(pd.notnull(df["category_id"]), -1)
    df["high_category"] = df["category_id"].apply(
        lambda x: THEME_CATEGORY.get(int(x), "הוצאות משתנות"))
    df["high_category"] = df[["high_category", "account_number"]].apply(categorize_by_account_number,
                                                                        axis=1)
    return df


@task()
async def transform_month_to_date_data(snapshot: pd.DataFrame, raw_data: pd.DataFrame,
                                       monthly_limit: Dict[str, float]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    snapshot = add_high_level_category_to_dataframe(snapshot)
    raw_data = add_high_level_category_to_dataframe(raw_data)
    snapshot_transformed = snapshot.groupby(["datetime", "high_category"])['month_to_date_expense'].sum()
    snapshot_transformed = snapshot_transformed.reset_index()
    snapshot_transformed["limit"] = snapshot_transformed["high_category"].apply(lambda x: monthly_limit.get(x))
    snapshot_transformed["diff"] = snapshot_transformed["limit"] - snapshot_transformed["month_to_date_expense"]
    return snapshot_transformed, raw_data


@flow()
async def riseup_telegram_flow(start_time: str = None
                               , constant_expenses: float = 13000
                               , dynamic_expenses_eden: float = 2500
                               , dynamic_expenses_noam: float = 2000
                               , dynamic_expenses_mutual: float = 1000
                               , transport_expenses: float = 850
                               , supermarket_expenses: float = 2600
                               , timeout: int = 60 * 1
                               ):
    monthly_limit = {
        HighLevelCategories.GROCERIES.value: supermarket_expenses,
        HighLevelCategories.TRANSPORTATION.value: transport_expenses,
        HighLevelCategories.FIXED.value: constant_expenses,
        HighLevelCategories.VARIABLE_EXPENSE_NOAM.value: dynamic_expenses_noam,
        HighLevelCategories.VARIABLE_EXPENSE_EDEN.value: dynamic_expenses_eden,
        HighLevelCategories.VARIABLE_EXPENSE_MUTUAL.value: dynamic_expenses_mutual
    }
    credentials = await get_flow_db_secrets()
    start_time = get_today() if not start_time else start_time
    snapshot = await fetch_month_to_date_aggregated_snapshot(credentials, start_date=start_time,
                                                             table_name="mart_mtd_expenses")
    raw_data = await fetch_month_to_date_raw_data(credentials, start_date=start_time, table_name="clean_transactions")
    prep_snapshot, prep_data = await transform_month_to_date_data(snapshot, raw_data, monthly_limit)
    # await async_load_to_mysql(prep_snapshot, credentials, "mtd_snapshot")
    await send_monthly_progress_to_telegram(prep_snapshot)
    await enrich_with_more_details(data=prep_data, timeout=timeout, wait_for=[send_monthly_progress_to_telegram])


if __name__ == '__main__':
    param = {}
    asyncio.run(riseup_telegram_flow(**param))

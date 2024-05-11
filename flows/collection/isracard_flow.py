import json
import sys
from datetime import datetime, timedelta
from subprocess import run, PIPE
from typing import List, Dict, Any, Optional, Union

from prefect import flow, task
from pydantic import ValidationError

from flows.common.tasks.mysql_task import load_to_mysql
from src.core.common import DATE_FORMAT
from src.core.common import get_db_secrets
from src.core.collection.model import IsracardCredentials
from src.interface import MONGO_CREDIT_TABLE_NAME, IBS_ISRACARD_PATH, INTERFACE_WORKING_DIR
from src.interface.collection.isracard.model import IsracardCardCredentialsFactory
from flows.common.tasks.mongo_task import load_transactions_to_mongo_task

sys.path.append("../../src/core")
sys.path.append("../../src/interface")
from src.interface.common.utils import validate_documents, unpack_to_unnested_format, translate_to_mysql_format


@task()
def get_flow_db_secrets() -> Dict[str, str]:
    cred = get_db_secrets()
    cred.update({"mongo_table_name": "transactions",
                 "mysql_table_name": "credit_transactions"})
    return cred


def get_isracard_secrets(card_suffix: str) -> IsracardCredentials:
    credential_factory = IsracardCardCredentialsFactory()
    return credential_factory.get_credentials(card_suffix)


def transform_scraper_params(start_date: Optional[datetime.date] = None
                             , future_months_to_scrape: Optional[str] = None) -> Dict[str, Union[datetime, int]]:
    future_months_to_scrape = 1 if not future_months_to_scrape else future_months_to_scrape
    if future_months_to_scrape < 1:
        raise ValidationError(f'future_months_to_scrape {future_months_to_scrape} is not valid')
    if start_date:
        datetime.strptime(start_date, DATE_FORMAT)

    start_date = datetime.strftime(datetime.now() - timedelta(days=31), DATE_FORMAT) if not start_date else start_date
    return dict(start_date=start_date, future_months_to_scrape=future_months_to_scrape)


@task()
def fetch(card_suffix: str, time_param: Dict[str, Union[datetime, int]]):
    isracard_credentials_block = get_isracard_secrets(card_suffix)
    cmd = ["node"
        , IBS_ISRACARD_PATH
        , "--date"
        , f"{time_param['start_date']}"
        , "--months"
        , f"{time_param['future_months_to_scrape']}"
        , '--id'
        , f"{isracard_credentials_block.user_name.get_secret_value()}"
        , '--card6num'
        , f"{isracard_credentials_block.cardnum.get_secret_value()}"
        , '--password'
        , f"{isracard_credentials_block.password.get_secret_value()}"
           ]
    res = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, cwd=INTERFACE_WORKING_DIR)
    if res.returncode == 0 and not res.stderr:
        return json.loads(res.stdout)
    raise ChildProcessError(f'fetching process failed: {res.stderr}')


@task()
def translate_to_mysql_data_model(data: List[Dict[str, Any]]):
    validate_documents(data)
    transformed_data = unpack_to_unnested_format(data)
    transformed_data = translate_to_mysql_format(transformed_data)
    return transformed_data


@flow
def scrape_isracard(card_suffix: str, start_date: Optional[str] = None,
                    future_months_to_scrape: Optional[int] = None):
    credentials = get_flow_db_secrets()
    scraper_params = transform_scraper_params(start_date=start_date
                                              , future_months_to_scrape=future_months_to_scrape)
    data = fetch(card_suffix, scraper_params)
    load_transactions_to_mongo_task(data, credentials, table_name=MONGO_CREDIT_TABLE_NAME)
    processed_data = translate_to_mysql_data_model(data)
    load_to_mysql(processed_data, credentials, 'credit_transaction')


@flow
def backfill_isracard(card_suffix: str, fields_to_update: List[str], start_date: Optional[str] = None,
                      future_months_to_scrape: Optional[int] = None):
    credentials = get_flow_db_secrets()
    scraper_params = transform_scraper_params(start_date=start_date
                                              , future_months_to_scrape=future_months_to_scrape)
    data = fetch(card_suffix, scraper_params)
    processed_data = translate_to_mysql_data_model(data)
    load_to_mysql(processed_data, credentials, 'credit_transaction', fields_to_update, identifier="id")


if __name__ == '__main__':
    # flow_param = dict(start_date="2022-11-01", future_months_to_scrape=24, card_suffix='5094')
    # scrape_isracard(**flow_param)

    flow_param = dict(start_date="2024-04-01", future_months_to_scrape=24, card_suffix='5094', fields_to_update=["category_raw"])
    backfill_isracard(**flow_param)

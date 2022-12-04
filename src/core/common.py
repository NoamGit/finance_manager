import os
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
import pandas as pd
from src.core.collection.model import MongoCredentials, MysqlCredentials

load_dotenv()


def get_db_secrets():
    mongocredentials_block = MongoCredentials.load("mongo-cred")
    mysqlcredentials_block = MysqlCredentials.load("mysql-cred")
    secrets = dict(mongo_host=mongocredentials_block.mongo_host
                   , mongo_port=int(mongocredentials_block.mongo_port)
                   , mongo_username=mongocredentials_block.mongo_username.get_secret_value()
                   , mongo_password=mongocredentials_block.mongo_password.get_secret_value()
                   , mysql_host=mongocredentials_block.mongo_host
                   , mysql_port=mysqlcredentials_block.mysql_port
                   , mysql_username=mysqlcredentials_block.mysql_username.get_secret_value()
                   , mysql_password=mysqlcredentials_block.mysql_password.get_secret_value()
                   , mysql_database=mysqlcredentials_block.mysql_database
                   )
    return secrets


def set_db_secrets_as_env_variables(credentials: Dict[str, Any]):
    for key, value in credentials.items():
        os.environ[key.upper()] = str(value)


def get_date_range(start_date: Optional[str] = None, month_future_to_predict: Optional[int] = 1) -> Tuple[str, str]:
    """
    Get date range for prediction.
    return: A tuple with start_date and end_date
    """
    if start_date is None:
        start_date = pd.to_datetime('today').strftime('%Y-%m-%d')
    end_date = pd.to_datetime(start_date) + pd.DateOffset(months=month_future_to_predict)
    end_date = end_date.strftime('%Y-%m-%d')
    return start_date, end_date

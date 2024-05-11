import os
import platform
import subprocess
from hashlib import md5
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
import pandas as pd
from src.core.collection.model import MongoCredentials, MysqlCredentials
from src.core.constants import LOCAL_UBUNTU_HOST

load_dotenv()

DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

async def async_get_db_secrets():
    mongocredentials_block = await MongoCredentials.load("mongo-cred")
    mysqlcredentials_block = await MysqlCredentials.load("mysql-cred")
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

def get_running_env(env: str = None)->str:
    machine_name = os.getenv('HOSTNAME', os.getenv('COMPUTERNAME', platform.node())).split('.')[0]
    if machine_name == "noam-ubuntu" or env == "PROD":
        return "PROD"
    else:
        return "DEV"

def get_db_secrets(env:str = None):
    mongocredentials_block = MongoCredentials.load("mongo-cred")
    mysqlcredentials_block = MysqlCredentials.load("mysql-cred")
    host = "local" if get_running_env(env) == "DEV" else LOCAL_UBUNTU_HOST
    secrets = dict(mongo_host= host
                   , mongo_port=int(mongocredentials_block.mongo_port)
                   , mongo_username=mongocredentials_block.mongo_username.get_secret_value()
                   , mongo_password=mongocredentials_block.mongo_password.get_secret_value()
                   , mysql_host=host
                   , mysql_port=mysqlcredentials_block.mysql_port
                   , mysql_username=mysqlcredentials_block.mysql_username.get_secret_value()
                   , mysql_password=mysqlcredentials_block.mysql_password.get_secret_value()
                   , mysql_database=mysqlcredentials_block.mysql_database
                   )
    return secrets


def set_db_secrets_as_env_variables(credentials: Dict[str, Any]):
    for key, value in credentials.items():
        os.environ[key.upper()] = str(value)


def get_date_range(start_date: Optional[str] = None
                   , month_future_to_predict: Optional[int] = 1
                   , dataset_id: Optional[str] = None) -> Optional[Tuple[str, str]]:
    """
    Get date range for prediction.
    return: A tuple with start_date and end_date
    """
    if dataset_id:
        return None
    if start_date is None:
        start_date = (pd.to_datetime('today') - pd.to_timedelta('14D')).strftime(DATE_FORMAT)
    end_date = pd.to_datetime(start_date) + pd.DateOffset(months=month_future_to_predict)
    end_date = end_date.strftime(DATE_FORMAT)
    return start_date, end_date

def get_git_commit_sha():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()

def hash_dataframe(df: Optional[pd.DataFrame])->Optional[str]:
    if df is None:
        return None
    data_json = df.to_json()
    return md5(data_json.encode()).hexdigest()
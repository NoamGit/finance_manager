from typing import Dict, Any, Optional, List, Tuple

from prefect import task
from pydantic import BaseModel
from sqlalchemy import create_engine


def get_mysql_client(mysql_param: Dict[str, str]):
    host = mysql_param.get('mysql_host')
    database = mysql_param.get('mysql_database')
    mysql_port = mysql_param.get('mysql_port')
    username = mysql_param.get('mysql_username')
    password = mysql_param.get('mysql_password')
    uri = f"mysql+pymysql://{username}:{password}@{host}:{mysql_port}/{database}"
    db = create_engine(uri)
    return db


def get_insert_query(table_name: str, data: Optional[List[Dict[str, Any]]]) -> str:
    d = next(iter(data))
    assert isinstance(d,BaseModel)
    num_objects = len(d.dict())
    fields_str = "(" + ", ".join([f"`{k}`" for k in d.dict().keys()]) + ")"
    values_str = "(" + ", ".join(["%s" for _ in range(num_objects)]) + ")"
    query = f"INSERT IGNORE INTO  `{table_name}` {fields_str}  VALUES{values_str}"
    return query


def translate_to_sql_credit_format(data) -> List[Tuple]:
    records = [tuple(d.dict().values()) for d in data]
    return records


@task()
async def async_load_to_mysql(data: Optional[List[Dict[str, Any]]], mysql_param: Dict[str, str], table_name: str):
    _load_to_mysql(data, mysql_param, table_name)


@task()
def load_to_mysql(data: Optional[List[Dict[str, Any]]], mysql_param: Dict[str, str], table_name: str):
    _load_to_mysql(data, mysql_param, table_name)


def _load_to_mysql(data: Optional[List[Dict[str, Any]]], mysql_param: Dict[str, str], table_name: str):
    db = get_mysql_client(mysql_param)
    query = get_insert_query(table_name, data)
    records = translate_to_sql_credit_format(data)
    result = db.execute(query, records)
    result.close()

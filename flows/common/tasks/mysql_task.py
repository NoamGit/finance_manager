from typing import Dict, Any, Optional, List

from prefect import task

from src.interface.common.utils import _load_to_mysql


@task()
async def async_load_to_mysql(data: Optional[List[Dict[str, Any]]], mysql_param: Dict[str, str], table_name: str):
    _load_to_mysql(data, mysql_param, table_name)


@task()
def load_to_mysql(data: Optional[List[Dict[str, Any]]], mysql_param: Dict[str, str], table_name: str,
                  fields_to_update: Optional[List[str]] = None, identifier: Optional[str] = None):
    _load_to_mysql(data, mysql_param, table_name, fields_to_update, identifier)

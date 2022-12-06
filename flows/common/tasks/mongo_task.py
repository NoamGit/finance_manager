from typing import Dict, Any, Optional, List

from prefect.tasks import task

from src.interface.common.utils import load_to_mongo, \
    load_transactions_to_mongo


@task()
def load_balance_to_mongo(mongo_docs: Optional[List[Dict[str, Any]]], mongo_param: Dict[str, str], table_name: str):
    pass


@task()
def load_transactions_to_mongo_task(mongo_docs: Optional[List[Dict[str, Any]]], mongo_param: Dict[str, str],
                                    table_name: str):
    load_transactions_to_mongo(mongo_docs, mongo_param, table_name)


@task()
async def async_load_transactions_to_mongo_task(mongo_docs: Optional[List[Dict[str, Any]]], mongo_param: Dict[str, str],
                                                table_name: str):
    load_transactions_to_mongo(mongo_docs, mongo_param, table_name)


@task()
def load_to_mongo_task(mongo_doc: Optional[Dict[str, Any]], mongo_param: Dict[str, str], table_name: str):
    load_to_mongo(mongo_doc, mongo_param, table_name)

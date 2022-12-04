from hashlib import sha256
from typing import Dict, Any, Optional, List, Iterable

import pymongo
from pymongo.errors import BulkWriteError
from prefect.tasks import task
from pymongo.write_concern import WriteConcern

from src.interface.common.utils import validate_documents, unpack_to_unnested_format


def get_mongo_client(mongo_param: Dict[str, str]):
    host = mongo_param.get('mongo_host')
    port = mongo_param.get('mongo_port')
    username = mongo_param.get('mongo_username')
    password = mongo_param.get('mongo_password')
    uri = f"mongodb://{username}:{password}@{host}:{port}"
    db = pymongo.MongoClient(uri)
    return db['prod-db']


@task()
def load_balance_to_mongo(mongo_docs: Optional[List[Dict[str, Any]]], mongo_param: Dict[str, str], table_name: str):
    pass


@task()
def create_mongo_key(ingredients: Iterable[str]) -> str:
    return sha256(",".join(ingredients).encode('utf-8')).hexdigest()


def load_transactions_to_mongo(mongo_docs: Optional[List[Dict[str, Any]]], mongo_param: Dict[str, str],
                               table_name: str):
    db = get_mongo_client(mongo_param)
    validate_documents(mongo_docs)
    transformed_mongo_docs = unpack_to_unnested_format(mongo_docs)
    transactions = db.get_collection(table_name)
    transactions.create_index('identifier', unique=True, background=True)
    transactions.with_options(write_concern=WriteConcern(w=0))
    try:
        transactions.insert_many(
            transformed_mongo_docs
            , ordered=False
        )
    except BulkWriteError as e:
        panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
        if len(panic_list) > 0:
            raise e


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


def load_to_mongo(mongo_doc: Optional[Dict[str, Any]], mongo_param: Dict[str, str], table_name: str):
    assert 'mongo_key' in mongo_doc
    db = get_mongo_client(mongo_param)
    collection = db.get_collection(table_name)
    collection.create_index('mongo_key', unique=True, background=True)
    collection.with_options(write_concern=WriteConcern(w=0))
    collection.update_one(
        filter=dict(mongo_key=mongo_doc.get('mongo_key')),
        update={"$set": mongo_doc}
        , upsert=True
    )


def read_from_mongo(query: Dict[str, str], mongo_param: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    table_name = mongo_param.get('table_name')
    assert 'mongo_key' in query
    db = get_mongo_client(mongo_param)
    collection = db.get_collection(table_name)
    return collection.find_one(query)

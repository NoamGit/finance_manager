import json
import logging
from datetime import datetime, timedelta
from hashlib import sha256
from typing import List, Dict, Any, Optional, Tuple, Iterable

import prefect
import pymongo
from prefect import get_run_logger, task
from pydantic import BaseModel
from pymongo import WriteConcern
from pymongo.errors import BulkWriteError
from sqlalchemy import create_engine

from src.core.common import TIMESTAMP_FORMAT, DATE_FORMAT, get_running_env
from src.core.constants import LOCAL_UBUNTU_HOST
from src.interface.common.model import MySqlTransaction, MySqlBalance


def get_logger() -> Optional[logging.Logger]:
    task_run_context = prefect.context.TaskRunContext.get()
    return get_run_logger() if task_run_context else None


def validate_documents(doc: List[Dict[str, Any]]):
    if not doc:
        raise ValueError("documents must be provided")
    return


def get_today():
    return datetime.strftime(datetime.now(), DATE_FORMAT)

def unpack_to_unnested_format(data: List[Dict[str, Any]], account_number: str = 'NA') -> List[Dict[str, Any]]:
    if not data:
        raise ValueError("no value to transform")

    res = []
    data_structure_type = infer_data_structure_type(data)
    if not data_structure_type:
        raise ValueError("data structure type not recognized")
    if data_structure_type == 'list_of_transactions':
        append_transaction_list_collection(data, res, account_number)
    elif data_structure_type == 'list_of_dicts_of_transactions':
        for d in data:
            [append_transactions_collection(v, res) for k, v in d]
    elif isinstance(data, dict) and data_structure_type == 'ibs_data_structure':
        accounts = next(iter(data['accounts']))
        trans_data, account_number = accounts['txns'], accounts['accountNumber']
        append_transaction_list_collection(trans_data, res, account_number)
    return res


def infer_data_structure_type(data: List[Dict[str, Any]]) -> Optional[str]:
    if isinstance(data, dict) and 'accounts' in data:
        return 'ibs_data_structure'
    d = next(iter(data))
    if not isinstance(d, dict):
        return
    if 'identifier' in d:
        return 'list_of_transactions'
    d_ = next(iter(d.values()))
    if isinstance(d_, dict) and 'txns' in d_:
        return 'list_of_dicts_of_transactions'
    return


def add_transaction_date_and_account_to_balance_data(balance: Dict[str, Any], transactions: List[Dict[str, Any]],
                                                     account_number: str) -> Dict[str, Any]:
    last_transaction_date = max(
        [datetime.strptime(txn.get('date'), TIMESTAMP_FORMAT) for txn in transactions]).date() + timedelta(
        days=1)
    balance.update({'last_transaction_date': datetime.strftime(last_transaction_date, DATE_FORMAT)})
    balance.update({'accountNumber': account_number})
    return balance


def append_transactions_collection(account_transactions: Dict[str, Any], updating_result: List[Dict[str, Any]]):
    account_number = account_transactions.get('accountNumber')
    index = account_transactions.get('index')
    for t in account_transactions.get('txns', []):
        transformed_txns = json.loads(t.json())
        transformed_txns.update({"accountNumber": str(account_number),
                                 "index": index})
        updating_result.append(transformed_txns)


def append_transaction_list_collection(account_transactions: List[Dict[str, Any]],
                                       updating_result: List[Dict[str, Any]], account_number: str):
    for trans in account_transactions:
        trans.update({"accountNumber": str(account_number)})
        updating_result.append(trans)


# region MySQL utils
def translate_balance_to_mysql_format(balance: Dict[str, Any]) -> MySqlBalance:
    return MySqlBalance(
        balance=balance.get('balance'),
        credit_limit=balance.get('creditLimit'),
        date=balance.get('last_transaction_date'),
        credit_utilization=balance.get('creditUtilization'),
        balance_currency=balance.get('balanceCurrency'),
        account_number=balance.get('accountNumber')
    )


def translate_to_mysql_format(data: List[Dict[str, Any]], create_id: bool = False) -> List[Dict[str, Any]]:
    res = []
    for d in data:
        try:
            id_dict = {k: d[k] for k in ['identifier', 'description', 'date', 'processedDate', 'chargedAmount']}
            unique_id = sha256(json.dumps(id_dict, sort_keys=True).encode('utf-8')).hexdigest() if create_id else d.get(
                'identifier')
            transaction = MySqlTransaction(
                id=unique_id,
                description=d.get('description'),
                notes=d.get('memo'),
                processed_date=d.get('processedDate'),
                date=d.get('date'),
                original_amount=d.get('originalAmount'),
                charged_amount=d.get('chargedAmount'),
                category_raw=d.get('category'),
                account_number=d.get('accountNumber'),
                type=d.get('type')
            )
            res.append(transaction)
        except Exception as e:
            raise e
    return res


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
    assert isinstance(d, BaseModel)
    num_objects = len(d.dict())
    fields_str = "(" + ", ".join([f"`{k}`" for k in d.dict().keys()]) + ")"
    values_str = "(" + ", ".join(["%s" for _ in range(num_objects)]) + ")"
    query = f"INSERT IGNORE INTO  `{table_name}` {fields_str}  VALUES{values_str}"
    return query


def get_update_query(table_name: str, data: Optional[List[Dict[str, Any]]], fields_to_update: List[str],
                     id_field: str) -> str:
    d = next(iter(data))
    assert isinstance(d, BaseModel)
    fields_str = ", ".join([f"`{k}` = %s" for k in fields_to_update])
    id_str = f"{id_field} = %s"
    query = f"UPDATE `{table_name}` SET {fields_str}  WHERE {id_str}"
    return query


def translate_to_sql_credit_format(data, fields_to_update: Optional[List[str]] = None) -> List[Tuple]:
    records = []
    for d in data:
        d_ = d.dict()
        r_ = tuple([d_[f] for f in fields_to_update]) if fields_to_update else tuple(d_.values())
        records.append(r_)
    return records


def _load_to_mysql(data: Optional[List[Dict[str, Any]]], mysql_param: Dict[str, str], table_name: str,
                   fields_to_update: Optional[List[str]] = None, identifier: Optional[str] = None):
    db = get_mysql_client(mysql_param)
    if fields_to_update and identifier:
        query = get_update_query(table_name, data, fields_to_update, identifier)
        fields_to_update += [identifier]
    else:
        query = get_insert_query(table_name, data)
    records = translate_to_sql_credit_format(data, fields_to_update)
    result = db.execute(query, records)
    result.close()


# endregion

# region Mongo utils
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


def get_mongo_client(mongo_param: Dict[str, str]):
    env = "PROD"
    host = "local" if get_running_env(env) == "DEV" else LOCAL_UBUNTU_HOST
    host = host
    port = mongo_param.get('mongo_port')
    username = mongo_param.get('mongo_username')
    password = mongo_param.get('mongo_password')
    uri = f"mongodb://{username}:{password}@{host}:{port}"
    db = pymongo.MongoClient(uri)
    return db['prod-db']


def create_mongo_key(ingredients: Iterable[str]) -> str:
    return sha256(",".join(ingredients).encode('utf-8')).hexdigest()

# endregion

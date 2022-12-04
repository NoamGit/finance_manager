import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from src.interface.common.model import MySqlTransaction, MySqlBalance


def validate_documents(doc: List[Dict[str, Any]]):
    if not doc:
        raise ValueError("documents must be provided")
    return


def translate_balance_to_mysql_format(balance: Dict[str, Any]) -> MySqlBalance:
    return MySqlBalance(
        balance=balance.get('balance'),
        credit_limit=balance.get('creditLimit'),
        date=balance.get('last_transaction_date'),
        credit_utilization=balance.get('creditUtilization'),
        balance_currency=balance.get('balanceCurrency'),
        account_number=balance.get('accountNumber')
    )


def translate_to_mysql_format(data: List[Dict[str, Any]]):
    res = []
    for d in data:
        try:
            transaction = MySqlTransaction(
                id=d.get('identifier'),
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
        [datetime.strptime(txn.get('date'), '%Y-%m-%dT%H:%M:%S.%fZ') for txn in transactions]).date() + timedelta(
        days=1)
    balance.update({'last_transaction_date': datetime.strftime(last_transaction_date, '%Y-%m-%d')})
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

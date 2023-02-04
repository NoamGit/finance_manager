import json
from typing import Optional, Dict, Any, Tuple, List

from prefect import task, flow
from subprocess import run, PIPE

from flows.common.tasks.mysql_task import load_to_mysql
from src.core.common import DATE_FORMAT
from src.core.common import get_db_secrets
from src.interface import IBS_OTSAR_PATH, INTERFACE_WORKING_DIR, MONGO_BANK_ACCOUNT_TABLE_NAME
from src.interface.common.model import MySqlTransaction, MySqlBalance
from datetime import datetime, timedelta

from src.interface.common.utils import translate_to_mysql_format, unpack_to_unnested_format, \
    translate_balance_to_mysql_format, add_transaction_date_and_account_to_balance_data, validate_documents, \
    create_mongo_key
from flows.common.tasks.mongo_task import load_to_mongo_task
from src.core.collection.model import BankCredentials


@task()
def get_credentials() -> Dict[str, str]:
    cred = get_db_secrets()
    cred.update({"mongo_table_name": "transactions",
                 "mysql_table_name": "bank_transactions"})
    return cred


@task()
def validate_inputs(start_date: str):
    start_date = datetime.strptime(start_date, DATE_FORMAT)
    assert start_date.year > 2000


@task(retries=2, retry_delay_seconds=20)
def fetch(start_date: str):
    bankcredentials_block = BankCredentials.load("otsar-cred")

    cmd = ["node"
        , IBS_OTSAR_PATH
        , "--date"
        , f"'{start_date}'"
        , '--username'
        , f"{bankcredentials_block.user_name.get_secret_value()}"
        , '--password'
        , f"{bankcredentials_block.password.get_secret_value()}"
           ]
    res = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, cwd=INTERFACE_WORKING_DIR)
    if res.returncode == 0 and not res.stderr:
        return json.loads(res.stdout)
    raise ChildProcessError(f'fetching process failed: {res.stderr}')


@task()
def translate_bank_transaction_to_mysql_data_model(data: Dict[str, Any]) -> Tuple[
    List[MySqlTransaction], List[MySqlBalance]]:
    account_data = next(iter(data.get('accounts')))
    transactions = account_data.get('txns')
    validate_documents(transactions)
    balance = account_data.get('summary')
    account_number = account_data.get('accountNumber')
    balance = add_transaction_date_and_account_to_balance_data(balance, transactions, account_number)
    transformed_balance_data = translate_balance_to_mysql_format(balance)
    transformed_transaction_data = unpack_to_unnested_format(transactions, account_number)
    transformed_transaction_data = translate_to_mysql_format(transformed_transaction_data, create_id=True)
    return transformed_transaction_data, [transformed_balance_data]


@flow
def scrape_otsar_hahayal(start_date: Optional[str] = None):
    secrets = get_credentials()
    start_date = start_date or (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    validate_inputs(start_date)

    raw_trans = fetch(start_date, wait_for=[validate_inputs])
    account_number = raw_trans.get('accounts', [{}])[0].get('accountNumber')
    raw_trans['mongo_key'] = create_mongo_key((start_date, account_number))

    load_to_mongo_task(raw_trans, mongo_param=secrets, table_name=MONGO_BANK_ACCOUNT_TABLE_NAME, wait_for=[create_mongo_key])

    processed_transaction_data, processed_balance_data = translate_bank_transaction_to_mysql_data_model(raw_trans)
    load_to_mysql(processed_transaction_data, secrets, 'credit_transaction')
    load_to_mysql(processed_balance_data, secrets, 'bank_balance')


if __name__ == '__main__':
    flow_param = dict(start_date=None)
    scrape_otsar_hahayal(**flow_param)

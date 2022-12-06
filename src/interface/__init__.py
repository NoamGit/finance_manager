from pathlib import Path

IBS_OTSAR_PATH = "collection/isracard/otsar_hahayal/fetch_otsar_hahayal.js"
IBS_ISRACARD_PATH = "collection/isracard/fetch_isracard.js"
INTERFACE_WORKING_DIR = str(Path(__file__).parent.absolute())

MONGO_BANK_ACCOUNT_TABLE_NAME = 'bank_account_transactions'
MONGO_CREDIT_TABLE_NAME = 'credit_transactions'
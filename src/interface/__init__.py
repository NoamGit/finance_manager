from pathlib import Path

IBS_OTSAR_PATH = "otsar_hahayal/fetch_otsar_hahayal.js"
IBS_ISRACARD_PATH = "isracard/fetch_isracard.js"
EXEC_WORKING_DIR = str(Path(__file__).parent.absolute())

MONGO_BANK_ACCOUNT_TABLE_NAME = 'bank_account_transactions'
MONGO_CREDIT_TABLE_NAME = 'credit_transactions'
from typing import NewType, Dict

from prefect.blocks.core import Block
from pydantic import BaseModel, SecretStr


class CreditCardUserCredentials(BaseModel):
    user_name: str
    card_number: str
    password: str


ScraperCredentials = NewType('Credentials', Dict[str, str])


class IsracardCredentials(Block):
    user_name: SecretStr
    cardnum: SecretStr
    password: SecretStr


class BankCredentials(Block):
    user_name: SecretStr
    password: SecretStr


class MongoCredentials(Block):
    # mongo secrets
    mongo_host: str
    mongo_port: str
    mongo_table_name: str
    mongo_username: SecretStr
    mongo_password: SecretStr


class MysqlCredentials(Block):
    # mysql secrets
    mysql_port: str
    mysql_database: str
    mysql_username: SecretStr
    mysql_root_password: SecretStr
    mysql_password: SecretStr

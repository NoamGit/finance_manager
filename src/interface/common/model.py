from typing import Optional

from pydantic import BaseModel


class MySqlTransaction(BaseModel):
    description: str
    notes: Optional[str]
    date: str
    processed_date: str
    charged_amount: float
    original_amount: float
    category: Optional[int]
    category_raw: Optional[str]
    account_number: str
    id: str
    type: Optional[str]


class MySqlBalance(BaseModel):
    balance: float
    credit_limit: float
    date: str
    credit_utilization: float
    balance_currency: str
    account_number: str


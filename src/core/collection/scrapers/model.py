from enum import Enum
from typing import Optional, Union, List, Callable, Any

from pydantic import BaseModel


# region ScraperModels

class ScaperProgressTypes(Enum):
    initializing = 'INITIALIZING'
    start_scraping = 'START_SCRAPING'
    logging_in = 'LOGGING_IN'
    login_success = 'LOGIN_SUCCESS'
    login_failed = 'LOGIN_FAILED'
    change_password = 'CHANGE_PASSWORD'
    end_scraping = 'END_SCRAPING'
    terminating = 'TERMINATING'


class ScaperScrapingResult(Enum):
    pass


class ScraperErrorTypes(Enum):
    invalid_password = 'INVALID_PASSWORD',
    change_password = 'CHANGE_PASSWORD',
    timeout = 'TIMEOUT',
    account_blocked = 'ACCOUNT_BLOCKED',
    generic = 'GENERIC',
    general = 'GENERAL_ERROR'


class ScraperLoginResult(BaseModel):
    success: bool
    error_type: Optional[ScraperErrorTypes]
    error_message: Optional[str]  # only on success=false


class HttpStatusTypes(Enum):
    OK = 200


class LoginOptions(BaseModel):
    login_url: str
    fields: Any
    submit_button_selector: Union[str, Callable]
    post_action: Callable
    check_readiness: Optional[bool] = False
    pre_action: Optional[Any] = False
    user_agent: Optional[Any] = False

    # {
    #     loginUrl: `${
    #                     BASE_URL} / MatafLoginService / MatafLoginServlet?bankId = OTSARPRTAL & site = Private & KODSAFA = HE
    # `,
    # fields: createLoginFields(credentials),
    # submitButtonSelector: '#continueBtn',
    # postAction: async () = > waitForPostLogin(this.page),
    #                          possibleResults: getPossibleLoginResults(this.page),
    # }


# endregion

# region Transactions

class TransactionTypes(Enum):
    NORMAL = 'normal'
    INSTALLMENTS = 'installments'


class TransactionStatuses(Enum):
    COMPLETED = 'completed'
    PENDING = 'pending'


class TransactionInstallments(BaseModel):
    number: float
    total: int


class Transaction(BaseModel):
    type: TransactionTypes  # sometimes called Asmachta
    identifier: Optional[Union[str, int]]
    date: str  # ISO date string
    processedDate: str  # ISO date string
    originalAmount: float
    originalCurrency: str
    chargedAmount: float
    chargedCurrency: Optional[str]
    description: str
    memo: Optional[str]
    status: TransactionStatuses
    installments: Optional[TransactionInstallments]
    category: Optional[str]


class TransactionsAccount(BaseModel):
    accountNumber: str
    balance: Optional[float]
    txns: List[Transaction]

# endregion

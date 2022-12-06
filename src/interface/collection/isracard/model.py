from typing import Dict, Optional

from dotenv import load_dotenv
from pydantic import BaseModel

from src.core.collection.model import IsracardCredentials

load_dotenv()


class IsracardScrapedTransaction(BaseModel):
    dealSumType: str
    voucherNumberRatzOutbound: str
    voucherNumberRatz: str
    moreInfo: str
    dealSumOutbound: bool
    currencyId: str
    dealSum: float
    fullPurchaseDate: str
    fullPurchaseDateOutbound: str
    fullSupplierNameHeb: str
    fullSupplierNameOutbound: str
    paymentSum: float
    paymentSumOutbound: float


class IsracardCardCredentialsFactory():
    user_map: Dict[str, IsracardCredentials] = {
        '1029': IsracardCredentials.load("noam-isracard-cred"),
        '5094': IsracardCredentials.load("eden-isracard-cred")    }

    def get_credentials(self, card_suffix: str) -> Optional[IsracardCredentials]:
        return self.user_map.get(card_suffix)

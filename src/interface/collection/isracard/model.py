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
    user_map: Dict[str, str] = {
        '1029': "noam-isracard-cred",
        '5094': "eden-isracard-cred"}

    def get_credentials(self, card_suffix: str):
        block_identifier = self.user_map.get(card_suffix)
        return IsracardCredentials.load(block_identifier)


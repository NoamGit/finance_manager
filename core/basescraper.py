from typing import Dict, Optional


class BaseScraper():
    card: str
    user: str
    password: str
    account_num: str

    def set_credentials(self, card: Optional[str] = None,
                        user: Optional[str] = None,
                        password: Optional[str] = None,
                        account_num: Optional[str] = None):
        self.card = card
        self.user = user
        self.password = password
        self.account_num = account_num

import os

import pytest
import os
from interface.scrapers.isracard_scarper import IsracardScraper


@pytest.fixture()
def scraper():
    scraper = IsracardScraper()
    scraper.set_credentials(user=os.getenv("ISRACARD_USERNAME_NOAM")
                            ,card=os.getenv("ISRACARD_CARDNUM_NOAM")
                            ,password=os.getenv("ISRACARD_PASSWORD_NOAM"))
    return scraper


def test_login(scraper):
    pass
import asyncio
import pytest
import os

from src.core.collection.scrapers.basescraper import BaseScraper
from src.core.collection.scrapers.model import ScraperLoginResult
from src.interface.isracard.scraper import IsracardScraper
from src.core import ScraperCredentials
from dotenv import load_dotenv

load_dotenv()
MONTHS_TO_SCRAPE = 4


@pytest.fixture()
def scraper():
    scraper = IsracardScraper(start_date="01/02/2022"
                              , future_months_to_scrape=MONTHS_TO_SCRAPE)
    return scraper


@pytest.fixture()
def credentials():
    credentials = dict(user=os.getenv("ISRACARD_USERNAME_NOAM")
                       , card=os.getenv("ISRACARD_CARDNUM_NOAM")
                       , password=os.getenv("ISRACARD_PASSWORD_NOAM"))
    for k, v in credentials.items():
        if not v:
            raise EnvironmentError('make sure you set the credentials in your environment')
    return ScraperCredentials(credentials)


async def close_browser(scraper: BaseScraper):
    if hasattr(scraper, 'browser'):
        await scraper.browser.close()


def test_login(scraper, credentials):
    res = asyncio.run(scraper.login(credentials))
    asyncio.run(close_browser(scraper))
    assert res == ScraperLoginResult(success=True)


def test_fetch_data(scraper, credentials):
    async def flow():
        await scraper.login(credentials)
        res = await scraper.fetch_data()
        return res

    res = asyncio.run(flow())
    asyncio.run(close_browser(scraper))
    assert len(res) == MONTHS_TO_SCRAPE
    assert next(iter(res[0].keys())) == '1029'


def test_extract_last_month_data(scraper):
    pytest.fail()

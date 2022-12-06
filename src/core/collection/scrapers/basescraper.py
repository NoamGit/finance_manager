import logging
from datetime import datetime
from typing import Dict, Optional, Any, List

from abc import abstractmethod

from requests import Response

from src.core.collection.scrapers.model import Transaction, TransactionTypes
from pyppeteer.page import Page

logger = logging.getLogger(__name__)
# logger = get_run_logger()

VIEWPORT_WIDTH = 1024
VIEWPORT_HEIGHT = 768
DAY_LEADING_DATE_FORMAT = '%d/%m/%Y'
YEAR_LEADING_DATE_FORMAT = '%Y-%m-%d'


class BaseScraper():
    def __init__(self, **options):
        self.options = options

    @abstractmethod
    async def fetch_data(self, *args, **kwargs):
        raise NotImplementedError

    async def fetch_all_transactions(self, *args, **kwargs):
        raise NotImplementedError

    def _filter_old_transactions(self, transactions: List[Transaction], start_date: datetime.date,
                                 combine_installments: bool):

        def _is_old_transaction(transaction: Transaction) -> bool:
            combine_needed_and_initial_or_normal = combine_installments \
                                                   and (self._is_normal_transaction(transaction)
                                                        or self._is_initial_installment_transaction(transaction))
            is_old_transaction = ((not combine_installments)
                                  and start_date <= datetime.fromisoformat(transaction.date).date()) \
                                 or (combine_needed_and_initial_or_normal
                                     and start_date <= datetime.fromisoformat(transaction.date).date())
            return is_old_transaction

        return [t for t in transactions if _is_old_transaction(t)]

    def _is_initial_installment_transaction(self, transaction: Transaction) -> bool:
        return self._is_installment_transaction(
            transaction) and transaction.installments and transaction.installments.number == 1

    @staticmethod
    def _is_normal_transaction(transaction: Transaction) -> bool:
        return (transaction and (transaction.type == TransactionTypes.NORMAL))

    @staticmethod
    def _is_installment_transaction(transaction: Transaction) -> bool:
        return transaction and transaction.type == TransactionTypes.INSTALLMENTS

    @staticmethod
    def _validate_input_start_date(input_start_date: Optional[str]) -> bool:
        # validation format DD/MM/YYYY
        if not isinstance(input_start_date, str):
            return False
        try:
            datetime.strptime(input_start_date, DAY_LEADING_DATE_FORMAT)
            return True
        except Exception as e:
            logger.exception(e)
            return False


def fetch_get_within_page(page: Page, url: str):
    page_request_js = """(url) => {
        var myHeaders = {'Access-Control-Allow-Credentials': 'true'};
        var requestOptions = {
          method: 'GET',
          redirect: 'follow'
        };
        
        return new Promise((resolve) => {fetch(url, requestOptions)
        .then(
            result => {
                if (result.status === 204) {resolve(null);} 
                else {resolve(result.json());}
                }
            )
        .catch(error => reject(error))});
    }
    """
    response = page.evaluate(page_request_js, url)
    return response


def fetch_post_within_page(page: Page, url: str, data: Dict[str, Any], extra_headers: Dict[str, Any] = None) -> \
        Optional[Response]:
    headers = {}
    if extra_headers:
        headers.update(extra_headers)

    page_request_js = """(url, data, extraHeaders) => {
        var raw = JSON.stringify(data)
    
        var requestOptions = {
          method: 'POST',
          headers: Object.assign({ 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' }, extraHeaders),
          body: raw,
          redirect: 'follow'
        };
    
        return new Promise((resolve) => {fetch(url, requestOptions)
            .then(
                result => {
                    if (result.status === 204) {resolve(null);} 
                    else {resolve(result.json());}
                    }
                )
            .catch(error => reject(error))});
    }
    """
    response = page.evaluate(page_request_js, url, data, headers)
    return response


def handle_response(response: Response) -> Optional[Dict[str, Any]]:
    try:
        # no content response
        if response.status_code == 204:
            return
        else:
            return response.json()
    except Exception as e:
        raise e


def wait_until_element_found(page: Page, submit_button_pattern: str):
    raise NotImplementedError


async def click_button(page: Page, button_selector: str):
    button_element = await page.querySelector(button_selector)
    page_request_js = """(el) => (el as HTMLElement).click()"""
    await page.evaluate(page_request_js, button_element)


def fill_input(page: Page, selector_name: str, value: str):
    """
      await pageOrFrame.$eval(inputSelector, (input: Element) => {
        const inputElement = input;
        // @ts-ignore
        inputElement.value = '';
      });
      await pageOrFrame.type(inputSelector, inputValue);
    }
    """

    selector = page.querySelector(selector_name)
    page_request_js = """(input: Element) => {
        const inputElement = input;
        inputElement.value = '';
    }"""
    page.evaluate(page_request_js, selector)
    page.type(selector_name, value)

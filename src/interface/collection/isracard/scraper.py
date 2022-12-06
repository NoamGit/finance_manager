import logging
from datetime import datetime
from typing import Dict, Optional, Any, Tuple, List
import re
import urllib.parse
from deprecated import deprecated

from pyppeteer.page import Page

from src.core import CreditCardUserCredentials, ScraperCredentials
from src.core.collection.scrapers.basescraper import fetch_post_within_page, fetch_get_within_page, DATE_FORMAT
from src.core.collection.scrapers.basescraperwithbrowser import BaseScraperWithBrowser
from src.core import SHEKEL_CURRENCY_KEYWORD, ALT_SHEKEL_CURRENCY, SHEKEL_CURRENCY
from src.core import deep_get
from src.core.collection.scrapers.model import ScaperProgressTypes, ScraperLoginResult, ScraperErrorTypes, Transaction, \
    TransactionStatuses, \
    TransactionTypes
from src.interface.collection.isracard.constants import COUNTRY_CODE, ID_TYPE, INSTALLMENTS_KEYWORD

logger = logging.getLogger(__name__)

@deprecated(reason="use fetch_isracard.js instead")
class IsracardScraper(BaseScraperWithBrowser):
    base_url: str
    company_code: str
    services_url: str

    def __init__(self, **options):
        super().__init__(**options)
        self.base_url = 'https://digital.isracard.co.il'
        self.company_code = '11'
        self.services_url = f"{self.base_url}/services/ProxyRequestHandler.ashx"

    @staticmethod
    def _get_change_password_result():
        logger.info(ScaperProgressTypes.change_password)
        return ScraperLoginResult(success=False, error_type=ScraperErrorTypes.change_password)

    @staticmethod
    def _get_login_success():
        logger.info(ScaperProgressTypes.login_success)
        return ScraperLoginResult(success=True)

    @staticmethod
    def _get_installment_info(transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if (not transaction.get('moreInfo')) or (INSTALLMENTS_KEYWORD in transaction.get('moreInfo')):
            return None

        matches = re.match("/\d+/g", transaction.get('moreInfo'))
        if not matches or len(matches) < 2:
            return None

        return dict(number=int(matches[0]), total=int(matches[1]))

    @staticmethod
    def _convert_currency(currency: str):
        if (currency == SHEKEL_CURRENCY_KEYWORD or currency == ALT_SHEKEL_CURRENCY):
            return SHEKEL_CURRENCY
        return currency

    def validate_id_data(self, data: Optional[Dict[str, Any]]) -> bool:
        if (not data) \
                or not (data_header := data.get("Header")) \
                or data_header.get('Status') != '1' \
                or (not data.get('ValidateIdDataBean')):
            raise Exception("couldn't validate id")
        return True

    async def validate_user_exists(self, credentials: CreditCardUserCredentials) -> Dict[str, Any]:
        validate_url = f"{self.services_url}?reqName=ValidateIdData"
        request_data = {
            'id': credentials.user_name,
            'cardSuffix': credentials.card_number,
            'countryCode': COUNTRY_CODE,
            'idType': ID_TYPE,
            'checkLevel': '1',
            'companyCode': self.company_code,
        }

        # assuming there is no need for async request here. We could use aiohttp instead https://www.twilio.com/blog/asynchronous-http-requests-in-python-with-aiohttp
        id_data_reponse = await fetch_post_within_page(self.page, validate_url, request_data)
        self.validate_id_data(id_data_reponse)
        return id_data_reponse

    async def login_with_username(self, validated_response: Dict[str, Any], credentials: CreditCardUserCredentials) -> \
            Tuple[
                Optional[str], Optional[str]]:
        logging_status_code = None
        validated_id_data_bean = validated_response.get('ValidateIdDataBean')
        validation_return_code = validated_id_data_bean.get(
            'returnCode') if validated_id_data_bean else validated_id_data_bean
        if validation_return_code == '1':
            user_name = validated_id_data_bean.get('userName', '')
            logger.debug(f"user validated with return code {validation_return_code}")

            login_url = f"{self.services_url}?reqName=performLogonI"
            request_data = {
                'KodMishtamesh': user_name,
                'MisparZihuy': credentials.user_name,
                'Sisma': credentials.password,
                'cardSuffix': credentials.card_number,
                'countryCode': COUNTRY_CODE,
                'idType': ID_TYPE,
            }

            logging_result = await fetch_post_within_page(self.page, login_url, request_data)
            logging_status_code = logging_result['status'] if logging_result else None
            logger.debug(f"user login with status code {logging_status_code}")
        return logging_status_code, validation_return_code

    async def login(self, credentials: ScraperCredentials) -> ScraperLoginResult:
        # await self.page.setRequestInterception(True)
        #   this.page.on('request', (request) => {
        #     if (request.url().includes('detector-dom.min.js')) {
        #       debug('force abort for request do download detector-dom.min.js resource');
        #       request.abort();
        #     } else {
        #       request.continue();
        #     }
        #   });
        #
        logger.info('navigate to login page')
        await self.setup_session()
        await self.navigate_to(f'{self.base_url}/personalarea/Login')
        logger.info(ScaperProgressTypes.logging_in)

        id_data_response = await self.validate_user_exists(credentials)
        logging_status_code, validation_return_code = await self.login_with_username(
            validated_response=id_data_response, credentials=credentials)
        login_status = self.get_login_status(logging_status_code, validation_return_code)
        if not login_status.success:
            raise ConnectionError(login_status.error_message)
        return login_status

    async def fetch_data(self):
        result = await super().fetch_data(servicesUrl=self.services_url, companyCode=self.company_code)
        return result

    async def fetch_and_process_transaction(self, page: Page, start_date: datetime, end_month: datetime.date) -> Dict[
        str, Any]:
        accounts = await self.fetch_accounts(end_month)
        raw_transactions = await self.fetch_transaction(end_month)
        if not self.valid_transactions(raw_transactions):
            return {}
        transactions = self.process_transaction(raw_transactions, start_date, accounts)
        return transactions

    def get_login_status(self, logging_status_code: Optional[str],
                         validation_return_code: Optional[str]) -> ScraperLoginResult:
        if validation_return_code == '4':
            return self._get_change_password_result()
        elif validation_return_code == '1' and logging_status_code == '1':
            return self._get_login_success()
        elif validation_return_code == '1' and logging_status_code == '3':
            return self._get_change_password_result()

        logger.info(ScaperProgressTypes.login_failed)
        return ScraperLoginResult(success=False, error_type=ScraperErrorTypes.invalid_password)

    async def fetch_accounts(self, end_month: datetime.date) -> List[Dict[str, Any]]:
        data_url = self._get_accounts_url(end_month)
        result = await fetch_get_within_page(self.page, data_url)
        res_header = result.get("Header", {})
        if result and res_header.get("Status") == '1' and result.get("DashboardMonthBean"):
            cards_charges = result.get("DashboardMonthBean", {}).get('cardsCharges')
            if cards_charges:
                return [{
                    "index": int(card_charge.get("cardIndex")),
                    "account_number": card_charge.get("cardNumber"),
                    "processed_date": self._parse_billing_date(card_charge.get("billingDate"))
                } for card_charge in cards_charges]

        return []

    def _parse_billing_date(self, billing_date: Optional[str]) -> str:
        return str(datetime.strptime(billing_date, DATE_FORMAT).date())

    def _get_accounts_url(self, end_month: datetime.date) -> str:
        billing_date = datetime.strftime(end_month, DATE_FORMAT)
        params = {
            "reqName": 'DashboardMonth',
            "actionCode": '0',
            "billingDate": billing_date,
            "format": 'Json'
        }
        return self.services_url + "?" + urllib.parse.urlencode(params)

    def _get_transaction_url(self, end_month: datetime.date) -> str:
        month_str = f"0{end_month.month + 1}" if end_month.month + 1 < 10 else str(end_month.month)
        year = end_month.year
        params = {
            "reqName": 'CardsTransactionsList',
            "month": month_str,
            "year": str(year),
            "requiredDate": 'N'
        }
        return self.services_url + "?" + urllib.parse.urlencode(params)

    async def fetch_transaction(self, end_month: datetime.date):
        data_url = self._get_transaction_url(end_month)
        return await fetch_get_within_page(self.page, data_url)

    def process_transaction(self, raw_transactions: Dict[str, Any], start_date: datetime.date,
                            accounts: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """

          {
            const accountTxns: ScrapedAccountsWithIndex = {};
            accounts.forEach((account) => {
              const txnGroups: ScrapedCurrentCardTransactions[] = _.get(dataResult, `CardsTransactionsListBean.Index${account.index}.CurrentCardTransactions`);


                if (!options.combineInstallments) {
                  allTxns = fixInstallments(allTxns);
                }
                allTxns = filterOldTransactions(allTxns, startMoment, options.combineInstallments || false);

                accountTxns[account.accountNumber] = {
                  accountNumber: account.accountNumber,
                  index: account.index,
                  txns: allTxns,
                };
              }
            });
            return accountTxns;
          }
        }

        :param raw_transactions:
        :param start_date:
        :param accounts:
        :return:
        """
        result = {}
        for account in accounts:
            account_index = str(account.get('index'))
            account_number = str(account.get('account_number'))
            processed_date = str(account.get('processed_date'))
            account_transactions = self._get_account_transaction(raw_transactions, account_index)
            account_transactions = self._convert_transactions_currency_to_shekels(account_transactions, processed_date)
            account_transactions = self._filter_old_transactions(account_transactions, start_date,
                                                                 self.options.get("combineInstallments", False))
            # decided to skip over fixInstallments operation
            result[account_number] = dict(
                accountNumber=account_number,
                index=account_index,
                txns=account_transactions,
            )
        return result

    def valid_transactions(self, raw_transactions) -> bool:
        if not isinstance(raw_transactions, dict):
            return False
        header = raw_transactions.get('Header', {})
        if header.get('Status') == '1' and raw_transactions.get('CardsTransactionsListBean'):
            return True
        return False

    def _get_account_transaction(self, raw_transactions: Dict[str, Any], account_index: str) -> List[Dict[str, Any]]:
        return deep_get(raw_transactions, "CardsTransactionsListBean", f"Index{account_index}",
                        "CurrentCardTransactions")

    def _convert_transactions_currency_to_shekels(self, account_transactions: List[Dict[str, Any]],
                                                  processed_date: str) -> List[Transaction]:
        if not account_transactions:
            return []

        all_transactions = []
        for transaction in account_transactions:
            converted_transactions_israel = self._convert_transaction_by_currency(transaction.get("txnIsrael")
                                                                                  , processed_date)
            all_transactions += converted_transactions_israel
            converted_transactions_abroad = self._convert_transaction_by_currency(transaction.get("txnAbroad")
                                                                                  , processed_date)
            all_transactions += converted_transactions_abroad
        return all_transactions

    def _convert_transaction_by_currency(self, transactions: List[Any], processed_date: str):
        if not transactions:
            return []

        filtered_transactions = self._filter_transactions(transactions)
        return self._parse_final_transaction(filtered_transactions, processed_date)

    def _filter_transactions(self, transactions):
        return [t for t in transactions if t.get('dealSumType', '') != '1'
                and t.get('voucherNumberRatz', '') != '000000000'
                and t.get('voucherNumberRatzOutbound', '') != '000000000']

    def _parse_final_transaction(self, filtered_transactions: List[Dict[str, Any]],
                                 processed_date: str) -> List[Transaction]:
        res = []
        for t in filtered_transactions:
            is_outbound = t.get('dealSumOutbound')
            txn_date_str = t.get('fullPurchaseDateOutbound') if is_outbound else t.get('fullPurchaseDate')
            txn_datetime = datetime.strptime(txn_date_str, DATE_FORMAT)
            built_transaction = Transaction(
                type=self._get_transaction_type(t),
                identifier=int(t.get('voucherNumberRatzOutbound') if is_outbound else t.get('voucherNumberRatz')),
                date=txn_datetime.isoformat(),
                processedDate=processed_date,
                originalAmount=-float(t.get('dealSumOutbound')) if is_outbound else -float(t.get('dealSum')),
                originalCurrency=self._convert_currency(t.get('currencyId')),
                chargedAmount=-float(t.get('paymentSumOutbound')) if is_outbound else -float(t.get('paymentSum')),
                description=t.get('fullSupplierNameOutbound') if is_outbound else t.get('fullSupplierNameHeb'),
                memo=t.get('moreInfo') or '',
                installments=self._get_installment_info(t) or None,
                status=TransactionStatuses.COMPLETED.value,
            )
            res.append(built_transaction)
        return res

    def _get_transaction_type(self, transaction: Dict[str, Any]) -> str:
        return TransactionTypes.INSTALLMENTS.value if self._get_installment_info(
            transaction) else TransactionTypes.NORMAL.value

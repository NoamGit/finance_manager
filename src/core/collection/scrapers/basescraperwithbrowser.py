import asyncio
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, Optional, List
from src.core.common import DATE_FORMAT

from dateutil.relativedelta import relativedelta
from pyppeteer import launch
from pyppeteer.browser import Browser
from pyppeteer.errors import PageError
from pyppeteer.page import Page

from src.core.collection.scrapers.basescraper import BaseScraper, VIEWPORT_WIDTH, VIEWPORT_HEIGHT, logger, click_button, \
    wait_until_element_found, fill_input, DATE_FORMAT
from src.core.collection.scrapers.model import LoginOptions, ScraperLoginResult, ScaperProgressTypes, HttpStatusTypes
from src.core.collection.model import ScraperCredentials


class BaseScraperWithBrowser(BaseScraper):
    browser: Browser
    page: Page

    async def setup_session(self):
        self.browser = await launch(headless=True
                                    , ignoreDefaultArgs=['--enable-automation']
                                    , dumpio=True
                                    )
        self.page = await self.browser.newPage()

    @property
    def get_view_port(self):
        return {"width": VIEWPORT_WIDTH,
                "height": VIEWPORT_HEIGHT}

    @abstractmethod
    def get_login_options(self, credentials: ScraperCredentials) -> LoginOptions:
        pass

    async def initialize_scraper(self):
        """
        debug('initialize scraper');
        this.emitProgress(ScaperProgressTypes.Initializing);

        let env: Record<string, any> | undefined;
        if (this.options.verbose) {
          env = { DEBUG: '*', ...process.env };
        }

        if (typeof this.options.browser !== 'undefined' && this.options.browser !== null) {
          debug('use custom browser instance provided in options');
          this.browser = this.options.browser;
        } else {
          const executablePath = this.options.executablePath || undefined;
          const args = this.options.args || [];

          const headless = !this.options.showBrowser;
          debug(`launch a browser with headless mode = ${headless}`);
          this.browser = await puppeteer.launch({
            env,
            headless,
            executablePath,
            args,
          });
          """
        # TODO
        raise NotImplementedError

    async def login(self, credentials: ScraperCredentials):
        if not credentials or not self.page:
            raise ValueError("missing credentials or page in login")

        logger.debug("execute login process")
        login_options = self.get_login_options(credentials)

        if login_options.user_agent:
            logger.debug("set custom user agent provided in options")
            raise NotImplementedError("login_options.user_agent")

        logger.debug('navigate to login url')
        await self.navigate_to(login_options.login_url)

        await self.wait_for_login_page_load(login_options)

        login_page = self.page
        if login_options.pre_action:
            logger.debug("execute 'preAction' interceptor provided in login options")
            raise NotImplementedError("login_options.pre_action")

        logger.debug('fill login form')
        await self.fill_login_form(login_page, login_options)

        if login_options.post_action:
            logger.debug("execute 'postAction' interceptor provided in login options")
            await login_options.post_action()
        else:
            logger.debug("wait for page navigation")
            raise NotImplementedError

        logger.debug("check login result")
        return await self.get_login_status()

    async def get_login_status(self) -> ScraperLoginResult:
        """
       const current = await getCurrentUrl(this.page, true);
       const loginResult = await getKeyByValue(loginOptions.possibleResults, current, this.page);
       debug(`handle login results ${loginResult}`);
      return handleLoginResult(this, loginResult);
        """
        # TODO
        raise NotImplementedError

    async def fill_login_form(self, login_page: Page, login_options: LoginOptions):
        await self.fill_inputs(login_page, login_options.fields)
        logger.debug('click on login submit button');
        if (isinstance(login_options.submit_button_selector, str)):
            await click_button(login_page, login_options.submit_button_selector)
        else:
            await login_options.submit_button_selector()
        logger.info(ScaperProgressTypes.logging_in);

    async def wait_for_login_page_load(self, login_options: LoginOptions):
        if login_options.check_readiness:
            logger.debug("execute 'checkReadiness' interceptor provided in login options")
            raise NotImplementedError("login_options.check_readiness")
        elif isinstance(login_options.submit_button_selector, str):
            logger.debug("wait until submit button is available")
            await wait_until_element_found(self.page, login_options.submit_button_selector)

    async def fill_inputs(self, page: Page, fields: Any):
        for field in fields:
            if 'selector' not in field and 'value' not in field:
                raise AttributeError("login_option fields is not valid")
            await fill_input(page, field['selector'], field['value'])

    async def navigate_to(self, url: str, page: Optional[Page] = None, timeout: Optional[int] = None):
        page = page if not self.page else self.page
        if not page:
            return

        options = dict(timeout=timeout, waitUntil='domcontentloaded')
        response = await page.goto(url, options)

        if not response or response.status != HttpStatusTypes.OK.value:
            raise PageError(f"Error while trying to navigate to url {url}")

    async def fetch_data(self, **extra_options):
        start_date = self.get_start_date()
        self.options.update({**extra_options})
        return await self.fetch_all_transactions(self.page, start_date)

    async def fetch_all_transactions(self, page: Page, start_date: datetime.date):
        future_moths_to_scrape = self._get_list_of_end_dates(start_date, self.options.get("future_months_to_scrape", 1))
        loop = asyncio.get_event_loop()
        fetched_and_gathered = await asyncio.gather(
            *(self.fetch_and_process_transaction(page, start_date, end_month) for end_month in future_moths_to_scrape),
            loop=loop)
        return fetched_and_gathered

    def _get_list_of_end_dates(self, start_date: datetime, max_dates: int) -> List[datetime]:
        return [start_date + relativedelta(months=_m) for _m in range(1, max_dates + 1)]

    @abstractmethod
    async def fetch_and_process_transaction(self, page: Page, start_date: datetime, end_month: datetime):
        pass

    def get_start_date(self) -> datetime.date:
        min_start_date = (datetime.now() - timedelta(days=365)).date()
        default_start_date = (datetime.now() - timedelta(days=30)).date()
        input_start_date = self.options.get('start_date', default_start_date)
        input_start_date = datetime.strptime(input_start_date, DATE_FORMAT).date() if self._validate_input_start_date(
            input_start_date) else min_start_date
        return max(min_start_date, input_start_date)

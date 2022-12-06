from src.core.collection.scrapers.basescraperwithbrowser import BaseScraperWithBrowser
from src.core import ScraperCredentials
from src.core.collection.scrapers.model import ScraperLoginResult


class OtsarHahayalScraper(BaseScraperWithBrowser):

    async def login(self, credentials: ScraperCredentials) -> ScraperLoginResult:
        # logger.info('navigate to login page')
        # return login_status
        """
        getLoginOptions(credentials: ScraperCredentials) {
            return {
              loginUrl: `${BASE_URL}/MatafLoginService/MatafLoginServlet?bankId=OTSARPRTAL&site=Private&KODSAFA=HE`,
              fields: createLoginFields(credentials),
              submitButtonSelector: '#continueBtn',
              postAction: async () => waitForPostLogin(this.page),
              possibleResults: getPossibleLoginResults(this.page),
            };
          }
        """
        pass

    async def fetch_data(self):
        # return result
        pass

    """

      async fetchData() {
        const defaultStartMoment = moment().subtract(1, 'years').add(1, 'day');
        const startDate = this.options.startDate || defaultStartMoment.toDate();
        const startMoment = moment.max(defaultStartMoment, moment(startDate));

        const url = getTransactionsUrl();
        await this.navigateTo(url);

        const accounts = await fetchTransactions(this.page, startMoment);

        return {
          success: true,
          accounts,
        };
      }
    """

    @staticmethod
    def get_login_fields(credentials: ScraperCredentials):
        if 'username' not in credentials or 'password' not in credentials:
            return AttributeError("credential object misses username or password fields")
        return [
            {'selector': '#username', 'value': credentials['username']},
            {'selector': '#password', 'value': credentials['password']},
        ]

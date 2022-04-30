from selenium import webdriver

from core.basescraper import BaseScraper


class IsracardScraper(BaseScraper):
    driver = webdriver.Firefox()
    base_url = "https://digital.isracard.co.il/personalarea/Login/"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def login(self):
        self.driver.get(self.base_url)


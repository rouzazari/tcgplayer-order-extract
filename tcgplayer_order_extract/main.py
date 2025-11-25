import json
import logging
import os
import pickle
import time
from urllib.parse import urlparse

from selenium import webdriver
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
from selenium.webdriver.common.window import WindowTypes
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from tcgplayer_order_extract.storage import S3Storage, LocalStorage


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TCGPlayerOrderExtractor:
    COOKIES_FILE = r'C:\temp\cookies.pkl'
    ORDERS_DIR = r'C:\temp\orders'

    def __init__(self, username, password, storage, **kwargs):
        self.driver = None
        self.wait = None
        self.logged_in = False
        self.order_window = None

        self.username = username
        self.password = password

        if storage['type'] == 'LocalStorage':
            self.storage = LocalStorage(storage['path'])
        elif storage['type'] == 'S3Storage':
            self.storage = S3Storage(storage['bucket_name'])

    def wait_for_element(self, selector_type=By.CSS_SELECTOR, selector=None):
        return self.wait.until(ec.element_to_be_clickable((selector_type, selector)))

    def initialize_driver(self):
        options = uc.ChromeOptions()
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        self.driver = uc.Chrome(options=options, headless=False, use_subprocess=False)
        self.wait = WebDriverWait(self.driver, 10)

        # load cookies
        self.driver.get("https://store.tcgplayer.com/admin/Seller/Dashboard/")
        sign_in_button = self.driver.find_element(By.XPATH, "//button[contains(., 'Sign In')]")
        with open(self.COOKIES_FILE, 'rb') as file:
            cookies = pickle.load(file)
        for cookie in cookies:
            self.driver.add_cookie(cookie)

    def login(self, username, password):
        self.driver.get("https://store.tcgplayer.com/admin/Seller/Dashboard/")
        time.sleep(1)

        # look for login, if no login, assume already logged in
        try:
            sign_in_button = self.driver.find_element(By.XPATH, "//button[contains(., 'Sign In')]")
        except NoSuchElementException:
            logger.info('already logged in')
            self.logged_in = True
            return

        # otherwise, login
        self.wait_for_element(By.XPATH, "//button[contains(., 'Sign In')]")
        email_box = self.driver.find_element(By.NAME, "Email")
        email_box.send_keys(username)
        time.sleep(1)
        password_box = self.driver.find_element(By.NAME, "Password")
        password_box.send_keys(password)
        time.sleep(1)
        sign_in_button.click()
        self.logged_in = True
        logger.info('completed log in')

    def navigate_to_orders(self, date_from, date_to):
        url = f"https://sellerportal.tcgplayer.com/orders?orderDateFrom={date_from}&orderDateTo={date_to}&fulfillmentTypes=Normal&searchRange=Custom&page=1&size=500&sortBy"
        self.driver.get(url)
        self.wait_for_element(By.ID, "searchTerm")
        self.order_window = self.driver.current_window_handle

    def extract_orders(self):
        try:
            order_links = self.wait.until(
                ec.presence_of_all_elements_located((By.CSS_SELECTOR, "a[data-testid='OrderIndex_Table_OrderLink']")))
        except TimeoutException:
            logger.info('no orders found')
            return

        logger.info(f'found {len(order_links)} orders')

        for link in order_links:
            # get order href and go to url
            order_href = link.get_attribute("href")
            self.driver.switch_to.new_window(WindowTypes.TAB)
            self.driver.get(order_href)
            time.sleep(2.0)

            order_number = os.path.basename(urlparse(order_href).path)

            responses = []
            logs = self.driver.get_log('performance')
            for entry in logs:
                log = json.loads(entry['message'])['message']
                if log['method'] == 'Network.responseReceived':
                    response = log['params']['response']
                    response['requestId'] = log['params']['requestId']
                    if 'url' in response and f'https://order-management-api.tcgplayer.com/orders/{order_number}' in response['url']:
                        responses += [response]
                        response['body'] = self.driver.execute_cdp_cmd('Network.getResponseBody',
                                                                       {'requestId': response['requestId']})['body']
                        response['body_json'] = json.loads(response['body'])
            if not responses:
                logger.warning(f'error getting order data for order number {order_number}')
                continue

            f = f'{order_number}.json'
            self.storage.save_file(responses, f)

            # return to order window
            self.driver.close()
            time.sleep(1.5)
            self.driver.switch_to.window(self.order_window)
            time.sleep(2.0)

    def run(self, date_from, date_to):
        try:
            self.initialize_driver()
            self.login(self.username, self.password)
            self.navigate_to_orders(date_from, date_to)
            self.extract_orders()
        finally:
            if self.driver:

                # store cookies for later sessions
                cookies = self.driver.get_cookies()
                with open(self.COOKIES_FILE, 'wb') as f:
                    pickle.dump(cookies, f)

                # clean up driver
                self.driver.close()

if __name__ == "__main__":
    # TODO: make these commandline arguments and/or from config file
    init_args = {
        'username': "",
        'password': "",
        # LocalStorage
        # 'storage': {
        #     'type': 'LocalStorage',
        #     'path': 'C:\\temp\\orders',
        # },
        # S3Storage
        'storage': {
            'type': 'S3Storage',
            'bucket_name': '',
        }
    }

    extractor = TCGPlayerOrderExtractor(**init_args)

    run_args = {
        'date_from': "11/24/2025",
        'date_to': "11/24/2025",
    }

    extractor.run(**run_args)


import json
import time
from datetime import datetime
from random import uniform
import re
from typing import List
from urllib.parse import urljoin
from httpx import Client
from dataclasses import dataclass, field
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait
import concurrent.futures
import threading
import sqlite3
from selectolax.parser import HTMLParser
import math

@dataclass
class DNBScraper:
    base_url: str = 'https://www.dnb.com/'
    cookies: List[dict] = None
    proxies: List[str] = field(default_factory=lambda: [
        "192.126.191.64:8800",
        "192.126.191.254:8800",
        "172.93.139.182:8800",
        "192.126.190.93:8800",
        "172.93.142.97:8800",
        "172.93.139.243:8800",
        "172.93.142.88:8800",
        "192.126.190.242:8800",
        "192.126.191.106:8800",
        "192.126.191.192:8800"
    ])
    uas: List[str] = field(default_factory=lambda: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'insomnia / 2023.5.8',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
    ])
    ip_index: int = 0
    ua_index: int = 0


    def fetch(self, url):
        print(f'Fetching {url}')
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': 'www.dnb.com',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests':'1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0'
        }

        req_cookies = {}
        for cookie in self.cookies:
            req_cookies[cookie['name']] = cookie['value']

        with Client(headers=headers, cookies=req_cookies) as client:
            response = client.get(self.base_url)
            # print(detect(response.content))
            print(response.headers)
        if response.status_code != 200:
            response.raise_for_status()

        return response

    def webdriver_setup(self):
        print('Preparing Webdriver...', end='')
        if self.ip_index > 5:
            self.ip_index = 0
        if self.ua_index > 2:
            self.ua_index = 0
        ua = self.uas[self.ua_index]
        proxy = self.proxies[self.ip_index]
        print(ua, proxy)
        ip, port = proxy.split(sep=':')
        ff_opt = Options()
        ff_opt.add_argument('-headless')
        ff_opt.add_argument('--no-sandbox')
        ff_opt.set_preference("general.useragent.override", ua)
        ff_opt.page_load_strategy = 'eager'

        ff_opt.set_preference('network.proxy.type', 1)
        ff_opt.set_preference('network.proxy.socks', ip)
        ff_opt.set_preference('network.proxy.socks_port', int(port))
        ff_opt.set_preference('network.proxy.socks_version', 4)
        ff_opt.set_preference('network.proxy.socks_remote_dns', True)
        ff_opt.set_preference('network.proxy.http', ip)
        ff_opt.set_preference('network.proxy.http_port', int(port))
        ff_opt.set_preference('network.proxy.ssl', ip)
        ff_opt.set_preference('network.proxy.ssl_port', int(port))
        ff_opt.set_preference('dom.webdriver.enable', False)
        ff_opt.set_preference('useAutomationExtension', False)

        driver = WebDriver(options=ff_opt)
        self.ip_index += 1
        self.ua_index += 1
        print('Completed!!!')
        return driver


    def get_company_url_by_search(self):
        print('Getting company url...')
        driver = self.webdriver_setup()
        endpoint = 'https://www.dnb.com/apps/dnb/servlets/CompanySearchServlet?pageNumber=1&pageSize=50&resourcePath=%2Fcontent%2Fdnb-us%2Fen%2Fhome%2Fsite-search-results%2Fjcr:content%2Fcontent-ipar-cta%2Fsinglepagesearch&returnNav=true&searchTerm=silk&'
        url = urljoin(self.base_url, endpoint)
        driver.maximize_window()
        wait = WebDriverWait(driver, 15)

        # driver.get(self.base_url)
        # driver.find_element(By.CSS_SELECTOR, 'a[href="/business-directory.html"]').click()
        # wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'button#truste-consent-button'))).click()
        # driver.implicitly_wait(4)
        # wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'div.SinglePageSearch.basecomp.RootBaseComponent.section')))
        # > div > div > div:nth-of-type(2) > div > div > div > div > input'

        driver.get(url)
        time.sleep(15)
        driver.find_element(By.CSS_SELECTOR, 'a#rawdata-tab').click()
        json_str = driver.find_element(By.CSS_SELECTOR, 'pre.data').text
        print(json_str)
        data = json.loads
        print('Completed!!!')
        # input('Press any key to close')
        driver.close()


    def get_location_urls(self):
        print('Getting location url...')
        driver = self.webdriver_setup()
        endpoint = 'business-directory/company-information.fabric_mills.us.html?page=1'
        url = urljoin(self.base_url, endpoint)
        driver.maximize_window()
        wait = WebDriverWait(driver, 15)
        driver.get(url)
        try:
            wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, 'button#truste-consent-button'))).click()
        except:
            pass
        time.sleep(uniform(0.1, 1.0))
        js_code = "arguments[0].scrollIntoView();"
        element = driver.find_element(By.CSS_SELECTOR, 'button.show-more')
        driver.execute_script(js_code, element)
        element.click()
        location_div = wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'div.locationResults')))
        locations = location_div.find_elements(By.CSS_SELECTOR, 'a')
        location_urls = []
        for location in locations:
            num_companies = 0
            location_url = location.get_attribute('href')
            num_companies_str = location.find_element(By.CSS_SELECTOR, 'span.number-countries').text.strip()
            pattern = r"\((\d+)\)"
            match = re.search(pattern, num_companies_str)
            if match:
                num_companies = int(match.group(1))
            if num_companies != 0:
                print(num_companies)
                num_pages = math.ceil(num_companies / 50)
                print(num_pages)
            else:
                num_pages = num_companies
            location_urls.append((location_url, num_pages))
        print('Completed!!!')
        self.cookies = driver.get_cookies()
        driver.close()
        return location_urls

    def get_company_urls_v1(self, urls):
        response = self.fetch(urls[0])

    def fetch_company_url(self, url):
        print(f'Company in {url}...')
        driver = self.webdriver_setup()
        driver.maximize_window()
        wait = WebDriverWait(driver, 15)
        driver.get(self.base_url)
        # wait.until(ec.element_to_be_clickable((By.CSS_SELECTOR, 'div#page')))
        time.sleep(1)
        for cookie in self.cookies:
            driver.add_cookie(cookie)
        driver.get(url)
        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, 'div#companyResults')))
        result = driver.page_source
        driver.close()
        print('Completed!!!')
        return result

    def get_company_urls_v2(self, urls):
        print(f'Getting company url...')
        # args = ((driver, url) for url in urls)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
           htmls = list(executor.map(lambda x: self.fetch_company_url(x), urls))

        return htmls

    def fetch_company_urls_v2_sync(self, urls):
        print(f'Getting company url...')
        conn = sqlite3.connect('dnb.db')
        curr = conn.cursor()
        curr.execute(
            """
            CREATE TABLE IF NOT EXISTS loc_htmls(
            url TEXT,
            date TEXT,
            data BLOB
            ) 
            """
        )
        for url in urls:
            print(url)
            if url[1] > 1:
                url_with_pages = [url[0] + f'?page={page}' for page in range(1, url[1]+1)]
                for url_with_page in url_with_pages:
                    html = self.fetch_company_url(url_with_page)
                    current = (url[0], datetime.now(), html)
                    curr.execute("INSERT INTO loc_htmls(url,date,data) VALUES(?,?,?)", current)
                    conn.commit()
            else:
                html = self.fetch_company_url(url[0])
                current = (url[0], datetime.now(), html)
                curr.execute("INSERT INTO loc_htmls(url,date,data) VALUES(?,?,?)", current)
                conn.commit()


    def get_company_urls(self):
        conn = sqlite3.connect("dnb.db")
        curr = conn.cursor()

        curr.execute("SELECT data FROM loc_htmls")
        datas = curr.fetchall()
        company_urls = []
        for data in datas:
            tree = HTMLParser(data[0])
            company_elements = tree.css('div#companyResults > div')
            for element in company_elements[1:]:
                company_url = urljoin(self.base_url, element.css_first('a').attributes.get('href'))
                company_urls.append(company_url)
        print(company_urls)
        return company_urls


    def main(self):
        location_urls = self.get_location_urls()
        self.fetch_company_urls_v2_sync(location_urls[5:6])


if __name__ == '__main__':
    s = DNBScraper()
    # s.main()
    print(len(s.get_company_urls()))
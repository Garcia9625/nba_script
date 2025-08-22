from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
import time
from bs4 import BeautifulSoup
import lxml

def bot_setup(headless=False):
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_argument("disable-infobars")
    options.add_argument("--incognito")
    if headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(service=ChromeService(), options=options)
    driver.implicitly_wait(10)
    driver.maximize_window()
    return driver

with open('urls.txt', 'r') as f:
    urls = f.read().splitlines()

driver = bot_setup(headless=False)
for url in urls:
    driver.get(url)
    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, 'lxml')

    dom = lxml.etree.HTML(str(soup))
    containers = ['https://www.basketball-reference.com' + txt for txt in dom.xpath('//div[@id="div_roster"]//td[@data-stat="player"]/a/@href')]
    with open('players.txt', 'a') as f:
        for container in containers:
            f.write(container)
            f.write('\n')

    print(f'url number {urls.index(url) + 1} out of {len(urls)}')
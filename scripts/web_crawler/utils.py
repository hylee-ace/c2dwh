from selenium.webdriver import Firefox, FirefoxOptions
from bs4 import BeautifulSoup
import os, threading


class CustomDriver(Firefox):
    def __init__(self, headless=False):
        # service = FirefoxService()
        # service.path = "./geckodriver"
        option = FirefoxOptions()

        option.add_argument("--headless") if headless else None
        option.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.",
        )
        super().__init__(options=option)


class Crawler:
    __crawled = set()
    __temp = set()

    def __init__(self, base_url: str, *, headless=False):
        self.__url = base_url
        self.__headless = headless

    def inspect(self, tag: str, *, attr: str, filter_attrs: dict = {}, **kwargs):
        with CustomDriver(self.__headless) as driver:
            driver.get(self.__url)
            soup = BeautifulSoup(driver.page_source, "html.parser")

        targets = soup.find_all(tag, attrs=filter_attrs, **kwargs)

        for i in targets:
            Crawler.__temp.add(i.get(attr).strip())

        print(f"Explored {len(Crawler.__temp)} links.")

        return Crawler.__temp

    @classmethod
    def save_result(cls, path: str, lock: threading.Lock):
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with lock:
            with open(path, "a") as file:
                for i in cls.__crawled:
                    file.write(i + "\n")

    def check_result():

        pass

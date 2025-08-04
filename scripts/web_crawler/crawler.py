from utils import CustomDriver, colorized
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from lxml import html
import requests


class Crawler:
    crawled = set()
    queue = set()
    history = set()

    # def __init__(self, base_url: str):
    #     self.__baseurl = base_url
    #     Crawler

    @classmethod
    def crawl(cls, url: str):
        pass

    @staticmethod
    def inspect(
        url: str,
        *,
        helper: str,
        xpath: str = None,
        tag: str = None,
        attr: str = None,
        filter_attrs: dict = {},
        **kwargs,
    ):
        """
        Return list of inspected HTML tag values from given URL.

        Parameters
        ---
        url : str
            Source domain
        helper : str
            Support **Selenium**, **Beautifulsoup** and **lxml**
        xpath : str, optional
            Use with **Selenium** or **lxml** (default: **None**)
        tag : str, optional
            Inspected tag, use with **Beautifulsoup** (default: **None**)
        attr: str, optional
            Indicate selected attribute of a tag, use with **Beautifulsoup** (default: **None**)
        filter_attrs: dict, optional
            Specific attributes with exact value, use with **Beautifulsoup** (default: **None**)
        kwargs: any, optional
            Use with **Beautifulsoup** (default: **None**)
        """

        data = []
        resp = None

        # check prerequisite and prepare reps
        if helper.lower() in [
            "beautifulsoup",
            "beautifulsoup4",
            "bs4",
        ]:
            if xpath:
                print(
                    "Only use tag, attr, filter_attrs and kwargs related to Beautifulsoup."
                )
                return
        elif helper.lower() == "lxml":
            if any([tag, attr, filter_attrs, kwargs]):
                print("Only use Xpath with lxml.")
                return
        elif helper.lower() == "selenium":
            if any([tag, attr, filter_attrs, kwargs]) and xpath:
                print(
                    "Avoid tag, attr, filter_attrs and kwargs related to Beautifulsoup when using Xpath, and vice versa."
                )
                return
        else:
            print("Only support Selenium, Beautifulsoup and lxml libraries.")
            return

        # main work
        if helper.lower() == "selenium":  # dynamic content
            if any([tag, attr, filter_attrs, kwargs, xpath]):
                with CustomDriver() as driver:
                    driver.get(url)

                    if xpath:  # lxml
                        page = html.fromstring(driver.page_source)
                        data = page.xpath(xpath)
                        return data
                    else:  # bs4
                        soup = BeautifulSoup(driver.page_source, "html.parser")
                        tags = soup.find_all(tag, filter_attrs, **kwargs)

                        for i in tags:
                            data.append(i.get(attr) if attr else i)

                        return data
            else:
                print("Missing lxml or Beautifulsoup related parameters.")
                return
        elif helper.lower() == "lxml":  # static content
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            page = html.fromstring(resp.content)
            return page.xpath(xpath)
        else:  # static content
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            tags = soup.find_all(tag, filter_attrs, **kwargs)

            for i in tags:
                data.append(i.get(attr) if attr else i)

            return data

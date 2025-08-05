from .utils import CustomDriver, colorized
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from lxml import html
from requests.exceptions import HTTPError, Timeout, ConnectionError
from selenium.common.exceptions import WebDriverException, TimeoutException
import requests, time


class Crawler:
    crawled = set()
    queue = set()
    history = set()

    # def __init__(self, base_url: str):
    #     self.__baseurl = base_url
    #     Crawler

    # @classmethod
    # def crawl(cls, url: str):
    #     pass

    @staticmethod
    def inspect(
        url: str,
        *,
        helper: str,
        xpath: str = None,
        tag: str = None,
        attr: str = None,
        filter_attrs: dict = None,
        **kwargs,
    ):
        """
        Return list of inspected HTML tag values from given URL. Avoid passing broken URL when using **Selenium**.

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
        final_e = None  # final exception (if caught)
        turns = 3  # retry turns
        driver = None  # for selenium
        helper = helper.lower()

        # check prerequisite and prepare reps
        if helper in ["beautifulsoup", "beautifulsoup4", "bs4"]:
            if xpath:
                print(
                    "Only use tag, attr, filter_attrs and kwargs related to Beautifulsoup."
                )
                return
        elif helper == "lxml":
            if any([tag, attr, filter_attrs, kwargs]):
                print("Only use xpath along with lxml.")
                return
        elif helper == "selenium":
            if any([tag, attr, filter_attrs, kwargs]) and xpath:
                print(
                    "Avoid tag, attr, filter_attrs and kwargs related to Beautifulsoup when using xpath, and vice versa."
                )
                return
        else:
            print("Only support Selenium, Beautifulsoup and lxml libraries.")
            return

        # handle retry for static content
        def retry_load():
            nonlocal final_e, resp, turns, driver

            if helper == "selenium":
                try:
                    driver = CustomDriver()
                    driver.set_page_load_timeout(15)
                    driver.get(url)
                except WebDriverException as e:  # connection lost
                    final_e = e
                    driver.quit()
                    driver = None
                    return
                except TimeoutException as e:
                    print(f"{e}. Retrying...")
                    while turns > 0:
                        try:
                            driver = CustomDriver()
                            driver.set_page_load_timeout(15)
                            driver.get(url)
                            break
                        except Exception as e:
                            print(f"{e}. Retrying...")
                            final_e = e
                            driver.quit()
                            driver = None  # reset if fail again
                        turns -= 1
                        time.sleep(5)

            else:
                try:
                    resp = requests.get(url, timeout=10)
                    resp.raise_for_status()
                except (HTTPError, ConnectionError) as e:
                    final_e = e
                    return
                except Timeout as e:  # retry
                    print(f"{e}. Retrying...")
                    while turns > 0:
                        try:
                            resp = requests.get(url, timeout=10)
                            resp.raise_for_status()
                            break
                        except Exception as e:
                            print(f"{e}. Retrying...")
                            final_e = e
                            resp = None  # reset resp if fail again
                        turns -= 1
                        time.sleep(5)

        # main work
        if helper == "selenium":  # dynamic content
            if not any([tag, attr, filter_attrs, kwargs, xpath]):
                print("Missing lxml or Beautifulsoup related parameters.")
                return

            retry_load()

            if final_e or not driver:
                if turns == 3:  # connection lost case
                    print(final_e)
                    print("(Make sure the connection and webdriver remains stable)")
                    return
                print("Failed 3 times.", {final_e})
                return
            if not driver:
                print("Failed 3 times.", {final_e})
                return

            if xpath:  # use lxml
                page = html.fromstring(driver.page_source)
                data = page.xpath(xpath)
                driver.quit()
                return data
            else:  # use bs4
                soup = BeautifulSoup(driver.page_source, "html.parser")
                tags = soup.find_all(tag, filter_attrs, **kwargs)

                for i in tags:
                    data.append(i.get(attr) if attr else i)

                driver.quit()
                return data
        elif helper == "lxml":  # static content
            retry_load()

            if not resp or resp.status_code != 200:
                if turns == 3:  # connection lost case
                    print(final_e, "(Make sure the connection remains stable)")
                    return
                print("Failed 3 times.", {final_e})
                return

            page = html.fromstring(resp.content)

            if xpath:
                return page.xpath(xpath)

            return html.tostring(page, pretty_print=True, encoding="unicode")
        else:  # static content
            retry_load()

            if not resp or resp.status_code != 200:
                if turns == 3:  # connection lost case
                    print(final_e, "(Make sure the connection remains stable)")
                    return
                print("Failed 3 times.", {final_e})
                return

            soup = BeautifulSoup(resp.content, "html.parser")
            tags = soup.find_all(tag, filter_attrs, **kwargs)

            for i in tags:
                data.append(i.get(attr) if attr else i)

            return data

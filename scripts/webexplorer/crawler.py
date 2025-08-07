from utils.utils import CustomWebDriver, async_get
from bs4 import BeautifulSoup
from lxml import html
from selenium.common.exceptions import WebDriverException, TimeoutException
from urllib.parse import urljoin
import time, threading, httpx, asyncio


class Crawler:
    """ """

    base_url = None
    queue = set()
    crawled = set()

    def __init__(self, base_url: str, crawled: set):
        if not Crawler.base_url:
            Crawler.base_url = base_url
            Crawler.queue.add(base_url)

        Crawler.crawled.update(crawled)

    @staticmethod
    def crawl(
        url: str, *, lock: threading.Lock, output: set = None, client: httpx.Client
    ):
        # start inspecting
        res = [
            urljoin(url, i.strip())
            for i in Crawler.inspect(
                url,
                helper="lxml",
                xpath="//a[contains(@href,'shop')and not(contains(@href,'add-to-cart'))and not(contains(@href,'#'))]/@href",
                client=client,
            )
        ]  # founded urls

        # update queue and crawled
        with lock:
            Crawler.queue.remove(url)  # remove inspected url
            Crawler.queue.update(
                i for i in res if i not in Crawler.crawled
            )  # put new urls in queue for inspecting
            Crawler.crawled.add(url)  # put inspected url to crawled list
            Crawler.crawled.update(res)  # also update founded urls to crawled list
            output.update(Crawler.crawled)  # update (for writing to file)

    @staticmethod
    def inspect(
        url: str,
        *,
        helper: str,
        client: httpx.Client,
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
                    driver = CustomWebDriver()
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
                            driver = CustomWebDriver()
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
                pass

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
            # retry_load()
            resp = async_get(url, client=client)

            if not resp:
                print(f"Can not inspect {url}. {resp}")
                return

            page = html.fromstring(resp.content)

            if xpath:
                return page.xpath(xpath)

            return html.tostring(page, pretty_print=True, encoding="unicode")
        else:  # static content
            # retry_load()
            resp = async_get(url, client=client)

            if not resp:
                print(f"Can not inspect {url}. {resp}")
                return

            soup = BeautifulSoup(resp.content, "html.parser")
            tags = soup.find_all(tag, filter_attrs, **kwargs)

            for i in tags:
                data.append(i.get(attr) if attr else i)

            return data

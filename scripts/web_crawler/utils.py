from selenium.webdriver import Firefox, FirefoxOptions
from bs4 import BeautifulSoup
import threading, time, functools


class CustomDriver(Firefox):
    """
    Customized webdriver for Firefox browser. In latest Selenium, service is no longer necessary,
    if facing any problems with GeckoDriver, uncomment service setup.
    """

    def __init__(self):
        # service = FirefoxService()
        # service.path = "./geckodriver"
        option = FirefoxOptions()

        option.add_argument("--headless")
        option.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.",
        )
        super().__init__(options=option)


class Crawler:
    crawled = set()
    queue = set()
    history = set()

    def __init__(self, base_url: str):
        self.__url = base_url

    @staticmethod
    def inspect(
        url: str, tag: str, *, attr: str = None, filter_attrs: dict = {}, **kwargs
    ):
        data = []

        with CustomDriver() as driver:
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, "html.parser")

        tags = soup.find_all(tag, filter_attrs, **kwargs)

        for i in tags:
            data.append(i.get(attr).strip() if attr else i)

        return data


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


def runtime(obj: object):
    """
    Decorator for estimating runtime of a process.
    """

    @functools.wraps(obj)
    def wrapper(*args, **kwargs):
        res = None
        error = False
        event = threading.Event()

        def stopwatch():
            global start
            start = time.time()
            while not event.is_set():
                global now
                now = time.time() - start
                print(
                    f"\rRuntime: {colorized(timetext(now),color=33)}",
                    "\033[?25l",
                    end="\r",
                    flush=True,
                )
                time.sleep(0.1)

        def get_obj_value():
            nonlocal res, error
            try:
                res = obj(*args, **kwargs)
            except Exception as e:
                error = True
                print(
                    f"[{colorized('x',color=31,bold=True)}] Error occurs in {colorized(obj.__qualname__,color=31)} >> {e}"
                )
            finally:
                event.set()

        sw_thread = threading.Thread(target=stopwatch)
        obj_thread = threading.Thread(target=get_obj_value)

        obj_thread.start(), sw_thread.start()
        sw_thread.join(), obj_thread.join()

        if not error:
            print(f"Finished in {colorized(timetext(now),color=32)}\033[?25h")

        return res

    return wrapper


def colorized(text: str | int | float, *, color: str | int, bold: bool = False):
    """
    Change printed text color in terminal.

    ---
        _**BLACK**_: 30
        _**RED**_: 31
        _**GREEN**_: 32
        _**YELLOW**_: 33
        _**BLUE**_: 34
        _**MAGENTA**_: 35
        _**CYAN**_: 36
        _**WHITE**_: 37
    """

    msg = "Only support BLACK(30), RED(31), GREEN(32), YELLOW(33), BLUE(34), MAGENTA(35), CYAN(36) and WHITE(37)."
    codes = {
        "black": 30,
        "red": 31,
        "green": 32,
        "yellow": 33,
        "blue": 34,
        "magenta": 35,
        "cyan": 36,
        "white": 37,
    }
    bold_code = "01;" if bold else ""

    if isinstance(color, int):
        if color not in codes.values():
            print(msg)
            return
        text = f"\033[{bold_code}{color}m{text}\033[0m"
    else:
        color = color.lower()
        if color not in codes:
            print(msg)
            return
        text = f"\033[{bold_code}{codes[color]}m{text}\033[0m"

    return text


def timetext(second: int | float):
    """
    Convert seconds to actual time text.
    """
    text = f"{second:.2f}s"
    m = h = 0

    if second >= 60:
        m = int(second / 60)
        second = second % 60
        text = f"{m}{'m'if m else ''}{second:.2f}s"

    if m >= 60:
        h = int(second / 3600)
        m = int(second % 3600 / 60)
        second = second % 3600 % 60
        text = f"{h}{'h'if m else''}{m}{'m'if m else ''}{second:.2f}s"

    return text

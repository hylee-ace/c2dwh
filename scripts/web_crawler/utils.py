from selenium.webdriver import Firefox, FirefoxOptions
from bs4 import BeautifulSoup
import os, threading, time


class CustomDriver(Firefox):
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


def runtime(func: object, *args, **kwargs):
    """Estimate runtime of a process."""

    val = None

    # get func returned values
    def return_val():
        nonlocal val  # to make val public
        try:
            val = func(*args, **kwargs)
            return val
        except Exception as e:
            print(f"[Error from {func}]. {e}")

    # prepare thread
    func_thread = threading.Thread(target=return_val)
    func_thread.start()
    start = time.time()

    while func_thread.is_alive():
        # runtime text
        current = time.time() - start
        m = 0
        text = f"{current:.2f}s"

        if current >= 60:
            m = int(current / 60)
            current = current % 60
            text = f"{m}min{current:.2f}s"

        print(f"\rRuntime: \033[33m{text}\033[0m", end="", flush=True)
        time.sleep(0.1)

    func_thread.join()

    # endtime text
    end = time.time() - start
    m2 = 0
    text2 = f"{end:.2f}s"

    if end >= 60:
        m2 = int(end / 60)
        end = end % 60
        text2 = f"{m2}min{end:.2f}s"

    print("\rFinished in \033[32m{}\033[0m".format(text2))

    return val


def colorized(text: str, *, color: str | int, bold: bool = False):
    """Change printed text color in terminal."""

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

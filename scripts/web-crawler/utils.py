from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from selenium.webdriver import Firefox, FirefoxService, FirefoxOptions, FirefoxProfile
import os


def save_to_file(data: list[str] | set[str], path: str):
    os.makedirs(path, exist_ok=True)
    with open("links.txt", "w") as file:
        for i in data:
            file.write(i + "\n")


domain = "https://cellphones.com.vn/"
links = set()  # prevent duplicates

# # firefox webdriver setup
# service = FirefoxService(executable_path="./web-driver/geckodriver")
# option = FirefoxOptions()
# profile = FirefoxProfile()

# profile.set_preference(
#     "general.useragent.override",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.",
# )  # use when running in background
# option.binary_location = "/opt/firefox/firefox"
# option.add_argument("--headless")
# option.profile = profile

# access url
with Firefox() as driver:
    driver.get(domain)
    soup = BeautifulSoup(driver.page_source, "html.parser")

a_tags = soup.find_all(
    "a",
    href=lambda x: x
    and urlparse(urljoin(domain, x)).hostname == urlparse(domain).hostname
    and urlparse(urljoin(domain, x)).path.endswith(".html"),
)

# # meta_data = soup.find_all("meta", attrs={"data-n-head": "ssr", "property": "og:type"})

# for i in a_tags:
#     links.add(i.attrs["href"].strip())
#     print(i.attrs["href"].strip())


print(len(a_tags))
# print(len(links))


# save_to_file(links)

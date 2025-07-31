from selenium.webdriver import Firefox, FirefoxOptions, FirefoxProfile, FirefoxService


class FireFoxDriver(Firefox):
    # __service = FirefoxService()
    __option = FirefoxOptions()
    __profile = FirefoxProfile()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        FireFoxDriver.__profile.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.",
        )
        FireFoxDriver.__option.binary_location = "/opt/firefox/firefox"
        # WebDriver.__option.add_argument("--headless")
        FireFoxDriver.__option.profile = FireFoxDriver.__profile


with FireFoxDriver() as dr:
    dr.get("http://cellphones.com.vn")
    print(dr.page_source)

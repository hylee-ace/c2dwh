from webcrawler import Crawler
from utils import runtime
import asyncio


@runtime
def main():
    main_page = "https://cellphones.com.vn/"

    crawler = Crawler(
        main_page,
        search="//a[contains(@href,'.html')]/@href",
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
        "Connection": "keep-alive",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Referer": "https://www.google.com/",
        "DNT": "1",  # not track request header
        "Upgrade-Insecure-Requests": "1",
    }

    asyncio.run(
        crawler.execute(
            headers=headers,
            timeout=20,
            chunksize=500,
            semaphore=asyncio.Semaphore(50),
        )
    )

    print(crawler.crawled)
    print(len(crawler.crawled))

    crawler.reset()


if __name__ == "__main__":
    main()

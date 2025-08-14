from webcrawler import Crawler
from utils import runtime
import asyncio


@runtime
def main():
    site = "https://cellphones.com.vn/"
    save_path = "./scripts/webcrawler/crawled/cellphones.csv"

    crawler = Crawler(
        site,
        search="//a[substring(@href,string-length(@href)-4)='.html' and contains(@href,'https://cellphones.com.vn/')]/@href",
        save_in=save_path,
    )

    asyncio.run(
        crawler.execute(
            timeout=20.0,
            chunksize=1000,
            semaphore=asyncio.Semaphore(200),
        )
    )

    crawler.reset()


if __name__ == "__main__":
    main()

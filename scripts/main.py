from webcrawler import Crawler
from utils import runtime
import asyncio


@runtime
def main():
    main_page = "https://quotes.toscrape.com/"
    save_path = "./scripts/webcrawler/crawled/quotestoscrape.txt"

    crawler = Crawler(
        main_page,
        search="//a[not (contains(@href,'login'))and not (contains(@href,'zyte'))and not (contains(@href,'goodreads'))]/@href",
        save_in=save_path,
    )

    asyncio.run(
        crawler.execute(
            timeout=20,
            chunksize=500,
            semaphore=asyncio.Semaphore(150),
        )
    )

    crawler.reset()



if __name__ == "__main__":
    main()

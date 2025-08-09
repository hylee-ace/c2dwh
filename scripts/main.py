from webcrawler import Crawler
from utils import runtime, colorized
import asyncio, os


@runtime
def main():
    main_page = "https://books.toscrape.com/"
    save_path = "./scripts/webcrawler/crawled/bookstoscrape.txt"

    crawler = Crawler(
        main_page,
        search="//a[contains(@href,'.html')]/@href",
        save_in=save_path,
    )

    asyncio.run(
        crawler.execute(
            timeout=20,
            chunksize=500,
            semaphore=asyncio.Semaphore(50),
        )
    )

    if crawler.history:
        new = len(crawler.valid - crawler.history)
        text = (
            f"({new} more url{'s'if new>1 else ''})"
            if new > 0
            else "(There are no new urls)"
        )

    print(
        f"Crawled: {colorized(len(crawler.crawled),33)} | Valid: {colorized(len(crawler.valid),32)} {text if crawler.history else ''}"
    )

    crawler.reset()


if __name__ == "__main__":
    main()

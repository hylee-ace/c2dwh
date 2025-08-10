from webcrawler import Crawler
from utils import runtime
import asyncio, csv


@runtime
def main():
    main_page = "https://cellphones.com.vn/"
    save_path = "./scripts/webcrawler/crawled/cellphones.csv"

    crawler = Crawler(
        main_page,
        search=f"//a[substring(@href,string-length(@href)-4)='.html' and contains(@href,'{main_page}')]/@href",
        save_in=save_path,
    )

    # asyncio.run(
    #     crawler.execute(
    #         timeout=20,
    #         chunksize=1000,
    #         semaphore=asyncio.Semaphore(200),
    #     )
    # )

    with open(save_path, "r") as file:
        reader = csv.reader(file, skipinitialspace=True)
        for i in reader:
            print(i)


if __name__ == "__main__":
    main()

from bs4 import BeautifulSoup
from web_crawler.utils import Crawler, CustomDriver
from urllib.parse import urlparse, urljoin
import os, time
from concurrent.futures import ThreadPoolExecutor, as_completed


def main():
    domain = "https://scrapeme.live/shop/"

    """     
    - run thread with queue, then we have links from i.results
    - put links into done (done.update), update crawled from queue, take difference between i.results and queue to get new queue
    - take difference between queue and crawled to ensure not repeat links
    - if queue empty then stop

    tool=Crawler()
    queue={domain}
    done=set()
    crawled=set()

    while queue:
        temp=set()

        threadpool(worker=5):
            futures=[submit(tool.inspect,i) for i in queue]

            for i in as_complete(futures):
                done.update(i.result)
                temp.update(i.result)

        crawled.update(queue)
        queue=temp.difference(queue)
        queue.difference(crawled)


                queue{domain} --> i.results{1,2,3} --> crawled{domain,1,2,3}, ran={domain}
                    |             |
    crawled.update  |   i.res.differece
                    |     |
                queue{1,2,3} --> i.results{3,8,9,5,1,4} --> crawled{domain,1,2,3,4,5,8,9}, ran{domain,1,2,3}
                    |             |
    crawled.update  |   i.res.differece
                    |     |
                queue{4,5,8,9} --> i.results{1,2,3,6,1,3,3} --> crawled{domain,1,2,3,4,5,8,9,6}, ran{domain,1,2,3,4,5,8,9}
                    |             |
    crawled.update  |   i.res.differece
                    |   |
                queue{6} --> i.results{1,2,4,8} --> crawled{domain,1,2,3,4,5,8,9,6}, ran{domain,1,2,3,4,5,6,8,9}
                    |             |
    crawled.update  |   i.res.differece
                    |   |
                queue{} --> i.results{} --> crawled{1,2,3,4,5,8,9,6} (stop)
    
    """

    # crawled = {0, 1, 2, 3, 4, 5, 8, 9, 6}
    # queue = set()
    # history = {0, 1, 2, 3, 4, 5, 8, 9, 6}

    # # 4
    # res = []

    # crawled.update(queue, res)
    # history.update(queue)

    # queue = set(res).difference(queue)
    # queue.difference_update(history)

    # print(crawled)
    # print(queue)
    # print(history)

    def crawler(url):
        tool = Crawler(urljoin(domain, url), headless=True)
        print("Inspecting " + url)
        return tool.inspect(
            "a",
            attr="href",
            href=lambda x: x
            and urlparse(urljoin(domain, x)).hostname == urlparse(domain).hostname
            and "add-to-cart" not in urljoin(domain, x)
            and "shop" in urljoin(domain, x)
            and "#" not in urljoin(domain, x),
        )

    queue = {domain}
    crawled = set()
    history = set()

    while queue:
        temp = set()

        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(crawler, i) for i in queue]

            for i in as_completed(futures):
                temp.update(i.result())
                crawled.update(queue, temp)

        history.update(queue)
        queue = temp.difference(queue)
        queue.difference_update(history)

        print(len(crawled), len(history), len(queue))


if __name__ == "__main__":
    main()

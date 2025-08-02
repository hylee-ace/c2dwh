from bs4 import BeautifulSoup
from web_crawler.utils import Crawler
from urllib.parse import urlparse, urljoin
import os, time
from concurrent.futures import ThreadPoolExecutor, as_completed


def main():
    domain = "https://scrapeme.live/shop/"

    """     
    - run thread with queue, then we have links from i.results
    - put links and queue into crawled
    - put queue into history to track
    - new queue is equal to i.resutl taking difference on old queue
    - take difference_update between queue and history to ensure not repeat old links
    - if queue empty then stop

    queue{domain} --> i.results{1,2,3} --> crawled{domain,1,2,3}, history{domain}
        |             |
        |   i.res.differece(queue) 
        |   queue.difference_update(history)
        |     |
    queue{1,2,3} --> i.results{3,8,9,5,1,4} --> crawled{domain,1,2,3,4,5,8,9}, history{domain,1,2,3}
        |             |
        |   i.res.differece(queue) 
        |   queue.difference_update(history)
        |     |
    queue{4,5,8,9} --> i.results{1,2,3,6,1,3,3} --> crawled{domain,1,2,3,4,5,8,9,6}, history{domain,1,2,3,4,5,8,9}
        |             |
        |   i.res.differece(queue) 
        |   queue.difference_update(history)
        |   |
    queue{6} --> i.results{1,2,4,8} --> crawled{domain,1,2,3,4,5,8,9,6}, history{domain,1,2,3,4,5,6,8,9}
        |             |
        |   i.res.differece(queue) 
        |   queue.difference_update(history)
        |   |
    queue{} --> i.results{} --> crawled{1,2,3,4,5,8,9,6} (stop)
    
    """

    # def crawler(url):
    #     tool = Crawler(urljoin(domain, url), headless=True)
    #     print("Inspecting " + url)
    #     return tool.inspect(
    #         "a",
    #         attr="href",
    #         href=lambda x: x
    #         and urlparse(urljoin(domain, x)).hostname == urlparse(domain).hostname
    #         and "add-to-cart" not in urljoin(domain, x)
    #         and "shop" in urljoin(domain, x)
    #         and "#" not in urljoin(domain, x),
    #     )

    # queue = {domain}
    # crawled = set()
    # history = set()

    # while queue:
    #     temp = set()

    #     with ThreadPoolExecutor(max_workers=5) as ex:
    #         futures = [ex.submit(crawler, i) for i in queue]

    #         for i in as_completed(futures):
    #             temp.update(i.result())
    #             crawled.update(queue, temp)

    #     history.update(queue)
    #     queue = temp.difference(queue)
    #     queue.difference_update(history)

    #     print(len(crawled), len(history), len(queue))

    t = Crawler.inspect(domain, "a")
    print(len(t))


if __name__ == "__main__":
    main()

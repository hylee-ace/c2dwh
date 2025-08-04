from web_crawler.crawler import Crawler
from web_crawler.utils import Cursor, runtime, colorized
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading, requests
from lxml import html


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
    
    """

    @runtime
    def crawl():
        crawler = Crawler()
        crawler.queue.add(domain)
        lock = threading.Lock()

        while crawler.queue:
            temp = set()

            with ThreadPoolExecutor(max_workers=30) as ex:
                futures = [ex.submit(Crawler.explore, i) for i in crawler.queue]

                for i in as_completed(futures):
                    try:
                        with lock:  # prevent race condition
                            crawler.crawled.update(crawler.queue, i.result())
                            temp.update(i.result())
                    except Exception as e:
                        print(f"[{colorized('x',31,bold=True)}] Error >> {e}")
                        print(i.cancelled(),i.done())
                        continue

            crawler.history.update(crawler.queue)
            crawler.queue = temp.difference(crawler.queue)
            crawler.queue.difference_update(crawler.history)

        print(
            f"Exploring completed. Found {colorized(str(len(crawler.crawled))+' urls',32)}.",
            Cursor.clear,
        )

    crawl()


if __name__ == "__main__":
    main()

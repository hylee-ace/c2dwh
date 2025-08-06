from webexplorer import WebScout
from utils import runtime
from urllib.parse import urljoin
import threading


def main():
    base = "https://scrapeme.live/shop/"
    done = list()
    # lock = threading.Lock()

    WebScout(base)

    # @runtime
    def work():
        nonlocal done
        while WebScout.queue:
            todolist = sorted(WebScout.queue)
            threads = [threading.Thread()]
            chunk = 20 if len(todolist) > 20 else len(todolist)

            for i in range(chunk):
                scout = WebScout(todolist[i])
                if not WebScout.in_use:
                    continue
                t = threading.Thread(target=scout.crawl, args=(done,))
                threads.append(t)

            for i in threads:
                i.start()
                i.join()

            print(f"Queue: {len(scout.queue)} | Found: {len(done)}")

    work()
    print(done)


if __name__ == "__main__":
    main()

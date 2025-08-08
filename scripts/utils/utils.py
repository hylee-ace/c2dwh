import threading, time, functools, httpx, asyncio, inspect, os


class Cursor:
    """
    Terminal cursor behaviors.
    """

    clear = "\033[K"
    hide = "\033[?25l"
    reveal = "\033[?25h"


# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #


def runtime(func: object):
    """
    Decorator for estimating runtime of a process.
    """
    is_async = inspect.iscoroutinefunction(func)

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        res = None
        error = False
        event = threading.Event()

        # background work
        def stopwatch():
            global start
            start = time.perf_counter()
            while not event.is_set():
                print(
                    f"\rRuntime: {colorized(timetext(time.perf_counter()-start),33)}",
                    Cursor.hide,
                    end="\r",
                    flush=True,
                )
                time.sleep(0.095)

        def get_value():
            nonlocal res, error
            try:
                res = func(*args, **kwargs)
            except Exception as e:
                error = True
                print(
                    f"[{colorized('x',31,bold=True)}] Error occurs in {colorized(func.__qualname__,31)} >> {e}"
                )
            finally:
                event.set()

        # initialize threads
        sw_thread = threading.Thread(target=stopwatch)
        fn_thread = threading.Thread(target=get_value)

        fn_thread.start(), sw_thread.start()
        fn_thread.join()

        if not error:
            print(
                f"Finished in {colorized(timetext(time.perf_counter()-start),32)}",
                Cursor.reveal,
            )

        return res

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        res = None
        error = False
        event = threading.Event()

        # background work
        def stopwatch():
            global start
            start = time.perf_counter()
            while not event.is_set():
                print(
                    f"\rRuntime: {colorized(timetext(time.perf_counter()-start),33)}",
                    Cursor.hide,
                    end="\r",
                    flush=True,
                )
                time.sleep(0.095)

        async def get_value():
            nonlocal res, error
            try:
                res = await func(*args, **kwargs)
            except Exception as e:
                error = True
                print(
                    f"[{colorized('x',31,bold=True)}] Error occurs in {colorized(func.__qualname__,31)} >> {e}"
                )
            finally:
                event.set()

        # initialize thread
        sw_thread = threading.Thread(target=stopwatch)
        sw_thread.start()

        await get_value()

        if not error:
            print(
                f"Finished in {colorized(timetext(time.perf_counter()-start),32)}",
                Cursor.reveal,
            )

        return res

    return async_wrapper if is_async else sync_wrapper


def colorized(chars: str | int | float, color: str | int, *, bold: bool = False):
    """
    Customized characters color in terminal.

    ---
        BLACK: 30
        RED: 31
        GREEN: 32
        YELLOW: 33
        BLUE: 34
        MAGENTA: 35
        CYAN: 36
        WHITE: 37
    """

    msg = "Only support BLACK(30), RED(31), GREEN(32), YELLOW(33), BLUE(34), MAGENTA(35), CYAN(36) and WHITE(37)."
    codes = {
        "black": 30,
        "red": 31,
        "green": 32,
        "yellow": 33,
        "blue": 34,
        "magenta": 35,
        "cyan": 36,
        "white": 37,
    }

    if isinstance(color, int):
        if color not in codes.values():
            print(msg)
            return
        chars = f"\033[0{'1'if bold else '0'};{color}m{chars}\033[0m"
    else:
        color = color.lower()
        if color not in codes:
            print(msg)
            return
        chars = f"\033[0{'1'if bold else '0'}{codes[color]}m{chars}\033[0m"

    return chars


def timetext(second: int | float):
    """
    Convert seconds to actual time text.
    """

    h = int(second / 3600)
    m = int(second % 3600 / 60)
    s = second % 3600 % 60

    text = f"{h if h>0 else ''}{'h'if h>0 else ''}{'0' if h>0 and m<10 else ''}{m if m>0 else ''}{'m'if m>0 else ''}{s:.2f}s"

    return text


async def async_get(
    url,
    *,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore = None,
    retries: int = 3,
    delay: int | float = 2.0,
):
    """
    Perform asynchronous GET request. Return response object or None if all retries fail.
    """
    if semaphore:
        async with semaphore:  # limit number of concurrent processes
            for _ in range(retries):
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    return resp
                except httpx.HTTPStatusError as e:
                    print(e)
                    return None
                except httpx.RequestError as e:
                    print(f"{e}. Retrying...")
                    await asyncio.sleep(delay)
            return None

    for _ in range(retries):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            print(e)
            return None
        except httpx.RequestError as e:
            print(f"{e}. Retrying...")
            await asyncio.sleep(delay)
    return None


def save_to_file(data: list[str] | str, path: str):
    """
    Save data to given path.
    """

    if path and path != "":
        os.makedirs(os.path.dirname(path), exist_ok=True)
    else:
        print("Invalid path.")
        return

    mode = "a" if os.path.exists(path) else "w"

    try:
        with open(path, mode) as file:
            file.writelines(data)
    except Exception as e:
        print(f"Saving to {path} failed. {e}")
        return

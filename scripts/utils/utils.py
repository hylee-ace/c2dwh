import threading, time, functools, inspect, os, csv, boto3, json
from botocore.exceptions import ClientError as AwsClientError
from botocore.client import BaseClient


class Cursor:
    """
    Terminal cursor behaviors.
    """

    clear = "\033[K"
    hide = "\033[?25l"
    reveal = "\033[?25h"


# classes
# ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** ********** #
# functions


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


def dict_to_csv(data: dict | list[dict], path: str):
    """
    Save dict-type or list of dict-type data as CSV file.
    """

    if path:
        if os.path.isdir(path):
            print(f"Invalid path. {path} is a directory.")
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
    else:
        print("Invalid path.")
        return

    mode = "a" if os.path.exists(path) and os.path.getsize(path) else "w"
    header = data.keys() if isinstance(data, dict) else data[0].keys()

    try:
        with open(path, mode) as file:
            writer = csv.DictWriter(file, header)
            if mode == "w":
                writer.writeheader()
            if isinstance(data, dict):
                writer.writerow(data)
            else:
                writer.writerows(data)
    except Exception as e:
        print(f"Saving to {path} failed >> {e}")
        return


def csv_reader(path: str, *, fields: str | list[str] | None = None):
    """
    Read CSV file and return list of dict-type data. Can extract by field names.
    """

    data = []
    header = []

    if os.path.isdir(path):
        print(f"Invalid path. {path} is a directory.")
        return

    if fields:
        if isinstance(fields, str):
            header.append(fields)
        else:
            header.extend(fields)

    try:
        if header:
            with open(path, "r") as file:
                reader = csv.DictReader(file, skipinitialspace=True)
                for i in reader:
                    new = {}
                    for j in header:
                        if j not in reader.fieldnames:
                            print(f"'{j}' does not exist in header.")
                            return
                        new[j] = i[j]
                    data.append(new)
        else:
            with open(path, "r") as file:
                reader = csv.DictReader(file, skipinitialspace=True)
                for i in reader:
                    data.append(i)
    except Exception as e:
        print(f"Cannot read {path} >> {e}")
    finally:
        return data


def s3_file_uploader(
    path: str, *, client: BaseClient | None = None, bucket: str, key: str
):
    """
    Upload file to AWS S3 bucket basing on given key name.
    """

    if os.path.isdir(path):
        print(f"Invalid path. {path} is a directory.")
        return

    # get aws credentials
    with open("/home/jh97/MyWorks/Documents/.aws_cdt.json", "r") as file:
        content = json.load(file)
        key_id = content["access_key"]
        secret_key = content["secrect_access_key"]

    # initialize client
    if not client:
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_key,
        )

    try:
        client.upload_file(Filename=path, Bucket=bucket, Key=key)
    except AwsClientError as e:
        print(f"Cannot upload {os.path.basename(path)} >> {e.response}")

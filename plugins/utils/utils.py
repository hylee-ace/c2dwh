import threading, time, functools, inspect, os, csv, boto3, json, re
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
    with open("/home/jh97/MyWorks/Documents/.aws_iam_token.json", "r") as file:
        content = json.load(file)
        key_id = content["aws_access_key_id"]
        secret_key = content["aws_secret_access_key"]

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


def athena_sql_executor(
    query: str,
    *,
    client: BaseClient | None = None,
    database: str | None = None,
    output_location: str | None = None,
    encrypt_config: dict | None = None,
):
    """
    Execute SQL query on AWS Athena.
    """
    data = {}
    is_select = (
        True
        if re.search(r"^\s*select|^\s*with.*?as.*?\(.*\)\s*select", query.lower(), re.S)
        else False
    )

    # get aws credentials
    with open("/home/jh97/MyWorks/Documents/.aws_iam_token.json", "r") as file:
        content = json.load(file)
        key_id = content["aws_access_key_id"]
        secret_key = content["aws_secret_access_key"]

    # initialize client
    if not client:
        client = boto3.client(
            "athena",
            region_name="us-east-1",
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_key,
        )

    # execute the query
    try:
        resp = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": "default" if not database else database,
                "Catalog": "AwsDataCatalog",
            },
            ResultConfiguration={
                "OutputLocation": (
                    "s3://c2dwh-athena-queries/"
                    if not output_location
                    else output_location
                ),
                "EncryptionConfiguration": (
                    {"EncryptionOption": "SSE_S3"}
                    if not encrypt_config
                    else encrypt_config
                ),
            },
        )
        data["query_execution_id"] = resp["QueryExecutionId"]
    except AwsClientError as e:
        print(f"Cannot execute the query >> {e.response}")
        return data

    # wait for executing
    while True:
        execution = client.get_query_execution(
            QueryExecutionId=resp["QueryExecutionId"]
        )
        data["query_execution_state"] = execution["QueryExecution"]["Status"]["State"]

        if data["query_execution_state"] in [
            "FAILED",
            "CANCELLED",
        ]:
            print(
                f'Execution {colorized(data["query_execution_state"],31)} >>',
                execution["QueryExecution"]["Status"]
                .get("AthenaError", {})
                .get("ErrorMessage"),
            )
            return data
        if data["query_execution_state"] == "SUCCEEDED":
            if is_select:
                break
            else:
                print(f'Execution {colorized(data["query_execution_state"],32)}.')
                return data

        time.sleep(0.2)

    # normalize result (only for SELECT queries)
    data["data"] = []
    paginator = client.get_paginator("get_query_results")

    columns = None
    for page in paginator.paginate(QueryExecutionId=resp["QueryExecutionId"]):
        rows = page["ResultSet"]["Rows"]

        if not columns:  # get list of columns and skip 1st row from 1st page only
            columns = [
                i.get("VarCharValue") for i in page["ResultSet"]["Rows"][0]["Data"]
            ]
            rows = rows[1:]

        for row in rows:
            data["data"].append(
                {
                    columns[i]: row["Data"][i].get("VarCharValue")
                    for i in range(len(columns))
                }
            )

    return data


def s3_folder_cleaner(
    prefix: str | None = None, *, client: BaseClient | None = None, bucket: str
):
    """
    Free up AWS S3 bucket folder by prefix name. If prefix name is empty, everything in bucket will be removed.
    """

    # initialize client
    if not client:
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    # get objects list
    paginator = client.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix if prefix else ""):
            objs = [{"Key": i["Key"]} for i in page["Contents"]]
            client.delete_objects(Bucket=bucket, Delete={"Objects": objs})
            for i in page["Contents"]:
                print(f"Removed {i['Key']}.")
    except AwsClientError as e:
        print(f"Error occurs while cleaning {prefix if prefix else bucket} >> {e}")

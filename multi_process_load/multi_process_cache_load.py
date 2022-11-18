import collections
import datetime
import glob
import gzip
import logging
import multiprocessing
import os
from copy import deepcopy
from multiprocessing import JoinableQueue, Lock, Process, cpu_count
from optparse import OptionParser
from typing import Any, Dict, List, Tuple

import redis

from load_env import load_environ

load_environ()
AppsInstalled = collections.namedtuple(
    "AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"]
)
NORMAL_ERR_RATE = 0.01


def get_redis_conn(
    key_and_port: Dict[str, int]
) -> Tuple[Dict[str, redis.StrictRedis], bool]:
    conn_dict: Dict = {}
    key_and_port_err: Dict = {}
    err: bool = True
    while err:
        for key, port in key_and_port.items():
            conn_dict[key] = redis.StrictRedis(
                username=os.environ.get("REDIS_LOGIN"),
                password=os.environ.get("REDIS_PWD"),
                host=os.environ.get("REDIS_HOST"),
                port=port,
            )
            try:
                conn_dict[key].ping()
            except redis.exceptions.ConnectionError:
                key_and_port_err[key] = port
        if len(key_and_port_err) == 0:
            err = False
        key_and_port = deepcopy(key_and_port_err)

    return conn_dict, err


def dot_rename(path: str) -> None:
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))


def parse_appsinstalled(line: str) -> AppsInstalled | None:
    line_parts: List[str] = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps: List[int] = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps: List[int] = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        pass
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def establish_all_connections() -> Dict[str, redis.Redis]:
    redis_conn, err = get_redis_conn(
        {
            "idfa": int(os.environ.get("REDIS_idfa")),
            "gaid": int(os.environ.get("REDIS_gaid")),
            "adid": int(os.environ.get("REDIS_adid")),
            "dvid": int(os.environ.get("REDIS_dvid")),
        }
    )
    return redis_conn


def load_in_redis(redis_conn: Dict[str, redis.Redis], app_line: AppsInstalled) -> bool:
    key = "%s:%s" % (app_line.dev_type, app_line.dev_id)

    redis_conn[app_line.dev_type].set(
        key, f"{app_line.lat}:{app_line.lon}-{app_line.apps}"
    )
    return True


def config_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )


class Producer(Process):
    """
    Producer class
    Adds rows to the queue for processing by consumers
    :param: queue - Shared queue for all processes
    :param: lock - Shared lock for all processes
    :param: fn - file to be processed by the process
    """

    def __init__(self, queue: JoinableQueue, lock: Lock, fn: Any):
        Process.__init__(self)
        self.queue = queue
        self.lock = lock
        self.file = fn
        self.logger = logging.getLogger(__name__)

    def run(self):
        config_logging()
        self.logger.setLevel(logging.INFO)
        self.logger.info(f"Producer {os.getpid()} processing %s" % self.file)
        fd = gzip.open(self.file)
        for line in fd:
            self.queue.put(line)

        fd.close()
        self.logger.info(f"Producer {os.getpid()} finish %s" % self.file)
        dot_rename(self.file)
        self.logger.info(f"Producer {os.getpid()} rename file %s" % self.file)
        self.queue.task_done()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()


class Consumer(Process):
    """
    Consumer class
    Takes the lines from the queue and adds them to the corresponding Redis
    :param: queue - Shared queue for all processes
    :param: lock - Shared lock for all processes
    """

    def __init__(self, queue: JoinableQueue, lock: Lock):
        Process.__init__(self)
        self.daemon = True
        self.queue = queue
        self.lock = lock
        self.logger = logging.getLogger(__name__)

    def run(self):
        config_logging()
        self.logger.setLevel(logging.INFO)
        self.logger.info(f"Start consumer {os.getpid()}")
        redis_connection_pools = establish_all_connections()
        self.logger.info(f"Redis is ok in {os.getpid()}")
        while True:
            line = self.queue.get()
            if line == "STOP":
                break
            line = line.strip()
            appsinstalled = parse_appsinstalled(line.decode("utf-8"))
            if not appsinstalled:
                continue
            _ = load_in_redis(redis_connection_pools, appsinstalled)
        self.queue.task_done()
        self.logger.info(f"Consumer {os.getpid()} end work")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()


def main(options: Any) -> None:
    queue = JoinableQueue()
    lock = Lock()
    producer_workers: List[multiprocessing.Process] = []
    consumer_workers: List[multiprocessing.Process] = []

    for fn in glob.iglob(".." + options.pattern):
        producer_workers.append(Producer(queue, lock, fn))
    for i in range(cpu_count() - 2):
        consumer_workers.append(Consumer(queue, lock))

    for producer in producer_workers:
        producer.start()
    for consumer in consumer_workers:
        consumer.start()
    for producer in producer_workers:
        producer.join()
    for _ in range(len(consumer_workers)):
        queue.put("STOP")
    for consumer in consumer_workers:
        consumer.join()

    queue.close()
    queue.join_thread()


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    (opts, args) = op.parse_args()
    config_logging()
    logger = logging.getLogger(__name__)
    start = datetime.datetime.now()
    logger.info(f"Start program at {start}")
    main(opts)
    logger.info(f"Run time: {datetime.datetime.now() - start}")
    # Processing two tsv.gz by 512mb on 16 cores = 7.32 min

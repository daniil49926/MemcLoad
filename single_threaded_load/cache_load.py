import glob
import gzip
import logging
import os
import sys
import datetime
import redis
import collections
from optparse import OptionParser
from copy import deepcopy
from typing import Dict, Tuple, Any
from load_env import load_environ

load_environ()
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
NORMAL_ERR_RATE = 0.01


def get_redis_conn(key_and_port: Dict[str, int]) -> Tuple[Dict[str, redis.Redis], bool]:
    conn_dict = {}
    key_and_port_err = {}
    err = True
    while err:
        for key, port in key_and_port.items():
            conn_dict[key] = redis.Redis(
                username=os.environ.get('REDIS_LOGIN'),
                password=os.environ.get('REDIS_PWD'),
                host=os.environ.get('REDIS_HOST'),
                port=port
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
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def establish_all_connections() -> Dict[str, redis.Redis]:
    redis_conn, err = get_redis_conn({
        'idfa': int(os.environ.get('REDIS_idfa')),
        'gaid': int(os.environ.get('REDIS_gaid')),
        'adid': int(os.environ.get('REDIS_adid')),
        'dvid': int(os.environ.get('REDIS_dvid'))
    })
    if err:
        logging.error('Redis is not ok')
        sys.exit(1)
    logging.info('Redis is ok')
    return redis_conn


def load_in_redis(redis_conn: Dict[str, redis.Redis], app_line: AppsInstalled) -> bool:
    key = "%s:%s" % (app_line.dev_type, app_line.dev_id)
    redis_conn[app_line.dev_type].set(key, f'{app_line.lat}:{app_line.lon}-{app_line.apps}')
    return True


def main(options: Any) -> None:
    redis_connection_pools = establish_all_connections()
    for fn in glob.iglob('../' + options.pattern):
        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        for line in fd:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line.decode('utf-8'))
            if not appsinstalled:
                errors += 1
                continue
            ok = load_in_redis(redis_connection_pools, appsinstalled)
            if ok:
                processed += 1
            else:
                errors += 1
        if not processed:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    (opts, args) = op.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S'
    )
    start = datetime.datetime.now()
    main(opts)
    logging.info(f'Run time: {datetime.datetime.now() - start}')
    # Process two tsv.gz by 512mb = 52.15 min

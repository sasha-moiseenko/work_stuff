#!/usr/bin/env python
import logging
import fnmatch
import subprocess

import shell as shell
from shell import CommandError
import fire
import sys
from functools import wraps
import errno
import os
import signal


# class TimeoutError(Exception):
#     pass
#
#
# def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
#     def decorator(func):
#         def _handle_timeout(signum, frame):
#             raise TimeoutError(error_message)
#
#         def wrapper(*args, **kwargs):
#             signal.signal(signal.SIGALRM, _handle_timeout)
#             signal.alarm(seconds)
#             try:
#                 result = func(*args, **kwargs)
#             finally:
#                 signal.alarm(0)
#             return result
#
#         return wraps(func)(wrapper)
#
#     return decorator


def setup_log(name):
    logger = logging.getLogger(name)  # > set up a new name for a new logger

    logger.setLevel(logging.INFO)

    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    filename = f"/var/log/{name}.log"
    log_handler = logging.FileHandler(filename)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(log_format)

    logger.addHandler(log_handler)

    return logger


# @timeout(120, os.strerror(errno.ETIMEDOUT))
def find(pattern, way, dst_table='migration.gate_nat'):
    logger = setup_log('error_handling')
    dirs2=[d for i  in subprocess.check_output(f'find {way} -name {pattern}')]
    print(dirs2)
    db_connection_data = f'clickhouse-client -h SRV-DSS-WH-1 -u uploader'
    query = f'\"INSERT INTO {dst_table}(start_time, src_ip, src_port, src_xlated_ip, src_xlated_port, dst_ip, dst_port) SELECT _col2, coalesce(_col3, 0)+2147483648, coalesce(_col4,0)+32768, coalesce(_col5,0)+2147483648, coalesce(_col6,0)+32768, coalesce(_col7,0)+2147483648, coalesce(_col8,0)+32768 FROM input(\'_col0 Decimal(38,0), _col1 Decimal(38,0), _col2 Datetime, _col3 Nullable(Int32), _col4 Nullable(Int16), _col5 Nullable(Int32), _col6 Nullable(Int16), _col7 Nullable(Int32), _col8 Nullable(Int16)\') FORMAT ORC\"'
    sh = shell.Shell(record_errors=True, record_output=True)
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files if fnmatch.fnmatch(f_name, pattern)]
    print(dirs)
    for i in dirs:
        logger.info(f'{i} start unloading')
        print(i)
        # try:
        #     sh.run(
        #         f'{db_connection_data} --query={query} < {i}')
        # except TimeoutError as e:
        #     print(e)
        #     continue
        c = sh.run(f'{db_connection_data} --query={query} < {i}')
        print(c.pid)
        print(c.errors())
        if c.errors():
            logger.info(f'{c.errors()} I\'m trying do it later')
        else:
            print("i'm here")
            logger.info('file ready to delete')
            sh.run(f'mv {i} {i}.deleting')
       logger.info(f'{sh.output()}')


def main():
    fire.Fire(find)


if __name__ == "__main__":
    main()

# for launching use flags --pattern --way
# example:
# migration_error_handling.py --pattern='*.error' --way='/data/migration/sample/'

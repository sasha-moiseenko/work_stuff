#!/usr/bin/env python3

import os
import paramiko
import logging
import fnmatch
import re
from scp import SCPClient
import time


ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('SRV-DSS-WH-31', username='svcdeeplog_')
print('connected')
scp_ = SCPClient(ssh.get_transport())


def setup_log(name):
    logger = logging.getLogger(name)  # > set up a new name for a new logger
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    filename = f'/var/log/{name}.log'
    log_handler = logging.FileHandler(filename)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(log_format)

    logger.addHandler(log_handler)

    return logger


def find(pattern, way):
    logger = setup_log('error_handling')
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files if fnmatch.fnmatch(f_name, pattern)]
    for i in dirs:
        logger.info(f'I\'ve found {i}')
        if 'nat' in i:
            print(i)
            print('open')
            reg_1 = fr'\d.*'
            reg_2 = fr'.*?[\/]'
            found = re.findall(reg_1, i)
            print(found)
            print(found[0])
            path = re.findall(reg_2, i)
            path = ''.join(path)
            print(path)
            timestamp = int(time.time())
            print(f'/opt/clickhouse/error_files/nat/{found[0]}')
            scp_.put(i, f'/opt/clickhouse/error_files/nat/{timestamp}.{found[0]}')
        if 'edr' in i:
            print(i)
            print('open')
            reg_1 = fr'\d.*'
            reg_2 = fr'.*?[\/]'
            found = re.findall(reg_1, i)
            print(found)
            print(found[0])
            path = re.findall(reg_2, i)
            path = ''.join(path)
            print(path)
            print(f'/opt/clickhouse/error_files/edr/{found[0]}')
            scp_.put(i, f'/opt/clickhouse/error_files/edr/{found[0]}')

    scp_.close()
    ssh.close()


if __name__ == "__main__":
    find('*error', '/data/migration/sample/')

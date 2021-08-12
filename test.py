#!/usr/bin/env python3

import os
import paramiko
import logging
import fnmatch
import re
import pathlib
from scp import SCPClient


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


ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('SRV-DSS-WH-31', username='svcdeeplog_')
scp_ = SCPClient(ssh.get_transport())


def find(pattern, way):
    logger = setup_log('error_handling')
    logger.info('ssh tunnel is connected')
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files if fnmatch.fnmatch(f_name, pattern)]
    for i in dirs:
        logger.info(f'I\'ve found {i}')
        if 'nat' in i:
            size = os.system(f'du -h {i}')
            logger.info(f'it\'s a nat file {i} size={size}')
            reg_1 = fr'\d.*'
            found = re.findall(reg_1, i)
            logger.info(f'/opt/clickhouse/nat/{found[0]} will be transferred')
            scp_.put(i, f'/opt/clickhouse/nat/{found[0]}')
            logger.info(f'{found[0]} was transferred to wh')
        if 'edr' in i:
            size = os.system(f'du -h {i}')
            logger.info(f'it\'s a edr file {i} size={size}')
            reg_1 = fr'\d.*'
            found = re.findall(reg_1, i)
            logger.info(f'/opt/clickhouse//edr/{found[0]} will be transferred')
            scp_.put(i, f'/opt/clickhouse//edr/{found[0]}')
            logger.info(f'{found[0]} was transferred to wh')

    scp_.close()
    ssh.close()


if __name__ == "__main__":
    find('*error', '/home/')

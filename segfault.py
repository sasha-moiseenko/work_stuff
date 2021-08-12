import subprocess
from file_read_backwards import FileReadBackwards
from datetime import date
import logging
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
def find_log():
    logger = setup_log('zdpi_service_restart')
    search_date = date.today()
    print(search_date)
    command_start = ('systemctl start zdpid')
    command_kill = ('systemctl stop zdpid')
    log = open('/home/sasha/zdpi', 'r')
    for line in log:
        # print(line)
        if str(search_date) in line and 'SEGFAULT' in line:
            print(line,'yos')
            try:
                subprocess.check_output(command, shell=True)
            except subprocess.CalledProcessError as err:
                logger.error(f'Err code: {err.returncode}')
                logger.error(err.output)
                logger.error(err)
find_log()import subprocess
from datetime import date
import logging
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
def find_log():
    logger = setup_log('zdpi_service_restart')
    search_date = date.today()
    print(search_date)
    command = ('service zdpi restart')
    log = open('/home/sasha/zdpi', 'r')
    for line in log:
        # print(line)
        if str(search_date) in line and 'SEGFAULT' in line:
            print(line,'yos')
            try:
                subprocess.check_output(command, shell=True)
            except subprocess.CalledProcessError as err:
                logger.error(f'Err code: {err.returncode}')
                logger.error(err.output)
                logger.error(err)
find_log()
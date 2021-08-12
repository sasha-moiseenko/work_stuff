import pyorc
import csv
import glob
import os
import queue
import multiprocessing
import logging
import time


def setup_log(name):
    logger = logging.getLogger(name)  # > set up a new name for a new logger
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    filename = f'/home/sasha/{name}.log'
    log_handler = logging.FileHandler(filename)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(log_format)

    logger.addHandler(log_handler)

    return logger


logger = setup_log('orc')


def find(way):
    dirs = glob.glob(way)
    for f in dirs:
        os.rename(f, f'{f}.orc')
    return dirs


fields = ['_col0', '_col1', '_col2', '_col3', '_col4',
          '_col5', '_col6', '_col7', '_col8', '_col9',
          '_col10', '_col11', '_col12', '_col13',
          '_col14', '_col15', '_col16', '_col17', '_col18',
          '_col19', '_col20', '_col21', '_col22', '_col23',
          '_col24', '_col25', '_col26', '_col27', '_col28', '_col29', '_col30', '_col31']


def process_orc(file_to_process):
    print(file_to_process)
    try:
        csv_file = open(f'{file_to_process}.csv', 'w')
        csv_out = csv.writer(csv_file)
        with open(f"{file_to_process}.orc", "rb") as data:
            csv_out.writerow(fields)
            reader = pyorc.Reader(data)
            for row in reader:
                csv_out.writerow(row)
            os.remove(file_to_process)
    except(ValueError, FileNotFoundError) as err:
        os.remove(new_file_name)
        os.rename(file_to_process, f'{file_to_process}.error')
        raise err
    except FileExistsError as err:
        raise err
    # logger.info(f'Problem was occurred with {file} parsing. See {err}')
    finally:
        csv_file.close()

# print('new_file close')


def worker(q):
    worker_id = os.getpid()
    logger.info(f'{worker_id} worker started')

    while True:
        try:
            file_to_process = q.get()
            logger.info(f'worker {worker_id}: processing {file_to_process}')
            process_orc(file_to_process)
        except Exception as e:
            logger.error(f'worker {worker_id}: error: {e}')


if __name__ == '__main__':
    workers_count = 4
    files_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(workers_count, worker, (files_queue,))

    while True:
        orcs = find("/home/sasha/day_id=01/*")

        if len(orcs) > 0:
            logger.info('New files', orcs)
            for file in orcs:
                print('put', file)
                files_queue.put(file)
                print('in queue')
        else:
            logger.info('no new files')

        time.sleep(30)

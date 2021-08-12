#!/usr/bin/env python3
import datetime
from tempfile import NamedTemporaryFile
import shutil
import csv
import ipaddress
import os
import glob
import logging
import re
import gzip
import time
from tempfile import NamedTemporaryFile
import functools
import multiprocessing


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


logger = setup_log('fix_dpi_parser')


def find(way):
    dirs = glob.glob(way)
    return dirs


def dict_create():
    radius_files = glob.glob('/usr/svas/make_radlist_file/store/radlist_*.csv')
    if radius_files:
        filepath = max(radius_files)
        logger.info(f'radlist {filepath}')
        with open(filepath, mode='r') as infile:
            reader = csv.reader(infile)
            radius_dict = {rows[1]: rows[0] for rows in reader}
        return radius_dict


radius_dict = dict_create()
fields = ['start_time', 'end_time', 'transaction_time', 'packets_ul', 'packets_dl', 'bytes_ul', 'bytes_dl',
          'pdu_id', 'probe_id', 'site_id', 'location_id',
          'dr_status', 'dr_type', 'ip_src', 'ip_dst', 'port_src',
          'port_dst', 'protocol', 'opcode', 'teid_cp_ul', 'teid_cp_dl',
          'teid_up_ul', 'teid_up_dl', 'thr_put_ul', 'thr_put_dl', 'imsi',
          'msisdn', 'imei', 'user_ip_v4', 'user_ip_v6', 'netw_ip_v4',
          'netw_ip_v6', 'apn', 'mcc', 'mnc', 'lac', 'rac', 'cell',
          'rat_type', 'app_protocol', 'app_group', 'application',
          'app_attributes', 'ssl_server_name', 'ssl_session_id',
          'ssl_version', 'flow_id', 'tcp_rtt_ul', 'tcp_rtt_dl',
          'tcp_server_delay', 'tcp_invalid_chksum_ul', 'tcp_invalid_chksum_dl',
          'tcp_window_size_ul', 'tcp_window_size_dl', 'tcp_window_scale_ul',
          'tcp_window_scale_dl', 'tcp_network_limit', 'tcp_thr_put_max_ul',
          'tcp_thr_put_max_dl', 'tcp_mss', 'tcp_window_upd_pkt_ul',
          'tcp_window_upd_pkt_dl', 'tcp_zero_window_pkt_ul',
          'tcp_zero_window_pkt_dl', 'tcp_t_out_retrans_ul',
          'tcp_t_out_retrans_dl', 'tcp_fast_retrans_ul',
          'tcp_fast_retrans_dl', 'tcp_ramp_up_time_ul',
          'tcp_ramp_up_time_dl', 'tcp_ramp_up_drop_time_ul',
          'tcp_ramp_up_drop_time_dl', 'tcp_out_of_order_ul',
          'tcp_out_of_order_dl', 'unknown_col']


def process_file(file_to_process):
    file_name = file_to_process
    file_name_regexp = r'\d{6,}.*.csv'
    file_name_to_move = re.findall(file_name_regexp, file_name)[0]
    new_file_name = f'/data/ready_csv/{file_name_to_move}.ready'

    try:
        new_file = open(new_file_name, 'x')
        zero_logins = open(f'/data/files_zero_logins/{datetime.datetime.now()}.csv', 'x')

        with gzip.open(file_name, 'rt') as csv_file:
            reader = csv.DictReader(csv_file, fieldnames=fields)
            writer = csv.DictWriter(new_file, extrasaction='ignore', fieldnames=fields)
            writer_zero_logins = csv.DictWriter(zero_logins, fieldnames=fields)
            headers = {}
            for n in writer.fieldnames:
                headers[n] = n
            writer.writerow(headers)
            # writer_zero_logins.writerow(headers)
            for row in reader:
                if row['msisdn'] == '0':
                    if radius_dict.get(str(row['ip_src'])):
                        row['msisdn'] = radius_dict.get(str(row['ip_src']))
                    else:
                        writer_zero_logins.writerow(row)
                if 'ffff' in row['ip_src']:
                    row['ip_src'] = ipaddress.IPv6Address(row['ip_src']).ipv4_mapped
                if 'ffff' in row['ip_dst']:
                    row['ip_dst'] = ipaddress.IPv6Address(row['ip_dst']).ipv4_mapped
                    writer.writerow(row)
            # print('!!!!!!', file_to_process)
            os.remove(file_to_process)
    except(ValueError, FileNotFoundError) as err:
        os.remove(new_file_name)
        os.rename(file_to_process, f'{file_to_process}.error')
        raise err
    except FileExistsError as err:
        raise err
        # logger.info(f'Problem was occurred with {file} parsing. See {err}')
    finally:
        # print('new_file close')
        new_file.close()
        # print('new_file close')
        zero_logins.close()


def worker(q):
    worker_id = os.getpid()
    logger.info(f'{worker_id} worker started')

    while True:
        try:
            file_to_process = q.get()
            logger.info(f'worker {worker_id}: processing {file_to_process}')
            process_file(file_to_process)
        except Exception as e:
            logger.error(f'worker {worker_id}: error: {e}')


if __name__ == '__main__':
    workers_count = 4
    files_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(workers_count, worker, (files_queue,))


    while True:
        dpi_files = find('/data/upload/fix_dpi/probe0*/*.csv.gz')

        if len(dpi_files) > 0:
            logger.info('New files', dpi_files)
            for file in dpi_files:
                # logger.info('put', file)
                files_queue.put(file)
                # print('in queue')
        else:
            logger.info('no new files')

        time.sleep(30)



    # pool.close()
    # pool.join()
    # print('all tasks are done')

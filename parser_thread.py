#!/usr/bin/env python3

import csv
import glob
import gzip
import ipaddress
import logging
import os
import queue
import re
import shutil
import threading
import time
from tempfile import NamedTemporaryFile
import functools
import multiprocessing

files_queue = queue.Queue()

'''scp -r root@10.128.192.49:/data/upload/fix_dpi/probe01/* /home/sasha/dpi/'''


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


logger = setup_log('dpi_parser')


def find(way):
    dirs = glob.glob(way)
    return dirs


def dict_create():
    radius_files = glob.glob('/home/sasha/radlist/radlist_*.csv')
    if radius_files:
        filepath = max(radius_files)
        logger.info(f'radlist {filepath}')
        with open(filepath, mode='r') as infile:
            reader = csv.reader(infile)
            radius_dict = {rows[1]: rows[0] for rows in reader}
        return radius_dict


radius_dict = dict_create()


def upload_file(file):
    print(file, 'upload_file')
    # ready_files = find('/home/sasha/ready_files/*.ready')
    file_to_upload_regexp = r'\d{5,}_.*.csv'
    file_to_upload_name = re.findall(file_to_upload_regexp, file)[0]
    file_to_upload = open(f'/home/sasha/files_to_upload/{file_to_upload_name}.ch', 'x')
    # for f in ready_files:
    with open(file, 'r') as parser:
        logger.info(f'start parsing {file}')
        print(f'open new_file {file}')
        reader2 = csv.DictReader(parser, dialect='excel')
        writer2 = csv.DictWriter(file_to_upload, reader2.fieldnames, dialect='excel')
        writer2.writeheader()
        for i in reader2:
            if i['msisdn'] != '0':
                # print(i)
                writer2.writerow(i)
            # parser.close()
        logger.info(f'finish parsing {file}. New file for upload to ch is {file_to_upload}')
        print(f'new file for delete ----> {file}')
    file_to_upload.close()
    # parser.close()
    print("Remove file")
    os.remove(file)


def process_file(file_to_process):
    print(file_to_process)
    try:
        # print(radius_dict)
        logger.info(f'start parsing {file_to_process}')
        file_name = file_to_process
        print(file_name)
        file_name_regexp = r'\d{6,}.*.csv'
        file_name_to_move = re.findall(file_name_regexp, file_name)[0]
        print(f'{file_name_to_move} <----- file_name_to_move')
        new_file = open(f'/home/sasha/ready_files/{file_name_to_move}.ready', 'x')
        print(f'{new_file} <--- new_file')
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
        with gzip.open(file_name, 'rt') as csv_file:
            print('gzip')
            reader = csv.DictReader(csv_file, fieldnames=fields)
            writer = csv.DictWriter(new_file, extrasaction='ignore', fieldnames=fields)
            headers = {}
            for n in writer.fieldnames:
                headers[n] = n
            writer.writerow(headers)
            for row in reader:
                if 'ffff' in row['ip_src']:
                    row['ip_src'] = ipaddress.IPv6Address(row['ip_src']).ipv4_mapped
                if 'ffff' in row['ip_dst']:
                    row['ip_dst'] = ipaddress.IPv6Address(row['ip_dst']).ipv4_mapped
                    writer.writerow(row)
                if row['msisdn'] == '0':
                    row['msisdn'] = radius_dict.get(str(row['ip_src']))
                writer.writerow(row)
        logger.info(f'finish parsing {file_to_process}. New ready file is {new_file}')
        new_file.close()
        new_file = f'/home/sasha/ready_files/{file_name_to_move}.ready'
        upload_file(new_file)
        print(new_file)
        os.remove(file_to_process)
    except(FileExistsError, ValueError, FileNotFoundError) as err:
        print("EXC!!")
        logger.info(f'Problem was occurred with {file_to_process} parsing. See {err}')


def worker():
    while True:
        file_to_process = files_queue.get()
        print('in queue', file_to_process)
        try:
            print(f'Working on file: {file_to_process}')
            process_file(file_to_process)
        except Exception as e:
            print(e)
        files_queue.task_done()


if __name__ == '__main__':
    while True:
        dpi_files = find('/home/sasha/dpi/*.csv.gz')
        print(dpi_files)
        for file in dpi_files:
            # mark files as waiting for processing
            print('put', file)
            files_queue.put(file)
            print('in queue')
        time.sleep(0.2)

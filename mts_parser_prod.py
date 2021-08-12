#!/usr/bin/env python3


from tempfile import NamedTemporaryFile
import shutil
import csv
import ipaddress
import os
import glob
import logging
import re
import gzip
import datetime


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


logger = setup_log('fix_dpi_parser_prod')


def find(way):
    dirs = glob.glob(way)
    return dirs


def dict_create():
    radius_files = glob.glob('/home/sasha/radlist/radlist_*.csv')
    if radius_files:
        filepath = max(radius_files)
        print(filepath)
        with open(filepath, mode='r') as infile:
            reader = csv.reader(infile)
            radius_dict = {rows[1]: rows[0] for rows in reader}
            print(radius_dict)
        return radius_dict


def is_ok(row):
    if row['msisdn'] == '0':
        return False
    return True


radius_dict = dict_create()

dpi_files = find('/home/sasha/dpi/*.csv.gz')
for file in dpi_files:
    try:
        # print(radius_dict)
        logger.info(f'start parsing {file}')
        file_name = file
        file_name_regexp = r'\d{6,}.*.csv'
        file_name_to_move = re.findall(file_name_regexp, file_name)[0]
        new_file = open(f'/home/sasha/ready_files/{file_name_to_move}.ready', 'x')
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
                    # if radius_dict.get(str(row['ip_src'])):
                    #     row['msisdn'] = radius_dict.get(str(row['ip_src']))
                    #     writer.writerow(row)
                    # else:
                    #     print(row['ip_src'], '---->', radius_dict.get(str(row['ip_src'])))
                    #     continue
        logger.info(f'finish parsing {file}. New ready file is {new_file}')
        os.remove(file)
    except(FileExistsError, ValueError, FileNotFoundError) as err:
        logger.info(f'Problem was occurred with {file} parsing. See {err}')

    try:
        ready_files = find('/home/sasha/ready_files/*.ready')
        print(ready_files)
        file_to_upload_regexp = r'\d{5,}_.*.csv'
        file_to_upload_name = re.findall(file_to_upload_regexp, file_name)[0]

        file_to_upload = open(f'/home/sasha/files_to_upload/{file_to_upload_name}.ch', 'x')
        zero_logins = open(f'/home/sasha/files_zero_logins/{datetime.datetime.now()}.csv', 'x')
        for f in ready_files:
            with open(f, 'r') as parser:
                reader2 = csv.DictReader(parser, dialect='excel')
                writer2 = csv.DictWriter(file_to_upload, reader2.fieldnames, dialect='excel')
                writer2.writeheader()
                writer_zero_logins = csv.DictWriter(zero_logins, fieldnames=reader2.fieldnames)
                writer_zero_logins.writeheader()
                for i in reader2:
                    print(i)
                    if i['msisdn'] != '0':
                        writer2.writerow(i)
                    if i['msisdn'] == '0':
                        writer_zero_logins.writerow(i)

            parser.close()

            # new_file.close()
            # file_to_upload.close()

        logger.info(f'finish parsing {f}. New file for ch is {file_to_upload}')
        os.remove(f)
    except (FileExistsError, ValueError, FileNotFoundError) as err:
        logger.info(f'Problem was occurred with {f} parsing. See {err}')

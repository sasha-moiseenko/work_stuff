from tempfile import NamedTemporaryFile
import shutil
import csv
import ipaddress
import os
import glob
import logging
import re


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


logger = setup_log('fix_dpi_parser')


def find(way):
    dirs = glob.glob(way)
    return dirs


def dict_create():
    radius_files = glob.glob('/home/sasha/radlist_*.csv')
    if radius_files:
        filepath = max(radius_files)
    with open(filepath, mode='r') as infile:
        reader = csv.reader(infile)
        my_dict = {rows[1]: rows[0] for rows in reader}
        return my_dict


a = dict_create()

dpi_files = find('/home/sasha/1627*.csv')
for file in dpi_files:
    try:
        print(file)
        logger.info(f'start parsing {file}')
        file_name = file
        file_name_regexp = r'\d{6,}.*'
        file_name_to_move = re.findall(file_name_regexp, file_name)[0]
        print(file_name_to_move)
        new_file = open(f'/home/sasha/edr/{file_name_to_move}.ready', 'x')

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

        with open(file_name, 'r') as csv_file:
            reader = csv.DictReader(csv_file, fieldnames=fields)
            writer = csv.DictWriter(new_file, fieldnames=fields)
            for row in reader:
                # print(row)
                if 'ffff' in row['ip_src']:
                    row['ip_src'] = ipaddress.IPv6Address(row['ip_src']).ipv4_mapped
                    # print(row)
                if 'ffff' in row['ip_dst']:
                    row['ip_dst'] = ipaddress.IPv6Address(row['ip_dst']).ipv4_mapped
                writer.writerow(row)
                if row['msisdn'] == '0':
                    row['msisdn'] = a.get(row['ip_src'])
                writer.writerow(row)
            csv_file.close()
            new_file.close()
            os.remove(file)

        logger.info(f'finish parsing {file}. New file is {new_file}')
    except (FileExistsError, ValueError, FileNotFoundError) as err:
        logger.info(f'Problem was occurred with {file} parsing. See {err}')


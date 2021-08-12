# import glob
# import re
# import csv
# import ipaddress
#
#
# def find(way):
#     dirs = glob.glob(way)
#     # print(dirs)
#     return dirs
#
#
# def dict_create():
#     radius_files = glob.glob('/home/sasha/radlist_*.csv')
#     if radius_files:
#         filepath = max(radius_files)
#         with open(filepath, mode='r') as infile:
#             reader = csv.reader(infile)
#             radius_dict = {rows[1]: rows[0] for rows in reader}
#             # print(radius_dict)
#         return radius_dict
#
#
# radius_dict = dict_create()
#
# dpi_files = find('/home/sasha/16275*.csv')
# print(dpi_files)
# for file in dpi_files:
#     with open(file, 'r') as csv_file:
#         fields = ['start_time', 'end_time', 'transaction_time', 'packets_ul', 'packets_dl', 'bytes_ul', 'bytes_dl',
#                   'pdu_id', 'probe_id', 'site_id', 'location_id',
#                   'dr_status', 'dr_type', 'ip_src', 'ip_dst', 'port_src',
#                   'port_dst', 'protocol', 'opcode', 'teid_cp_ul', 'teid_cp_dl',
#                   'teid_up_ul', 'teid_up_dl', 'thr_put_ul', 'thr_put_dl', 'imsi',
#                   'msisdn', 'imei', 'user_ip_v4', 'user_ip_v6', 'netw_ip_v4',
#                   'netw_ip_v6', 'apn', 'mcc', 'mnc', 'lac', 'rac', 'cell',
#                   'rat_type', 'app_protocol', 'app_group', 'application',
#                   'app_attributes', 'ssl_server_name', 'ssl_session_id',
#                   'ssl_version', 'flow_id', 'tcp_rtt_ul', 'tcp_rtt_dl',
#                   'tcp_server_delay', 'tcp_invalid_chksum_ul', 'tcp_invalid_chksum_dl',
#                   'tcp_window_size_ul', 'tcp_window_size_dl', 'tcp_window_scale_ul',
#                   'tcp_window_scale_dl', 'tcp_network_limit', 'tcp_thr_put_max_ul',
#                   'tcp_thr_put_max_dl', 'tcp_mss', 'tcp_window_upd_pkt_ul',
#                   'tcp_window_upd_pkt_dl', 'tcp_zero_window_pkt_ul',
#                   'tcp_zero_window_pkt_dl', 'tcp_t_out_retrans_ul',
#                   'tcp_t_out_retrans_dl', 'tcp_fast_retrans_ul',
#                   'tcp_fast_retrans_dl', 'tcp_ramp_up_time_ul',
#                   'tcp_ramp_up_time_dl', 'tcp_ramp_up_drop_time_ul',
#                   'tcp_ramp_up_drop_time_dl', 'tcp_out_of_order_ul',
#                   'tcp_out_of_order_dl', 'unknown_col']
#         new_file = open(f'/home/sasha/nat/test.csv', 'x')
#         reader = csv.DictReader(csv_file, fieldnames=fields)
#         writer = csv.DictWriter(new_file, fieldnames=fields)
#         for row in reader:
#             if 'ffff' in row['ip_src']:
#                 row['ip_src'] = ipaddress.IPv6Address(row['ip_src']).ipv4_mapped
#             if 'ffff' in row['ip_dst']:
#                 row['ip_dst'] = ipaddress.IPv6Address(row['ip_dst']).ipv4_mapped
#             writer.writerow(row)
#             # print(row['ip_src'])
#             if row['msisdn'] == '0':
#                 print(row['ip_src'], '---->' , radius_dict.get(str(row['ip_src'])))
#                 # print(row['msisdn'])
#
#         new_file.close()
#         csv_file.close()
import datetime

print(datetime.datetime.now())
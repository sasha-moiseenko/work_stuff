import logging
import subprocess
import os
import asyncio
import paramiko

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


async def to_dst_insert(wh):
    wh_list = ['SRV-DSS-WH-1', 'SRV-DSS-WH-3', 'SRV-DSS-WH-5', 'SRV-DSS-WH-7', 'SRV-DSS-WH-9', 'SRV-DSS-WH-11',
               'SRV-DSS-WH-13', 'SRV-DSS-WH-15', 'SRV-DSS-WH-17', 'SRV-DSS-WH-19',
               'SRV-DSS-WH-21', 'SRV-DSS-WH-23', 'SRV-DSS-WH-25',
               'SRV-DSS-WH-29', 'SRV-DSS-WH-31', 'SRV-DSS-WH-33', 'SRV-DSS-WH-27']

    logger = setup_log('to_dst_tables_insert')
    for wh in wh_list:
        query_edr = "INSERT INTO deeplog.edr_by_dst(start_time, end_time, duration, msisdn, imei, imsi, mcc, mnc, lac, rac, tac, cell_id, ip_protocol, src_ipv4, src_ipv6, src_port, src_pub_ipv4, src_pub_port, dst_ipv4, dst_ipv6, dst_port, bytes_sent, bytes_rcvd, app_protocol, sn_direction, pp_protocol, http_host, http_domain, http_uri, http_referer, http_url, REC_DATE) SELECT start_time, end_time, duration, msisdn, imei, imsi, mcc, mnc, lac, rac, tac, cell_id, ip_protocol, assumeNotNull(src_ipv4), src_ipv6, src_port, assumeNotNull(src_pub_ipv4), src_pub_port, assumeNotNull(dst_ipv4), dst_ipv6, dst_port, bytes_sent, bytes_rcvd, app_protocol, sn_direction, pp_protocol, http_host, http_domain, http_uri, http_referer, http_url, REC_DATE FROM deeplog.edr  WHERE toDateTime(REC_DATE) BETWEEN \'2021-07-02 00:00:00\' AND \'2021-07-09 14:28:00\'"
        query_dpi = "INSERT INTO deeplog.clickstream_by_dst SELECT * FROM deeplog.clickstream_by_date_login WHERE toDateTime(REC_DATE) BETWEEN \'2021-07-02 00:00:00\' AND \'2021-07-07 10:30:00\'"
        logger.info('start inserting')
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(wh, username='svcdeeplog_')
        print('connected')
        logger.info(f'start inserting at {wh} \n')
        db_connection_data = f'clickhouse-client -h {wh} -u uploader'
        edr_insert = f'{db_connection_data} --query={query_edr}'
        dpi_insert = f'{db_connection_data} --query={query_dpi}'
        client.exec_command(edr_insert)
        logger.info(f'data insert was done to deeplog.edr_by_dst')
        client.exec_command(dpi_insert)
        logger.info(f'data insert was done to deeplog.clickstream_by_dst')
        client.close()



coroutines = []
for i in wh_list:
    coroutines.append(to_dst_insert(i))
while True:
    for coroutine in coroutines:
        coroutine.







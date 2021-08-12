import logging
import fnmatch
import subprocess
import os


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


def nat_inserting(pattern, way, dst_table='migration.gate_traffic_edr'):
    logger = setup_log('edr_insert')
    db_connection_data = f'clickhouse-client --password 1234'
    query = f'\"INSERT INTO {dst_table}(file_id, file_row_nr, start_time, duration, msisdn, imei, imsi, cell_id, ip_protocol, src_ip, src_port, dst_ip, dst_port, bytes_sent, bytes_rcvd, lac, src_xlated_ip, src_xlated_port, app_protocol, sn_direction, pp_protocol_id, http_host, http_domain, http_uri, http_referer, http_url) SELECT _col0,_col1,_col2,_col3, coalesce (_col4,0), coalesce (_col5,0) ,coalesce (_col6,0) ,_col7,_col8,_col9,_col10,_col11,_col12,_col13,_col14,_col15,_col16,_col17,_col18,_col19,_col20,_col21,_col22,_col23,_col24,_col25 FROM input(\'_col0 Decimal(38,0),_col1 Decimal(38,0),_col2 Datetime,_col3 Decimal(38,0),_col4 Nullable(Decimal(38,0)),_col5 Nullable(Decimal(38,0)),_col6 Nullable(Decimal(38,0)),_col7 Decimal(38,0),_col8 UInt8,_col9 IPv4,_col10 UInt16,_col11 IPv4,_col12 UInt16,_col13 Decimal(38,0),_col14 Decimal(38,0),_col15 UInt16,_col16 Nullable(IPv4),_col17 Nullable(UInt16),_col18 UInt16,_col19 UInt8,_col20 UInt16,_col21 Nullable(String),_col22 Nullable(String),_col23 Nullable(String),_col24 Nullable(String),_col25 Nullable(String)\') FORMAT ORC\"'
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files if fnmatch.fnmatch(f_name, pattern)]
    logger.info(f'uploading from {dirs}')
    for i in dirs:
        logger.info(f'{i} start unloading\n')
        with open(f'{i}') as infile:
            command = f'{db_connection_data} --query={query}'
            command_for_delete = f'rm {i}'
            logger.info(f'Command\'s in process: {command}')
            try:
                subprocess.check_output(command, stdin=infile, shell=True, stderr=subprocess.STDOUT)
                logger.info(f'ready to delete {i}')
                subprocess.check_output(command_for_delete, shell=True)
                logger.info(f'{i} was deleted')
            except subprocess.CalledProcessError as err:
                logger.error(f'Err code: {err.returncode}')
                logger.error(err.output)
                logger.error(err)


if __name__ == "__main__":
    nat_inserting('/home/sasha/edr')

# for launching use flags --pattern --way
# example:
# migration_error_handling.py --pattern='*.error' --way='/data/migration/sample/'

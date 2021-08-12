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


def edr_inserting(way, dst_table='migration.gate_nat'):
    logger = setup_log('nat_insert')
    db_connection_data = f'clickhouse-client -h SRV-DSS-WH-1 -u uploader'
    query = f'\"INSERT INTO {dst_table}(start_time, src_ip, src_port, src_xlated_ip, src_xlated_port, dst_ip, dst_port) SELECT _col2, coalesce(_col3, 0)+2147483648, coalesce(_col4,0)+32768, coalesce(_col5,0)+2147483648, coalesce(_col6,0)+32768, coalesce(_col7,0)+2147483648, coalesce(_col8,0)+32768 FROM input(\'_col0 Decimal(38,0), _col1 Decimal(38,0), _col2 Datetime, _col3 Nullable(Int32), _col4 Nullable(Int16), _col5 Nullable(Int32), _col6 Nullable(Int16), _col7 Nullable(Int32), _col8 Nullable(Int16)\') FORMAT ORC\"'
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files]
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
    edr_inserting('/opt/clickhouse/nat')

# for launching use flags --pattern --way
# example:
# migration_error_handling.py --pattern='*.error' --way='/data/migration/sample/'

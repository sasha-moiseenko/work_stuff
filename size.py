#!/usr/bin/env python3

import os
import fnmatch


def find(way):
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files ]
    print(dirs)


if __name__ == "__main__":
    find('/home/sasha/nat')
scp /data/migration/sample/nat_mob_202008/day_id=19/000031_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202008/day_id=19/000011_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202008/day_id=20/000070_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=03/000019_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=15/000027_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=15/000050_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=15/000010_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp data/migration/sample/nat_mob_202009/day_id=18/000030_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=19/000028_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=19/000052_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=20/000067_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202009/day_id=20/000048_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202101/day_id=02/000049_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
scp /data/migration/sample/nat_mob_202101/day_id=02/000013_0.error svcdeeplog_@SRV-DSS-WH-31:/opt/clickhouse/error_files/nat
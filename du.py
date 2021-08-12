import os
import fnmatch


def find(pattern, way):
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) if 'orc' in dirs for
            f_name in files if fnmatch.fnmatch(f_name, pattern)]
    print(dirs)
    for i in dirs:
        if 'orc' in i:
            print(i)


find('*error', '/home/')

"INSERT INTO ${DST_TABLE}(start_time, src_ip, src_port, src_xlated_ip, src_xlated_port, dst_ip, dst_port) SELECT _col2, coalesce(_col3, 0)+2147483648, coalesce(_col4,0)+32768, coalesce(_col5,0)+2147483648, coalesce(_col6,0)+32768, coalesce(_col7,0)+2147483648, coalesce(_col8,0)+32768 FROM input('_col0 Decimal(38,0), _col1 Decimal(38,0), _col2 Datetime, _col3 Nullable(Int32), _col4 Nullable(Int16), _col5 Nullable(Int32), _col6 Nullable(Int16), _col7 Nullable(Int32), _col8 Nullable(Int16)') FORMAT ORC"

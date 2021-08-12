import os
import gzip
import shutil


def join_dirs(way, archive_name):
    dirs = [os.path.join(root, f_name) for root, dirs, files in os.walk(way) for
            f_name in files]
    print(dirs)
    if dirs:
        try:
            for i in dirs:
                with open(i, 'r') as f_in, gzip.open(f'{archive_name}.gz', 'w') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            print(e)
        finally:
            print('bla-bla')


# def replace_file():
#     """Extract archive to temporary directory, replace file, replace archive """
#     # tempdir
#     with tempfile.TemporaryDirectory() as td:
#         # dirname to Path
#         tdp = pathlib.Path(td)
#
#         # extract archive to temporry directory
#         with tarfile.open(f'{archive_name}.tar.gz') as r:
#             r.extractall(td)
#
#         # print(list(tdp.iterdir()), file=sys.stderr)
#
#         # replace target in temporary directory
#         (tdp / replace).write_bytes(pathlib.Path(replacement).read_bytes())
#
#         # replace archive, from all files in tempdir
#         with tarfile.open(f'{archive_name}.tar.gz', "w:gz") as w:
#             for f in tdp.iterdir():
#                 w.add(f, arcname=f.name)
#     # done
#
#
# def test():
#     join_dirs('/home/sasha/orc_dumps', 'test')
#     # add_file_not()
#     replace_file()

join_dirs('/home/sasha/orc_dumps', 'test')



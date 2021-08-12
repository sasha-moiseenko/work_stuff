# ! /usr/bin/env python
import pandas as pd
import argparse
import os
import pyarrow.orc as orc

# usage: ./converter.py --help

CONVERTERS = ['parquet2json', 'orc2json', 'orc2parquet']


def parquet2json(filename):
    df = pd.read_parquet(f'{filename}.parquet')
    df.to_json(f'{filename}.json', orient='records')


def orc2json(filename):
    data = orc.ORCFile(f'{filename}.orc')
    df = data.read().to_pandas()
    df.to_json(f'{filename}.json', orient='records')


def orc2parquet(filename):
    data = orc.ORCFile(f'{filename}.orc')
    df = data.read().to_pandas()
    df.to_parquet(f'{filename}.parquet', compression='gzip')


def convert(args):
    converter = args.converter
    filename = os.path.splitext(args.sourcefile)[0]

    switcher = {
        "parquet2json": parquet2json,
        "orc2json": orc2json,
        "orc2parquet": orc2parquet
    }

    converter_func = switcher.get(converter)
    converter_func(filename)


def main():
    parser = argparse.ArgumentParser(description="Convert parquet to json | ocr to json | orc to parquet.")
    parser.add_argument("converter", choices=CONVERTERS)
    parser.add_argument("sourcefile")
    parser.set_defaults(func=convert)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

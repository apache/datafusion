import unittest
import tempfile
import datetime
import os.path
import shutil

import numpy
import pyarrow
import datafusion

# used to write parquet files
import pyarrow.parquet


def data():
    data = numpy.concatenate([
        numpy.random.normal(0, 0.01, size=50),
        numpy.random.normal(50, 0.01, size=50)
    ])
    return pyarrow.array(data)


def data_with_nans():
    data = numpy.random.normal(0, 0.01, size=50)
    mask = numpy.random.randint(0, 2, size=50)
    data[mask==0] = numpy.NaN
    return data


def data_datetime(f):
    data = [
        datetime.datetime.now(),
        datetime.datetime.now() - datetime.timedelta(days=1),
        datetime.datetime.now() + datetime.timedelta(days=1),
    ]
    return pyarrow.array(data, type=pyarrow.timestamp(f), mask=numpy.array([False, True, False]))


def data_timedelta(f):
    data = [
        datetime.timedelta(days=100),
        datetime.timedelta(days=1),
        datetime.timedelta(seconds=1),
    ]
    return pyarrow.array(data, type=pyarrow.duration(f), mask=numpy.array([False, True, False]))


def data_binary_other():
    return numpy.array([
        1, 0, 0
    ], dtype='u4')


def write_parquet(path, data):
    table = pyarrow.Table.from_arrays([data], names=['a'])
    pyarrow.parquet.write_table(table, path)
    return path

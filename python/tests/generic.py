# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import numpy
import pyarrow

# used to write parquet files
import pyarrow.parquet


def data():
    data = numpy.concatenate(
        [numpy.random.normal(0, 0.01, size=50), numpy.random.normal(50, 0.01, size=50)]
    )
    return pyarrow.array(data)


def data_with_nans():
    data = numpy.random.normal(0, 0.01, size=50)
    mask = numpy.random.randint(0, 2, size=50)
    data[mask == 0] = numpy.NaN
    return data


def data_datetime(f):
    data = [
        datetime.datetime.now(),
        datetime.datetime.now() - datetime.timedelta(days=1),
        datetime.datetime.now() + datetime.timedelta(days=1),
    ]
    return pyarrow.array(
        data, type=pyarrow.timestamp(f), mask=numpy.array([False, True, False])
    )


def data_timedelta(f):
    data = [
        datetime.timedelta(days=100),
        datetime.timedelta(days=1),
        datetime.timedelta(seconds=1),
    ]
    return pyarrow.array(
        data, type=pyarrow.duration(f), mask=numpy.array([False, True, False])
    )


def data_binary_other():
    return numpy.array([1, 0, 0], dtype="u4")


def write_parquet(path, data):
    table = pyarrow.Table.from_arrays([data], names=["a"])
    pyarrow.parquet.write_table(table, path)
    return path

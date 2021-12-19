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

import pyarrow as pa
import pytest

from datafusion import ExecutionContext, column
from datafusion import functions as f


@pytest.fixture
def df():
    ctx = ExecutionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, 6])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]])


def test_built_in_aggregation(df):
    col_a = column("a")
    col_b = column("b")
    df = df.aggregate(
        [],
        [f.max(col_a), f.min(col_a), f.count(col_a), f.approx_distinct(col_b)],
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([3])
    assert result.column(1) == pa.array([1])
    assert result.column(2) == pa.array([3], type=pa.uint64())
    assert result.column(3) == pa.array([2], type=pa.uint64())

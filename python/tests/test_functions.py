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
from datafusion import ExecutionContext
from datafusion import functions as f


@pytest.fixture
def df():
    ctx = ExecutionContext()
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["Hello", "World", "!"]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]])


def test_lit(df):
    """test lit function"""
    df = df.select(
        f.lit(1),
        f.lit("1"),
        f.lit("OK"),
        f.lit(3.14),
        f.lit(True),
        f.lit(b"hello world"),
    )
    result = df.collect()
    assert len(result) == 1
    result = result[0]
    assert result.column(0) == pa.array([1] * 3)
    assert result.column(1) == pa.array(["1"] * 3)
    assert result.column(2) == pa.array(["OK"] * 3)
    assert result.column(3) == pa.array([3.14] * 3)
    assert result.column(4) == pa.array([True] * 3)
    assert result.column(5) == pa.array([b"hello world"] * 3)


def test_lit_arith(df):
    """test lit function within arithmatics"""
    df = df.select(f.lit(1) + f.col("b"), f.concat(f.col("a"), f.lit("!")))
    result = df.collect()
    assert len(result) == 1
    result = result[0]
    assert result.column(0) == pa.array([5, 6, 7])
    assert result.column(1) == pa.array(["Hello!", "World!", "!!"])

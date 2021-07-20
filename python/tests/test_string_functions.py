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


def test_string_functions(df):
    df = df.select(f.md5(f.col("a")), f.lower(f.col("a")))
    result = df.collect()
    assert len(result) == 1
    result = result[0]
    assert result.column(0) == pa.array(
        [
            "8b1a9953c4611296a827abf8c47804d7",
            "f5a7924e621e84c9280a9a27e1bcb7f6",
            "9033e0e305f247c0c3c80d0c7848c8b3",
        ]
    )
    assert result.column(1) == pa.array(["hello", "world", "!"])

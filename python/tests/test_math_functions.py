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
import numpy as np


@pytest.fixture
def df():
    ctx = ExecutionContext()
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays([pa.array([0.1, -0.7, 0.55])], names=["value"])
    return ctx.create_dataframe([[batch]])


def test_math_functions(df):
    values = np.array([0.1, -0.7, 0.55])
    col_v = f.col("value")
    df = df.select(
        f.abs(col_v),
        f.sin(col_v),
        f.cos(col_v),
        f.tan(col_v),
        f.asin(col_v),
        f.acos(col_v),
        f.exp(col_v),
        f.ln(col_v + f.lit(1)),
        f.log2(col_v + f.lit(1)),
        f.log10(col_v + f.lit(1)),
    )
    result = df.collect()
    assert len(result) == 1
    result = result[0]
    assert result.column(0) == pa.array(np.abs(values))
    assert result.column(1) == pa.array(np.sin(values))
    assert result.column(2) == pa.array(np.cos(values))
    assert result.column(3) == pa.array(np.tan(values))
    assert result.column(4) == pa.array(np.arcsin(values))
    assert result.column(5) == pa.array(np.arccos(values))
    assert result.column(6) == pa.array(np.exp(values))
    assert result.column(7) == pa.array(np.log(values + 1.0))
    assert result.column(8) == pa.array(np.log2(values + 1.0))
    assert result.column(9) == pa.array(np.log10(values + 1.0))

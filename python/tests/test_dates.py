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

from datetime import datetime

import pyarrow as pa
import numpy as np
import pandas as pd
import pytest
from datafusion import ExecutionContext


@pytest.fixture
def ctx():
    return ExecutionContext()


@pytest.mark.parametrize(
    ("input_values", "input_type", "output_type"),
    [
        (
            [datetime(1970, 1, 1), datetime(1970, 1, 2), datetime(1970, 1, 3)],
            pa.date32(),
            pa.date32(),
        ),
        (
            [datetime(1970, 1, 1), datetime(1970, 1, 2), datetime(1970, 1, 3)],
            pa.date64(),
            pa.date64(),
        ),
        (
            [datetime(1970, 1, 1), datetime(1970, 1, 2), datetime(1970, 1, 3)],
            pa.timestamp("ms"),
            pa.timestamp("ms"),
        ),
        (
            [datetime(1970, 1, 1), datetime(1970, 1, 2), datetime(1970, 1, 3)],
            pa.timestamp("s"),
            pa.timestamp("s"),
        ),
        (
            [datetime(1970, 1, 1), datetime(1970, 1, 2), datetime(1970, 1, 3)],
            pa.timestamp("us"),
            pa.timestamp("us"),
        ),
        (
            [datetime(1970, 1, 1), datetime(1970, 1, 2), datetime(1970, 1, 3)],
            pa.timestamp("ns"),
            pa.timestamp("ns"),
        ),
        (
            [0, 1, 2],
            pa.time32("s"),
            pa.time32("s"),
        ),
        (
            [0, 1, 2],
            pa.time64("us"),
            pa.time64("us"),
        ),
    ],
)
def test_datetypes(ctx, input_values, input_type, output_type):
    batch = pa.RecordBatch.from_arrays(
        [pa.array(input_values, type=input_type)], names=["a"]
    )

    df = ctx.create_dataframe([[batch]])
    result = df.collect()[0]
    assert result.column(0).type == output_type

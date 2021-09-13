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

import numpy as np
import pyarrow as pa
import pytest

from datafusion import ExecutionContext
from . import generic as helpers


@pytest.fixture
def ctx():
    return ExecutionContext()


def test_no_table(ctx):
    with pytest.raises(Exception, match="DataFusion error"):
        ctx.sql("SELECT a FROM b").collect()


def test_register_csv(ctx, tmp_path):
    path = tmp_path / "test.csv"

    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [1.1, 2.2, 3.3, 4.4],
        ],
        names=["int", "str", "float"],
    )
    pa.csv.write_csv(table, path)

    ctx.register_csv("csv", path)
    ctx.register_csv("csv1", str(path))
    ctx.register_csv(
        "csv2",
        path,
        has_header=True,
        delimiter=",",
        schema_infer_max_records=10,
    )
    alternative_schema = pa.schema(
        [
            ("some_int", pa.int16()),
            ("some_bytes", pa.string()),
            ("some_floats", pa.float32()),
        ]
    )
    ctx.register_csv("csv3", path, schema=alternative_schema)

    assert ctx.tables() == {"csv", "csv1", "csv2", "csv3"}

    for table in ["csv", "csv1", "csv2"]:
        result = ctx.sql(f"SELECT COUNT(int) FROM {table}").collect()
        result = pa.Table.from_batches(result)
        assert result.to_pydict() == {f"COUNT({table}.int)": [4]}

    result = ctx.sql("SELECT * FROM csv3").collect()
    result = pa.Table.from_batches(result)
    assert result.schema == alternative_schema

    with pytest.raises(
        ValueError, match="Delimiter must be a single character"
    ):
        ctx.register_csv("csv4", path, delimiter="wrong")


def test_register_parquet(ctx, tmp_path):
    path = helpers.write_parquet(tmp_path / "a.parquet", helpers.data())
    ctx.register_parquet("t", path)
    assert ctx.tables() == {"t"}

    result = ctx.sql("SELECT COUNT(a) FROM t").collect()
    result = pa.Table.from_batches(result)
    assert result.to_pydict() == {"COUNT(t.a)": [100]}


def test_execute(ctx, tmp_path):
    data = [1, 1, 2, 2, 3, 11, 12]

    # single column, "a"
    path = helpers.write_parquet(tmp_path / "a.parquet", pa.array(data))
    ctx.register_parquet("t", path)

    assert ctx.tables() == {"t"}

    # count
    result = ctx.sql("SELECT COUNT(a) FROM t").collect()

    expected = pa.array([7], pa.uint64())
    expected = [pa.RecordBatch.from_arrays([expected], ["COUNT(a)"])]
    assert result == expected

    # where
    expected = pa.array([2], pa.uint64())
    expected = [pa.RecordBatch.from_arrays([expected], ["COUNT(a)"])]
    result = ctx.sql("SELECT COUNT(a) FROM t WHERE a > 10").collect()
    assert result == expected

    # group by
    results = ctx.sql(
        "SELECT CAST(a as int), COUNT(a) FROM t GROUP BY CAST(a as int)"
    ).collect()

    # group by returns batches
    result_keys = []
    result_values = []
    for result in results:
        pydict = result.to_pydict()
        result_keys.extend(pydict["CAST(t.a AS Int32)"])
        result_values.extend(pydict["COUNT(t.a)"])

    result_keys, result_values = (
        list(t) for t in zip(*sorted(zip(result_keys, result_values)))
    )

    assert result_keys == [1, 2, 3, 11, 12]
    assert result_values == [2, 2, 1, 1, 1]

    # order by
    result = ctx.sql(
        "SELECT a, CAST(a AS int) FROM t ORDER BY a DESC LIMIT 2"
    ).collect()
    expected_a = pa.array([50.0219, 50.0152], pa.float64())
    expected_cast = pa.array([50, 50], pa.int32())
    expected = [
        pa.RecordBatch.from_arrays(
            [expected_a, expected_cast], ["a", "CAST(t.a AS Int32)"]
        )
    ]
    np.testing.assert_equal(expected[0].column(1), expected[0].column(1))


def test_cast(ctx, tmp_path):
    """
    Verify that we can cast
    """
    path = helpers.write_parquet(tmp_path / "a.parquet", helpers.data())
    ctx.register_parquet("t", path)

    valid_types = [
        "smallint",
        "int",
        "bigint",
        "float(32)",
        "float(64)",
        "float",
    ]

    select = ", ".join(
        [f"CAST(9 AS {t}) AS A{i}" for i, t in enumerate(valid_types)]
    )

    # can execute, which implies that we can cast
    ctx.sql(f"SELECT {select} FROM t").collect()


@pytest.mark.parametrize(
    ("fn", "input_types", "output_type", "input_values", "expected_values"),
    [
        (
            lambda x: x,
            [pa.float64()],
            pa.float64(),
            [-1.2, None, 1.2],
            [-1.2, None, 1.2],
        ),
        (
            lambda x: x.is_null(),
            [pa.float64()],
            pa.bool_(),
            [-1.2, None, 1.2],
            [False, True, False],
        ),
    ],
)
def test_udf(
    ctx, tmp_path, fn, input_types, output_type, input_values, expected_values
):
    # write to disk
    path = helpers.write_parquet(
        tmp_path / "a.parquet", pa.array(input_values)
    )
    ctx.register_parquet("t", path)
    ctx.register_udf("udf", fn, input_types, output_type)

    batches = ctx.sql("SELECT udf(a) AS tt FROM t").collect()
    result = batches[0].column(0)

    assert result == pa.array(expected_values)


_null_mask = np.array([False, True, False])


@pytest.mark.parametrize(
    "arr",
    [
        pa.array(["a", "b", "c"], pa.utf8(), _null_mask),
        pa.array(["a", "b", "c"], pa.large_utf8(), _null_mask),
        pa.array([b"1", b"2", b"3"], pa.binary(), _null_mask),
        pa.array([b"1111", b"2222", b"3333"], pa.large_binary(), _null_mask),
        pa.array([False, True, True], None, _null_mask),
        pa.array([0, 1, 2], None),
        helpers.data_binary_other(),
        helpers.data_date32(),
        helpers.data_with_nans(),
        # C data interface missing
        pytest.param(
            pa.array([b"1111", b"2222", b"3333"], pa.binary(4), _null_mask),
            marks=pytest.mark.xfail,
        ),
        pytest.param(helpers.data_datetime("s"), marks=pytest.mark.xfail),
        pytest.param(helpers.data_datetime("ms"), marks=pytest.mark.xfail),
        pytest.param(helpers.data_datetime("us"), marks=pytest.mark.xfail),
        pytest.param(helpers.data_datetime("ns"), marks=pytest.mark.xfail),
        # Not writtable to parquet
        pytest.param(helpers.data_timedelta("s"), marks=pytest.mark.xfail),
        pytest.param(helpers.data_timedelta("ms"), marks=pytest.mark.xfail),
        pytest.param(helpers.data_timedelta("us"), marks=pytest.mark.xfail),
        pytest.param(helpers.data_timedelta("ns"), marks=pytest.mark.xfail),
    ],
)
def test_simple_select(ctx, tmp_path, arr):
    path = helpers.write_parquet(tmp_path / "a.parquet", arr)
    ctx.register_parquet("t", path)

    batches = ctx.sql("SELECT a AS tt FROM t").collect()
    result = batches[0].column(0)

    np.testing.assert_equal(result, arr)

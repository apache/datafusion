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

from typing import List

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from datafusion import Accumulator, ExecutionContext, column, udaf


class Summarize(Accumulator):
    """
    Interface of a user-defined accumulation.
    """

    def __init__(self):
        self._sum = pa.scalar(0.0)

    def state(self) -> List[pa.Scalar]:
        return [self._sum]

    def update(self, values: pa.Array) -> None:
        # Not nice since pyarrow scalars can't be summed yet.
        # This breaks on `None`
        self._sum = pa.scalar(self._sum.as_py() + pc.sum(values).as_py())

    def merge(self, states: pa.Array) -> None:
        # Not nice since pyarrow scalars can't be summed yet.
        # This breaks on `None`
        self._sum = pa.scalar(self._sum.as_py() + pc.sum(states).as_py())

    def evaluate(self) -> pa.Scalar:
        return self._sum


class NotSubclassOfAccumulator:
    pass


class MissingMethods(Accumulator):
    def __init__(self):
        self._sum = pa.scalar(0)

    def state(self) -> List[pa.Scalar]:
        return [self._sum]


@pytest.fixture
def df():
    ctx = ExecutionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, 6])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]])


def test_errors(df):
    with pytest.raises(TypeError):
        udaf(
            NotSubclassOfAccumulator,
            pa.float64(),
            pa.float64(),
            [pa.float64()],
            volatility="immutable",
        )

    accum = udaf(
        MissingMethods,
        pa.int64(),
        pa.int64(),
        [pa.int64()],
        volatility="immutable",
    )
    df = df.aggregate([], [accum(column("a"))])

    msg = (
        "Can't instantiate abstract class MissingMethods with abstract "
        "methods evaluate, merge, update"
    )
    with pytest.raises(Exception, match=msg):
        df.collect()


def test_aggregate(df):
    summarize = udaf(
        Summarize,
        pa.float64(),
        pa.float64(),
        [pa.float64()],
        volatility="immutable",
    )

    df = df.aggregate([], [summarize(column("a"))])

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([1.0 + 2.0 + 3.0])


def test_group_by(df):
    summarize = udaf(
        Summarize,
        pa.float64(),
        pa.float64(),
        [pa.float64()],
        volatility="immutable",
    )

    df = df.aggregate([column("b")], [summarize(column("a"))])

    batches = df.collect()

    arrays = [batch.column(1) for batch in batches]
    joined = pa.concat_arrays(arrays)
    assert joined == pa.array([1.0 + 2.0, 3.0])

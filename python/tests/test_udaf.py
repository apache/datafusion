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

import unittest
import pyarrow
import pyarrow.compute
import datafusion

f = datafusion.functions


class Accumulator:
    """
    Interface of a user-defined accumulation.
    """

    def __init__(self):
        self._sum = pyarrow.scalar(0.0)

    def to_scalars(self) -> [pyarrow.Scalar]:
        return [self._sum]

    def update(self, values: pyarrow.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pyarrow.scalar(
            self._sum.as_py() + pyarrow.compute.sum(values).as_py()
        )

    def merge(self, states: pyarrow.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pyarrow.scalar(
            self._sum.as_py() + pyarrow.compute.sum(states).as_py()
        )

    def evaluate(self) -> pyarrow.Scalar:
        return self._sum


class TestCase(unittest.TestCase):
    def _prepare(self):
        ctx = datafusion.ExecutionContext()

        # create a RecordBatch and a new DataFrame from it
        batch = pyarrow.RecordBatch.from_arrays(
            [pyarrow.array([1, 2, 3]), pyarrow.array([4, 4, 6])],
            names=["a", "b"],
        )
        return ctx.create_dataframe([[batch]])

    def test_aggregate(self):
        df = self._prepare()

        udaf = f.udaf(
            Accumulator, pyarrow.float64(), pyarrow.float64(), [pyarrow.float64()]
        )

        df = df.aggregate([], [udaf(f.col("a"))])

        # execute and collect the first (and only) batch
        result = df.collect()[0]

        self.assertEqual(result.column(0), pyarrow.array([1.0 + 2.0 + 3.0]))

    def test_group_by(self):
        df = self._prepare()

        udaf = f.udaf(
            Accumulator, pyarrow.float64(), pyarrow.float64(), [pyarrow.float64()]
        )

        df = df.aggregate([f.col("b")], [udaf(f.col("a"))])

        # execute and collect the first (and only) batch
        batches = df.collect()
        arrays = [batch.column(1) for batch in batches]
        joined = pyarrow.concat_arrays(arrays)
        self.assertEqual(joined, pyarrow.array([1.0 + 2.0, 3.0]))

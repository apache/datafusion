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

import pyarrow as pa
import datafusion
f = datafusion.functions


class TestCase(unittest.TestCase):

    def _prepare(self):
        ctx = datafusion.ExecutionContext()

        # create a RecordBatch and a new DataFrame from it
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
            names=["a", "b"],
        )
        return ctx.create_dataframe([[batch]])

    def test_select(self):
        df = self._prepare()

        df = df.select(
            f.col("a") + f.col("b"),
            f.col("a") - f.col("b"),
        )

        # execute and collect the first (and only) batch
        result = df.collect()[0]

        self.assertEqual(result.column(0), pa.array([5, 7, 9]))
        self.assertEqual(result.column(1), pa.array([-3, -3, -3]))

    def test_filter(self):
        df = self._prepare()

        df = df \
            .select(
                f.col("a") + f.col("b"),
                f.col("a") - f.col("b"),
            ) \
            .filter(f.col("a") > f.lit(2))

        # execute and collect the first (and only) batch
        result = df.collect()[0]

        self.assertEqual(result.column(0), pa.array([9]))
        self.assertEqual(result.column(1), pa.array([-3]))

    def test_sort(self):
        df = self._prepare()
        df = df.sort([
            f.col("b").sort(ascending=False)
        ])

        table = pa.Table.from_batches(df.collect())
        expected = {'a': [3, 2, 1], 'b': [6, 5, 4]}
        self.assertEqual(table.to_pydict(), expected)

    def test_limit(self):
        df = self._prepare()

        df = df.limit(1)

        # execute and collect the first (and only) batch
        result = df.collect()[0]

        self.assertEqual(len(result.column(0)), 1)
        self.assertEqual(len(result.column(1)), 1)

    def test_udf(self):
        df = self._prepare()

        # is_null is a pa function over arrays
        udf = f.udf(lambda x: x.is_null(), [pa.int64()], pa.bool_())

        df = df.select(udf(f.col("a")))

        self.assertEqual(df.collect()[0].column(0), pa.array([False, False, False]))

    def test_join(self):
        ctx = datafusion.ExecutionContext()

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
            names=["a", "b"],
        )
        df = ctx.create_dataframe([[batch]])

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2]), pa.array([8, 10])],
            names=["a", "c"],
        )
        df1 = ctx.create_dataframe([[batch]])

        df = df.join(df1, on="a", how="inner")
        df = df.sort([
            f.col("a").sort(ascending=True)
        ])
        table = pa.Table.from_batches(df.collect())

        expected = {'a': [1, 2], 'c': [8, 10], 'b': [4, 5]}
        self.assertEqual(table.to_pydict(), expected)

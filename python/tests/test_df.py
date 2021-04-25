import unittest

import pyarrow
import datafusion
f = datafusion.functions


class TestCase(unittest.TestCase):

    def _prepare(self):
        ctx = datafusion.ExecutionContext()

        # create a RecordBatch and a new DataFrame from it
        batch = pyarrow.RecordBatch.from_arrays(
            [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
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

        self.assertEqual(result.column(0), pyarrow.array([5, 7, 9]))
        self.assertEqual(result.column(1), pyarrow.array([-3, -3, -3]))

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

        self.assertEqual(result.column(0), pyarrow.array([9]))
        self.assertEqual(result.column(1), pyarrow.array([-3]))

    def test_limit(self):
        df = self._prepare()

        df = df.limit(1)

        # execute and collect the first (and only) batch
        result = df.collect()[0]

        self.assertEqual(len(result.column(0)), 1)
        self.assertEqual(len(result.column(1)), 1)

    def test_udf(self):
        df = self._prepare()

        # is_null is a pyarrow function over arrays
        udf = f.udf(lambda x: x.is_null(), [pyarrow.int64()], pyarrow.bool_())

        df = df.select(udf(f.col("a")))

        self.assertEqual(df.collect()[0].column(0), pyarrow.array([False, False, False]))

    def test_join(self):
        ctx = datafusion.ExecutionContext()

        batch = pyarrow.RecordBatch.from_arrays(
            [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
            names=["a", "b"],
        )
        df = ctx.create_dataframe([[batch]])

        batch = pyarrow.RecordBatch.from_arrays(
            [pyarrow.array([1, 2]), pyarrow.array([8, 10])],
            names=["a", "c"],
        )
        df1 = ctx.create_dataframe([[batch]])

        df = df.join(df1, on="a", how="inner")

        # execute and collect the first (and only) batch
        batch = df.collect()[0]

        if batch.column(0) == pyarrow.array([1, 2]):
            self.assertEqual(batch.column(0), pyarrow.array([1, 2]))
            self.assertEqual(batch.column(1), pyarrow.array([8, 10]))
            self.assertEqual(batch.column(2), pyarrow.array([4, 5]))
        else:
            self.assertEqual(batch.column(0), pyarrow.array([2, 1]))
            self.assertEqual(batch.column(1), pyarrow.array([10, 8]))
            self.assertEqual(batch.column(2), pyarrow.array([5, 4]))

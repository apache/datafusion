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


import os
import shutil
import tempfile
import unittest
from decimal import Decimal

import numpy
import psycopg2
from tests.generic import *

import datafusion


class PostgresComparisonTestCase(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.pg_conn = psycopg2.connect(
            user="postgres",
            password=os.environ.get("POSTGRES_PASSWORD"),
            database=os.environ.get("POSTGRES_DB"),
            host=os.environ.get("POSTGRES_HOST"),
            port=os.environ.get("POSTGRES_PORT"),
        )
        numpy.random.seed(1)

    def tearDown(self):
        self.pg_conn.close()
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def _query_and_compare(self, sql: str, delta=None):
        ctx = datafusion.ExecutionContext()
        batches = ctx.sql(sql).collect()

        # shall only return a single batch
        (batch,) = batches

        with self.pg_conn.cursor() as cur:
            cur.execute(sql)
            fetched = cur.fetchall()

        self.assertEqual(batch.num_rows, len(fetched), msg="result size should match")

        # RecordBatch is column oriented, in order to compare, we'll need to transpose it
        batch = zip(*[i.tolist() for i in batch])

        for got, expected in zip(batch, fetched):
            if delta is None:
                self.assertEqual(expected, got)
            else:
                for i, j in zip(got, expected):
                    self.assertAlmostEqual(Decimal(i), Decimal(j), delta)

    def test_simple_select_1(self):
        sql = "SELECT 1"
        self._query_and_compare(sql)

    def test_simple_select_scalars(self):
        sql = "SELECT 1, 2, 3"
        self._query_and_compare(sql)

    def test_simple_select_math_expressions(self):
        sql = """SELECT
        1 + 2,
        1 - 2,
        1 * 2,
        1 / 2,
        +1,
        -1
        """
        self._query_and_compare(sql)

    def test_simple_select_math_functions(self):
        sql = """SELECT
        sqrt(3.0),
        sin(1.0),
        cos(1.0),
        tan(2.0),
        exp(2.0),
        abs(-1.0)
        """
        self._query_and_compare(sql, delta=1e-6)

    def test_simple_select_string_functions(self):
        sql = """SELECT
        ascii('x'),
        btrim('xyxtrimyyx', 'xyz'),
        chr(65),
        concat('abcde', 2, NULL, 22),
        concat_ws(',', 'abcde', 2, NULL, 22),
        initcap('hi THOMAS'),
        lower('TOM'),
        ltrim('zzzytest', 'xyz'),
        repeat('Pg', 4),
        replace('abcdefabcdef', 'cd', 'XX'),
        rtrim('testxxzx', 'xyz'),
        split_part('abc~@~def~@~ghi', '~@~', 2),
        starts_with('alphabet', 'alph')
        """
        self._query_and_compare(sql)

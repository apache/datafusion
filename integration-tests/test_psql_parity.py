# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
import numpy as np
import io
import os
import subprocess
from pathlib import Path
import unittest

pg_db, pg_user, pg_host, pg_port = [
    os.environ.get(i)
    for i in (
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    )
]


def generate_csv_from_datafusion(fname: str):
    return subprocess.check_output(
        [
            "./target/debug/datafusion-cli",
            "-f",
            fname,
            "--format",
            "csv",
            "-q",
        ],
    )


def generate_csv_from_psql(fname: str):
    return subprocess.check_output(
        [
            "psql",
            "-d",
            pg_db,
            "-h",
            pg_host,
            "-p",
            pg_port,
            "-U",
            pg_user,
            "-X",
            "--csv",
            "-f",
            fname,
        ]
    )


class PsqlParityTest(unittest.TestCase):
    def test_parity(self):
        root = Path(os.path.dirname(__file__)) / "sqls"
        files = set(root.glob("*.sql"))
        self.assertEqual(len(files), 2, msg="tests are missed")
        for fname in files:
            with self.subTest(fname=fname):
                datafusion_output = pd.read_csv(
                    io.BytesIO(generate_csv_from_datafusion(fname))
                )
                psql_output = pd.read_csv(io.BytesIO(generate_csv_from_psql(fname)))
                self.assertTrue(
                    np.allclose(datafusion_output, psql_output),
                    msg=f"data fusion output={datafusion_output}, psql_output={psql_output}",
                )


if __name__ == "__main__":
    unittest.main()

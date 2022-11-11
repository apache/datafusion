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

import io
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pg_db, pg_user, pg_host, pg_port = [
    os.environ.get(i)
    for i in (
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    )
]

CREATE_TABLE_SQL_FILE = "integration-tests/create_test_table.sql"


def generate_csv_from_datafusion(fname: str):
    return subprocess.check_output(
        [
            "./datafusion-cli/target/debug/datafusion-cli",
            "-f",
            CREATE_TABLE_SQL_FILE,
            "-f",
            fname,
            "--format",
            "csv",
            "-q",
        ],
    )


def generate_csv_from_psql(fname: str):

    cmd = ["psql"]

    if pg_db is not None:
        cmd.extend(["-d", pg_db])

    if pg_user is not None:
        cmd.extend(["-U", pg_user])

    if pg_host is not None:
        cmd.extend(["-h", pg_host])

    if pg_port is not None:
        cmd.extend(["-p", pg_port])

    cmd.extend([
        "-X",
        "--csv",
        "-f",
        fname,
    ])

    return subprocess.check_output(cmd)

root = Path(os.path.dirname(__file__)) / "sqls"
test_files = set(root.glob("*.sql"))


class TestPsqlParity:
    def test_tests_count(self):
        assert len(test_files) == 26, "tests are missed"

    @pytest.mark.parametrize("fname", test_files, ids=str)
    def test_sql_file(self, fname):
        datafusion_output = pd.read_csv(io.BytesIO(generate_csv_from_datafusion(fname)))
        psql_output = pd.read_csv(io.BytesIO(generate_csv_from_psql(fname)))
        np.testing.assert_allclose(datafusion_output, psql_output, equal_nan=True, verbose=True)

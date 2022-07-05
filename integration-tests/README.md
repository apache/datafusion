<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Integration Tests

These test run SQL queries against both DataFusion and Postgres and compare the results for parity.

## Setup

Set the following environment variables as appropriate for your environment. They are all optional.

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_HOST`
- `POSTGRES_PORT`

Create a Postgres database and then create the test table by running this script:

```bash
psql -d "$POSTGRES_DB" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
  -f create_test_table_postgres.sql
```

Populate the table by running this command:

```bash
psql -d "$POSTGRES_DB" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
  -c "\copy test FROM '$(pwd)/testing/data/csv/aggregate_test_100.csv' WITH (FORMAT csv, HEADER true);"
```

## Run Tests

Run `pytest` from the root of the repository.

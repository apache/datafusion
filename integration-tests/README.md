# DataFusion Integration Tests

These test run SQL queries against both DataFusion and Postgres and compare the results for parity.

## Setup

Optionally, set the following environment variables as appropriate for your environment. They are all optional.

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

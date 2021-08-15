# DataFusion Command-line Interface

The DataFusion CLI allows SQL queries to be executed by an in-process DataFusion context, or by a distributed
Ballista context.

```
USAGE:
    datafusion-cli [FLAGS] [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -q, --quiet      Reduce printing other than the results and work quietly
    -V, --version    Prints version information

OPTIONS:
    -c, --batch-size <batch-size>    The batch size of each query, or use DataFusion default
    -p, --data-path <data-path>      Path to your data, default to current directory
    -f, --file <file>...             Execute commands from file(s), then exit
        --format <format>            Output format [default: table]  [possible values: csv, tsv, table, json, ndjson]
        --host <host>                Ballista scheduler host
        --port <port>                Ballista scheduler port
```

## Example

Create a CSV file to query.

```bash
$ echo "1,2" > data.csv
```

```sql
$ datafusion-cli

DataFusion CLI v4.0.0-SNAPSHOT

> CREATE EXTERNAL TABLE foo (a INT, b INT) STORED AS CSV LOCATION 'data.csv';
0 rows in set. Query took 0.001 seconds.

> SELECT * FROM foo;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.017 seconds.
```

## Ballista

The DataFusion CLI can connect to a Ballista scheduler for query execution.

```bash
datafusion-cli --host localhost --port 50050
```
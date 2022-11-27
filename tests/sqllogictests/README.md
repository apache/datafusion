#### Overview

This is the Datafusion implementation of [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki). We use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) as a parser/runner of `.slt` files in `test_files`.

#### Running tests

`cargo run -p datafusion-sqllogictests`

#### Setup


#### sqllogictests

> :warning: **Warning**:Datafusion's sqllogictest implementation and migration is still in progress. Definitions taken from https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

sqllogictest is a program originally written for SQLite to verify the correctness of SQL queries against the SQLLite engine. The program is engine-agnostic and can parse sqllogictest files (`.slt`), runs queries against an SQL engine and compare the output to the expected output.

Tests in the `.slt` file are a sequence of query record generally starting with `CREATE` statements to populate tables and then further queries to test the populated data (arrow-datafusion exception).

Query records follow the format:
```sql
# <test_name>
query <type_string> <sort_mode> <label>
<sql_query>
----
<expected_result>
```

- `test_name`: Uniquely identify the test name (arrow-datafusion only)
- `type_string`: A short string that specifies the number of result columns and the expected datatype of each result column. There is one character in the <type_string> for each result column. The characters codes are "T" for a text result, "I" for an integer result, and "R" for a floating-point result.
- (Optional) `label`: sqllogictest stores a hash of the results of this query under the given label. If the label is reused, then sqllogictest verifies that the results are the same. This can be used to verify that two or more queries in the same test script that are logically equivalent always generate the same output. 
- `expected_result`: In the results section, integer values are rendered as if by printf("%d"). Floating point values are rendered as if by printf("%.3f"). NULL values are rendered as "NULL". Empty strings are rendered as "(empty)". Within non-empty strings, all control characters and unprintable characters are rendered as "@". 

##### Example

```sql
# group_by_distinct
query TTI
SELECT a, b, COUNT(DISTINCT c) FROM my_table GROUP BY a, b ORDER BY a, b
----
foo bar 10
foo baz 5
foo     4
        3
```

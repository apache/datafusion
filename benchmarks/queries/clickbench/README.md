# ClickBench queries

This directory contains queries for the ClickBench benchmark https://benchmark.clickhouse.com/

ClickBench is focused on aggregation and filtering performance (though it has no Joins)

## Files:
* `queries.sql` - Actual ClickBench queries, downloaded from the [ClickBench repository]
* `extended.sql` - "Extended" DataFusion specific queries. 

[ClickBench repository]: https://github.com/ClickHouse/ClickBench/blob/main/datafusion/queries.sql

## "Extended" Queries 
The "extended" queries are not part of the official ClickBench benchmark. 
Instead they are used to test other DataFusion features that are not 
covered by the standard benchmark

Each description below is for the corresponding line in `extended.sql` (line 1
is `Q0`, line 2 is `Q1`, etc.)  

### Q0
Models initial Data exploration, to understand some statistics of data. 
Import Query Properties: multiple `COUNT DISTINCT` on strings

```sql
SELECT 
    COUNT(DISTINCT "SearchPhrase"), COUNT(DISTINCT "MobilePhone"), COUNT(DISTINCT "MobilePhoneModel") 
FROM hits;
```





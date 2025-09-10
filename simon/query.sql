set datafusion.sql_parser.dialect = 'BigQuery';

CREATE EXTERNAL TABLE test
STORED AS CSV
LOCATION '/Users/svs/code/datafusion/simon/data.csv'
OPTIONS ('has_header' 'true');

select * from test
|> where afdeling = 'a'
|> extend budget - new_budget as diff

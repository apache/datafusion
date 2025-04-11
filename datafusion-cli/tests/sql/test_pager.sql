CREATE EXTERNAL TABLE aggregate_test_100 (
  c1  VARCHAR NOT NULL,
  c2  TINYINT NOT NULL,
  c3  SMALLINT NOT NULL,
  c4  SMALLINT,
  c5  INT,
  c6  BIGINT NOT NULL,
  c7  SMALLINT NOT NULL,
  c8  INT NOT NULL,
  c9  BIGINT UNSIGNED NOT NULL,
  c10 VARCHAR NOT NULL,
  c11 FLOAT NOT NULL,
  c12 DOUBLE NOT NULL,
  c13 VARCHAR NOT NULL
)
STORED AS CSV
LOCATION '../testing/data/csv/aggregate_test_100.csv'
OPTIONS ('format.has_header' 'true');

-- Generate a long output to test the pager feature
select c13 from aggregate_test_100 ORDER BY c13;

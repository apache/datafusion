CREATE TABLE aggregate_test_100_by_sql
(
    c1  character varying NOT NULL,
    c2  integer           NOT NULL,
    c3  smallint          NOT NULL,
    c4  smallint,
    c5  integer,
    c6  bigint            NOT NULL,
    c7  smallint          NOT NULL,
    c8  integer           NOT NULL,
    c9  bigint            NOT NULL,
    c10 character varying NOT NULL,
    c11 double precision  NOT NULL,
    c12 double precision  NOT NULL,
    c13 character varying NOT NULL
);

COPY aggregate_test_100_by_sql
    FROM '/opt/data/csv/aggregate_test_100.csv'
    DELIMITER ','
    CSV HEADER;

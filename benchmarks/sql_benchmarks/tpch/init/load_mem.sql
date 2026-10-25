CREATE EXTERNAL TABLE nation_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/nation/nation.1.parquet';

CREATE EXTERNAL TABLE region_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/region/region.1.parquet';

CREATE EXTERNAL TABLE supplier_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/supplier/supplier.1.parquet';

CREATE EXTERNAL TABLE customer_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/customer/customer.1.parquet';

CREATE EXTERNAL TABLE part_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/part/part.1.parquet';

CREATE EXTERNAL TABLE partsupp_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/partsupp/partsupp.1.parquet';

CREATE EXTERNAL TABLE orders_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/orders/orders.1.parquet';

CREATE EXTERNAL TABLE lineitem_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/lineitem/lineitem.1.parquet';

CREATE TABLE nation
(
    n_nationkey BIGINT  NOT NULL,
    n_name      VARCHAR NOT NULL,
    n_regionkey BIGINT  NOT NULL,
    n_comment   VARCHAR NOT NULL,
    PRIMARY KEY (n_nationkey)
) AS SELECT * FROM nation_raw;

CREATE TABLE region
(
    r_regionkey BIGINT  NOT NULL,
    r_name      VARCHAR NOT NULL,
    r_comment   VARCHAR NOT NULL,
    PRIMARY KEY (r_regionkey)
) AS SELECT * FROM region_raw;

CREATE TABLE supplier
(
    s_suppkey   BIGINT         NOT NULL,
    s_name      VARCHAR        NOT NULL,
    s_address   VARCHAR        NOT NULL,
    s_nationkey BIGINT         NOT NULL,
    s_phone     VARCHAR        NOT NULL,
    s_acctbal   DECIMAL(15, 2) NOT NULL,
    s_comment   VARCHAR        NOT NULL,
    PRIMARY KEY (s_suppkey)
) AS SELECT * FROM supplier_raw;

CREATE TABLE customer
(
    c_custkey    BIGINT         NOT NULL,
    c_name       VARCHAR        NOT NULL,
    c_address    VARCHAR        NOT NULL,
    c_nationkey  BIGINT         NOT NULL,
    c_phone      VARCHAR        NOT NULL,
    c_acctbal    DECIMAL(15, 2) NOT NULL,
    c_mktsegment VARCHAR        NOT NULL,
    c_comment    VARCHAR        NOT NULL,
    PRIMARY KEY (c_custkey)
) AS SELECT * FROM customer_raw;

CREATE TABLE part
(
    p_partkey     BIGINT         NOT NULL,
    p_name        VARCHAR        NOT NULL,
    p_mfgr        VARCHAR        NOT NULL,
    p_brand       VARCHAR        NOT NULL,
    p_type        VARCHAR        NOT NULL,
    p_size        INT            NOT NULL,
    p_container   VARCHAR        NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment     VARCHAR        NOT NULL,
    PRIMARY KEY (p_partkey)
) AS SELECT * FROM part_raw;

CREATE TABLE partsupp
(
    ps_partkey    BIGINT         NOT NULL,
    ps_suppkey    BIGINT         NOT NULL,
    ps_availqty   INT            NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment    VARCHAR        NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
) AS SELECT * FROM partsupp_raw;

CREATE TABLE orders
(
    o_orderkey      BIGINT         NOT NULL,
    o_custkey       BIGINT         NOT NULL,
    o_orderstatus   VARCHAR        NOT NULL,
    o_totalprice    DECIMAL(15, 2) NOT NULL,
    o_orderdate     DATE           NOT NULL,
    o_orderpriority VARCHAR        NOT NULL,
    o_clerk         VARCHAR        NOT NULL,
    o_shippriority  INT            NOT NULL,
    o_comment       VARCHAR        NOT NULL,
    PRIMARY KEY (o_orderkey)
) AS SELECT * FROM orders_raw;

CREATE TABLE lineitem
(
    l_orderkey      BIGINT         NOT NULL,
    l_partkey       BIGINT         NOT NULL,
    l_suppkey       BIGINT         NOT NULL,
    l_linenumber    INT            NOT NULL,
    l_quantity      DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount      DECIMAL(15, 2) NOT NULL,
    l_tax           DECIMAL(15, 2) NOT NULL,
    l_returnflag    VARCHAR        NOT NULL,
    l_linestatus    VARCHAR        NOT NULL,
    l_shipdate      DATE           NOT NULL,
    l_commitdate    DATE           NOT NULL,
    l_receiptdate   DATE           NOT NULL,
    l_shipinstruct  VARCHAR        NOT NULL,
    l_shipmode      VARCHAR        NOT NULL,
    l_comment       VARCHAR        NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
) AS SELECT * FROM lineitem_raw;

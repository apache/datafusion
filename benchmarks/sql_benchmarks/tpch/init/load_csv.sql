CREATE EXTERNAL TABLE nation
(
    n_nationkey BIGINT       NOT NULL,
    n_name      CHAR(25)     NOT NULL,
    n_regionkey BIGINT       NOT NULL,
    n_comment   VARCHAR(152) NOT NULL,
    PRIMARY KEY (n_nationkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/nation/nation.1.csv';

CREATE EXTERNAL TABLE region
(
    r_regionkey BIGINT       NOT NULL,
    r_name      CHAR(25)     NOT NULL,
    r_comment   VARCHAR(152) NOT NULL,
    PRIMARY KEY (r_regionkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/region/region.1.csv';

CREATE EXTERNAL TABLE supplier
(
    s_suppkey   BIGINT        NOT NULL,
    s_name      CHAR(25)      NOT NULL,
    s_address   VARCHAR(40)   NOT NULL,
    s_nationkey BIGINT        NOT NULL,
    s_phone     CHAR(15)      NOT NULL,
    s_acctbal   DECIMAL(15, 2) NOT NULL,
    s_comment   VARCHAR(101)  NOT NULL,
    PRIMARY KEY (s_suppkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/supplier/supplier.1.csv';

CREATE EXTERNAL TABLE customer
(
    c_custkey    BIGINT        NOT NULL,
    c_name       VARCHAR(25)   NOT NULL,
    c_address    VARCHAR(40)   NOT NULL,
    c_nationkey  BIGINT        NOT NULL,
    c_phone      CHAR(15)      NOT NULL,
    c_acctbal    DECIMAL(15, 2) NOT NULL,
    c_mktsegment CHAR(10)      NOT NULL,
    c_comment    VARCHAR(117)  NOT NULL,
    PRIMARY KEY (c_custkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/customer/customer.1.csv';

CREATE EXTERNAL TABLE part
(
    p_partkey     BIGINT        NOT NULL,
    p_name        VARCHAR(55)   NOT NULL,
    p_mfgr        CHAR(25)      NOT NULL,
    p_brand       CHAR(10)      NOT NULL,
    p_type        VARCHAR(25)   NOT NULL,
    p_size        INT           NOT NULL,
    p_container   CHAR(10)      NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment     VARCHAR(23)   NOT NULL,
    PRIMARY KEY (p_partkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/part/part.1.csv';

CREATE EXTERNAL TABLE partsupp
(
    ps_partkey    BIGINT        NOT NULL,
    ps_suppkey    BIGINT        NOT NULL,
    ps_availqty   INT           NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment    VARCHAR(199)  NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/partsupp/partsupp.1.csv';

CREATE EXTERNAL TABLE orders
(
    o_orderkey      BIGINT        NOT NULL,
    o_custkey       BIGINT        NOT NULL,
    o_orderstatus   CHAR(1)       NOT NULL,
    o_totalprice    DECIMAL(15, 2) NOT NULL,
    o_orderdate     DATE          NOT NULL,
    o_orderpriority CHAR(15)      NOT NULL,
    o_clerk         CHAR(15)      NOT NULL,
    o_shippriority  INT           NOT NULL,
    o_comment       VARCHAR(79)   NOT NULL,
    PRIMARY KEY (o_orderkey)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/orders/orders.1.csv';

CREATE EXTERNAL TABLE lineitem
(
    l_orderkey      BIGINT        NOT NULL,
    l_partkey       BIGINT        NOT NULL,
    l_suppkey       BIGINT        NOT NULL,
    l_linenumber    INT           NOT NULL,
    l_quantity      DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount      DECIMAL(15, 2) NOT NULL,
    l_tax           DECIMAL(15, 2) NOT NULL,
    l_returnflag    CHAR(1)       NOT NULL,
    l_linestatus    CHAR(1)       NOT NULL,
    l_shipdate      DATE          NOT NULL,
    l_commitdate    DATE          NOT NULL,
    l_receiptdate   DATE          NOT NULL,
    l_shipinstruct  CHAR(25)      NOT NULL,
    l_shipmode      CHAR(10)      NOT NULL,
    l_comment       VARCHAR(44)   NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
) STORED AS CSV LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/csv/lineitem/lineitem.1.csv';

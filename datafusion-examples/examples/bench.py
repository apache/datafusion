import polars as pl
import time
from datetime import date
from datafusion import SessionContext

t = time.time()

#file = "/home/dev/arrow-datafusion/benchmarks/data/tpch_sf10/lineitem/part-0.parquet"


file = "/home/dev/arrow-datafusion/test_out/benchon.parquet"
#file = "/home/dev/arrow-datafusion/test_out/uncompressed.parquet"


# Create a DataFusion context
ctx = SessionContext()

# Register table with context
ctx.register_parquet('test', file)

times = []
for i in range(5):
    
    t = time.time()
    df = pl.scan_parquet(file) \
        .filter(pl.col("l_shipdate") <= date(1998, 9, 2)) \
        .group_by("l_returnflag", "l_linestatus") \
        .agg([
            pl.col("l_quantity").sum().alias("sum_qty"),
            pl.col("l_extendedprice").sum().alias("sum_base_price"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum().alias("sum_disc_price"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")) * (1 + pl.col("l_tax"))).sum().alias("sum_charge"),
            pl.col("l_quantity").mean().alias("avg_qty"),
            pl.col("l_extendedprice").mean().alias("avg_price"),
            pl.col("l_discount").mean().alias("avg_disc"),
            pl.count().alias("count_order")
        ]
        ) \
        .sort([pl.col("l_returnflag"), pl.col("l_linestatus")])
    df = df.collect()

    print(f"polars agg query {time.time()-t}s")

    #t = time.time()
    #pl.scan_parquet(file).sink_parquet("test_out/pl.parquet")
    #print(f"polars re-endcode job {time.time()-t}")


    t = time.time()

    
    query = """
   select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    test
where
        l_shipdate <= date '1998-09-02'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
    """

    # Execute SQL
    df = ctx.sql(f"{query}").cache()
    elapsed = time.time() - t
    times.append(elapsed)
    print(f"datafusion agg query {elapsed}s")

    # t = time.time()
    # df = ctx.sql("copy test to 'test_out/df.parquet'")
    # df.show()
    # print(f"datafusion reendcode job {time.time() - t}s")

print(sum(times)/len(times))


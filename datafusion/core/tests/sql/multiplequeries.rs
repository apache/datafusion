use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;

#[tokio::test]
async fn tpch_q15_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
    #orders.o_orderpriority ASC NULLS LAST
        Projection: #orders.o_orderpriority, #COUNT(UInt8(1)) AS order_count
            Aggregate: groupBy=[[#orders.o_orderpriority]], aggr=[[COUNT(UInt8(1))]]
                Filter: EXISTS (                                                         -- plan
                    Subquery: Projection: *                                              -- proj
                        Filter: #lineitem.l_orderkey = #orders.o_orderkey                -- filter
                            TableScan: lineitem projection=None                          -- filter.input
                )
                    TableScan: orders projection=None                                    -- plan.inputs
                 */
    let sql = r#"
            with revenue_view as (
                select
                l_suppkey as supplier_no,
                sum(l_extendedprice * (1 - l_discount)) as total_revenue
                from
                lineitem
                where
                l_shipdate >= '1996-01-01'
                and l_shipdate < '1996-04-01'
                group by
                l_suppkey)
            select
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            from
                supplier,
                revenue_view
            where
                s_suppkey = supplier_no
                and total_revenue = (
                select
                    max(total_revenue)
                from
                    revenue_view
                )
            order by
                s_suppkey
        "#;

    // assert plan
    let plan = ctx
        .create_logical_plan(sql)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let actual = format!("{}", plan.display_indent());

    let expected = r#"Sort: #supplier.s_suppkey ASC NULLS LAST
    Projection: #supplier.s_suppkey, #supplier.s_name, #supplier.s_address, #supplier.s_phone, #supplier.total_revenue
      Filter: #supplier.total_revenue = (Subquery: Projection: #MAX(revenue_view.total_revenue)
    Aggregate: groupBy=[[]], aggr=[[MAX(#revenue_view.total_revenue)]]
      Projection: #lineitem.l_suppkey AS supplier_no, #SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS total_revenue, alias=revenue_view
        Aggregate: groupBy=[[#lineitem.l_suppkey]], aggr=[[SUM(#lineitem.l_extendedprice * Int64(1) - #lineitem.l_discount)]]
          Filter: #lineitem.l_shipdate >= Utf8("1996-01-01") AND #lineitem.l_shipdate < Utf8("1996-04-01")
            TableScan: lineitem)
        Inner Join: #supplier.s_suppkey = #revenue_view.supplier_no
          TableScan: supplier
          Projection: #lineitem.l_suppkey AS supplier_no, #SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS total_revenue, alias=revenue_view
            Aggregate: groupBy=[[#lineitem.l_suppkey]], aggr=[[SUM(#lineitem.l_extendedprice * Int64(1) - #lineitem.l_discount)]]
              Filter: #lineitem.l_shipdate >= Utf8("1996-01-01") AND #lineitem.l_shipdate < Utf8("1996-04-01")
                TableScan: lineitem"#
        .to_string();
    
    assert_eq!(actual, &expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+--------------------+-------------------+-----------------+---------------+",
        "| s_suppkey | s_name             | s_address         | s_phone         | total_revenue |",
        "+-----------+--------------------+-------------------+-----------------+---------------+",
        "| 8449      | Supplier#000008449 | Wp34zim9qYFbVctdW | 20-469-856-8873 | 1772627.2087  |",
        "+-----------+--------------------+-------------------+-----------------+---------------+",
    ];
    
    assert_batches_eq!(expected, &results);

    Ok(())
}

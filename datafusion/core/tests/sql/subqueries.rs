use super::*;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;
use crate::sql::{execute_to_batches};

/// https://github.com/apache/arrow-datafusion/issues/171
#[tokio::test]
async fn tpch_q20() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"
        select s_name, s_address
        from supplier, nation
        where s_suppkey in (
            select ps_suppkey from partsupp
            where ps_partkey in ( select p_partkey from part where p_name like 'forest%' )
              and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem
                where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1994-01-01' + interval '1' year
            )
        )
        and s_nationkey = n_nationkey and n_name = 'CANADA'
        order by s_name;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn tpch_q20_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"
        select ps_suppkey from partsupp
        where ps_partkey in ( select p_partkey from part where p_name like 'forest%' )
          and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem
            where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1994-01-01' + interval '1' year
        ) order by ps_suppkey;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn tpch_q20_decorrelated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"
        select ps_suppkey
        from partsupp ps
        inner join part on ps.ps_partkey = part.p_partkey and p_name like 'forest%'
        inner join (
            select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as threshold from lineitem
            where l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1994-01-01' + interval '1' year
            group by l_partkey, l_suppkey
        ) av on av.l_suppkey=ps.ps_suppkey and av.l_partkey=ps.ps_partkey and ps.ps_availqty > av.threshold
        order by ps_suppkey;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn scalar_subquery() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select * from (values (1)) where column1 > ( select 0.5 );";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

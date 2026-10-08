// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tests for `datafusion.sql_parser.column_labels`, which attaches a readable
//! label to each output column of a SQL query as field metadata.

use super::*;
use datafusion_common::metadata::{COLUMN_LABEL_KEY, COLUMN_LABEL_OF_KEY};
use datafusion_common::test_util::batches_to_string;
use insta::assert_snapshot;

/// Returns a context with tables `t`, `t2`, and `t_pk`, and with column
/// labels on.
async fn labels_ctx() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    for sql in [
        "CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT) AS VALUES (1, 2, 3), (4, 5, 6)",
        "CREATE TABLE t2 (a BIGINT, d BIGINT) AS VALUES (1, 10), (4, 40)",
        "CREATE TABLE t_pk (id BIGINT PRIMARY KEY, v BIGINT) AS VALUES (1, 10), (2, 20)",
        "SET datafusion.sql_parser.column_labels = true",
    ] {
        ctx.sql(sql).await?.collect().await?;
    }
    Ok(ctx)
}

/// Runs each query and lists its output columns as `name => label`, or as
/// `name` for a column without a label.
async fn labels(ctx: &SessionContext, queries: &[&str]) -> Result<String> {
    let mut lines = vec![];
    for sql in queries {
        let df = ctx.sql(sql).await?;
        // Every case also checks that the labeled query runs
        df.clone().collect().await?;
        lines.push(sql.to_string());
        for field in df.schema().fields() {
            let metadata = field.metadata();
            match metadata.get(COLUMN_LABEL_KEY) {
                Some(label) => {
                    assert_eq!(metadata.get(COLUMN_LABEL_OF_KEY), Some(field.name()));
                    lines.push(format!("  {} => {label}", field.name()));
                }
                None => lines.push(format!("  {}", field.name())),
            }
        }
    }
    Ok(lines.join("\n"))
}

#[tokio::test]
async fn column_labels_off_by_default() -> Result<()> {
    let ctx = labels_ctx().await?;
    ctx.sql("SET datafusion.sql_parser.column_labels = false")
        .await?
        .collect()
        .await?;
    assert_snapshot!(labels(&ctx, &["SELECT a + 1, sum(b) FROM t GROUP BY a"]).await?, @r"
    SELECT a + 1, sum(b) FROM t GROUP BY a
      t.a + Int64(1)
      sum(t.b)
    ");
    Ok(())
}

#[tokio::test]
async fn column_labels_expressions() -> Result<()> {
    let ctx = labels_ctx().await?;
    let queries = [
        "SELECT 1, 'foo'",
        "SELECT 1 + 2",
        "SELECT (a + b) * c FROM t",
        "SELECT t.a + 1 FROM t",
        "SELECT coalesce(NULL, 1)",
        "SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM t",
        // CAST is left out of labels, as it is of names
        "SELECT CAST(a AS DOUBLE) + 1 FROM t",
        // Plain columns and explicit aliases need no label
        "SELECT * FROM t",
        "SELECT a + 1 AS x FROM t",
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r#"
    SELECT 1, 'foo'
      Int64(1) => 1
      Utf8("foo") => foo
    SELECT 1 + 2
      Int64(1) + Int64(2) => 1 + 2
    SELECT (a + b) * c FROM t
      t.a + t.b * t.c => (a + b) * c
    SELECT t.a + 1 FROM t
      t.a + Int64(1) => a + 1
    SELECT coalesce(NULL, 1)
      coalesce(NULL,Int64(1)) => coalesce(NULL, 1)
    SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM t
      CASE WHEN t.a > Int64(0) THEN Utf8("pos") ELSE Utf8("neg") END => CASE WHEN a > 0 THEN pos ELSE neg END
    SELECT CAST(a AS DOUBLE) + 1 FROM t
      t.a + Int64(1) => a + 1
    SELECT * FROM t
      a
      b
      c
    SELECT a + 1 AS x FROM t
      x
    "#);
    Ok(())
}

#[tokio::test]
async fn column_labels_aggregate_functions() -> Result<()> {
    let ctx = labels_ctx().await?;
    let queries = [
        "SELECT sum(a), count(DISTINCT b) FROM t",
        "SELECT avg(c) FILTER (WHERE c > 5) FROM t",
        "SELECT count(*), count(1) FROM t",
        "SELECT array_agg(a ORDER BY b DESC) FROM t",
        "SELECT b, sum(a) + 1 FROM t GROUP BY b",
        "SELECT a + 1, sum(b) FROM t GROUP BY a + 1",
        "SELECT a + 1, b, sum(c) FROM t GROUP BY CUBE (a + 1, b)",
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r"
    SELECT sum(a), count(DISTINCT b) FROM t
      sum(t.a) => sum(a)
      count(DISTINCT t.b) => count(DISTINCT b)
    SELECT avg(c) FILTER (WHERE c > 5) FROM t
      avg(t.c) FILTER (WHERE t.c > Int64(5)) => avg(c) FILTER (WHERE c > 5)
    SELECT count(*), count(1) FROM t
      count(*)
      count(Int64(1)) => count(1)
    SELECT array_agg(a ORDER BY b DESC) FROM t
      array_agg(t.a) ORDER BY [t.b DESC NULLS FIRST] => array_agg(a) ORDER BY b DESC
    SELECT b, sum(a) + 1 FROM t GROUP BY b
      b
      sum(t.a) + Int64(1) => sum(a) + 1
    SELECT a + 1, sum(b) FROM t GROUP BY a + 1
      t.a + Int64(1) => a + 1
      sum(t.b) => sum(b)
    SELECT a + 1, b, sum(c) FROM t GROUP BY CUBE (a + 1, b)
      t.a + Int64(1) => a + 1
      b
      sum(t.c) => sum(c)
    ");
    Ok(())
}

#[tokio::test]
async fn column_labels_ordered_set_aggregates() -> Result<()> {
    let ctx = labels_ctx().await?;
    let queries = [
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY c) FROM t",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY c DESC) FROM t",
        "SELECT percentile_cont(c, 0.5) FROM t",
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r"
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY c) FROM t
      percentile_cont(Float64(0.5)) WITHIN GROUP [t.c ASC NULLS LAST] => percentile_cont(0.5) WITHIN GROUP (ORDER BY c)
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY c DESC) FROM t
      percentile_cont(Float64(0.5)) WITHIN GROUP [t.c DESC NULLS FIRST] => percentile_cont(0.5) WITHIN GROUP (ORDER BY c DESC)
    SELECT percentile_cont(c, 0.5) FROM t
      percentile_cont(t.c,Float64(0.5)) => percentile_cont(c, 0.5)
    ");
    Ok(())
}

#[tokio::test]
async fn column_labels_window_functions() -> Result<()> {
    let ctx = labels_ctx().await?;
    let queries = [
        "SELECT row_number() OVER (ORDER BY a) FROM t",
        "SELECT sum(a) OVER (PARTITION BY b) FROM t",
        "SELECT sum(a) OVER (ORDER BY a DESC NULLS FIRST) FROM t",
        "SELECT sum(a) OVER (ORDER BY a ASC NULLS FIRST) FROM t",
        "SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "SELECT sum(b) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "SELECT count(*) OVER () FROM t",
        "SELECT count(*) OVER (PARTITION BY b) AS x, count(*) OVER (PARTITION BY c) FROM t",
        // The planner picks the default frame before aggregation, where `a`
        // can have ties, so the default frame is RANGE
        "SELECT row_number() OVER (ORDER BY a) FROM t GROUP BY a",
        "SELECT sum(a) OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)",
        // Ordered by a unique key, the default frame is ROWS
        "SELECT sum(v) OVER (ORDER BY id) FROM t_pk",
        "SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t_pk",
        "SELECT sum(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t_pk",
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r"
    SELECT row_number() OVER (ORDER BY a) FROM t
      row_number() ORDER BY [t.a ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => row_number() OVER (ORDER BY a)
    SELECT sum(a) OVER (PARTITION BY b) FROM t
      sum(t.a) PARTITION BY [t.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING => sum(a) OVER (PARTITION BY b)
    SELECT sum(a) OVER (ORDER BY a DESC NULLS FIRST) FROM t
      sum(t.a) ORDER BY [t.a DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(a) OVER (ORDER BY a DESC)
    SELECT sum(a) OVER (ORDER BY a ASC NULLS FIRST) FROM t
      sum(t.a) ORDER BY [t.a ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(a) OVER (ORDER BY a NULLS FIRST)
    SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
      sum(t.a) ORDER BY [t.a ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    SELECT sum(b) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
      sum(t.b) ORDER BY [UInt64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(b) OVER ()
    SELECT count(*) OVER () FROM t
      count(*) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING => count(*) OVER ()
    SELECT count(*) OVER (PARTITION BY b) AS x, count(*) OVER (PARTITION BY c) FROM t
      x
      count(*) PARTITION BY [t.c] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING => count(*) OVER (PARTITION BY c)
    SELECT row_number() OVER (ORDER BY a) FROM t GROUP BY a
      row_number() ORDER BY [t.a ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => row_number() OVER (ORDER BY a)
    SELECT sum(a) OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)
      sum(t.a) PARTITION BY [t.b] ORDER BY [t.c ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(a) OVER (PARTITION BY b ORDER BY c)
    SELECT sum(v) OVER (ORDER BY id) FROM t_pk
      sum(t_pk.v) ORDER BY [t_pk.id ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(v) OVER (ORDER BY id)
    SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t_pk
      sum(t_pk.v) ORDER BY [t_pk.id ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(v) OVER (ORDER BY id)
    SELECT sum(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t_pk
      sum(t_pk.v) ORDER BY [t_pk.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW => sum(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ");
    Ok(())
}

#[tokio::test]
async fn column_labels_ctes_derived_tables_and_views() -> Result<()> {
    let ctx = labels_ctx().await?;
    ctx.sql("CREATE VIEW v AS SELECT a + 1 FROM t")
        .await?
        .collect()
        .await?;
    let queries = [
        "WITH x AS (SELECT sum(a) FROM t GROUP BY b) SELECT * FROM x",
        "SELECT * FROM (SELECT a + 1 FROM t)",
        "SELECT * FROM v",
        "SELECT x FROM (SELECT a + 1 AS x FROM t)",
        // A column reference typed in the outermost SELECT gets no label
        r#"SELECT "t.a + Int64(1)" FROM (SELECT a + 1 FROM t)"#,
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r#"
    WITH x AS (SELECT sum(a) FROM t GROUP BY b) SELECT * FROM x
      sum(t.a) => sum(a)
    SELECT * FROM (SELECT a + 1 FROM t)
      t.a + Int64(1) => a + 1
    SELECT * FROM v
      t.a + Int64(1) => a + 1
    SELECT x FROM (SELECT a + 1 AS x FROM t)
      x
    SELECT "t.a + Int64(1)" FROM (SELECT a + 1 FROM t)
      t.a + Int64(1)
    "#);
    Ok(())
}

#[tokio::test]
async fn column_labels_other_plan_nodes() -> Result<()> {
    let ctx = labels_ctx().await?;
    let queries = [
        r#"SELECT a + 1 FROM t ORDER BY "t.a + Int64(1)""#,
        "SELECT a + 1, b FROM t ORDER BY 1 DESC LIMIT 1",
        "SELECT b, sum(a) FROM t GROUP BY b HAVING sum(a) > 1",
        "SELECT DISTINCT a + 1 FROM t",
        "SELECT t.a + 1, t2.d * 2 FROM t JOIN t2 ON t.a = t2.a",
        "SELECT a + 1 FROM t UNION ALL SELECT a + 2 FROM t",
        "SELECT a + 1 FROM t WHERE a IN (SELECT a FROM t2)",
        // These can't be traced, so they get no label
        "SELECT DISTINCT ON (b) a + 1 FROM t ORDER BY b",
        "SELECT unnest([1, 2])",
        "SELECT * FROM t JOIN t2 USING (a)",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n * 2 FROM r",
        "SELECT (SELECT max(d) FROM t2) + 1",
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r#"
    SELECT a + 1 FROM t ORDER BY "t.a + Int64(1)"
      t.a + Int64(1) => a + 1
    SELECT a + 1, b FROM t ORDER BY 1 DESC LIMIT 1
      t.a + Int64(1) => a + 1
      b
    SELECT b, sum(a) FROM t GROUP BY b HAVING sum(a) > 1
      b
      sum(t.a) => sum(a)
    SELECT DISTINCT a + 1 FROM t
      t.a + Int64(1) => a + 1
    SELECT t.a + 1, t2.d * 2 FROM t JOIN t2 ON t.a = t2.a
      t.a + Int64(1) => a + 1
      t2.d * Int64(2) => d * 2
    SELECT a + 1 FROM t UNION ALL SELECT a + 2 FROM t
      t.a + Int64(1) => a + 1
    SELECT a + 1 FROM t WHERE a IN (SELECT a FROM t2)
      t.a + Int64(1) => a + 1
    SELECT DISTINCT ON (b) a + 1 FROM t ORDER BY b
      t.a + Int64(1)
    SELECT unnest([1, 2])
      UNNEST(make_array(Int64(1),Int64(2)))
    SELECT * FROM t JOIN t2 USING (a)
      a
      b
      c
      d
    WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n * 2 FROM r
      r.n * Int64(2)
    SELECT (SELECT max(d) FROM t2) + 1
      max(t2.d) + Int64(1)
    "#);
    Ok(())
}

/// Two columns can share a label, because their names stay unique.
#[tokio::test]
async fn column_labels_can_repeat() -> Result<()> {
    let ctx = labels_ctx().await?;
    let queries = [
        "SELECT t.a + 1, t2.a + 1 FROM t JOIN t2 ON t.a = t2.a",
        "SELECT 1, '1'",
        r#"SELECT a + 1, b AS "a + 1" FROM t"#,
    ];
    assert_snapshot!(labels(&ctx, &queries).await?, @r#"
    SELECT t.a + 1, t2.a + 1 FROM t JOIN t2 ON t.a = t2.a
      t.a + Int64(1) => a + 1
      t2.a + Int64(1) => a + 1
    SELECT 1, '1'
      Int64(1) => 1
      Utf8("1") => 1
    SELECT a + 1, b AS "a + 1" FROM t
      t.a + Int64(1) => a + 1
      a + 1
    "#);

    let results = ctx.sql("SELECT 1, '1'").await?.to_string().await?;
    assert_snapshot!(results, @r"
    +---+---+
    | 1 | 1 |
    +---+---+
    | 1 | 1 |
    +---+---+
    ");
    Ok(())
}

/// Statements that store column names get no labels.
#[tokio::test]
async fn column_labels_not_stored() -> Result<()> {
    let ctx = labels_ctx().await?;
    ctx.sql("CREATE TABLE ctas AS SELECT a + 1 FROM t")
        .await?
        .collect()
        .await?;
    ctx.sql("CREATE VIEW v AS SELECT a + 1 FROM t")
        .await?
        .collect()
        .await?;
    for table in ["ctas", "v"] {
        let schema = ctx.table(table).await?.schema().as_arrow().clone();
        let field = schema.field(0);
        assert_eq!(field.name(), "t.a + Int64(1)");
        assert!(field.metadata().get(COLUMN_LABEL_KEY).is_none());
    }
    Ok(())
}

/// The labels reach the batches returned by `collect()` and don't stop
/// `ORDER BY ... LIMIT` from planning as TopK.
#[tokio::test]
async fn column_labels_reach_results() -> Result<()> {
    let ctx = labels_ctx().await?;
    let df = ctx
        .sql("SELECT a + 1, sum(b) FROM t GROUP BY a ORDER BY a + 1 DESC LIMIT 1")
        .await?;

    let physical = df.clone().create_physical_plan().await?;
    let physical = displayable(physical.as_ref()).indent(true).to_string();
    assert_contains!(&physical, "TopK(fetch=1)");

    let batches = df.collect().await?;
    assert_snapshot!(batches_to_string(&batches), @r"
    +----------------+----------+
    | t.a + Int64(1) | sum(t.b) |
    +----------------+----------+
    | 5              | 5        |
    +----------------+----------+
    ");
    for batch in &batches {
        let labels: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.metadata().get(COLUMN_LABEL_KEY).cloned())
            .collect();
        assert_eq!(
            labels,
            [Some("a + 1".to_string()), Some("sum(b)".to_string())]
        );
    }
    Ok(())
}

/// `DataFrame::to_string` shows labels as headers, until a column is renamed.
#[tokio::test]
async fn column_labels_in_dataframe_display() -> Result<()> {
    let ctx = labels_ctx().await?;
    let sql = "SELECT a + 1, sum(b), row_number() OVER (ORDER BY a) \
               FROM t GROUP BY a ORDER BY a LIMIT 1";

    let results = ctx.sql(sql).await?.to_string().await?;
    assert_snapshot!(results, @r"
    +-------+--------+--------------------------------+
    | a + 1 | sum(b) | row_number() OVER (ORDER BY a) |
    +-------+--------+--------------------------------+
    | 2     | 2      | 1                              |
    +-------+--------+--------------------------------+
    ");

    // Code that reads columns by name keeps working
    let results = ctx
        .sql(sql)
        .await?
        .select_columns(&["sum(t.b)"])?
        .to_string()
        .await?;
    assert_snapshot!(results, @r"
    +--------+
    | sum(b) |
    +--------+
    | 2      |
    +--------+
    ");

    // A renamed column still carries the old label, which no longer applies
    let results = ctx
        .sql(sql)
        .await?
        .with_column_renamed("sum(t.b)", "total")?
        .to_string()
        .await?;
    assert_snapshot!(results, @r"
    +-------+-------+--------------------------------+
    | a + 1 | total | row_number() OVER (ORDER BY a) |
    +-------+-------+--------------------------------+
    | 2     | 2     | 1                              |
    +-------+-------+--------------------------------+
    ");
    Ok(())
}

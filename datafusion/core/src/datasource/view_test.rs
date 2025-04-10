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

//! View data source which uses a LogicalPlan as it's input.

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::execution::context::SessionConfig;
    use crate::execution::options::ParquetReadOptions;
    use crate::prelude::SessionContext;
    use crate::test_util::parquet_test_data;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_expr::{col, lit};

    #[tokio::test]
    async fn issue_3242() -> Result<()> {
        // regression test for https://github.com/apache/datafusion/pull/3242
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("create view v as select 1 as a, 2 as b, 3 as c")
            .await?
            .collect()
            .await?;

        let results = session_ctx
            .sql("select * from (select b from v)")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---+
        | b |
        +---+
        | 2 |
        +---+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn create_view_return_empty_dataframe() -> Result<()> {
        let session_ctx = SessionContext::new();

        let df = session_ctx
            .sql("CREATE VIEW xyz AS SELECT 1")
            .await?
            .collect()
            .await?;

        assert!(df.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn query_view() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------+---------+---------+
        | column1 | column2 | column3 |
        +---------+---------+---------+
        | 1       | 2       | 3       |
        | 4       | 5       | 6       |
        +---------+---------+---------+
        "###);

        let view_sql =
            "CREATE VIEW replace_xyz AS SELECT * REPLACE (column1*2 as column1) FROM xyz";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("SELECT * FROM replace_xyz")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------+---------+---------+
        | column1 | column2 | column3 |
        +---------+---------+---------+
        | 2       | 2       | 3       |
        | 8       | 5       | 6       |
        +---------+---------+---------+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_alias() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(SessionConfig::new());

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1 AS column1_alias, column2 AS column2_alias FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("SELECT column1_alias FROM xyz")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------------+
        | column1_alias |
        +---------------+
        | 1             |
        | 4             |
        +---------------+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_inline_alias() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(SessionConfig::new());

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz (column1_alias, column2_alias) AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx
            .sql("SELECT column2_alias, column1_alias FROM xyz")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------------+---------------+
        | column2_alias | column1_alias |
        +---------------+---------------+
        | 2             | 1             |
        | 5             | 4             |
        +---------------+---------------+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_projection() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT column1 FROM xyz")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------+
        | column1 |
        +---------+
        | 1       |
        | 4       |
        +---------+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn query_view_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT column1 FROM xyz WHERE column2 = 5")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------+
        | column1 |
        +---------+
        | 4       |
        +---------+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn query_join_views() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT column1, column2 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let view_sql = "CREATE VIEW lmn AS SELECT column1, column3 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND (table_name = 'xyz' OR table_name = 'lmn')").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 2);

        let results = session_ctx
            .sql("SELECT * FROM xyz JOIN lmn USING (column1) ORDER BY column2")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------+---------+---------+
        | column2 | column1 | column3 |
        +---------+---------+---------+
        | 2       | 1       | 3       |
        | 5       | 4       | 6       |
        +---------+---------+---------+
        "###);

        Ok(())
    }

    #[tokio::test]
    async fn filter_pushdown_view() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain.snappy.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_table("t1", ctx.table("test").await?.into_view())?;

        ctx.sql("CREATE VIEW t2 as SELECT * FROM t1").await?;

        let df = ctx
            .table("t2")
            .await?
            .filter(col("id").eq(lit(1)))?
            .select_columns(&["bool_col", "int_col"])?;

        let plan = df.explain(false, false)?.collect().await?;

        // Filters all the way to Parquet
        let formatted = arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(formatted.contains("FilterExec: id@0 = 1"));
        Ok(())
    }

    #[tokio::test]
    async fn limit_pushdown_view() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain.snappy.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_table("t1", ctx.table("test").await?.into_view())?;

        ctx.sql("CREATE VIEW t2 as SELECT * FROM t1").await?;

        let df = ctx
            .table("t2")
            .await?
            .limit(0, Some(10))?
            .select_columns(&["bool_col", "int_col"])?;

        let plan = df.explain(false, false)?.collect().await?;
        // Limit is included in DataSourceExec
        let formatted = arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(formatted.contains("DataSourceExec: "));
        assert!(formatted.contains("file_type=parquet"));
        assert!(formatted.contains("projection=[bool_col, int_col], limit=10"));
        Ok(())
    }

    #[tokio::test]
    async fn create_view_plan() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let dataframe = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT * FROM abc")
            .await?;
        let plan = dataframe.into_optimized_plan()?;
        let actual = format!("{}", plan.display_indent());
        let expected = "\
        Explain\
        \n  CreateView: Bare { table: \"xyz\" }\
        \n    TableScan: abc projection=[column1, column2, column3]";
        assert_eq!(expected, actual);

        let dataframe = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT * FROM abc WHERE column2 = 5")
            .await?;
        let plan = dataframe.into_optimized_plan()?;
        let actual = format!("{}", plan.display_indent());
        let expected = "\
        Explain\
        \n  CreateView: Bare { table: \"xyz\" }\
        \n    Filter: abc.column2 = Int64(5)\
        \n      TableScan: abc projection=[column1, column2, column3]";
        assert_eq!(expected, actual);

        let dataframe = session_ctx
            .sql("EXPLAIN CREATE VIEW xyz AS SELECT column1, column2 FROM abc WHERE column2 = 5")
            .await?;
        let plan = dataframe.into_optimized_plan()?;
        let actual = format!("{}", plan.display_indent());
        let expected = "\
        Explain\
        \n  CreateView: Bare { table: \"xyz\" }\
        \n    Filter: abc.column2 = Int64(5)\
        \n      TableScan: abc projection=[column1, column2]";
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn create_or_replace_view() -> Result<()> {
        let session_ctx = SessionContext::new_with_config(
            SessionConfig::new().with_information_schema(true),
        );

        session_ctx
            .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
            .await?
            .collect()
            .await?;

        let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let view_sql = "CREATE OR REPLACE VIEW xyz AS SELECT column1 FROM abc";
        session_ctx.sql(view_sql).await?.collect().await?;

        let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
        assert_eq!(results[0].num_rows(), 1);

        let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        insta::assert_snapshot!(batches_to_string(&results),@r###"
        +---------+
        | column1 |
        +---------+
        | 1       |
        | 4       |
        +---------+
        "###);

        Ok(())
    }
}

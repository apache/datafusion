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

use datafusion::prelude::*;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{TableReference, assert_contains};
use datafusion_expr::dml::MergeIntoOp;
use datafusion_expr::{Expr, LogicalPlan, WriteOp};

use tempfile::TempDir;

#[tokio::test]
async fn test_window_function() {
    let ctx = SessionContext::new();
    let df = ctx
        .sql(
            r#"SELECT
        t1.v1,
        SUM(t1.v1) OVER w + 1
        FROM
        generate_series(1, 10000) AS t1(v1)
        WINDOW
        w AS (ORDER BY t1.v1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);"#,
        )
        .await;
    assert!(df.is_ok());
}

#[tokio::test]
async fn unsupported_ddl_returns_error() {
    // Verify SessionContext::with_sql_options errors appropriately
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    // disallow ddl
    let options = SQLOptions::new().with_allow_ddl(false);

    let sql = "CREATE VIEW test_view AS SELECT * FROM test";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DDL not supported: CreateView"
    );

    // allow ddl
    let options = options.with_allow_ddl(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

#[tokio::test]
async fn unsupported_dml_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_dml(false);

    let sql = "INSERT INTO test VALUES (1)";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DML not supported: Insert Into"
    );

    let options = options.with_allow_dml(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

#[tokio::test]
async fn dml_output_schema() {
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};

    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();
    let sql = "INSERT INTO test VALUES (1)";
    let df = ctx.sql(sql).await.unwrap();
    let count_schema = &Schema::new(vec![Field::new("count", DataType::UInt64, false)]);
    assert_eq!(df.schema().as_arrow(), count_schema);
}

#[tokio::test]
async fn unsupported_copy_returns_error() {
    let tmpdir = TempDir::new().unwrap();
    let tmpfile = tmpdir.path().join("foo.parquet");

    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_dml(false);

    let sql = format!(
        "COPY (values(1)) TO '{}' STORED AS parquet",
        tmpfile.to_string_lossy()
    );
    let df = ctx.sql_with_options(&sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DML not supported: COPY"
    );

    let options = options.with_allow_dml(true);
    ctx.sql_with_options(&sql, options).await.unwrap();
}

#[tokio::test]
async fn unsupported_statement_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_statements(false);

    let sql = "set datafusion.execution.batch_size = 5";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: Statement not supported: SetVariable"
    );

    let options = options.with_allow_statements(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

// Disallow PREPARE and EXECUTE statements if `allow_statements` is false
#[tokio::test]
async fn disable_prepare_and_execute_statement() {
    let ctx = SessionContext::new();

    let prepare_sql = "PREPARE plan(INT) AS SELECT $1";
    let execute_sql = "EXECUTE plan(1)";
    let options = SQLOptions::new().with_allow_statements(false);
    let df = ctx.sql_with_options(prepare_sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: Statement not supported: Prepare"
    );
    let df = ctx.sql_with_options(execute_sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: Statement not supported: Execute"
    );

    let options = options.with_allow_statements(true);
    ctx.sql_with_options(prepare_sql, options).await.unwrap();
    ctx.sql_with_options(execute_sql, options).await.unwrap();
}

#[tokio::test]
async fn empty_statement_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // Give it an empty string which contains no statements
    let plan_res = state.create_logical_plan("").await;
    assert_eq!(
        plan_res.unwrap_err().strip_backtrace(),
        "Error during planning: No SQL statements were provided in the query string"
    );
}

#[tokio::test]
async fn multiple_statements_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // Give it a string that contains multiple statements
    let plan_res = state
        .create_logical_plan(
            "INSERT INTO test (x) VALUES (1); INSERT INTO test (x) VALUES (2)",
        )
        .await;
    assert_eq!(
        plan_res.unwrap_err().strip_backtrace(),
        "This feature is not implemented: The context currently only supports a single SQL statement"
    );
}

#[tokio::test]
async fn ddl_can_not_be_planned_by_session_state() {
    let ctx = SessionContext::new();

    // make a table via SQL
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // can not create a logical plan for catalog DDL
    let sql = "DROP TABLE test";
    let plan = state.create_logical_plan(sql).await.unwrap();
    let physical_plan = state.create_physical_plan(&plan).await;
    assert_eq!(
        physical_plan.unwrap_err().strip_backtrace(),
        "This feature is not implemented: Unsupported logical plan: DropTable"
    );
}

async fn merge_into_context() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE target (id INT)").await.unwrap();
    ctx.sql("CREATE TABLE source (id INT)").await.unwrap();
    ctx.sql("CREATE TABLE \"Target\" (id INT)").await.unwrap();
    ctx.sql("CREATE SCHEMA \"CaseSchema\"").await.unwrap();
    ctx.sql("CREATE TABLE \"CaseSchema\".\"Target\" (id INT)")
        .await
        .unwrap();
    ctx
}

async fn assert_merge_physical_error(ctx: &SessionContext, sql: &str, expected: &str) {
    let result = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("failed to plan MERGE SQL:\n{sql}\n{error}"))
        .create_physical_plan()
        .await;
    let err = match result {
        Ok(_) => panic!("expected physical planning to fail:\n{sql}"),
        Err(error) => error,
    };
    let actual = err.strip_backtrace();
    assert!(
        actual.contains(expected),
        "MERGE SQL:\n{sql}\n\nExpected:\n{expected}\n\nActual:\n{actual}"
    );
}

async fn merge_operation(ctx: &SessionContext, sql: &str) -> Box<MergeIntoOp> {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    let LogicalPlan::Dml(dml) = plan else {
        panic!("expected MERGE DML")
    };
    let WriteOp::MergeInto(merge_op) = dml.op else {
        panic!("expected MERGE operation")
    };
    merge_op
}

fn has_outer_reference_to(expr: &Expr, qualifier: &TableReference) -> bool {
    let mut found = false;
    expr.apply(|expr| {
        let outer_refs = match expr {
            Expr::Exists(exists) => Some(&exists.subquery.outer_ref_columns),
            Expr::InSubquery(in_subquery) => {
                Some(&in_subquery.subquery.outer_ref_columns)
            }
            Expr::SetComparison(set_comparison) => {
                Some(&set_comparison.subquery.outer_ref_columns)
            }
            Expr::ScalarSubquery(subquery) => Some(&subquery.outer_ref_columns),
            _ => None,
        };
        found = outer_refs.is_some_and(|outer_refs| {
            outer_refs.iter().any(|expr| {
                matches!(
                    expr,
                    Expr::OuterReferenceColumn(_, column)
                        if column.relation.as_ref() == Some(qualifier)
                )
            })
        });
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .unwrap();
    found
}

#[tokio::test]
async fn merge_into_distinguishes_target_alias_from_source_qualifier() {
    let ctx = merge_into_context().await;

    for target_ref in [
        "target",
        "public.target",
        "datafusion.public.target",
        "\"Target\"",
        "\"CaseSchema\".\"Target\"",
    ] {
        assert_merge_physical_error(
            &ctx,
            &format!(
                "MERGE INTO {target_ref} AS t USING source AS target \
                 ON t.id = target.id WHEN MATCHED THEN DELETE"
            ),
            "MERGE INTO not supported for Base table",
        )
        .await;
    }

    for (target_alias, target_column) in [
        ("T", "t.id"),
        ("\"T\"", "\"T\".id"),
        ("\"public.target\"", "\"public.target\".id"),
    ] {
        assert_merge_physical_error(
            &ctx,
            &format!(
                "MERGE INTO target AS {target_alias} USING source AS target \
                 ON {target_column} = target.id WHEN MATCHED THEN DELETE"
            ),
            "MERGE INTO not supported for Base table",
        )
        .await;
    }

    for source in [
        "(SELECT id FROM source) AS target",
        "target AS source_target",
    ] {
        assert_merge_physical_error(
            &ctx,
            &format!(
                "MERGE INTO target AS t USING {source} \
                 ON true WHEN MATCHED THEN DELETE"
            ),
            "MERGE INTO not supported for Base table",
        )
        .await;
    }

    for (target_ref, target_column, expected_qualifier) in [
        (
            "\"Target\"",
            "\"Target\".id",
            TableReference::bare("Target"),
        ),
        (
            "\"CaseSchema\".\"Target\"",
            "\"CaseSchema\".\"Target\".id",
            TableReference::partial("CaseSchema", "Target"),
        ),
    ] {
        let sql = format!(
            "MERGE INTO {target_ref} USING source AS s \
             ON {target_column} = s.id WHEN MATCHED THEN DELETE"
        );
        let merge_op = merge_operation(&ctx, &sql).await;
        assert_eq!(merge_op.target_qualifier(), &expected_qualifier);
        assert_merge_physical_error(
            &ctx,
            &sql,
            "MERGE INTO not supported for Base table",
        )
        .await;
    }
}

#[tokio::test]
async fn merge_into_rejects_outer_source_qualifier_matching_target_alias() {
    let ctx = merge_into_context().await;
    for (target_alias, source_alias, expected) in [
        ("t", "t", "target qualifier 't'"),
        ("t", "T", "target qualifier 't'"),
        ("\"T\"", "\"T\"", "target qualifier 'T'"),
    ] {
        let err = ctx
            .sql(&format!(
                "MERGE INTO target AS {target_alias} USING source AS {source_alias} \
                 ON true WHEN MATCHED THEN DELETE"
            ))
            .await
            .unwrap_err();
        assert_contains!(err.strip_backtrace(), expected);
        assert_contains!(err.strip_backtrace(), "conflicts with a source qualifier");
    }

    let err = ctx
        .sql(
            "MERGE INTO target AS t \
             USING (SELECT id AS source_id FROM source) AS t \
             ON true WHEN MATCHED THEN DELETE",
        )
        .await
        .unwrap_err();
    assert_contains!(
        err.strip_backtrace(),
        "MERGE target qualifier 't' conflicts with a source qualifier"
    );

    for target_ref in ["public.target", "datafusion.public.target"] {
        let err = ctx
            .sql(&format!(
                "MERGE INTO {target_ref} USING source AS target \
                 ON true WHEN MATCHED THEN DELETE"
            ))
            .await
            .unwrap_err();
        assert_contains!(err.strip_backtrace(), "conflicts with a source qualifier");
    }
}

#[tokio::test]
async fn merge_into_preserves_target_alias_in_correlated_subquery() {
    let ctx = merge_into_context().await;
    let direct_exists = "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS x WHERE x.id = t.id) \
         WHEN MATCHED THEN DELETE";
    let direct_in = "MERGE INTO target AS t USING source AS s \
         ON t.id IN (SELECT x.id FROM source AS x WHERE x.id = t.id) \
         WHEN MATCHED THEN DELETE";
    let direct_any = "MERGE INTO target AS t USING source AS s \
         ON t.id = ANY (SELECT x.id FROM source AS x WHERE x.id = t.id) \
         WHEN MATCHED THEN DELETE";
    let direct_all = "MERGE INTO target AS t USING source AS s \
         ON t.id = ALL (SELECT x.id FROM source AS x WHERE x.id = t.id) \
         WHEN MATCHED THEN DELETE";
    let direct_scalar = "MERGE INTO target AS t USING source AS s \
         ON t.id = (SELECT max(x.id) FROM source AS x WHERE x.id = t.id) \
         WHEN MATCHED THEN DELETE";

    for sql in [
        direct_exists,
        direct_in,
        direct_any,
        direct_all,
        direct_scalar,
    ] {
        let merge_op = merge_operation(&ctx, sql).await;
        assert_eq!(merge_op.target_qualifier(), &TableReference::bare("t"));
        assert!(has_outer_reference_to(
            &merge_op.on,
            &TableReference::bare("t")
        ));
    }

    let shadowed_correlation = "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS t \
           WHERE EXISTS (SELECT 1 FROM source AS x WHERE x.id = t.id)) \
         WHEN MATCHED THEN DELETE";
    let merge_op = merge_operation(&ctx, shadowed_correlation).await;
    assert!(!has_outer_reference_to(
        &merge_op.on,
        &TableReference::bare("t")
    ));

    for sql in [
        // Direct and deeply nested target correlation.
        direct_exists,
        direct_in,
        direct_any,
        direct_all,
        direct_scalar,
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS q \
           WHERE EXISTS (SELECT 1 FROM source AS x WHERE x.id = t.id)) \
         WHEN MATCHED THEN DELETE",
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS q \
           CROSS JOIN LATERAL (SELECT t.id) AS l) \
         WHEN MATCHED THEN DELETE",
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS q CROSS JOIN (\
           source AS x CROSS JOIN LATERAL (SELECT t.id) AS l\
         )) WHEN MATCHED THEN DELETE",
        // Source correlation, no correlation, and nested shadowing.
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS x WHERE x.id = s.id) \
         WHEN MATCHED THEN DELETE",
        "MERGE INTO target AS t USING source AS s \
         ON t.id = ANY (SELECT id FROM source) \
         WHEN MATCHED THEN DELETE",
        shadowed_correlation,
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT EXISTS (\
           SELECT 1 FROM source AS x WHERE x.id = t.id\
         ) FROM source AS t) \
         WHEN MATCHED THEN DELETE",
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS t \
           WHERE EXISTS (SELECT 1 FROM source AS q \
             WHERE EXISTS (SELECT 1 FROM source AS x WHERE x.id = t.id))) \
         WHEN MATCHED THEN DELETE",
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS t \
           CROSS JOIN LATERAL (SELECT t.id) AS l) \
         WHEN MATCHED THEN DELETE",
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT 1 FROM source AS t CROSS JOIN (\
           source AS x CROSS JOIN LATERAL (SELECT t.id) AS l\
         )) WHEN MATCHED THEN DELETE",
    ] {
        assert_merge_physical_error(&ctx, sql, "MERGE INTO not supported for Base table")
            .await;
    }

    // LIMIT expressions intentionally use an empty local scope. The nested
    // `t.id` therefore resolves to the MERGE target rather than `source AS t`,
    // and reaches the existing generic scalar-subquery placement restriction.
    assert_merge_physical_error(
        &ctx,
        "MERGE INTO target AS t USING source AS s \
         ON EXISTS (SELECT t.id FROM source AS t \
           LIMIT (SELECT t.id FROM source AS x LIMIT 1)) \
         WHEN MATCHED THEN DELETE",
        "Correlated scalar subquery can only be used",
    )
    .await;
}

#[tokio::test]
async fn merge_into_requires_boolean_conditions() {
    let ctx = merge_into_context().await;

    for (sql, expected) in [
        (
            "MERGE INTO target USING source ON 1 WHEN MATCHED THEN DELETE",
            "MERGE ON condition must be boolean type, but got Int64",
        ),
        (
            "MERGE INTO target USING source ON true \
             WHEN MATCHED AND 1 THEN DELETE",
            "MERGE WHEN condition must be boolean type, but got Int64",
        ),
        (
            "MERGE INTO target USING source ON NULL \
             WHEN MATCHED AND NULL THEN DELETE",
            "MERGE INTO not supported for Base table",
        ),
    ] {
        assert_merge_physical_error(&ctx, sql, expected).await;
    }
}

#[tokio::test]
async fn invalid_wrapped_negation_fails_during_planning() {
    let ctx = SessionContext::new();
    let err = ctx
        .sql("SELECT * FROM (SELECT 1) WHERE ((-'a') IS NULL)")
        .await
        .unwrap_err();

    assert_contains!(
        err.strip_backtrace(),
        "Unary operator '-' only supports signed numeric, interval and timestamp types"
    );
}

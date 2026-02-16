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

// Include tests in dataframe_functions
mod dataframe_functions;
mod describe;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeListArray,
    FixedSizeListBuilder, Float32Array, Float64Array, Int8Array, Int32Array,
    Int32Builder, LargeListArray, ListArray, ListBuilder, RecordBatch, StringArray,
    StringBuilder, StructBuilder, UInt32Array, UInt32Builder, UnionArray, record_batch,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{
    DataType, Field, Float32Type, Int32Type, Schema, UInt64Type, UnionFields, UnionMode,
};
use arrow::error::ArrowError;
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::{SortOptions, TimeUnit};
use datafusion::{assert_batches_eq, dataframe};
use datafusion_common::metadata::FieldMetadata;
use datafusion_functions_aggregate::count::{count_all, count_all_window};
use datafusion_functions_aggregate::expr_fn::{
    array_agg, avg, avg_distinct, count, count_distinct, max, median, min, sum,
    sum_distinct,
};
use datafusion_functions_nested::make_array::make_array_udf;
use datafusion_functions_window::expr_fn::{first_value, lead, row_number};
use insta::assert_snapshot;
use object_store::local::LocalFileSystem;
use rstest::rstest;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::prelude::{CsvReadOptions, JoinType, ParquetReadOptions};
use datafusion::test_util::{
    parquet_test_data, populate_csv_partitions, register_aggregate_csv, test_table,
    test_table_with_cache_factory, test_table_with_name,
};
use datafusion_catalog::TableProvider;
use datafusion_common::test_util::{batches_to_sort_string, batches_to_string};
use datafusion_common::{
    Constraint, Constraints, DFSchema, DataFusionError, ScalarValue, SchemaError,
    TableReference, UnnestOptions, assert_contains, internal_datafusion_err,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::file_format::format_as_file_type;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::expr::{GroupingSet, NullTreatment, Sort, WindowFunction};
use datafusion_expr::var_provider::{VarProvider, VarType};
use datafusion_expr::{
    Expr, ExprFunctionExt, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
    ScalarFunctionImplementation, SortExpr, TableType, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunctionDefinition, cast, col, create_udf, exists,
    in_subquery, lit, out_ref_col, placeholder, scalar_subquery, when, wildcard,
};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};

use datafusion::error::Result as DataFusionResult;
use datafusion::execution::options::JsonReadOptions;
use datafusion_functions_window::expr_fn::lag;

// Get string representation of the plan
async fn physical_plan_to_string(df: &DataFrame) -> String {
    let physical_plan = df
        .clone()
        .create_physical_plan()
        .await
        .expect("Error creating physical plan");

    let formatted = displayable(physical_plan.as_ref()).indent(true);
    formatted.to_string()
}

pub fn table_with_constraints() -> Arc<dyn TableProvider> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        dual_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(dual_schema, vec![vec![batch]])
        .unwrap()
        .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
            vec![0],
        )]));
    Arc::new(provider)
}

async fn assert_logical_expr_schema_eq_physical_expr_schema(df: DataFrame) -> Result<()> {
    let logical_expr_schema = Arc::clone(df.schema().inner());
    let batches = df.collect().await?;
    let physical_expr_schema = batches[0].schema();
    assert_eq!(logical_expr_schema, physical_expr_schema);
    Ok(())
}

#[tokio::test]
async fn test_array_agg_ord_schema() -> Result<()> {
    let ctx = SessionContext::new();

    let create_table_query = r#"
            CREATE TABLE test_table (
                "double_field" DOUBLE,
                "string_field" VARCHAR
            ) AS VALUES
                (1.0, 'a'),
                (2.0, 'b'),
                (3.0, 'c')
        "#;
    ctx.sql(create_table_query).await?;

    let query = r#"SELECT
        array_agg("double_field" ORDER BY "string_field") as "double_field",
        array_agg("string_field" ORDER BY "string_field") as "string_field"
    FROM test_table"#;

    let result = ctx.sql(query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

type WindowFnCase = (fn() -> Expr, &'static str);

#[tokio::test]
async fn with_column_window_functions() -> DataFusionResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    // Define test cases: (expr builder, alias name)
    let test_cases: Vec<WindowFnCase> = vec![
        (|| lag(col("a"), Some(1), None), "lag_val"),
        (|| lead(col("a"), Some(1), None), "lead_val"),
        (row_number, "row_num"),
    ];

    for (make_expr, alias) in test_cases {
        let df = ctx.table("t").await?;
        let expr = make_expr();
        let df_with = df.with_column(alias, expr)?;
        let df_schema = df_with.schema().clone();

        assert!(
            df_schema.has_column_with_unqualified_name(alias),
            "Schema does not contain expected column {alias}",
        );

        assert_eq!(2, df_schema.columns().len());
    }

    Ok(())
}

#[tokio::test]
async fn test_coalesce_schema() -> Result<()> {
    let ctx = SessionContext::new();

    let query = r#"SELECT COALESCE(null, 5)"#;

    let result = ctx.sql(query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

#[tokio::test]
async fn test_coalesce_from_values_schema() -> Result<()> {
    let ctx = SessionContext::new();

    let query = r#"SELECT COALESCE(column1, column2) FROM VALUES (null, 1.2)"#;

    let result = ctx.sql(query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

#[tokio::test]
async fn test_coalesce_from_values_schema_multiple_rows() -> Result<()> {
    let ctx = SessionContext::new();

    let query = r#"SELECT COALESCE(column1, column2)
        FROM VALUES
        (null, 1.2),
        (1.1, null),
        (2, 5);"#;

    let result = ctx.sql(query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

#[tokio::test]
async fn test_array_agg_schema() -> Result<()> {
    let ctx = SessionContext::new();

    let create_table_query = r#"
            CREATE TABLE test_table (
                "double_field" DOUBLE,
                "string_field" VARCHAR
            ) AS VALUES
                (1.0, 'a'),
                (2.0, 'b'),
                (3.0, 'c')
        "#;
    ctx.sql(create_table_query).await?;

    let query = r#"SELECT
        array_agg("double_field") as "double_field",
        array_agg("string_field") as "string_field"
    FROM test_table"#;

    let result = ctx.sql(query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

#[tokio::test]
async fn test_array_agg_distinct_schema() -> Result<()> {
    let ctx = SessionContext::new();

    let create_table_query = r#"
            CREATE TABLE test_table (
                "double_field" DOUBLE,
                "string_field" VARCHAR
            ) AS VALUES
                (1.0, 'a'),
                (2.0, 'b'),
                (2.0, 'a')
        "#;
    ctx.sql(create_table_query).await?;

    let query = r#"SELECT
        array_agg(distinct "double_field") as "double_field",
        array_agg(distinct "string_field") as "string_field"
    FROM test_table"#;

    let result = ctx.sql(query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

#[tokio::test]
async fn select_columns() -> Result<()> {
    // build plan using Table API

    let t = test_table().await?;
    let t2 = t.select_columns(&["c1", "c2", "c11"])?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100").await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn select_columns_with_nonexistent_columns() -> Result<()> {
    let t = test_table().await?;
    let t2 = t.select_columns(&["canada", "c2", "rocks"]);

    match t2 {
        Err(DataFusionError::SchemaError(boxed_err, _)) => {
            // Verify it's the first invalid column
            match boxed_err.as_ref() {
                SchemaError::FieldNotFound { field, .. } => {
                    assert_eq!(field.name(), "canada");
                }
                _ => panic!("Expected SchemaError::FieldNotFound for 'canada'"),
            }
        }
        _ => panic!("Expected SchemaError"),
    }

    Ok(())
}

#[tokio::test]
async fn select_expr() -> Result<()> {
    // build plan using Table API
    let t = test_table().await?;
    let t2 = t.select(vec![col("c1"), col("c2"), col("c11")])?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100").await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn select_all() -> Result<()> {
    let t = test_table().await?;
    let plan = t.select([wildcard()])?.logical_plan().clone();
    let sql_plan = create_plan("SELECT * FROM aggregate_test_100").await?;
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn select_exprs() -> Result<()> {
    // build plan using `select_expr``
    let t = test_table().await?;
    let plan = t
        .clone()
        .select_exprs(&["c1", "c2", "c11", "c2 * c11"])?
        .logical_plan()
        .clone();

    // build plan using select
    let expected_plan = t
        .select(vec![
            col("c1"),
            col("c2"),
            col("c11"),
            col("c2") * col("c11"),
        ])?
        .logical_plan()
        .clone();

    assert_same_plan(&expected_plan, &plan);

    Ok(())
}

#[tokio::test]
async fn select_with_window_exprs() -> Result<()> {
    // build plan using Table API
    let t = test_table().await?;
    let first_row = first_value(col("aggregate_test_100.c1"))
        .partition_by(vec![col("aggregate_test_100.c2")])
        .build()
        .unwrap();
    let t2 = t.select(vec![col("c1"), first_row])?;
    let plan = t2.logical_plan().clone();

    let sql_plan = create_plan(
        "select c1, first_value(c1) over (partition by c2) from aggregate_test_100",
    )
    .await?;

    assert_same_plan(&plan, &sql_plan);
    Ok(())
}

#[tokio::test]
async fn select_with_periods() -> Result<()> {
    // define data with a column name that has a "." in it:
    let array: Int32Array = [1, 10].into_iter().collect();
    let batch = RecordBatch::try_from_iter(vec![("f.c1", Arc::new(array) as _)])?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx.table("t").await?.select_columns(&["f.c1"])?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +------+
    | f.c1 |
    +------+
    | 1    |
    | 10   |
    +------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn select_columns_duplicated_names_from_different_qualifiers() -> Result<()> {
    let t1 = test_table_with_name("t1")
        .await?
        .select_columns(&["c1"])?
        .limit(0, Some(3))?;
    let t2 = test_table_with_name("t2")
        .await?
        .select_columns(&["c1"])?
        .limit(3, Some(3))?;
    let t3 = test_table_with_name("t3")
        .await?
        .select_columns(&["c1"])?
        .limit(6, Some(3))?;

    let join_res = t1
        .join(t2, JoinType::Left, &["t1.c1"], &["t2.c1"], None)?
        .join(t3, JoinType::Left, &["t1.c1"], &["t3.c1"], None)?;
    assert_snapshot!(
        batches_to_sort_string(&join_res.clone().collect().await.unwrap()),
        @r"
    +----+----+----+
    | c1 | c1 | c1 |
    +----+----+----+
    | b  | b  |    |
    | b  | b  |    |
    | c  |    |    |
    | d  |    | d  |
    +----+----+----+
    "
    );

    let select_res = join_res.select_columns(&["c1"])?;
    assert_snapshot!(
        batches_to_sort_string(&select_res.clone().collect().await.unwrap()),
        @r"
    +----+----+----+
    | c1 | c1 | c1 |
    +----+----+----+
    | b  | b  |    |
    | b  | b  |    |
    | c  |    |    |
    | d  |    | d  |
    +----+----+----+
    "
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns() -> Result<()> {
    // build plan using Table API
    let t = test_table().await?;
    let t2 = t.drop_columns(&["c2", "c11"])?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan =
        create_plan("SELECT c1,c3,c4,c5,c6,c7,c8,c9,c10,c12,c13 FROM aggregate_test_100")
            .await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn drop_columns_with_duplicates() -> Result<()> {
    // build plan using Table API
    let t = test_table().await?;
    let t2 = t.drop_columns(&["c2", "c11", "c2", "c2"])?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan =
        create_plan("SELECT c1,c3,c4,c5,c6,c7,c8,c9,c10,c12,c13 FROM aggregate_test_100")
            .await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn drop_columns_with_nonexistent_columns() -> Result<()> {
    // build plan using Table API
    let t = test_table().await?;
    let t2 = t.drop_columns(&["canada", "c2", "rocks"])?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan = create_plan(
        "SELECT c1,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 FROM aggregate_test_100",
    )
    .await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn drop_columns_with_empty_array() -> Result<()> {
    // build plan using Table API
    let t = test_table().await?;
    let drop_columns = vec![] as Vec<&str>;
    let t2 = t.drop_columns(&drop_columns)?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan = create_plan(
        "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 FROM aggregate_test_100",
    )
    .await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn drop_columns_qualified() -> Result<()> {
    // build plan using Table API
    let mut t = test_table().await?;
    t = t.select_columns(&["c1", "c2", "c11"])?;
    let mut t2 = test_table_with_name("another_table").await?;
    t2 = t2.select_columns(&["c1", "c2", "c11"])?;
    let mut t3 = t.join_on(
        t2,
        JoinType::Inner,
        [col("aggregate_test_100.c1").eq(col("another_table.c1"))],
    )?;
    t3 = t3.drop_columns(&["another_table.c2", "another_table.c11"])?;

    let plan = t3.logical_plan().clone();

    let sql = "SELECT aggregate_test_100.c1, aggregate_test_100.c2, aggregate_test_100.c11, another_table.c1 FROM (SELECT c1, c2, c11 FROM aggregate_test_100) INNER JOIN (SELECT c1, c2, c11 FROM another_table) ON aggregate_test_100.c1 = another_table.c1";
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx, "aggregate_test_100").await?;
    register_aggregate_csv(&ctx, "another_table").await?;
    let sql_plan = ctx.sql(sql).await?.into_unoptimized_plan();

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn drop_columns_qualified_find_qualified() -> Result<()> {
    // build plan using Table API
    let mut t = test_table().await?;
    t = t.select_columns(&["c1", "c2", "c11"])?;
    let mut t2 = test_table_with_name("another_table").await?;
    t2 = t2.select_columns(&["c1", "c2", "c11"])?;
    let mut t3 = t.join_on(
        t2.clone(),
        JoinType::Inner,
        [col("aggregate_test_100.c1").eq(col("another_table.c1"))],
    )?;
    t3 = t3.drop_columns(&t2.find_qualified_columns(&["c2", "c11"])?)?;

    let plan = t3.logical_plan().clone();

    let sql = "SELECT aggregate_test_100.c1, aggregate_test_100.c2, aggregate_test_100.c11, another_table.c1 FROM (SELECT c1, c2, c11 FROM aggregate_test_100) INNER JOIN (SELECT c1, c2, c11 FROM another_table) ON aggregate_test_100.c1 = another_table.c1";
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx, "aggregate_test_100").await?;
    register_aggregate_csv(&ctx, "another_table").await?;
    let sql_plan = ctx.sql(sql).await?.into_unoptimized_plan();

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn test_find_qualified_names() -> Result<()> {
    let t = test_table().await?;
    let column_names = ["c1", "c2", "c3"];
    let columns = t.find_qualified_columns(&column_names)?;

    // Expected results for each column
    let binding = TableReference::bare("aggregate_test_100");
    let expected = [
        (Some(&binding), "c1"),
        (Some(&binding), "c2"),
        (Some(&binding), "c3"),
    ];

    // Verify we got the expected number of results
    assert_eq!(
        columns.len(),
        expected.len(),
        "Expected {} columns, got {}",
        expected.len(),
        columns.len()
    );

    // Iterate over the results and check each one individually
    for (i, (actual, expected)) in columns.iter().zip(expected.iter()).enumerate() {
        let (actual_table_ref, actual_field_ref) = actual;
        let (expected_table_ref, expected_field_name) = expected;

        // Check table reference
        assert_eq!(
            actual_table_ref, expected_table_ref,
            "Column {i}: expected table reference {expected_table_ref:?}, got {actual_table_ref:?}"
        );

        // Check field name
        assert_eq!(
            actual_field_ref.name(),
            *expected_field_name,
            "Column {i}: expected field name '{expected_field_name}', got '{actual_field_ref}'"
        );
    }

    Ok(())
}

#[tokio::test]
async fn drop_with_quotes() -> Result<()> {
    // define data with a column name that has a "." in it:
    let array1: Int32Array = [1, 10].into_iter().collect();
    let array2: Int32Array = [2, 11].into_iter().collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("f\"c1", Arc::new(array1) as _),
        ("f\"c2", Arc::new(array2) as _),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx.table("t").await?.drop_columns(&["f\"c1"])?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r#"
    +------+
    | f"c2 |
    +------+
    | 11   |
    | 2    |
    +------+
    "#
    );

    Ok(())
}

#[tokio::test]
async fn drop_with_periods() -> Result<()> {
    // define data with a column name that has a "." in it:
    let array1: Int32Array = [1, 10].into_iter().collect();
    let array2: Int32Array = [2, 11].into_iter().collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("f.c1", Arc::new(array1) as _),
        ("f.c2", Arc::new(array2) as _),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx.table("t").await?.drop_columns(&["\"f.c1\""])?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +------+
    | f.c2 |
    +------+
    | 11   |
    | 2    |
    +------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn drop_columns_duplicated_names_from_different_qualifiers() -> Result<()> {
    let t1 = test_table_with_name("t1")
        .await?
        .select_columns(&["c1"])?
        .limit(0, Some(3))?;
    let t2 = test_table_with_name("t2")
        .await?
        .select_columns(&["c1"])?
        .limit(3, Some(3))?;
    let t3 = test_table_with_name("t3")
        .await?
        .select_columns(&["c1"])?
        .limit(6, Some(3))?;

    let join_res = t1
        .join(t2, JoinType::LeftMark, &["c1"], &["c1"], None)?
        .join(t3, JoinType::LeftMark, &["c1"], &["c1"], None)?;
    assert_snapshot!(
        batches_to_sort_string(&join_res.clone().collect().await.unwrap()),
        @r"
    +----+-------+-------+
    | c1 | mark  | mark  |
    +----+-------+-------+
    | b  | true  | false |
    | c  | false | false |
    | d  | false | true  |
    +----+-------+-------+
    "
    );

    let drop_res = join_res.drop_columns(&["mark"])?;
    assert_snapshot!(
        batches_to_sort_string(&drop_res.clone().collect().await.unwrap()),
        @r"
    +----+
    | c1 |
    +----+
    | b  |
    | c  |
    | d  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn aggregate() -> Result<()> {
    // build plan using DataFrame API
    // union so some of the distincts have a clearly distinct result
    let df = test_table().await?.union(test_table().await?)?;
    let group_expr = vec![col("c1")];
    let aggr_expr = vec![
        min(col("c4")).alias("min(c4)"),
        max(col("c4")).alias("max(c4)"),
        avg(col("c4")).alias("avg(c4)"),
        avg_distinct(col("c4")).alias("avg_distinct(c4)"),
        sum(col("c4")).alias("sum(c4)"),
        sum_distinct(col("c4")).alias("sum_distinct(c4)"),
        count(col("c4")).alias("count(c4)"),
        count_distinct(col("c4")).alias("count_distinct(c4)"),
    ];

    let df: Vec<RecordBatch> = df.aggregate(group_expr, aggr_expr)?.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df),
        @r"
    +----+---------+---------+---------------------+---------------------+---------+------------------+-----------+--------------------+
    | c1 | min(c4) | max(c4) | avg(c4)             | avg_distinct(c4)    | sum(c4) | sum_distinct(c4) | count(c4) | count_distinct(c4) |
    +----+---------+---------+---------------------+---------------------+---------+------------------+-----------+--------------------+
    | a  | -28462  | 32064   | 306.04761904761904  | 306.04761904761904  | 12854   | 6427             | 42        | 21                 |
    | b  | -28070  | 25286   | 7732.315789473684   | 7732.315789473684   | 293828  | 146914           | 38        | 19                 |
    | c  | -30508  | 29106   | -1320.5238095238096 | -1320.5238095238096 | -55462  | -27731           | 42        | 21                 |
    | d  | -24558  | 31106   | 10890.111111111111  | 10890.111111111111  | 392044  | 196022           | 36        | 18                 |
    | e  | -31500  | 32514   | -4268.333333333333  | -4268.333333333333  | -179270 | -89635           | 42        | 21                 |
    +----+---------+---------+---------------------+---------------------+---------+------------------+-----------+--------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn aggregate_assert_no_empty_batches() -> Result<()> {
    // build plan using DataFrame API
    let df = test_table().await?;
    let group_expr = vec![col("c1")];
    let aggr_expr = vec![
        min(col("c12")),
        max(col("c12")),
        avg(col("c12")),
        avg_distinct(col("c12")),
        sum(col("c12")),
        sum_distinct(col("c12")),
        count(col("c12")),
        count_distinct(col("c12")),
        median(col("c12")),
    ];

    let df: Vec<RecordBatch> = df.aggregate(group_expr, aggr_expr)?.collect().await?;
    // Empty batches should not be produced
    for batch in df {
        assert!(batch.num_rows() > 0);
    }

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_pk() -> Result<()> {
    // create the dataframe
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(table_with_constraints())?;

    // GROUP BY id
    let group_expr = vec![col("id")];
    let aggr_expr = vec![];
    let df = df.aggregate(group_expr, aggr_expr)?;

    // Since id and name are functionally dependant, we can use name among
    // expression even if it is not part of the group by expression and can
    // select "name" column even though it wasn't explicitly grouped
    let df = df.select(vec![col("id"), col("name")])?;

    assert_snapshot!(
        physical_plan_to_string(&df).await,
        @r"
    AggregateExec: mode=Single, gby=[id@0 as id, name@1 as name], aggr=[]
      DataSourceExec: partitions=1, partition_sizes=[1]
    "
    );

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+------+
    | id | name |
    +----+------+
    | 1  | a    |
    +----+------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_pk2() -> Result<()> {
    // create the dataframe
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(table_with_constraints())?;

    // GROUP BY id
    let group_expr = vec![col("id")];
    let aggr_expr = vec![];
    let df = df.aggregate(group_expr, aggr_expr)?;

    // Predicate refers to id, and name fields:
    // id = 1 AND name = 'a'
    let predicate = col("id").eq(lit(1i32)).and(col("name").eq(lit("a")));
    let df = df.filter(predicate)?;
    assert_snapshot!(
        physical_plan_to_string(&df).await,
        @r"
    AggregateExec: mode=Single, gby=[id@0 as id, name@1 as name], aggr=[], ordering_mode=Sorted
      FilterExec: id@0 = 1 AND name@1 = a
        DataSourceExec: partitions=1, partition_sizes=[1]
    "
    );

    // Since id and name are functionally dependant, we can use name among expression
    // even if it is not part of the group by expression.
    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+------+
    | id | name |
    +----+------+
    | 1  | a    |
    +----+------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_pk3() -> Result<()> {
    // create the dataframe
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(table_with_constraints())?;

    // GROUP BY id
    let group_expr = vec![col("id")];
    let aggr_expr = vec![];
    // group by id,
    let df = df.aggregate(group_expr, aggr_expr)?;

    // Predicate refers to id field
    // id = 1
    let predicate = col("id").eq(lit(1i32));
    let df = df.filter(predicate)?;
    // Select expression refers to id, and name columns.
    // id, name
    let df = df.select(vec![col("id"), col("name")])?;
    assert_snapshot!(
        physical_plan_to_string(&df).await,
        @r"
    AggregateExec: mode=Single, gby=[id@0 as id, name@1 as name], aggr=[], ordering_mode=PartiallySorted([0])
      FilterExec: id@0 = 1
        DataSourceExec: partitions=1, partition_sizes=[1]
    "
    );

    // Since id and name are functionally dependant, we can use name among expression
    // even if it is not part of the group by expression.
    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+------+
    | id | name |
    +----+------+
    | 1  | a    |
    +----+------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_pk4() -> Result<()> {
    // create the dataframe
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let df = ctx.read_table(table_with_constraints())?;

    // GROUP BY id
    let group_expr = vec![col("id")];
    let aggr_expr = vec![];
    let df = df.aggregate(group_expr, aggr_expr)?;

    // Predicate refers to id field
    // id = 1
    let predicate = col("id").eq(lit(1i32));
    let df = df.filter(predicate)?;
    // Select expression refers to id column.
    // id
    let df = df.select(vec![col("id")])?;

    // In this case aggregate shouldn't be expanded, since these
    // columns are not used.
    assert_snapshot!(
        physical_plan_to_string(&df).await,
        @r"
    AggregateExec: mode=Single, gby=[id@0 as id], aggr=[], ordering_mode=Sorted
      FilterExec: id@0 = 1
        DataSourceExec: partitions=1, partition_sizes=[1]
    "
    );

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+
    | id |
    +----+
    | 1  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_alias() -> Result<()> {
    let df = test_table().await?;

    let df = df
        // GROUP BY `c2 + 1`
        .aggregate(vec![col("c2") + lit(1)], vec![])?
        // SELECT `c2 + 1` as c2
        .select(vec![(col("c2") + lit(1)).alias("c2")])?
        // GROUP BY c2 as "c2" (alias in expr is not supported by SQL)
        .aggregate(vec![col("c2").alias("c2")], vec![])?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+
    | c2 |
    +----+
    | 2  |
    | 3  |
    | 4  |
    | 5  |
    | 6  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_union() -> Result<()> {
    let df = test_table().await?;

    let df1 = df
        .clone()
        // GROUP BY `c1`
        .aggregate(vec![col("c1")], vec![min(col("c2"))])?
        // SELECT `c1` , min(c2) as `result`
        .select(vec![col("c1"), min(col("c2")).alias("result")])?;
    let df2 = df
        .clone()
        // GROUP BY `c1`
        .aggregate(vec![col("c1")], vec![max(col("c3"))])?
        // SELECT `c1` , max(c3) as `result`
        .select(vec![col("c1"), max(col("c3")).alias("result")])?;

    let df_union = df1.union(df2)?;
    let df = df_union
        // GROUP BY `c1`
        .aggregate(
            vec![col("c1")],
            vec![sum(col("result")).alias("sum_result")],
        )?
        // SELECT `c1`, sum(result) as `sum_result`
        .select(vec![(col("c1")), col("sum_result")])?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+------------+
    | c1 | sum_result |
    +----+------------+
    | a  | 84         |
    | b  | 69         |
    | c  | 124        |
    | d  | 126        |
    | e  | 121        |
    +----+------------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_aggregate_subexpr() -> Result<()> {
    let df = test_table().await?;

    let group_expr = col("c2") + lit(1);
    let aggr_expr = sum(col("c3") + lit(2));

    let df = df
        // GROUP BY `c2 + 1`
        .aggregate(vec![group_expr.clone()], vec![aggr_expr.clone()])?
        // SELECT `c2 + 1` as c2 + 10, sum(c3 + 2) + 20
        // SELECT expressions contain aggr_expr and group_expr as subexpressions
        .select(vec![
            group_expr.alias("c2") + lit(10),
            (aggr_expr + lit(20)).alias("sum"),
        ])?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----------------+------+
    | c2 + Int32(10) | sum  |
    +----------------+------+
    | 12             | 431  |
    | 13             | 248  |
    | 14             | 453  |
    | 15             | 95   |
    | 16             | -146 |
    +----------------+------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_name_collision() -> Result<()> {
    let df = test_table().await?;

    let collided_alias = "aggregate_test_100.c2 + aggregate_test_100.c3";
    let group_expr = lit(1).alias(collided_alias);

    let df = df
        // GROUP BY 1
        .aggregate(vec![group_expr], vec![])?
        // SELECT `aggregate_test_100.c2 + aggregate_test_100.c3`
        .select(vec![
            (col("aggregate_test_100.c2") + col("aggregate_test_100.c3")),
        ])
        // The select expr has the same display_name as the group_expr,
        // but since they are different expressions, it should fail.
        .expect_err("Expected error");
    assert_snapshot!(df.strip_backtrace(), @r#"Schema error: No field named aggregate_test_100.c2. Valid fields are "aggregate_test_100.c2 + aggregate_test_100.c3"."#);

    Ok(())
}

#[tokio::test]
async fn window_using_aggregates() -> Result<()> {
    // build plan using DataFrame API
    let df = test_table().await?.filter(col("c1").eq(lit("a")))?;
    let mut aggr_expr = vec![
        (
            datafusion_functions_aggregate::first_last::first_value_udaf(),
            "first_value",
        ),
        (
            datafusion_functions_aggregate::first_last::last_value_udaf(),
            "last_val",
        ),
        (
            datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf(),
            "approx_distinct",
        ),
        (
            datafusion_functions_aggregate::approx_median::approx_median_udaf(),
            "approx_median",
        ),
        (
            datafusion_functions_aggregate::median::median_udaf(),
            "median",
        ),
        (datafusion_functions_aggregate::min_max::max_udaf(), "max"),
        (datafusion_functions_aggregate::min_max::min_udaf(), "min"),
    ]
    .into_iter()
    .map(|(func, name)| {
        let w = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(func),
            vec![col("c3")],
        );

        Expr::from(w)
            .null_treatment(NullTreatment::IgnoreNulls)
            .order_by(vec![col("c2").sort(true, true), col("c3").sort(true, true)])
            .window_frame(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            ))
            .build()
            .unwrap()
            .alias(name)
    })
    .collect::<Vec<_>>();
    aggr_expr.extend_from_slice(&[col("c2"), col("c3")]);

    let df: Vec<RecordBatch> = df.select(aggr_expr)?.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df),
        @r"
    +-------------+----------+-----------------+---------------+--------+-----+------+----+------+
    | first_value | last_val | approx_distinct | approx_median | median | max | min  | c2 | c3   |
    +-------------+----------+-----------------+---------------+--------+-----+------+----+------+
    |             |          |                 |               |        |     |      | 1  | -85  |
    | -85         | -101     | 14              | -12           | -12    | 83  | -101 | 4  | -54  |
    | -85         | -101     | 17              | -25           | -25    | 83  | -101 | 5  | -31  |
    | -85         | -12      | 10              | -32           | -34    | 83  | -85  | 3  | 13   |
    | -85         | -25      | 3               | -56           | -56    | -25 | -85  | 1  | -5   |
    | -85         | -31      | 18              | -29           | -28    | 83  | -101 | 5  | 36   |
    | -85         | -38      | 16              | -25           | -25    | 83  | -101 | 4  | 65   |
    | -85         | -43      | 7               | -43           | -43    | 83  | -85  | 2  | 45   |
    | -85         | -48      | 6               | -35           | -36    | 83  | -85  | 2  | -43  |
    | -85         | -5       | 4               | -37           | -40    | -5  | -85  | 1  | 83   |
    | -85         | -54      | 15              | -17           | -18    | 83  | -101 | 4  | -38  |
    | -85         | -56      | 2               | -70           | -70    | -56 | -85  | 1  | -25  |
    | -85         | -72      | 9               | -43           | -43    | 83  | -85  | 3  | -12  |
    | -85         | -85      | 1               | -85           | -85    | -85 | -85  | 1  | -56  |
    | -85         | 13       | 11              | -17           | -18    | 83  | -85  | 3  | 14   |
    | -85         | 13       | 11              | -25           | -25    | 83  | -85  | 3  | 13   |
    | -85         | 14       | 12              | -12           | -12    | 83  | -85  | 3  | 17   |
    | -85         | 17       | 13              | -11           | -8     | 83  | -85  | 4  | -101 |
    | -85         | 45       | 8               | -34           | -34    | 83  | -85  | 3  | -72  |
    | -85         | 65       | 17              | -17           | -18    | 83  | -101 | 5  | -101 |
    | -85         | 83       | 5               | -25           | -25    | 83  | -85  | 2  | -48  |
    +-------------+----------+-----------------+---------------+--------+-----+------+----+------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn window_aggregates_with_filter() -> Result<()> {
    // Define a small in-memory table to make expected values clear
    let ts: Int32Array = [1, 2, 3, 4, 5].into_iter().collect();
    let val: Int32Array = [-3, -2, 1, 4, -1].into_iter().collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(ts) as _),
        ("val", Arc::new(val) as _),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx.table("t").await?;

    // Build filtered window aggregates over ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    let mut exprs = vec![
        (datafusion_functions_aggregate::sum::sum_udaf(), "sum_pos"),
        (
            datafusion_functions_aggregate::average::avg_udaf(),
            "avg_pos",
        ),
        (
            datafusion_functions_aggregate::min_max::min_udaf(),
            "min_pos",
        ),
        (
            datafusion_functions_aggregate::min_max::max_udaf(),
            "max_pos",
        ),
        (
            datafusion_functions_aggregate::count::count_udaf(),
            "cnt_pos",
        ),
    ]
    .into_iter()
    .map(|(func, alias)| {
        let w = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(func),
            vec![col("val")],
        );

        Expr::from(w)
            .order_by(vec![col("ts").sort(true, true)])
            .window_frame(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            ))
            .filter(col("val").gt(lit(0)))
            .build()
            .unwrap()
            .alias(alias)
    })
    .collect::<Vec<_>>();
    exprs.extend_from_slice(&[col("ts"), col("val")]);

    let results = df.select(exprs)?.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +---------+---------+---------+---------+---------+----+-----+
    | sum_pos | avg_pos | min_pos | max_pos | cnt_pos | ts | val |
    +---------+---------+---------+---------+---------+----+-----+
    |         |         |         |         | 0       | 1  | -3  |
    |         |         |         |         | 0       | 2  | -2  |
    | 1       | 1.0     | 1       | 1       | 1       | 3  | 1   |
    | 5       | 2.5     | 1       | 4       | 2       | 4  | 4   |
    | 5       | 2.5     | 1       | 4       | 2       | 5  | -1  |
    +---------+---------+---------+---------+---------+----+-----+
    "
    );

    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/10346
#[tokio::test]
async fn test_select_over_aggregate_schema() -> Result<()> {
    let df = test_table()
        .await?
        .with_column("c", col("c1"))?
        .aggregate(vec![], vec![array_agg(col("c")).alias("c")])?
        .select(vec![col("c")])?;

    assert_eq!(df.schema().fields().len(), 1);
    let field = df.schema().field(0);
    // There are two columns named 'c', one from the input of the aggregate and the other from the output.
    // Select should return the column from the output of the aggregate, which is a list.
    assert!(matches!(field.data_type(), DataType::List(_)));

    Ok(())
}

#[tokio::test]
async fn test_distinct() -> Result<()> {
    let t = test_table().await?;
    let plan = t
        .select(vec![col("c1")])
        .unwrap()
        .distinct()
        .unwrap()
        .logical_plan()
        .clone();

    let sql_plan = create_plan("select distinct c1 from aggregate_test_100").await?;

    assert_same_plan(&plan, &sql_plan);
    Ok(())
}

#[tokio::test]
async fn test_distinct_sort_by() -> Result<()> {
    let t = test_table().await?;
    let plan = t
        .select(vec![col("c1")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![col("c1").sort(true, true)])
        .unwrap();

    let df_results = plan.clone().collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+
    | c1 |
    +----+
    | a  |
    | b  |
    | c  |
    | d  |
    | e  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_distinct_sort_by_unprojected() -> Result<()> {
    let t = test_table().await?;
    let err = t
        .select(vec![col("c1")])
        .unwrap()
        .distinct()
        .unwrap()
        // try to sort on some value not present in input to distinct
        .sort(vec![col("c2").sort(true, true)])
        .unwrap_err();
    assert_snapshot!(err.strip_backtrace(), @"Error during planning: For SELECT DISTINCT, ORDER BY expressions c2 must appear in select list");

    Ok(())
}

#[tokio::test]
async fn test_distinct_on() -> Result<()> {
    let t = test_table().await?;
    let plan = t
        .distinct_on(vec![col("c1")], vec![col("aggregate_test_100.c1")], None)
        .unwrap();

    let sql_plan =
        create_plan("select distinct on (c1) c1 from aggregate_test_100").await?;

    assert_same_plan(&plan.logical_plan().clone(), &sql_plan);

    let df_results = plan.clone().collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+
    | c1 |
    +----+
    | a  |
    | b  |
    | c  |
    | d  |
    | e  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_distinct_on_sort_by() -> Result<()> {
    let t = test_table().await?;
    let plan = t
        .select(vec![col("c1")])
        .unwrap()
        .distinct_on(
            vec![col("c1")],
            vec![col("c1")],
            Some(vec![col("c1").sort(true, true)]),
        )
        .unwrap()
        .sort(vec![col("c1").sort(true, true)])
        .unwrap();

    let df_results = plan.clone().collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+
    | c1 |
    +----+
    | a  |
    | b  |
    | c  |
    | d  |
    | e  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_distinct_on_sort_by_unprojected() -> Result<()> {
    let t = test_table().await?;
    let err = t
        .select(vec![col("c1")])
        .unwrap()
        .distinct_on(
            vec![col("c1")],
            vec![col("c1")],
            Some(vec![col("c1").sort(true, true)]),
        )
        .unwrap()
        // try to sort on some value not present in input to distinct
        .sort(vec![col("c2").sort(true, true)])
        .unwrap_err();
    assert_snapshot!(err.strip_backtrace(), @"Error during planning: For SELECT DISTINCT, ORDER BY expressions c2 must appear in select list");

    Ok(())
}

#[tokio::test]
async fn join() -> Result<()> {
    let left = test_table().await?.select_columns(&["c1", "c2"])?;
    let right = test_table_with_name("c2")
        .await?
        .select_columns(&["c1", "c3"])?;
    let left_rows = left.clone().collect().await?;
    let right_rows = right.clone().collect().await?;
    let join = left.join(right, JoinType::Inner, &["c1"], &["c1"], None)?;
    let join_rows = join.collect().await?;
    assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    assert_eq!(2008, join_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    Ok(())
}

#[tokio::test]
async fn join_coercion_unnamed() -> Result<()> {
    let ctx = SessionContext::new();

    // Test that join will coerce column types when necessary
    // even when the relations don't have unique names
    let left = ctx.read_batch(record_batch!(
        ("id", Int32, [1, 2, 3]),
        ("name", Utf8, ["a", "b", "c"])
    )?)?;
    let right = ctx.read_batch(record_batch!(
        ("id", Int32, [10, 3]),
        ("name", Utf8View, ["d", "c"]) // Utf8View is a different type
    )?)?;
    let cols = vec!["name", "id"];

    let filter = None;
    let join = right.join(left, JoinType::LeftAnti, &cols, &cols, filter)?;
    let results = join.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+------+
    | id | name |
    +----+------+
    | 10 | d    |
    +----+------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_on() -> Result<()> {
    let left = test_table_with_name("a")
        .await?
        .select_columns(&["c1", "c2"])?;
    let right = test_table_with_name("b")
        .await?
        .select_columns(&["c1", "c2"])?;
    let join = left.join_on(
        right,
        JoinType::Inner,
        [col("a.c1").not_eq(col("b.c1")), col("a.c2").eq(col("b.c2"))],
    )?;

    assert_snapshot!(join.logical_plan(), @r"
    Inner Join:  Filter: a.c1 != b.c1 AND a.c2 = b.c2
      Projection: a.c1, a.c2
        TableScan: a
      Projection: b.c1, b.c2
        TableScan: b
    ");

    Ok(())
}

#[tokio::test]
async fn join_on_filter_datatype() -> Result<()> {
    let left = test_table_with_name("a").await?.select_columns(&["c1"])?;
    let right = test_table_with_name("b").await?.select_columns(&["c1"])?;

    // JOIN ON untyped NULL
    let join = left.clone().join_on(
        right.clone(),
        JoinType::Inner,
        Some(Expr::Literal(ScalarValue::Null, None)),
    )?;
    assert_snapshot!(join.into_optimized_plan().unwrap(), @"EmptyRelation: rows=0");

    // JOIN ON expression must be boolean type
    let join = left.join_on(right, JoinType::Inner, Some(lit("TRUE")))?;
    let err = join.into_optimized_plan().unwrap_err();
    assert_snapshot!(
        err.strip_backtrace(),
        @r"
    type_coercion
    caused by
    Error during planning: Join condition must be boolean type, but got Utf8
    "
    );
    Ok(())
}

#[tokio::test]
async fn join_ambiguous_filter() -> Result<()> {
    let left = test_table_with_name("a")
        .await?
        .select_columns(&["c1", "c2"])?;
    let right = test_table_with_name("b")
        .await?
        .select_columns(&["c1", "c2"])?;

    let join = left
        .join_on(right, JoinType::Inner, [col("c1").eq(col("c1"))])
        .expect_err("join didn't fail check");
    assert_snapshot!(join.strip_backtrace(), @"Schema error: Ambiguous reference to unqualified field c1");

    Ok(())
}

#[tokio::test]
async fn limit() -> Result<()> {
    // build query using Table API
    let t = test_table().await?;
    let t2 = t.select_columns(&["c1", "c2", "c11"])?.limit(0, Some(10))?;
    let plan = t2.logical_plan().clone();

    // build query using SQL
    let sql_plan =
        create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10").await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn df_count() -> Result<()> {
    let count = test_table().await?.count().await?;
    assert_eq!(100, count);
    Ok(())
}

#[tokio::test]
async fn explain() -> Result<()> {
    // build query using Table API
    let df = test_table().await?;
    let df = df
        .select_columns(&["c1", "c2", "c11"])?
        .limit(0, Some(10))?
        .explain(false, false)?;
    let plan = df.logical_plan().clone();

    // build query using SQL
    let sql_plan =
        create_plan("EXPLAIN SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")
            .await?;

    // the two plans should be identical
    assert_same_plan(&plan, &sql_plan);

    Ok(())
}

#[tokio::test]
async fn registry() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx, "aggregate_test_100").await?;

    // declare the udf
    let my_fn: ScalarFunctionImplementation =
        Arc::new(|_: &[ColumnarValue]| unimplemented!("my_fn is not implemented"));

    // create and register the udf
    ctx.register_udf(create_udf(
        "my_fn",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        my_fn,
    ));

    // build query with a UDF using DataFrame API
    let df = ctx.table("aggregate_test_100").await?;

    let expr = df.registry().udf("my_fn")?.call(vec![col("c12")]);
    let df = df.select(vec![expr])?;

    // build query using SQL
    let sql_plan = ctx.sql("SELECT my_fn(c12) FROM aggregate_test_100").await?;

    // the two plans should be identical
    assert_same_plan(df.logical_plan(), sql_plan.logical_plan());

    Ok(())
}

#[tokio::test]
async fn sendable() {
    let df = test_table().await.unwrap();
    // dataframes should be sendable between threads/tasks
    let task = SpawnedTask::spawn(async move {
        df.select_columns(&["c1"])
            .expect("should be usable in a task")
    });
    task.join().await.expect("task completed successfully");
}

#[tokio::test]
async fn intersect() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c3"])?;
    let d2 = df.clone();
    let plan = df.intersect(d2)?;
    let result = plan.logical_plan().clone();
    let expected = create_plan(
        "SELECT c1, c3 FROM aggregate_test_100
            INTERSECT ALL SELECT c1, c3 FROM aggregate_test_100",
    )
    .await?;
    assert_same_plan(&result, &expected);
    Ok(())
}

#[tokio::test]
async fn except() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c3"])?;
    let d2 = df.clone();
    let plan = df.except(d2)?;
    let result = plan.logical_plan().clone();
    let expected = create_plan(
        "SELECT c1, c3 FROM aggregate_test_100
            EXCEPT ALL SELECT c1, c3 FROM aggregate_test_100",
    )
    .await?;
    assert_same_plan(&result, &expected);
    Ok(())
}

#[tokio::test]
async fn except_distinct() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c3"])?;
    let d2 = df.clone();
    let plan = df.except_distinct(d2)?;
    let result = plan.logical_plan().clone();
    let expected = create_plan(
        "SELECT c1, c3 FROM aggregate_test_100
            EXCEPT DISTINCT SELECT c1, c3 FROM aggregate_test_100",
    )
    .await?;
    assert_same_plan(&result, &expected);
    Ok(())
}

#[tokio::test]
async fn intersect_distinct() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c3"])?;
    let d2 = df.clone();
    let plan = df.intersect_distinct(d2)?;
    let result = plan.logical_plan().clone();
    let expected = create_plan(
        "SELECT c1, c3 FROM aggregate_test_100
            INTERSECT DISTINCT SELECT c1, c3 FROM aggregate_test_100",
    )
    .await?;
    assert_same_plan(&result, &expected);
    Ok(())
}

#[tokio::test]
async fn register_table() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c12"])?;
    let ctx = SessionContext::new();
    let df_impl = DataFrame::new(ctx.state(), df.logical_plan().clone());

    // register a dataframe as a table
    let table_provider = df_impl.clone().into_view();
    assert_eq!(table_provider.table_type(), TableType::View);
    ctx.register_table("test_table", table_provider)?;

    // pull the table out
    let table = ctx.table("test_table").await?;

    let group_expr = vec![col("c1")];
    let aggr_expr = vec![sum(col("c12"))];

    // check that we correctly read from the table
    let df_results = df_impl
        .aggregate(group_expr.clone(), aggr_expr.clone())?
        .collect()
        .await?;
    let table_results = &table.aggregate(group_expr, aggr_expr)?.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+-----------------------------+
    | c1 | sum(aggregate_test_100.c12) |
    +----+-----------------------------+
    | a  | 10.238448667882977          |
    | b  | 7.797734760124923           |
    | c  | 13.860958726523545          |
    | d  | 8.793968289758968           |
    | e  | 10.206140546981722          |
    +----+-----------------------------+
    "
    );

    // the results are the same as the results from the view, modulo the leaf table name
    assert_snapshot!(
        batches_to_sort_string(table_results),
        @r"
    +----+---------------------+
    | c1 | sum(test_table.c12) |
    +----+---------------------+
    | a  | 10.238448667882977  |
    | b  | 7.797734760124923   |
    | c  | 13.860958726523545  |
    | d  | 8.793968289758968   |
    | e  | 10.206140546981722  |
    +----+---------------------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn register_temporary_table() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c12"])?;
    let ctx = SessionContext::new();
    let df_impl = DataFrame::new(ctx.state(), df.logical_plan().clone());

    let df_table_provider = df_impl.clone().into_temporary_view();

    // check that we set the correct table_type
    assert_eq!(df_table_provider.table_type(), TableType::Temporary);

    // check that we can register a dataframe as a temporary table
    ctx.register_table("test_table", df_table_provider)?;

    Ok(())
}

/// Compare the formatted string representation of two plans for equality
fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
    assert_eq!(format!("{plan1:?}"), format!("{plan2:?}"));
}

/// Create a logical plan from a SQL query
async fn create_plan(sql: &str) -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx, "aggregate_test_100").await?;
    Ok(ctx.sql(sql).await?.into_unoptimized_plan())
}

#[tokio::test]
async fn with_column() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
    let ctx = SessionContext::new();
    let df_impl = DataFrame::new(ctx.state(), df.logical_plan().clone());

    let df = df_impl
        .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
        .with_column("sum", col("c2") + col("c3"))?;

    // check that new column added
    let df_results = df.clone().collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+----+-----+-----+
    | c1 | c2 | c3  | sum |
    +----+----+-----+-----+
    | a  | 3  | -12 | -9  |
    | a  | 3  | -72 | -69 |
    | a  | 3  | 13  | 16  |
    | a  | 3  | 13  | 16  |
    | a  | 3  | 14  | 17  |
    | a  | 3  | 17  | 20  |
    +----+----+-----+-----+
    "
    );

    // check that col with the same name overwritten
    let df_results_overwrite = df
        .clone()
        .with_column("c1", col("c2") + col("c3"))?
        .collect()
        .await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results_overwrite),
        @r"
    +-----+----+-----+-----+
    | c1  | c2 | c3  | sum |
    +-----+----+-----+-----+
    | -69 | 3  | -72 | -69 |
    | -9  | 3  | -12 | -9  |
    | 16  | 3  | 13  | 16  |
    | 16  | 3  | 13  | 16  |
    | 17  | 3  | 14  | 17  |
    | 20  | 3  | 17  | 20  |
    +-----+----+-----+-----+
    "
    );

    // check that col with the same name overwritten using same name as reference
    let df_results_overwrite_self = df
        .clone()
        .with_column("c2", col("c2") + lit(1))?
        .collect()
        .await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results_overwrite_self),
        @r"
    +----+----+-----+-----+
    | c1 | c2 | c3  | sum |
    +----+----+-----+-----+
    | a  | 4  | -12 | -9  |
    | a  | 4  | -72 | -69 |
    | a  | 4  | 13  | 16  |
    | a  | 4  | 13  | 16  |
    | a  | 4  | 14  | 17  |
    | a  | 4  | 17  | 20  |
    +----+----+-----+-----+
    "
    );

    Ok(())
}

// Test issues: https://github.com/apache/datafusion/issues/11982
// and https://github.com/apache/datafusion/issues/12425
// Window function was creating unwanted projection when using with_column() method.
#[tokio::test]
async fn test_window_function_with_column() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
    let ctx = SessionContext::new();
    let df_impl = DataFrame::new(ctx.state(), df.logical_plan().clone());
    let func = row_number().alias("row_num");

    // This first `with_column` results in a column without a `qualifier`
    let df_impl = df_impl.with_column("s", col("c2") + col("c3"))?;

    // This second `with_column` should only alias `func` as `"r"`
    let df = df_impl.with_column("r", func)?.limit(0, Some(2))?;

    df.clone().show().await?;
    assert_eq!(5, df.schema().fields().len());

    let df_results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+----+-----+-----+---+
    | c1 | c2 | c3  | s   | r |
    +----+----+-----+-----+---+
    | c  | 2  | 1   | 3   | 1 |
    | d  | 5  | -40 | -35 | 2 |
    +----+----+-----+-----+---+
    "
    );

    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/7790
// The join operation outputs two identical column names, but they belong to different relations.
#[tokio::test]
async fn with_column_join_same_columns() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1"])?;
    let ctx = SessionContext::new();

    let table = df.into_view();
    ctx.register_table("t1", table.clone())?;
    ctx.register_table("t2", table)?;
    let df = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            JoinType::Inner,
            &["c1"],
            &["c1"],
            None,
        )?
        .sort(vec![
            // make the test deterministic
            col("t1.c1").sort(true, true),
        ])?
        .limit(0, Some(1))?;

    let df_results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+----+
    | c1 | c1 |
    +----+----+
    | a  | a  |
    +----+----+
    "
    );

    let df_with_column = df.clone().with_column("new_column", lit(true))?;

    assert_snapshot!(
        df_with_column.logical_plan(),
        @r"
    Projection: t1.c1, t2.c1, Boolean(true) AS new_column
      Limit: skip=0, fetch=1
        Sort: t1.c1 ASC NULLS FIRST
          Inner Join: t1.c1 = t2.c1
            SubqueryAlias: t1
              Projection: aggregate_test_100.c1
                TableScan: aggregate_test_100
            SubqueryAlias: t2
              Projection: aggregate_test_100.c1
                TableScan: aggregate_test_100
    "
    );

    assert_snapshot!(
        df_with_column.clone().into_optimized_plan().unwrap(),
        @r"
    Projection: t1.c1, t2.c1, Boolean(true) AS new_column
      Sort: t1.c1 ASC NULLS FIRST, fetch=1
        Inner Join: t1.c1 = t2.c1
          SubqueryAlias: t1
            TableScan: aggregate_test_100 projection=[c1]
          SubqueryAlias: t2
            TableScan: aggregate_test_100 projection=[c1]
    "
    );

    let df_results = df_with_column.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+----+------------+
    | c1 | c1 | new_column |
    +----+----+------------+
    | a  | a  | true       |
    +----+----+------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn with_column_renamed() -> Result<()> {
    let df = test_table()
        .await?
        .select_columns(&["c1", "c2", "c3"])?
        .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
        .sort(vec![
            // make the test deterministic
            col("c1").sort(true, true),
            col("c2").sort(true, true),
            col("c3").sort(true, true),
        ])?
        .limit(0, Some(1))?
        .with_column("sum", col("c2") + col("c3"))?;

    let df_sum_renamed = df
        .with_column_renamed("sum", "total")?
        // table qualifier optional
        .with_column_renamed("c1", "one")?
        // accepts table qualifier
        .with_column_renamed("aggregate_test_100.c2", "two")?
        // no-op for missing column
        .with_column_renamed("c4", "boom")?;

    let references: Vec<_> = df_sum_renamed
        .schema()
        .iter()
        .map(|(a, _)| a.cloned())
        .collect();

    assert_eq!(
        references,
        vec![
            Some(TableReference::bare("aggregate_test_100")), // table name is preserved
            Some(TableReference::bare("aggregate_test_100")),
            Some(TableReference::bare("aggregate_test_100")),
            None // total column
        ]
    );

    let batches = &df_sum_renamed.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(batches),
        @r"
    +-----+-----+-----+-------+
    | one | two | c3  | total |
    +-----+-----+-----+-------+
    | a   | 3   | -72 | -69   |
    +-----+-----+-----+-------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn with_column_renamed_ambiguous() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
    let ctx = SessionContext::new();

    let table = df.into_view();
    ctx.register_table("t1", table.clone())?;
    ctx.register_table("t2", table)?;

    let actual_err = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            JoinType::Inner,
            &["c1"],
            &["c1"],
            None,
        )?
        // can be t1.c2 or t2.c2
        .with_column_renamed("c2", "AAA")
        .unwrap_err();
    assert_snapshot!(actual_err.strip_backtrace(), @"Schema error: Ambiguous reference to unqualified field c2");

    Ok(())
}

#[tokio::test]
async fn with_column_renamed_join() -> Result<()> {
    let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
    let ctx = SessionContext::new();

    let table = df.into_view();
    ctx.register_table("t1", table.clone())?;
    ctx.register_table("t2", table)?;
    let df = ctx
        .table("t1")
        .await?
        .join(
            ctx.table("t2").await?,
            JoinType::Inner,
            &["c1"],
            &["c1"],
            None,
        )?
        .sort(vec![
            // make the test deterministic
            col("t1.c1").sort(true, true),
            col("t1.c2").sort(true, true),
            col("t1.c3").sort(true, true),
            col("t2.c1").sort(true, true),
            col("t2.c2").sort(true, true),
            col("t2.c3").sort(true, true),
        ])?
        .limit(0, Some(1))?;

    let df_results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+----+-----+----+----+-----+
    | c1 | c2 | c3  | c1 | c2 | c3  |
    +----+----+-----+----+----+-----+
    | a  | 1  | -85 | a  | 1  | -85 |
    +----+----+-----+----+----+-----+
    "
    );

    let df_renamed = df.clone().with_column_renamed("t1.c1", "AAA")?;

    assert_snapshot!(
        df_renamed.logical_plan(),
        @r"
    Projection: t1.c1 AS AAA, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
      Limit: skip=0, fetch=1
        Sort: t1.c1 ASC NULLS FIRST, t1.c2 ASC NULLS FIRST, t1.c3 ASC NULLS FIRST, t2.c1 ASC NULLS FIRST, t2.c2 ASC NULLS FIRST, t2.c3 ASC NULLS FIRST
          Inner Join: t1.c1 = t2.c1
            SubqueryAlias: t1
              Projection: aggregate_test_100.c1, aggregate_test_100.c2, aggregate_test_100.c3
                TableScan: aggregate_test_100
            SubqueryAlias: t2
              Projection: aggregate_test_100.c1, aggregate_test_100.c2, aggregate_test_100.c3
                TableScan: aggregate_test_100
    "
    );

    assert_snapshot!(
        df_renamed.clone().into_optimized_plan().unwrap(),
        @r"
    Projection: t1.c1 AS AAA, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3
      Sort: t1.c1 ASC NULLS FIRST, t1.c2 ASC NULLS FIRST, t1.c3 ASC NULLS FIRST, t2.c1 ASC NULLS FIRST, t2.c2 ASC NULLS FIRST, t2.c3 ASC NULLS FIRST, fetch=1
        Inner Join: t1.c1 = t2.c1
          SubqueryAlias: t1
            TableScan: aggregate_test_100 projection=[c1, c2, c3]
          SubqueryAlias: t2
            TableScan: aggregate_test_100 projection=[c1, c2, c3]
    "
    );

    let df_results = df_renamed.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +-----+----+-----+----+----+-----+
    | AAA | c2 | c3  | c1 | c2 | c3  |
    +-----+----+-----+----+----+-----+
    | a   | 1  | -85 | a  | 1  | -85 |
    +-----+----+-----+----+----+-----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn with_column_renamed_case_sensitive() -> Result<()> {
    let config = SessionConfig::from_string_hash_map(&HashMap::from([(
        "datafusion.sql_parser.enable_ident_normalization".to_owned(),
        "false".to_owned(),
    )]))?;
    let ctx = SessionContext::new_with_config(config);
    let name = "aggregate_test_100";
    register_aggregate_csv(&ctx, name).await?;
    let df = ctx.table(name);

    let df = df
        .await?
        .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
        .limit(0, Some(1))?
        .sort(vec![
            // make the test deterministic
            col("c1").sort(true, true),
            col("c2").sort(true, true),
            col("c3").sort(true, true),
        ])?
        .select_columns(&["c1"])?;

    let df_renamed = df.clone().with_column_renamed("c1", "CoLuMn1")?;

    let res = &df_renamed.clone().collect().await?;

    assert_snapshot!(
        batches_to_sort_string(res),
        @r"
    +---------+
    | CoLuMn1 |
    +---------+
    | a       |
    +---------+
    "
    );

    let df_renamed = df_renamed
        .with_column_renamed("CoLuMn1", "c1")?
        .collect()
        .await?;

    assert_snapshot!(
        batches_to_sort_string(&df_renamed),
        @r"
    +----+
    | c1 |
    +----+
    | a  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn describe_lookup_via_quoted_identifier() -> Result<()> {
    let ctx = SessionContext::new();
    let name = "aggregate_test_100";
    register_aggregate_csv(&ctx, name).await?;
    let df = ctx.table(name);

    let df = df
        .await?
        .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
        .limit(0, Some(1))?
        .sort(vec![
            // make the test deterministic
            col("c1").sort(true, true),
            col("c2").sort(true, true),
            col("c3").sort(true, true),
        ])?
        .select_columns(&["c1"])?;

    let df_renamed = df.clone().with_column_renamed("c1", "CoLu.Mn[\"1\"]")?;

    let describe_result = df_renamed.describe().await?;
    describe_result
        .clone()
        .sort(vec![
            col("describe").sort(true, true),
            col("CoLu.Mn[\"1\"]").sort(true, true),
        ])?
        .show()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&describe_result.clone().collect().await?),
        @r#"
    +------------+--------------+
    | describe   | CoLu.Mn["1"] |
    +------------+--------------+
    | count      | 1            |
    | max        | a            |
    | mean       | null         |
    | median     | null         |
    | min        | a            |
    | null_count | 0            |
    | std        | null         |
    +------------+--------------+
    "#
    );

    Ok(())
}

#[tokio::test]
async fn cast_expr_test() -> Result<()> {
    let df = test_table()
        .await?
        .select_columns(&["c2", "c3"])?
        .limit(0, Some(1))?
        .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

    let df_results = df.clone().collect().await?;
    df.clone().show().await?;
    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +----+----+-----+
    | c2 | c3 | sum |
    +----+----+-----+
    | 2  | 1  | 3   |
    +----+----+-----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn row_writer_resize_test() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "column_1",
        DataType::Utf8,
        false,
    )]));

    let data = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some(
                "2a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            ),
            Some(
                "3a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800",
            ),
        ]))],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", data)?;

    let sql = r#"
        SELECT
            count(1)
        FROM
            test
        GROUP BY
            column_1"#;

    let df = ctx.sql(sql).await?;
    df.show_limit(10).await?;

    Ok(())
}

#[tokio::test]
async fn with_column_name() -> Result<()> {
    // define data with a column name that has a "." in it:
    let array: Int32Array = [1, 10].into_iter().collect();
    let batch = RecordBatch::try_from_iter(vec![("f.c1", Arc::new(array) as _)])?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx
        .table("t")
        .await?
        // try and create a column with a '.' in it
        .with_column("f.c2", lit("hello"))?;

    let df_results = df.collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&df_results),
        @r"
    +------+-------+
    | f.c1 | f.c2  |
    +------+-------+
    | 1    | hello |
    | 10   | hello |
    +------+-------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_cache_mismatch() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .sql("SELECT CASE WHEN true THEN NULL ELSE 1 END")
        .await?;
    let cache_df = df.cache().await;
    assert!(cache_df.is_ok());
    Ok(())
}

#[tokio::test]
async fn cache_test() -> Result<()> {
    let df = test_table()
        .await?
        .select_columns(&["c2", "c3"])?
        .limit(0, Some(1))?
        .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

    let cached_df = df.clone().cache().await?;

    assert_snapshot!(
        cached_df.clone().into_optimized_plan().unwrap(),
        @"TableScan: ?table? projection=[c2, c3, sum]"
    );

    let df_results = df.collect().await?;
    let cached_df_results = cached_df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&cached_df_results),
        @r"
    +----+----+-----+
    | c2 | c3 | sum |
    +----+----+-----+
    | 2  | 1  | 3   |
    +----+----+-----+
    "
    );

    assert_eq!(&df_results, &cached_df_results);

    Ok(())
}

#[tokio::test]
async fn cache_producer_test() -> Result<()> {
    let df = test_table_with_cache_factory()
        .await?
        .select_columns(&["c2", "c3"])?
        .limit(0, Some(1))?
        .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

    let cached_df = df.clone().cache().await?;

    assert_snapshot!(
        cached_df.clone().into_optimized_plan().unwrap(),
        @r"
    CacheNode
      Projection: aggregate_test_100.c2, aggregate_test_100.c3, CAST(CAST(aggregate_test_100.c2 AS Int64) + CAST(aggregate_test_100.c3 AS Int64) AS Int64) AS sum
        Projection: aggregate_test_100.c2, aggregate_test_100.c3
          Limit: skip=0, fetch=1
            TableScan: aggregate_test_100, fetch=1
    "
    );
    Ok(())
}

#[tokio::test]
async fn partition_aware_union() -> Result<()> {
    let left = test_table().await?.select_columns(&["c1", "c2"])?;
    let right = test_table_with_name("c2")
        .await?
        .select_columns(&["c1", "c3"])?
        .with_column_renamed("c2.c1", "c2_c1")?;

    let left_rows = left.clone().collect().await?;
    let right_rows = right.clone().collect().await?;
    let join1 =
        left.clone()
            .join(right.clone(), JoinType::Inner, &["c1"], &["c2_c1"], None)?;
    let join2 = left.join(right, JoinType::Inner, &["c1"], &["c2_c1"], None)?;

    let union = join1.union(join2)?;

    let union_rows = union.clone().collect().await?;

    assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    assert_eq!(4016, union_rows.iter().map(|x| x.num_rows()).sum::<usize>());

    let physical_plan = union.create_physical_plan().await?;
    let default_partition_count = SessionConfig::new().target_partitions();

    // For partition aware union, the output partition count should not be changed.
    assert_eq!(
        physical_plan.output_partitioning().partition_count(),
        default_partition_count
    );
    // For partition aware union, the output partition is the same with the union's inputs
    for child in physical_plan.children() {
        assert_eq!(
            physical_plan.output_partitioning(),
            child.output_partitioning()
        );
    }

    Ok(())
}

#[tokio::test]
async fn non_partition_aware_union() -> Result<()> {
    let left = test_table().await?.select_columns(&["c1", "c2"])?;
    let right = test_table_with_name("c2")
        .await?
        .select_columns(&["c1", "c2"])?
        .with_column_renamed("c2.c1", "c2_c1")?
        .with_column_renamed("c2.c2", "c2_c2")?;

    let left_rows = left.clone().collect().await?;
    let right_rows = right.clone().collect().await?;
    let join1 = left.clone().join(
        right.clone(),
        JoinType::Inner,
        &["c1", "c2"],
        &["c2_c1", "c2_c2"],
        None,
    )?;

    // join key ordering is different
    let join2 = left.join(
        right,
        JoinType::Inner,
        &["c2", "c1"],
        &["c2_c2", "c2_c1"],
        None,
    )?;

    let union = join1.union(join2)?;

    let union_rows = union.clone().collect().await?;

    assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
    assert_eq!(916, union_rows.iter().map(|x| x.num_rows()).sum::<usize>());

    let physical_plan = union.create_physical_plan().await?;
    let default_partition_count = SessionConfig::new().target_partitions();

    // For non-partition aware union, the output partitioning count should be the combination of all output partitions count
    assert!(matches!(
            physical_plan.output_partitioning(),
            Partitioning::UnknownPartitioning(partition_count) if *partition_count == default_partition_count * 2));
    Ok(())
}

#[tokio::test]
async fn verify_join_output_partitioning() -> Result<()> {
    let left = test_table().await?.select_columns(&["c1", "c2"])?;
    let right = test_table_with_name("c2")
        .await?
        .select_columns(&["c1", "c2"])?
        .with_column_renamed("c2.c1", "c2_c1")?
        .with_column_renamed("c2.c2", "c2_c2")?;

    let all_join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::RightSemi,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftMark,
        JoinType::RightMark,
    ];

    let default_partition_count = SessionConfig::new().target_partitions();

    for join_type in all_join_types {
        let join = left.clone().join(
            right.clone(),
            join_type,
            &["c1", "c2"],
            &["c2_c1", "c2_c2"],
            None,
        )?;
        let physical_plan = join.create_physical_plan().await?;
        let out_partitioning = physical_plan.output_partitioning();
        let join_schema = physical_plan.schema();

        match join_type {
            JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => {
                let left_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(Column::new_with_schema("c1", &join_schema)?),
                    Arc::new(Column::new_with_schema("c2", &join_schema)?),
                ];
                assert_eq!(
                    out_partitioning,
                    &Partitioning::Hash(left_exprs, default_partition_count)
                );
            }
            JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => {
                let right_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(Column::new_with_schema("c2_c1", &join_schema)?),
                    Arc::new(Column::new_with_schema("c2_c2", &join_schema)?),
                ];
                assert_eq!(
                    out_partitioning,
                    &Partitioning::Hash(right_exprs, default_partition_count)
                );
            }
            JoinType::Full => {
                assert!(matches!(
                        out_partitioning,
                    &Partitioning::UnknownPartitioning(partition_count) if partition_count == default_partition_count));
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_except_nested_struct() -> Result<()> {
    use arrow::array::StructArray;

    let nested_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("lat", DataType::Int32, true),
        Field::new("long", DataType::Int32, true),
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, true),
        Field::new(
            "nested",
            DataType::Struct(nested_schema.fields.clone()),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("id", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("lat", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("long", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
            ])),
        ],
    )
    .unwrap();

    let updated_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(12), Some(3)])),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("id", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("lat", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("long", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
            ])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let before = ctx.read_batch(batch).expect("Failed to make DataFrame");
    let after = ctx
        .read_batch(updated_batch)
        .expect("Failed to make DataFrame");

    let diff = before
        .except(after)
        .expect("Failed to except")
        .collect()
        .await?;
    assert_eq!(diff.len(), 1);
    Ok(())
}

#[tokio::test]
async fn nested_explain_should_fail() -> Result<()> {
    let ctx = SessionContext::new();
    // must be error
    let mut result = ctx.sql("explain select 1").await?.explain(false, false);
    assert!(result.is_err());
    // must be error
    result = ctx.sql("explain explain select 1").await;
    assert!(result.is_err());
    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/12065
#[tokio::test]
async fn filtered_aggr_with_param_values() -> Result<()> {
    let cfg = SessionConfig::new().set(
        "datafusion.sql_parser.dialect",
        &ScalarValue::from("PostgreSQL"),
    );
    let ctx = SessionContext::new_with_config(cfg);
    register_aggregate_csv(&ctx, "table1").await?;

    let df = ctx
        .sql("select count (c2) filter (where c3 > $1) from table1")
        .await?
        .with_param_values(vec![ScalarValue::from(10u64)]);

    let df_results = df?.collect().await?;
    assert_snapshot!(
        batches_to_string(&df_results),
        @r"
    +------------------------------------------------+
    | count(table1.c2) FILTER (WHERE table1.c3 > $1) |
    +------------------------------------------------+
    | 54                                             |
    +------------------------------------------------+
    "
    );

    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/13873
#[tokio::test]
async fn write_parquet_with_order() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));

    let ctx = SessionContext::new();
    let write_df = ctx.read_batch(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 5, 7, 3, 2])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5, 6])),
        ],
    )?)?;

    let test_path = tmp_dir.path().join("test.parquet");

    write_df
        .clone()
        .write_parquet(
            test_path.to_str().unwrap(),
            DataFrameWriteOptions::new().with_sort_by(vec![col("a").sort(true, true)]),
            None,
        )
        .await?;

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "data",
        test_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql("SELECT * FROM data").await?;
    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +---+---+
    | a | b |
    +---+---+
    | 1 | 2 |
    | 2 | 6 |
    | 3 | 5 |
    | 5 | 3 |
    | 7 | 4 |
    +---+---+
    "
    );

    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/13873
#[tokio::test]
async fn write_csv_with_order() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));

    let ctx = SessionContext::new();
    let write_df = ctx.read_batch(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 5, 7, 3, 2])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5, 6])),
        ],
    )?)?;

    let test_path = tmp_dir.path().join("test.csv");

    write_df
        .clone()
        .write_csv(
            test_path.to_str().unwrap(),
            DataFrameWriteOptions::new().with_sort_by(vec![col("a").sort(true, true)]),
            None,
        )
        .await?;

    let ctx = SessionContext::new();
    ctx.register_csv(
        "data",
        test_path.to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    let df = ctx.sql("SELECT * FROM data").await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +---+---+
    | a | b |
    +---+---+
    | 1 | 2 |
    | 2 | 6 |
    | 3 | 5 |
    | 5 | 3 |
    | 7 | 4 |
    +---+---+
    "
    );
    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/13873
#[tokio::test]
async fn write_json_with_order() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));

    let ctx = SessionContext::new();
    let write_df = ctx.read_batch(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 5, 7, 3, 2])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5, 6])),
        ],
    )?)?;

    let test_path = tmp_dir.path().join("test.json");

    write_df
        .clone()
        .write_json(
            test_path.to_str().unwrap(),
            DataFrameWriteOptions::new().with_sort_by(vec![col("a").sort(true, true)]),
            None,
        )
        .await?;

    let ctx = SessionContext::new();
    ctx.register_json(
        "data",
        test_path.to_str().unwrap(),
        JsonReadOptions::default().schema(&schema),
    )
    .await?;

    let df = ctx.sql("SELECT * FROM data").await?;
    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +---+---+
    | a | b |
    +---+---+
    | 1 | 2 |
    | 2 | 6 |
    | 3 | 5 |
    | 5 | 3 |
    | 7 | 4 |
    +---+---+
    "
    );
    Ok(())
}

// Test issue: https://github.com/apache/datafusion/issues/13873
#[tokio::test]
async fn write_table_with_order() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let location = tmp_dir.path().join("test_table/");

    let mut write_df = ctx
        .sql("values ('z'), ('x'), ('a'), ('b'), ('c')")
        .await
        .unwrap();

    // Ensure the column names and types match the target table
    write_df = write_df
        .with_column_renamed("column1", "tablecol1")
        .unwrap();

    // Ensure the column type matches the target table
    write_df =
        write_df.with_column("tablecol1", cast(col("tablecol1"), DataType::Utf8View))?;

    let sql_str =
        "create external table data(tablecol1 varchar) stored as parquet location '"
            .to_owned()
            + location.to_str().unwrap()
            + "'";

    ctx.sql(sql_str.as_str()).await?.collect().await?;

    // This is equivalent to INSERT INTO test.
    write_df
        .clone()
        .write_table(
            "data",
            DataFrameWriteOptions::new()
                .with_sort_by(vec![col("tablecol1").sort(true, true)]),
        )
        .await?;

    let df = ctx.sql("SELECT * FROM data").await?;
    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----------+
    | tablecol1 |
    +-----------+
    | a         |
    | b         |
    | c         |
    | x         |
    | z         |
    +-----------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_sort() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select b, count(*) from t1 group by b order by count(*)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    let df_results = ctx
        .table("t1")
        .await?
        .aggregate(vec![col("b")], vec![count_all()])?
        .sort(vec![count_all().sort(true, false)])?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&sql_results).unwrap(),
        @r"
    +---------------+------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                       |
    +---------------+------------------------------------------------------------------------------------------------------------+
    | logical_plan  | Projection: t1.b, count(*)                                                                                 |
    |               |   Sort: count(Int64(1)) AS count(*) AS count(*) ASC NULLS LAST                                             |
    |               |     Projection: t1.b, count(Int64(1)) AS count(*), count(Int64(1))                                         |
    |               |       Aggregate: groupBy=[[t1.b]], aggr=[[count(Int64(1))]]                                                |
    |               |         TableScan: t1 projection=[b]                                                                       |
    | physical_plan | ProjectionExec: expr=[b@0 as b, count(*)@1 as count(*)]                                                    |
    |               |   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]                                              |
    |               |     SortExec: expr=[count(*)@1 ASC NULLS LAST], preserve_partitioning=[true]                               |
    |               |       ProjectionExec: expr=[b@0 as b, count(Int64(1))@1 as count(*), count(Int64(1))@1 as count(Int64(1))] |
    |               |         AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[count(Int64(1))]                       |
    |               |           RepartitionExec: partitioning=Hash([b@0], 4), input_partitions=1                                 |
    |               |             AggregateExec: mode=Partial, gby=[b@0 as b], aggr=[count(Int64(1))]                            |
    |               |               DataSourceExec: partitions=1, partition_sizes=[1]                                            |
    |               |                                                                                                            |
    +---------------+------------------------------------------------------------------------------------------------------------+
    "
    );

    assert_snapshot!(
        pretty_format_batches(&df_results).unwrap(),
        @r"
    +---------------+----------------------------------------------------------------------------+
    | plan_type     | plan                                                                       |
    +---------------+----------------------------------------------------------------------------+
    | logical_plan  | Sort: count(*) ASC NULLS LAST                                              |
    |               |   Aggregate: groupBy=[[t1.b]], aggr=[[count(Int64(1)) AS count(*)]]        |
    |               |     TableScan: t1 projection=[b]                                           |
    | physical_plan | SortPreservingMergeExec: [count(*)@1 ASC NULLS LAST]                       |
    |               |   SortExec: expr=[count(*)@1 ASC NULLS LAST], preserve_partitioning=[true] |
    |               |     AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[count(*)]  |
    |               |       RepartitionExec: partitioning=Hash([b@0], 4), input_partitions=1     |
    |               |         AggregateExec: mode=Partial, gby=[b@0 as b], aggr=[count(*)]       |
    |               |           DataSourceExec: partitions=1, partition_sizes=[1]                |
    |               |                                                                            |
    +---------------+----------------------------------------------------------------------------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_in() -> Result<()> {
    let ctx = create_join_context()?;
    let sql_results = ctx
        .sql("SELECT a, b FROM t1 WHERE a in (SELECT count(*) FROM t2)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&sql_results).unwrap(),
        @r"
    +---------------+----------------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                                 |
    +---------------+----------------------------------------------------------------------------------------------------------------------+
    | logical_plan  | LeftSemi Join: CAST(t1.a AS Int64) = __correlated_sq_1.count(*)                                                      |
    |               |   TableScan: t1 projection=[a, b]                                                                                    |
    |               |   SubqueryAlias: __correlated_sq_1                                                                                   |
    |               |     Projection: count(Int64(1)) AS count(*)                                                                          |
    |               |       Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]                                                              |
    |               |         TableScan: t2 projection=[]                                                                                  |
    | physical_plan | HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(count(*)@0, CAST(t1.a AS Int64)@2)], projection=[a@0, b@1] |
    |               |   ProjectionExec: expr=[4 as count(*)]                                                                               |
    |               |     PlaceholderRowExec                                                                                               |
    |               |   ProjectionExec: expr=[a@0 as a, b@1 as b, CAST(a@0 AS Int64) as CAST(t1.a AS Int64)]                               |
    |               |     DataSourceExec: partitions=1, partition_sizes=[1]                                                                |
    |               |                                                                                                                      |
    +---------------+----------------------------------------------------------------------------------------------------------------------+
    "
    );

    // In the same SessionContext, AliasGenerator will increase subquery_alias id by 1
    // https://github.com/apache/datafusion/blame/cf45eb9020092943b96653d70fafb143cc362e19/datafusion/optimizer/src/alias.rs#L40-L43
    // for compare difference between sql and df logical plan, we need to create a new SessionContext here
    let ctx = create_join_context()?;
    let df_results = ctx
        .table("t1")
        .await?
        .filter(in_subquery(
            col("a"),
            Arc::new(
                ctx.table("t2")
                    .await?
                    .aggregate(vec![], vec![count_all()])?
                    .select(vec![count_all()])?
                    .into_optimized_plan()?,
            ),
        ))?
        .select(vec![col("a"), col("b")])?
        .explain(false, false)?
        .collect()
        .await?;

    // make sure sql plan same with df plan
    assert_snapshot!(
        pretty_format_batches(&df_results).unwrap(),
        @r"
    +---------------+----------------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                                 |
    +---------------+----------------------------------------------------------------------------------------------------------------------+
    | logical_plan  | LeftSemi Join: CAST(t1.a AS Int64) = __correlated_sq_1.count(*)                                                      |
    |               |   TableScan: t1 projection=[a, b]                                                                                    |
    |               |   SubqueryAlias: __correlated_sq_1                                                                                   |
    |               |     Aggregate: groupBy=[[]], aggr=[[count(Int64(1)) AS count(*)]]                                                    |
    |               |       TableScan: t2 projection=[]                                                                                    |
    | physical_plan | HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(count(*)@0, CAST(t1.a AS Int64)@2)], projection=[a@0, b@1] |
    |               |   ProjectionExec: expr=[4 as count(*)]                                                                               |
    |               |     PlaceholderRowExec                                                                                               |
    |               |   ProjectionExec: expr=[a@0 as a, b@1 as b, CAST(a@0 AS Int64) as CAST(t1.a AS Int64)]                               |
    |               |     DataSourceExec: partitions=1, partition_sizes=[1]                                                                |
    |               |                                                                                                                      |
    +---------------+----------------------------------------------------------------------------------------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_exist() -> Result<()> {
    let ctx = create_join_context()?;
    let sql_results = ctx
        .sql("SELECT a, b FROM t1 WHERE EXISTS (SELECT count(*) FROM t2)")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&sql_results).unwrap(),
        @r"
    +---------------+-----------------------------------------------------+
    | plan_type     | plan                                                |
    +---------------+-----------------------------------------------------+
    | logical_plan  | LeftSemi Join:                                      |
    |               |   TableScan: t1 projection=[a, b]                   |
    |               |   SubqueryAlias: __correlated_sq_1                  |
    |               |     EmptyRelation: rows=1                           |
    | physical_plan | NestedLoopJoinExec: join_type=RightSemi             |
    |               |   PlaceholderRowExec                                |
    |               |   DataSourceExec: partitions=1, partition_sizes=[1] |
    |               |                                                     |
    +---------------+-----------------------------------------------------+
    "
    );

    let df_results = ctx
        .table("t1")
        .await?
        .filter(exists(Arc::new(
            ctx.table("t2")
                .await?
                .aggregate(vec![], vec![count_all()])?
                .select(vec![count_all()])?
                .into_unoptimized_plan(),
            // Usually, into_optimized_plan() should be used here, but due to
            // https://github.com/apache/datafusion/issues/5771,
            // subqueries in SQL cannot be optimized, resulting in differences in logical_plan. Therefore, into_unoptimized_plan() is temporarily used here.
        )))?
        .select(vec![col("a"), col("b")])?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&df_results).unwrap(),
        @r"
    +---------------+-----------------------------------------------------+
    | plan_type     | plan                                                |
    +---------------+-----------------------------------------------------+
    | logical_plan  | LeftSemi Join:                                      |
    |               |   TableScan: t1 projection=[a, b]                   |
    |               |   SubqueryAlias: __correlated_sq_1                  |
    |               |     EmptyRelation: rows=1                           |
    | physical_plan | NestedLoopJoinExec: join_type=RightSemi             |
    |               |   PlaceholderRowExec                                |
    |               |   DataSourceExec: partitions=1, partition_sizes=[1] |
    |               |                                                     |
    +---------------+-----------------------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_window() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select count(*) OVER(ORDER BY a DESC RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING) from t1")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&sql_results).unwrap(),
        @r#"
    +---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                                                                                                                                                                                                                     |
    +---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | logical_plan  | Projection: count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING AS count(*) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING                                                                                                            |
    |               |   WindowAggr: windowExpr=[[count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING]]                                                                                                                                                                                  |
    |               |     TableScan: t1 projection=[a]                                                                                                                                                                                                                                                                         |
    | physical_plan | ProjectionExec: expr=[count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING@1 as count(*) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING]                                                                                               |
    |               |   BoundedWindowAggExec: wdw=[count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING: Field { "count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING": Int64 }, frame: RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING], mode=[Sorted] |
    |               |     SortExec: expr=[a@0 DESC], preserve_partitioning=[false]                                                                                                                                                                                                                                             |
    |               |       DataSourceExec: partitions=1, partition_sizes=[1]                                                                                                                                                                                                                                                  |
    |               |                                                                                                                                                                                                                                                                                                          |
    +---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    "#
    );

    let df_results = ctx
        .table("t1")
        .await?
        .select(vec![
            count_all_window()
                .order_by(vec![Sort::new(col("a"), false, true)])
                .window_frame(WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::Preceding(ScalarValue::UInt32(Some(6))),
                    WindowFrameBound::Following(ScalarValue::UInt32(Some(2))),
                ))
                .build()
                .unwrap(),
        ])?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&df_results).unwrap(),
        @r#"
    +---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                                                                                                                                                                                                                     |
    +---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | logical_plan  | Projection: count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING                                                                                                                                                                                                   |
    |               |   WindowAggr: windowExpr=[[count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING]]                                                                                                                                                                                  |
    |               |     TableScan: t1 projection=[a]                                                                                                                                                                                                                                                                         |
    | physical_plan | ProjectionExec: expr=[count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING@1 as count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING]                                                                                        |
    |               |   BoundedWindowAggExec: wdw=[count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING: Field { "count(Int64(1)) ORDER BY [t1.a DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING": Int64 }, frame: RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING], mode=[Sorted] |
    |               |     SortExec: expr=[a@0 DESC], preserve_partitioning=[false]                                                                                                                                                                                                                                             |
    |               |       DataSourceExec: partitions=1, partition_sizes=[1]                                                                                                                                                                                                                                                  |
    |               |                                                                                                                                                                                                                                                                                                          |
    +---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    "#
    );

    Ok(())
}

#[tokio::test]
// Test with `repartition_sorts` disabled, causing a full resort of the data
async fn union_with_mix_of_presorted_and_explicitly_resorted_inputs_with_repartition_sorts_false()
-> Result<()> {
    assert_snapshot!(
        union_with_mix_of_presorted_and_explicitly_resorted_inputs_impl(false).await?,
        @r"
    AggregateExec: mode=Final, gby=[id@0 as id], aggr=[], ordering_mode=Sorted
      SortPreservingMergeExec: [id@0 ASC NULLS LAST]
        AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[], ordering_mode=Sorted
          UnionExec
            DataSourceExec: file_groups={1 group: [[{testdata}/alltypes_tiny_pages.parquet]]}, projection=[id], output_ordering=[id@0 ASC NULLS LAST], file_type=parquet
            SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]
              DataSourceExec: file_groups={1 group: [[{testdata}/alltypes_tiny_pages.parquet]]}, projection=[id], file_type=parquet
    ");
    Ok(())
}

#[tokio::test]
// Test with `repartition_sorts` enabled to preserve pre-sorted partitions and avoid resorting
async fn union_with_mix_of_presorted_and_explicitly_resorted_inputs_with_repartition_sorts_true()
-> Result<()> {
    assert_snapshot!(
        union_with_mix_of_presorted_and_explicitly_resorted_inputs_impl(true).await?,
        @r"
    AggregateExec: mode=Final, gby=[id@0 as id], aggr=[], ordering_mode=Sorted
      SortPreservingMergeExec: [id@0 ASC NULLS LAST]
        AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[], ordering_mode=Sorted
          UnionExec
            DataSourceExec: file_groups={1 group: [[{testdata}/alltypes_tiny_pages.parquet]]}, projection=[id], output_ordering=[id@0 ASC NULLS LAST], file_type=parquet
            SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]
              DataSourceExec: file_groups={1 group: [[{testdata}/alltypes_tiny_pages.parquet]]}, projection=[id], file_type=parquet
    ");

    Ok(())
}

async fn union_with_mix_of_presorted_and_explicitly_resorted_inputs_impl(
    repartition_sorts: bool,
) -> Result<String> {
    let config = SessionConfig::default()
        .with_target_partitions(1)
        .with_repartition_sorts(repartition_sorts);
    let ctx = SessionContext::new_with_config(config);

    let testdata = parquet_test_data();

    // Register "sorted" table, that is sorted
    ctx.register_parquet(
        "sorted",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default()
            .file_sort_order(vec![vec![col("id").sort(true, false)]]),
    )
    .await?;

    // Register "unsorted" table
    ctx.register_parquet(
        "unsorted",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let source_sorted = ctx
        .table("sorted")
        .await
        .unwrap()
        .select(vec![col("id")])
        .unwrap();

    let source_unsorted = ctx
        .table("unsorted")
        .await
        .unwrap()
        .select(vec![col("id")])
        .unwrap();

    let source_unsorted_resorted =
        source_unsorted.sort(vec![col("id").sort(true, false)])?;

    let union = source_sorted.union(source_unsorted_resorted)?;

    let agg = union.aggregate(vec![col("id")], vec![])?;

    let df = agg;

    // To be able to remove user specific paths from the plan, for stable assertions
    let testdata_clean = Path::new(&testdata).canonicalize()?.display().to_string();
    let testdata_clean = testdata_clean.strip_prefix("/").unwrap_or(&testdata_clean);

    // Use displayable() rather than explain().collect() to avoid table formatting issues. We need
    // to replace machine-specific paths with variable lengths, which breaks table alignment and
    // causes snapshot mismatches.
    let physical_plan = df.create_physical_plan().await?;
    let displayable_plan = displayable(physical_plan.as_ref())
        .indent(true)
        .to_string()
        .replace(testdata_clean, "{testdata}");

    Ok(displayable_plan)
}

#[tokio::test]
async fn test_count_wildcard_on_aggregate() -> Result<()> {
    let ctx = create_join_context()?;
    register_alltypes_tiny_pages_parquet(&ctx).await?;

    let sql_results = ctx
        .sql("select count(*) from t1")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&sql_results).unwrap(),
        @r"
    +---------------+-----------------------------------------------------+
    | plan_type     | plan                                                |
    +---------------+-----------------------------------------------------+
    | logical_plan  | Projection: count(Int64(1)) AS count(*)             |
    |               |   Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]] |
    |               |     TableScan: t1 projection=[]                     |
    | physical_plan | ProjectionExec: expr=[4 as count(*)]                |
    |               |   PlaceholderRowExec                                |
    |               |                                                     |
    +---------------+-----------------------------------------------------+
    "
    );

    // add `.select(vec![count_wildcard()])?` to make sure we can analyze all node instead of just top node.
    let df_results = ctx
        .table("t1")
        .await?
        .aggregate(vec![], vec![count_all()])?
        .select(vec![count_all()])?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&df_results).unwrap(),
        @r"
    +---------------+---------------------------------------------------------------+
    | plan_type     | plan                                                          |
    +---------------+---------------------------------------------------------------+
    | logical_plan  | Aggregate: groupBy=[[]], aggr=[[count(Int64(1)) AS count(*)]] |
    |               |   TableScan: t1 projection=[]                                 |
    | physical_plan | ProjectionExec: expr=[4 as count(*)]                          |
    |               |   PlaceholderRowExec                                          |
    |               |                                                               |
    +---------------+---------------------------------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_count_wildcard_on_where_scalar_subquery() -> Result<()> {
    let ctx = create_join_context()?;

    let sql_results = ctx
        .sql("select a,b from t1 where (select count(*) from t2 where t1.a = t2.a)>0;")
        .await?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&sql_results).unwrap(),
        @r"
    +---------------+----------------------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                                       |
    +---------------+----------------------------------------------------------------------------------------------------------------------------+
    | logical_plan  | Projection: t1.a, t1.b                                                                                                     |
    |               |   Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.count(*) END > Int64(0)           |
    |               |     Projection: t1.a, t1.b, __scalar_sq_1.count(*), __scalar_sq_1.__always_true                                            |
    |               |       Left Join: t1.a = __scalar_sq_1.a                                                                                    |
    |               |         TableScan: t1 projection=[a, b]                                                                                    |
    |               |         SubqueryAlias: __scalar_sq_1                                                                                       |
    |               |           Projection: count(Int64(1)) AS count(*), t2.a, Boolean(true) AS __always_true                                    |
    |               |             Aggregate: groupBy=[[t2.a]], aggr=[[count(Int64(1))]]                                                          |
    |               |               TableScan: t2 projection=[a]                                                                                 |
    | physical_plan | FilterExec: CASE WHEN __always_true@3 IS NULL THEN 0 ELSE count(*)@2 END > 0, projection=[a@0, b@1]                        |
    |               |   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1                                                     |
    |               |     ProjectionExec: expr=[a@2 as a, b@3 as b, count(*)@0 as count(*), __always_true@1 as __always_true]                    |
    |               |       HashJoinExec: mode=CollectLeft, join_type=Right, on=[(a@1, a@0)], projection=[count(*)@0, __always_true@2, a@3, b@4] |
    |               |         CoalescePartitionsExec                                                                                             |
    |               |           ProjectionExec: expr=[count(Int64(1))@1 as count(*), a@0 as a, true as __always_true]                            |
    |               |             AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[count(Int64(1))]                                   |
    |               |               RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1                                             |
    |               |                 AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count(Int64(1))]                                        |
    |               |                   DataSourceExec: partitions=1, partition_sizes=[1]                                                        |
    |               |         DataSourceExec: partitions=1, partition_sizes=[1]                                                                  |
    |               |                                                                                                                            |
    +---------------+----------------------------------------------------------------------------------------------------------------------------+
    "
    );

    // In the same SessionContext, AliasGenerator will increase subquery_alias id by 1
    // https://github.com/apache/datafusion/blame/cf45eb9020092943b96653d70fafb143cc362e19/datafusion/optimizer/src/alias.rs#L40-L43
    // for compare difference between sql and df logical plan, we need to create a new SessionContext here
    let ctx = create_join_context()?;
    let agg_expr = count_all();
    let agg_expr_col = col(agg_expr.schema_name().to_string());
    let df_results = ctx
        .table("t1")
        .await?
        .filter(
            scalar_subquery(Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(out_ref_col(DataType::UInt32, "t1.a").eq(col("t2.a")))?
                    .aggregate(vec![], vec![agg_expr])?
                    .select(vec![agg_expr_col])?
                    .into_unoptimized_plan(),
            ))
            .gt(lit(ScalarValue::UInt8(Some(0)))),
        )?
        .select(vec![col("t1.a"), col("t1.b")])?
        .explain(false, false)?
        .collect()
        .await?;

    assert_snapshot!(
        pretty_format_batches(&df_results).unwrap(),
        @r"
    +---------------+----------------------------------------------------------------------------------------------------------------------------+
    | plan_type     | plan                                                                                                                       |
    +---------------+----------------------------------------------------------------------------------------------------------------------------+
    | logical_plan  | Projection: t1.a, t1.b                                                                                                     |
    |               |   Filter: CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) ELSE __scalar_sq_1.count(*) END > Int64(0)           |
    |               |     Projection: t1.a, t1.b, __scalar_sq_1.count(*), __scalar_sq_1.__always_true                                            |
    |               |       Left Join: t1.a = __scalar_sq_1.a                                                                                    |
    |               |         TableScan: t1 projection=[a, b]                                                                                    |
    |               |         SubqueryAlias: __scalar_sq_1                                                                                       |
    |               |           Projection: count(*), t2.a, Boolean(true) AS __always_true                                                       |
    |               |             Aggregate: groupBy=[[t2.a]], aggr=[[count(Int64(1)) AS count(*)]]                                              |
    |               |               TableScan: t2 projection=[a]                                                                                 |
    | physical_plan | FilterExec: CASE WHEN __always_true@3 IS NULL THEN 0 ELSE count(*)@2 END > 0, projection=[a@0, b@1]                        |
    |               |   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1                                                     |
    |               |     ProjectionExec: expr=[a@2 as a, b@3 as b, count(*)@0 as count(*), __always_true@1 as __always_true]                    |
    |               |       HashJoinExec: mode=CollectLeft, join_type=Right, on=[(a@1, a@0)], projection=[count(*)@0, __always_true@2, a@3, b@4] |
    |               |         CoalescePartitionsExec                                                                                             |
    |               |           ProjectionExec: expr=[count(*)@1 as count(*), a@0 as a, true as __always_true]                                   |
    |               |             AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[count(*)]                                          |
    |               |               RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1                                             |
    |               |                 AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count(*)]                                               |
    |               |                   DataSourceExec: partitions=1, partition_sizes=[1]                                                        |
    |               |         DataSourceExec: partitions=1, partition_sizes=[1]                                                                  |
    |               |                                                                                                                            |
    +---------------+----------------------------------------------------------------------------------------------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn join2() -> Result<()> {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("aa", batch1)?;

    let df1 = ctx.table("aa").await?;

    ctx.register_batch("aaa", batch2)?;

    let df2 = ctx.table("aaa").await?;

    let a = df1.join(df2, JoinType::Inner, &["a"], &["a"], None)?;

    let batches = a.collect().await?;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);

    Ok(())
}

#[tokio::test]
async fn sort_on_unprojected_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .sort(vec![Sort::new(col("b"), false, true)])
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----+
    | a   |
    +-----+
    | 100 |
    | 10  |
    | 10  |
    | 1   |
    +-----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn sort_on_distinct_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5])),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![col("a")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![Sort::new(col("a"), false, true)])
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----+
    | a   |
    +-----+
    | 100 |
    | 10  |
    | 1   |
    +-----+
    "
    );
    Ok(())
}

#[tokio::test]
async fn sort_on_distinct_unprojected_columns() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(Int32Array::from(vec![2, 3, 4, 5])),
        ],
    )?;

    // Cannot sort on a column after distinct that would add a new column
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let err = ctx
        .table("t")
        .await?
        .select(vec![col("a")])?
        .distinct()?
        .sort(vec![Sort::new(col("b"), false, true)])
        .unwrap_err();
    assert_snapshot!(err.strip_backtrace(), @"Error during planning: For SELECT DISTINCT, ORDER BY expressions b must appear in select list");
    Ok(())
}

#[tokio::test]
async fn sort_on_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .sort(vec![col("b").sort(true, true)])
        .unwrap_err();

    assert_snapshot!(err.strip_backtrace(), @"Schema error: Ambiguous reference to unqualified field b");
    Ok(())
}

#[tokio::test]
async fn group_by_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .aggregate(vec![col("b")], vec![max(col("a"))])
        .unwrap_err();

    assert_snapshot!(err.strip_backtrace(), @"Schema error: Ambiguous reference to unqualified field b");
    Ok(())
}

#[tokio::test]
async fn filter_on_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .filter(col("b").eq(lit(1)))
        .unwrap_err();

    assert_snapshot!(err.strip_backtrace(), @"Schema error: Ambiguous reference to unqualified field b");
    Ok(())
}

#[tokio::test]
async fn select_ambiguous_column() -> Result<()> {
    let err = create_test_table("t1")
        .await?
        .join(
            create_test_table("t2").await?,
            JoinType::Inner,
            &["a"],
            &["a"],
            None,
        )?
        .select(vec![col("b")])
        .unwrap_err();

    assert_snapshot!(err.strip_backtrace(), @"Schema error: Ambiguous reference to unqualified field b");
    Ok(())
}

#[tokio::test]
async fn filter_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![(col("a").eq(lit(10))).alias("a")])
        .unwrap()
        .filter(col("a"))
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +------+
    | a    |
    +------+
    | true |
    | true |
    +------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn select_with_alias_overwrite() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 10, 10, 100]))],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let df = ctx
        .table("t")
        .await?
        .select(vec![col("a").alias("a")])?
        .select(vec![(col("a").eq(lit(10))).alias("a")])?
        .select(vec![col("a")])?;

    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-------+
    | a     |
    +-------+
    | false |
    | true  |
    | true  |
    | false |
    +-------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_grouping_sets() -> Result<()> {
    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("a")],
        vec![col("b")],
        vec![col("a"), col("b")],
    ]));

    let df = create_test_table("test")
        .await?
        .aggregate(vec![grouping_set_expr], vec![count(col("a"))])?
        .sort(vec![
            Sort::new(col("a"), false, true),
            Sort::new(col("b"), false, true),
        ])?;

    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----------+-----+---------------+
    | a         | b   | count(test.a) |
    +-----------+-----+---------------+
    |           | 100 | 1             |
    |           | 10  | 2             |
    |           | 1   | 1             |
    | abcDEF    |     | 1             |
    | abcDEF    | 1   | 1             |
    | abc123    |     | 1             |
    | abc123    | 10  | 1             |
    | CBAdef    |     | 1             |
    | CBAdef    | 10  | 1             |
    | 123AbcDef |     | 1             |
    | 123AbcDef | 100 | 1             |
    +-----------+-----+---------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_grouping_sets_count() -> Result<()> {
    let ctx = SessionContext::new();

    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("c1")],
        vec![col("c2")],
    ]));

    let df = aggregates_table(&ctx)
        .await?
        .aggregate(vec![grouping_set_expr], vec![count(lit(1))])?
        .sort(vec![
            Sort::new(col("c1"), false, true),
            Sort::new(col("c2"), false, true),
        ])?;

    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +----+----+-----------------+
    | c1 | c2 | count(Int32(1)) |
    +----+----+-----------------+
    |    | 5  | 14              |
    |    | 4  | 23              |
    |    | 3  | 19              |
    |    | 2  | 22              |
    |    | 1  | 22              |
    | e  |    | 21              |
    | d  |    | 18              |
    | c  |    | 21              |
    | b  |    | 19              |
    | a  |    | 21              |
    +----+----+-----------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_grouping_set_array_agg_with_overflow() -> Result<()> {
    let ctx = SessionContext::new();

    let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
        vec![col("c1")],
        vec![col("c2")],
        vec![col("c1"), col("c2")],
    ]));

    let df = aggregates_table(&ctx)
        .await?
        .aggregate(
            vec![grouping_set_expr],
            vec![
                sum(col("c3")).alias("sum_c3"),
                avg(col("c3")).alias("avg_c3"),
            ],
        )?
        .sort(vec![
            Sort::new(col("c1"), false, true),
            Sort::new(col("c2"), false, true),
        ])?;

    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +----+----+--------+---------------------+
    | c1 | c2 | sum_c3 | avg_c3              |
    +----+----+--------+---------------------+
    |    | 5  | -194   | -13.857142857142858 |
    |    | 4  | 29     | 1.2608695652173914  |
    |    | 3  | 395    | 20.789473684210527  |
    |    | 2  | 184    | 8.363636363636363   |
    |    | 1  | 367    | 16.681818181818183  |
    | e  |    | 847    | 40.333333333333336  |
    | e  | 5  | -22    | -11.0               |
    | e  | 4  | 261    | 37.285714285714285  |
    | e  | 3  | 192    | 48.0                |
    | e  | 2  | 189    | 37.8                |
    | e  | 1  | 227    | 75.66666666666667   |
    | d  |    | 458    | 25.444444444444443  |
    | d  | 5  | -99    | -49.5               |
    | d  | 4  | 162    | 54.0                |
    | d  | 3  | 124    | 41.333333333333336  |
    | d  | 2  | 328    | 109.33333333333333  |
    | d  | 1  | -57    | -8.142857142857142  |
    | c  |    | -28    | -1.3333333333333333 |
    | c  | 5  | 24     | 12.0                |
    | c  | 4  | -43    | -10.75              |
    | c  | 3  | 190    | 47.5                |
    | c  | 2  | -389   | -55.57142857142857  |
    | c  | 1  | 190    | 47.5                |
    | b  |    | -111   | -5.842105263157895  |
    | b  | 5  | -1     | -0.2                |
    | b  | 4  | -223   | -44.6               |
    | b  | 3  | -84    | -42.0               |
    | b  | 2  | 102    | 25.5                |
    | b  | 1  | 95     | 31.666666666666668  |
    | a  |    | -385   | -18.333333333333332 |
    | a  | 5  | -96    | -32.0               |
    | a  | 4  | -128   | -32.0               |
    | a  | 3  | -27    | -4.5                |
    | a  | 2  | -46    | -15.333333333333334 |
    | a  | 1  | -88    | -17.6               |
    +----+----+--------+---------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn join_with_alias_filter() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;
    let t1_schema = t1.schema().clone();
    let t2_schema = t2.schema().clone();

    // filter: t1.a + CAST(Int64(1), UInt32) = t2.a + CAST(Int64(2), UInt32) as t1.a + 1 = t2.a + 2
    let filter = Expr::eq(
        col("t1.a") + lit(3i64).cast_to(&DataType::UInt32, &t1_schema)?,
        col("t2.a") + lit(1i32).cast_to(&DataType::UInt32, &t2_schema)?,
    )
    .alias("t1.b + 1 = t2.a + 2");

    let df = t1
        .join(t2, JoinType::Inner, &[], &[], Some(filter))?
        .select(vec![
            col("t1.a"),
            col("t2.a"),
            col("t1.b"),
            col("t1.c"),
            col("t2.b"),
            col("t2.c"),
        ])?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    Projection: t1.a, t2.a, t1.b, t1.c, t2.b, t2.c [a:UInt32, a:UInt32, b:Utf8, c:Int32, b:Utf8, c:Int32]
      Inner Join: t1.a + UInt32(3) = t2.a + UInt32(1) [a:UInt32, b:Utf8, c:Int32, a:UInt32, b:Utf8, c:Int32]
        TableScan: t1 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]
        TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]
    "
    );

    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+----+---+----+---+---+
    | a  | a  | b | c  | b | c |
    +----+----+---+----+---+---+
    | 1  | 3  | a | 10 | a | 1 |
    | 11 | 13 | c | 30 | c | 3 |
    +----+----+---+----+---+---+
    "
    );

    Ok(())
}

#[tokio::test]
async fn right_semi_with_alias_filter() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;

    // t1.a = t2.a and t1.c > 1 and t2.c > 1
    let filter = col("t1.a")
        .eq(col("t2.a"))
        .and(col("t1.c").gt(lit(1u32)))
        .and(col("t2.c").gt(lit(1u32)));

    let df = t1
        .join(t2, JoinType::RightSemi, &[], &[], Some(filter))?
        .select(vec![col("t2.a"), col("t2.b"), col("t2.c")])?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    RightSemi Join: t1.a = t2.a [a:UInt32, b:Utf8, c:Int32]
      Projection: t1.a [a:UInt32]
        Filter: t1.c > Int32(1) [a:UInt32, c:Int32]
          TableScan: t1 projection=[a, c] [a:UInt32, c:Int32]
      Filter: t2.c > Int32(1) [a:UInt32, b:Utf8, c:Int32]
        TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]
    "
    );

    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +-----+---+---+
    | a   | b | c |
    +-----+---+---+
    | 10  | b | 2 |
    | 100 | d | 4 |
    +-----+---+---+
    "
    );

    Ok(())
}

#[tokio::test]
async fn right_anti_filter_push_down() -> Result<()> {
    let join_ctx = create_join_context()?;
    let t1 = join_ctx.table("t1").await?;
    let t2 = join_ctx.table("t2").await?;

    // t1.a = t2.a and t1.c > 1 and t2.c > 1
    let filter = col("t1.a")
        .eq(col("t2.a"))
        .and(col("t1.c").gt(lit(1u32)))
        .and(col("t2.c").gt(lit(1u32)));

    let df = t1
        .join(t2, JoinType::RightAnti, &[], &[], Some(filter))?
        .select(vec![col("t2.a"), col("t2.b"), col("t2.c")])?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    RightAnti Join: t1.a = t2.a Filter: t2.c > Int32(1) [a:UInt32, b:Utf8, c:Int32]
      Projection: t1.a [a:UInt32]
        Filter: t1.c > Int32(1) [a:UInt32, c:Int32]
          TableScan: t1 projection=[a, c] [a:UInt32, c:Int32]
      TableScan: t2 projection=[a, b, c] [a:UInt32, b:Utf8, c:Int32]
    "
    );

    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+---+---+
    | a  | b | c |
    +----+---+---+
    | 13 | c | 3 |
    | 3  | a | 1 |
    +----+---+---+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_columns() -> Result<()> {
    const NUM_ROWS: usize = 4;
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+---------------------------------+--------------------------+
    | shape_id | points                          | tags                     |
    +----------+---------------------------------+--------------------------+
    | 1        | [{x: 5, y: -8}, {x: -3, y: -4}] | [tag1]                   |
    | 2        | [{x: 6, y: 2}, {x: -2, y: -8}]  | [tag1]                   |
    | 3        | [{x: -9, y: -7}, {x: -2, y: 5}] | [tag1, tag2, tag3, tag4] |
    | 4        |                                 | [tag1, tag2, tag3]       |
    +----------+---------------------------------+--------------------------+
    ");

    // Unnest tags
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.unnest_columns(&["tags"])?.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+---------------------------------+------+
    | shape_id | points                          | tags |
    +----------+---------------------------------+------+
    | 1        | [{x: 5, y: -8}, {x: -3, y: -4}] | tag1 |
    | 2        | [{x: 6, y: 2}, {x: -2, y: -8}]  | tag1 |
    | 3        | [{x: -9, y: -7}, {x: -2, y: 5}] | tag1 |
    | 3        | [{x: -9, y: -7}, {x: -2, y: 5}] | tag2 |
    | 3        | [{x: -9, y: -7}, {x: -2, y: 5}] | tag3 |
    | 3        | [{x: -9, y: -7}, {x: -2, y: 5}] | tag4 |
    | 4        |                                 | tag1 |
    | 4        |                                 | tag2 |
    | 4        |                                 | tag3 |
    +----------+---------------------------------+------+
    ");

    // Test aggregate results for tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df.unnest_columns(&["tags"])?.count().await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    // Unnest points
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.unnest_columns(&["points"])?.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+----------------+--------------------------+
    | shape_id | points         | tags                     |
    +----------+----------------+--------------------------+
    | 1        | {x: -3, y: -4} | [tag1]                   |
    | 1        | {x: 5, y: -8}  | [tag1]                   |
    | 2        | {x: -2, y: -8} | [tag1]                   |
    | 2        | {x: 6, y: 2}   | [tag1]                   |
    | 3        | {x: -2, y: 5}  | [tag1, tag2, tag3, tag4] |
    | 3        | {x: -9, y: -7} | [tag1, tag2, tag3, tag4] |
    | 4        |                | [tag1, tag2, tag3]       |
    +----------+----------------+--------------------------+
    ");

    // Test aggregate results for points.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df.unnest_columns(&["points"])?.count().await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    // Unnest both points and tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_columns(&["points"])?
        .unnest_columns(&["tags"])?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+----------------+------+
    | shape_id | points         | tags |
    +----------+----------------+------+
    | 1        | {x: -3, y: -4} | tag1 |
    | 1        | {x: 5, y: -8}  | tag1 |
    | 2        | {x: -2, y: -8} | tag1 |
    | 2        | {x: 6, y: 2}   | tag1 |
    | 3        | {x: -2, y: 5}  | tag1 |
    | 3        | {x: -2, y: 5}  | tag2 |
    | 3        | {x: -2, y: 5}  | tag3 |
    | 3        | {x: -2, y: 5}  | tag4 |
    | 3        | {x: -9, y: -7} | tag1 |
    | 3        | {x: -9, y: -7} | tag2 |
    | 3        | {x: -9, y: -7} | tag3 |
    | 3        | {x: -9, y: -7} | tag4 |
    | 4        |                | tag1 |
    | 4        |                | tag2 |
    | 4        |                | tag3 |
    +----------+----------------+------+
    ");

    // Test aggregate results for points and tags.
    let df = table_with_nested_types(NUM_ROWS).await?;
    let count = df
        .unnest_columns(&["points"])?
        .unnest_columns(&["tags"])?
        .count()
        .await?;
    assert_eq!(count, results.iter().map(|r| r.num_rows()).sum::<usize>());

    Ok(())
}

#[tokio::test]
async fn unnest_dict_encoded_columns() -> Result<()> {
    let strings = vec!["x", "y", "z"];
    let keys = Int32Array::from_iter(0..strings.len() as i32);

    let utf8_values = StringArray::from(strings.clone());
    let utf8_dict = DictionaryArray::new(keys.clone(), Arc::new(utf8_values));

    let make_array_udf_expr1 = make_array_udf().call(vec![col("column1")]);
    let batch =
        RecordBatch::try_from_iter(vec![("column1", Arc::new(utf8_dict) as ArrayRef)])?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;
    let df = ctx
        .table("test")
        .await?
        .select(vec![
            make_array_udf_expr1.alias("make_array_expr"),
            col("column1"),
        ])?
        .unnest_columns(&["make_array_expr"])?;

    let results = df.collect().await.unwrap();
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----------------+---------+
    | make_array_expr | column1 |
    +-----------------+---------+
    | x               | x       |
    | y               | y       |
    | z               | z       |
    +-----------------+---------+
    "
    );

    // make_array(dict_encoded_string,literal string)
    let make_array_udf_expr2 = make_array_udf().call(vec![
        col("column1"),
        lit(ScalarValue::new_utf8("fixed_string")),
    ]);
    let df = ctx
        .table("test")
        .await?
        .select(vec![
            make_array_udf_expr2.alias("make_array_expr"),
            col("column1"),
        ])?
        .unnest_columns(&["make_array_expr"])?;

    let results = df.collect().await.unwrap();
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----------------+---------+
    | make_array_expr | column1 |
    +-----------------+---------+
    | x               | x       |
    | fixed_string    | x       |
    | y               | y       |
    | fixed_string    | y       |
    | z               | z       |
    | fixed_string    | z       |
    +-----------------+---------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn unnest_column_nulls() -> Result<()> {
    let df = table_with_lists_and_nulls().await?;
    let results = df.clone().collect().await?;
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +--------+----+
    | list   | id |
    +--------+----+
    | [1, 2] | A  |
    |        | B  |
    | []     | C  |
    | [3]    | D  |
    +--------+----+
    "
    );

    // Unnest, preserving nulls (row with B is preserved)
    let options = UnnestOptions::new().with_preserve_nulls(true);

    let results = df
        .clone()
        .unnest_columns_with_options(&["list"], options)?
        .collect()
        .await?;
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +------+----+
    | list | id |
    +------+----+
    | 1    | A  |
    | 2    | A  |
    |      | B  |
    | 3    | D  |
    +------+----+
    "
    );

    let options = UnnestOptions::new().with_preserve_nulls(false);
    let results = df
        .unnest_columns_with_options(&["list"], options)?
        .collect()
        .await?;
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +------+----+
    | list | id |
    +------+----+
    | 1    | A  |
    | 2    | A  |
    | 3    | D  |
    +------+----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_fixed_list() -> Result<()> {
    let batch = get_fixed_list_batch()?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+----------------+
    | shape_id | tags           |
    +----------+----------------+
    | 1        |                |
    | 2        | [tag21, tag22] |
    | 3        | [tag31, tag32] |
    | 4        |                |
    | 5        | [tag51, tag52] |
    | 6        | [tag61, tag62] |
    +----------+----------------+
    "
    );

    let options = UnnestOptions::new().with_preserve_nulls(true);

    let results = df
        .unnest_columns_with_options(&["tags"], options)?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+-------+
    | shape_id | tags  |
    +----------+-------+
    | 1        |       |
    | 2        | tag21 |
    | 2        | tag22 |
    | 3        | tag31 |
    | 3        | tag32 |
    | 4        |       |
    | 5        | tag51 |
    | 5        | tag52 |
    | 6        | tag61 |
    | 6        | tag62 |
    +----------+-------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_fixed_list_drop_nulls() -> Result<()> {
    let batch = get_fixed_list_batch()?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+----------------+
    | shape_id | tags           |
    +----------+----------------+
    | 1        |                |
    | 2        | [tag21, tag22] |
    | 3        | [tag31, tag32] |
    | 4        |                |
    | 5        | [tag51, tag52] |
    | 6        | [tag61, tag62] |
    +----------+----------------+
    "
    );

    let options = UnnestOptions::new().with_preserve_nulls(false);

    let results = df
        .unnest_columns_with_options(&["tags"], options)?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+-------+
    | shape_id | tags  |
    +----------+-------+
    | 2        | tag21 |
    | 2        | tag22 |
    | 3        | tag31 |
    | 3        | tag32 |
    | 5        | tag51 |
    | 5        | tag52 |
    | 6        | tag61 |
    | 6        | tag62 |
    +----------+-------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_fixed_list_non_null() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tags_builder = FixedSizeListBuilder::new(StringBuilder::new(), 2);

    for idx in 0..6 {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        tags_builder
            .values()
            .append_value(format!("tag{}1", idx + 1));
        tags_builder
            .values()
            .append_value(format!("tag{}2", idx + 1));
        tags_builder.append(true);
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+----------------+
    | shape_id | tags           |
    +----------+----------------+
    | 1        | [tag11, tag12] |
    | 2        | [tag21, tag22] |
    | 3        | [tag31, tag32] |
    | 4        | [tag41, tag42] |
    | 5        | [tag51, tag52] |
    | 6        | [tag61, tag62] |
    +----------+----------------+
    "
    );

    let options = UnnestOptions::new().with_preserve_nulls(true);
    let results = df
        .unnest_columns_with_options(&["tags"], options)?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+-------+
    | shape_id | tags  |
    +----------+-------+
    | 1        | tag11 |
    | 1        | tag12 |
    | 2        | tag21 |
    | 2        | tag22 |
    | 3        | tag31 |
    | 3        | tag32 |
    | 4        | tag41 |
    | 4        | tag42 |
    | 5        | tag51 |
    | 5        | tag52 |
    | 6        | tag61 |
    | 6        | tag62 |
    +----------+-------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_aggregate_columns() -> Result<()> {
    const NUM_ROWS: usize = 5;

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df.select_columns(&["tags"])?.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +--------------------------+
    | tags                     |
    +--------------------------+
    | [tag1, tag2, tag3, tag4] |
    | [tag1, tag2, tag3]       |
    | [tag1, tag2]             |
    | [tag1]                   |
    | [tag1]                   |
    +--------------------------+
    "
    );

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_columns(&["tags"])?
        .aggregate(vec![], vec![count(col("tags"))])?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +-------------+
    | count(tags) |
    +-------------+
    | 11          |
    +-------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_no_empty_batches() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tag_id_builder = UInt32Builder::new();

    for shape_id in 1..=10 {
        for tag_id in 1..=10 {
            shape_id_builder.append_value(shape_id as u32);
            tag_id_builder.append_value((shape_id * 10 + tag_id) as u32);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tag_id", Arc::new(tag_id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df
        .clone()
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("tag_id")).alias("tag_id")],
        )?
        .collect()
        .await?;

    // Assert that there are no empty batches in result
    for rb in results {
        assert!(rb.num_rows() > 0);
    }
    Ok(())
}

#[tokio::test]
async fn unnest_array_agg() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tag_id_builder = UInt32Builder::new();

    for shape_id in 1..=3 {
        for tag_id in 1..=3 {
            shape_id_builder.append_value(shape_id as u32);
            tag_id_builder.append_value((shape_id * 10 + tag_id) as u32);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tag_id", Arc::new(tag_id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;

    // Assert that there are no empty batches in result
    for rb in results.clone() {
        assert!(rb.num_rows() > 0);
    }

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+--------+
    | shape_id | tag_id |
    +----------+--------+
    | 1        | 11     |
    | 1        | 12     |
    | 1        | 13     |
    | 2        | 21     |
    | 2        | 22     |
    | 2        | 23     |
    | 3        | 31     |
    | 3        | 32     |
    | 3        | 33     |
    +----------+--------+
    "
    );

    // Doing an `array_agg` by `shape_id` produces:
    let results = df
        .clone()
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("tag_id")).alias("tag_id")],
        )?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+--------------+
    | shape_id | tag_id       |
    +----------+--------------+
    | 1        | [11, 12, 13] |
    | 2        | [21, 22, 23] |
    | 3        | [31, 32, 33] |
    +----------+--------------+
    "
    );

    // Unnesting again should produce the original batch.
    let results = ctx
        .table("shapes")
        .await?
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("tag_id")).alias("tag_id")],
        )?
        .unnest_columns(&["tag_id"])?
        .collect()
        .await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+--------+
    | shape_id | tag_id |
    +----------+--------+
    | 1        | 11     |
    | 1        | 12     |
    | 1        | 13     |
    | 2        | 21     |
    | 2        | 22     |
    | 2        | 23     |
    | 3        | 31     |
    | 3        | 32     |
    | 3        | 33     |
    +----------+--------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_with_redundant_columns() -> Result<()> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tag_id_builder = UInt32Builder::new();

    for shape_id in 1..=3 {
        for tag_id in 1..=3 {
            shape_id_builder.append_value(shape_id as u32);
            tag_id_builder.append_value((shape_id * 10 + tag_id) as u32);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tag_id", Arc::new(tag_id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    let df = ctx.table("shapes").await?;

    let results = df.clone().collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+--------+
    | shape_id | tag_id |
    +----------+--------+
    | 1        | 11     |
    | 1        | 12     |
    | 1        | 13     |
    | 2        | 21     |
    | 2        | 22     |
    | 2        | 23     |
    | 3        | 31     |
    | 3        | 32     |
    | 3        | 33     |
    +----------+--------+
    "
    );

    // Doing an `array_agg` by `shape_id` produces:
    let df = df
        .clone()
        .aggregate(
            vec![col("shape_id")],
            vec![array_agg(col("shape_id")).alias("shape_id2")],
        )?
        .unnest_columns(&["shape_id2"])?
        .select(vec![col("shape_id")])?;

    let optimized_plan = df.clone().into_optimized_plan()?;

    let formatted = optimized_plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    Projection: shapes.shape_id [shape_id:UInt32]
      Unnest: lists[shape_id2|depth=1] structs[] [shape_id:UInt32, shape_id2:UInt32;N]
        Aggregate: groupBy=[[shapes.shape_id]], aggr=[[array_agg(shapes.shape_id) AS shape_id2]] [shape_id:UInt32, shape_id2:List(UInt32);N]
          TableScan: shapes projection=[shape_id] [shape_id:UInt32]
    "
    );

    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----------+
    | shape_id |
    +----------+
    | 1        |
    | 1        |
    | 1        |
    | 2        |
    | 2        |
    | 2        |
    | 3        |
    | 3        |
    | 3        |
    +----------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn unnest_analyze_metrics() -> Result<()> {
    const NUM_ROWS: usize = 5;

    let df = table_with_nested_types(NUM_ROWS).await?;
    let results = df
        .unnest_columns(&["tags"])?
        .explain(false, true)?
        .collect()
        .await?;
    let formatted = pretty_format_batches(&results).unwrap().to_string();
    assert_contains!(&formatted, "elapsed_compute=");
    assert_contains!(&formatted, "input_batches=1");
    assert_contains!(&formatted, "input_rows=5");
    assert_contains!(&formatted, "output_rows=11");
    assert_contains!(&formatted, "output_batches=1");

    Ok(())
}

#[tokio::test]
async fn unnest_multiple_columns() -> Result<()> {
    let df = table_with_mixed_lists().await?;
    // Default behavior is to preserve nulls.
    let results = df
        .clone()
        .unnest_columns(&["list", "large_list", "fixed_list"])?
        .collect()
        .await?;
    // list:        [1,2,3], null, [null], null,
    // large_list:  [null, 1.1], [2.2, 3.3, 4.4], null, [],
    // fixed_list:  null, [1,2], [3,4], null
    // string:      a, b, c, d
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +------+------------+------------+--------+
    | list | large_list | fixed_list | string |
    +------+------------+------------+--------+
    | 1    |            |            | a      |
    | 2    | 1.1        |            | a      |
    | 3    |            |            | a      |
    |      | 2.2        | 1          | b      |
    |      | 3.3        | 2          | b      |
    |      | 4.4        |            | b      |
    |      |            | 3          | c      |
    |      |            | 4          | c      |
    |      |            |            | d      |
    +------+------------+------------+--------+
    "
    );

    // Test with `preserve_nulls = false``
    let results = df
        .unnest_columns_with_options(
            &["list", "large_list", "fixed_list"],
            UnnestOptions::new().with_preserve_nulls(false),
        )?
        .collect()
        .await?;
    // list:        [1,2,3], null, [null], null,
    // large_list:  [null, 1.1], [2.2, 3.3, 4.4], null, [],
    // fixed_list:  null, [1,2], [3,4], null
    // string:      a, b, c, d
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +------+------------+------------+--------+
    | list | large_list | fixed_list | string |
    +------+------------+------------+--------+
    | 1    |            |            | a      |
    | 2    | 1.1        |            | a      |
    | 3    |            |            | a      |
    |      | 2.2        | 1          | b      |
    |      | 3.3        | 2          | b      |
    |      | 4.4        |            | b      |
    |      |            | 3          | c      |
    |      |            | 4          | c      |
    +------+------------+------------+--------+
    "
    );

    Ok(())
}

/// Test unnesting a non-nullable list.
#[tokio::test]
async fn unnest_non_nullable_list() -> Result<()> {
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![None]),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c1",
        DataType::new_list(DataType::Int32, true),
        false,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)])?;
    let ctx = SessionContext::new();
    let results = ctx
        .read_batches(vec![batch])?
        .unnest_columns(&["c1"])?
        .collect()
        .await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +----+
    | c1 |
    +----+
    | 1  |
    | 2  |
    |    |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_read_batches() -> Result<()> {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("number", DataType::Float32, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Float32Array::from(vec![1.12, 3.40, 2.33, 9.10, 6.66])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4, 5])),
                Arc::new(Float32Array::from(vec![1.11, 2.22, 3.33])),
            ],
        )
        .unwrap(),
    ];
    let df = ctx.read_batches(batches).unwrap();
    df.clone().show().await.unwrap();
    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+--------+
    | id | number |
    +----+--------+
    | 1  | 1.12   |
    | 2  | 3.4    |
    | 3  | 1.11   |
    | 3  | 2.33   |
    | 4  | 2.22   |
    | 4  | 9.1    |
    | 5  | 3.33   |
    | 5  | 6.66   |
    +----+--------+
    "
    );
    Ok(())
}
#[tokio::test]
async fn test_read_batches_empty() -> Result<()> {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    let batches = vec![];
    let df = ctx.read_batches(batches).unwrap();
    df.clone().show().await.unwrap();
    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    ++
    ++
    "
    );
    Ok(())
}

#[tokio::test]
async fn consecutive_projection_same_schema() -> Result<()> {
    let state = SessionStateBuilder::new().with_default_features().build();
    let ctx = SessionContext::new_with_state(state);

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0, 1]))])
            .unwrap();

    let df = ctx.read_batch(batch).unwrap();
    df.clone().show().await.unwrap();

    // Add `t` column full of nulls
    let df = df
        .with_column(
            "t",
            cast(Expr::Literal(ScalarValue::Null, None), DataType::Int32),
        )
        .unwrap();
    df.clone().show().await.unwrap();

    let df = df
        // (case when id = 1 then 10 else t) as t
        .with_column(
            "t",
            when(col("id").eq(lit(1)), lit(10))
                .otherwise(col("t"))
                .unwrap(),
        )
        .unwrap()
        // (case when id = 1 then 10 else t) as t2
        .with_column(
            "t2",
            when(col("id").eq(lit(1)), lit(10))
                .otherwise(col("t"))
                .unwrap(),
        )
        .unwrap();

    let results = df.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +----+----+----+
    | id | t  | t2 |
    +----+----+----+
    | 0  |    |    |
    | 1  | 10 | 10 |
    +----+----+----+
    "
    );

    Ok(())
}

async fn create_test_table(name: &str) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "abcDEF",
                "abc123",
                "CBAdef",
                "123AbcDef",
            ])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch(name, batch)?;

    ctx.table(name).await
}

async fn aggregates_table(ctx: &SessionContext) -> Result<DataFrame> {
    let testdata = datafusion::test_util::arrow_test_data();

    ctx.read_csv(
        format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await
}

fn create_join_context() -> Result<SessionContext> {
    let t1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let t2 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // define data.
    let batch1 = RecordBatch::try_new(
        t1,
        vec![
            Arc::new(UInt32Array::from(vec![1, 10, 11, 100])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    // define data.
    let batch2 = RecordBatch::try_new(
        t2,
        vec![
            Arc::new(UInt32Array::from(vec![3, 10, 13, 100])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        ],
    )?;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    // let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;

    Ok(ctx)
}

/// Create a data frame that contains nested types.
///
/// Create a data frame with nested types, each row contains:
/// - shape_id an integer primary key
/// - points A list of points structs {x, y}
/// - A list of tags.
async fn table_with_nested_types(n: usize) -> Result<DataFrame> {
    use rand::prelude::*;

    let mut shape_id_builder = UInt32Builder::new();
    let mut points_builder = ListBuilder::new(StructBuilder::from_fields(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ],
        5,
    ));
    let mut tags_builder = ListBuilder::new(StringBuilder::new());

    let mut rng = StdRng::seed_from_u64(197);

    for idx in 0..n {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        // Add a random number of points
        let num_points: usize = rng.random_range(0..4);
        if num_points > 0 {
            for _ in 0..num_points.max(2) {
                // Add x value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(0)
                    .unwrap()
                    .append_value(rng.random_range(-10..10));
                // Add y value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(1)
                    .unwrap()
                    .append_value(rng.random_range(-10..10));
                points_builder.values().append(true);
            }
        }

        // Append null if num points is 0.
        points_builder.append(num_points > 0);

        // Append tags.
        let num_tags: usize = rng.random_range(0..5);
        for id in 0..num_tags {
            tags_builder.values().append_value(format!("tag{}", id + 1));
        }
        tags_builder.append(num_tags > 0);
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("points", Arc::new(points_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    ctx.table("shapes").await
}

fn get_fixed_list_batch() -> Result<RecordBatch, ArrowError> {
    let mut shape_id_builder = UInt32Builder::new();
    let mut tags_builder = FixedSizeListBuilder::new(StringBuilder::new(), 2);

    for idx in 0..6 {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        if idx % 3 != 0 {
            tags_builder
                .values()
                .append_value(format!("tag{}1", idx + 1));
            tags_builder
                .values()
                .append_value(format!("tag{}2", idx + 1));
            tags_builder.append(true);
        } else {
            tags_builder.values().append_null();
            tags_builder.values().append_null();
            tags_builder.append(false);
        }
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    Ok(batch)
}

/// Create a table with different types of list columns and a string column.
async fn table_with_mixed_lists() -> Result<DataFrame> {
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
        Some(vec![None]),
        None,
    ]);

    let large_list_array =
        LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![None, Some(1.1)]),
            Some(vec![Some(2.2), Some(3.3), Some(4.4)]),
            None,
            Some(vec![]),
        ]);

    let fixed_list_array = FixedSizeListArray::from_iter_primitive::<UInt64Type, _, _>(
        vec![
            None,
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            None,
        ],
        2,
    );

    let string_array = StringArray::from(vec!["a", "b", "c", "d"]);

    let batch = RecordBatch::try_from_iter(vec![
        ("list", Arc::new(list_array) as ArrayRef),
        ("large_list", Arc::new(large_list_array) as ArrayRef),
        ("fixed_list", Arc::new(fixed_list_array) as ArrayRef),
        ("string", Arc::new(string_array) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("mixed_lists", batch)?;
    ctx.table("mixed_lists").await
}

/// A a data frame that a list of integers and string IDs
async fn table_with_lists_and_nulls() -> Result<DataFrame> {
    let mut list_builder = ListBuilder::new(UInt32Builder::new());
    let mut id_builder = StringBuilder::new();

    // [1, 2],  A
    list_builder.values().append_value(1);
    list_builder.values().append_value(2);
    list_builder.append(true);
    id_builder.append_value("A");

    // NULL, B
    list_builder.append(false);
    id_builder.append_value("B");

    // [],  C
    list_builder.append(true);
    id_builder.append_value("C");

    // [3], D
    list_builder.values().append_value(3);
    list_builder.append(true);
    id_builder.append_value("D");

    let batch = RecordBatch::try_from_iter(vec![
        ("list", Arc::new(list_builder.finish()) as ArrayRef),
        ("id", Arc::new(id_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    ctx.table("shapes").await
}

pub async fn register_alltypes_tiny_pages_parquet(ctx: &SessionContext) -> Result<()> {
    let testdata = parquet_test_data();
    ctx.register_parquet(
        "alltypes_tiny_pages",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(())
}
#[derive(Debug)]
struct HardcodedIntProvider {}

impl VarProvider for HardcodedIntProvider {
    fn get_value(&self, _var_names: Vec<String>) -> Result<ScalarValue, DataFusionError> {
        Ok(ScalarValue::Int64(Some(1234)))
    }

    fn get_type(&self, _: &[String]) -> Option<DataType> {
        Some(DataType::Int64)
    }
}

#[tokio::test]
async fn use_var_provider() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("foo", DataType::Int64, false),
        Field::new("bar", DataType::Int64, false),
    ]));

    let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![]])?);

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.optimizer.skip_failed_rules", false);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("csv_table", mem_table)?;
    ctx.register_variable(VarType::UserDefined, Arc::new(HardcodedIntProvider {}));

    let dataframe = ctx
        .sql("SELECT foo FROM csv_table WHERE bar > @var")
        .await?;
    dataframe.collect().await?;
    Ok(())
}

#[tokio::test]
async fn test_array_agg() -> Result<()> {
    let df = create_test_table("test")
        .await?
        .aggregate(vec![], vec![array_agg(col("a"))])?;

    let results = df.collect().await?;

    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-------------------------------------+
    | array_agg(test.a)                   |
    +-------------------------------------+
    | [abcDEF, abc123, CBAdef, 123AbcDef] |
    +-------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_dataframe_placeholder_missing_param_values() -> Result<()> {
    let ctx = SessionContext::new();

    // Creating LogicalPlans with placeholders should work.
    let df = ctx
        .read_empty()
        .unwrap()
        .with_column("a", lit(1))
        .unwrap()
        .filter(col("a").eq(placeholder("$0")))
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    Filter: a = $0 [a:Int32]
      Projection: Int32(1) AS a [a:Int32]
        EmptyRelation: rows=1 []
    "
    );

    // Executing LogicalPlans with placeholders that don't have bound values
    // should fail.
    let results = df.collect().await;
    let err_msg = results.unwrap_err().strip_backtrace();
    assert_snapshot!(
        err_msg,
        @"Execution error: Placeholder '$0' was not provided a value for execution."
    );

    // Providing a parameter value should resolve the error
    let df = ctx
        .read_empty()
        .unwrap()
        .with_column("a", lit(1))
        .unwrap()
        .filter(col("a").eq(placeholder("$0")))
        .unwrap()
        .with_param_values(vec![("0", ScalarValue::from(3i32))]) // <-- provide parameter value
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    Filter: a = Int32(3) [a:Int32]
      Projection: Int32(1) AS a [a:Int32]
        EmptyRelation: rows=1 []
    "
    );

    // N.B., the test is basically `SELECT 1 as a WHERE a = 3;` which returns no results.
    assert_snapshot!(
       batches_to_string(&df.collect().await.unwrap()),
        @r"
    ++
    ++
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_dataframe_placeholder_column_parameter() -> Result<()> {
    let ctx = SessionContext::new();

    // Creating LogicalPlans with placeholders should work
    let df = ctx.read_empty().unwrap().select_exprs(&["$1"]).unwrap();
    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    Projection: $1 [$1:Null;N]
      EmptyRelation: rows=1 []
    "
    );

    // Executing LogicalPlans with placeholders that don't have bound values
    // should fail.
    let results = df.collect().await;
    let err_msg = results.unwrap_err().strip_backtrace();
    assert_snapshot!(
        err_msg,
        @"Execution error: Placeholder '$1' was not provided a value for execution."
    );

    // Providing a parameter value should resolve the error
    let df = ctx
        .read_empty()
        .unwrap()
        .select_exprs(&["$1"])
        .unwrap()
        .with_param_values(vec![("1", ScalarValue::from(3i32))])
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r"
    Projection: Int32(3) AS $1 [$1:Int32]
      EmptyRelation: rows=1 []
    "
    );

    assert_snapshot!(
       batches_to_string(&df.collect().await.unwrap()),
        @r"
    +----+
    | $1 |
    +----+
    | 3  |
    +----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_dataframe_placeholder_like_expression() -> Result<()> {
    let ctx = SessionContext::new();

    // Creating LogicalPlans with placeholders should work
    let df = ctx
        .read_empty()
        .unwrap()
        .with_column("a", lit("foo"))
        .unwrap()
        .filter(col("a").like(placeholder("$1")))
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r#"
    Filter: a LIKE $1 [a:Utf8]
      Projection: Utf8("foo") AS a [a:Utf8]
        EmptyRelation: rows=1 []
    "#
    );

    // Executing LogicalPlans with placeholders that don't have bound values
    // should fail.
    let results = df.collect().await;
    let err_msg = results.unwrap_err().strip_backtrace();
    assert_snapshot!(
        err_msg,
        @"Execution error: Placeholder '$1' was not provided a value for execution."
    );

    // Providing a parameter value should resolve the error
    let df = ctx
        .read_empty()
        .unwrap()
        .with_column("a", lit("foo"))
        .unwrap()
        .filter(col("a").like(placeholder("$1")))
        .unwrap()
        .with_param_values(vec![("1", ScalarValue::from("f%"))])
        .unwrap();

    let logical_plan = df.logical_plan();
    let formatted = logical_plan.display_indent_schema().to_string();
    let actual = formatted.trim();

    assert_snapshot!(
        actual,
        @r#"
    Filter: a LIKE Utf8("f%") [a:Utf8]
      Projection: Utf8("foo") AS a [a:Utf8]
        EmptyRelation: rows=1 []
    "#
    );

    assert_snapshot!(
       batches_to_string(&df.collect().await.unwrap()),
        @r"
    +-----+
    | a   |
    +-----+
    | foo |
    +-----+
    "
    );

    Ok(())
}

#[rstest]
#[case(DataType::Utf8)]
#[case(DataType::LargeUtf8)]
#[case(DataType::Utf8View)]
#[tokio::test]
async fn write_partitioned_parquet_results(#[case] string_type: DataType) -> Result<()> {
    // Create an in memory table with schema C1 and C2, both strings
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", string_type.clone(), false),
        Field::new("c2", string_type.clone(), false),
    ]));

    let columns = [
        Arc::new(StringArray::from(vec!["abc", "def"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["123", "456"])) as ArrayRef,
    ]
    .map(|col| arrow::compute::cast(&col, &string_type).unwrap())
    .to_vec();

    let record_batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![record_batch]])?);

    // Register the table in the context
    // create partitioned input file and context
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    ctx.register_table("test", mem_table)?;

    let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
    let local_url = Url::parse("file://local").unwrap();
    ctx.register_object_store(&local_url, local);

    // execute a simple query and write the results to parquet
    let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
    let out_dir_url = format!("file://{out_dir}");

    // Write the results to parquet with partitioning
    let df = ctx.sql("SELECT c1, c2 FROM test").await?;
    let df_write_options =
        DataFrameWriteOptions::new().with_partition_by(vec![String::from("c2")]);

    df.write_parquet(&out_dir_url, df_write_options, None)
        .await?;

    // Explicitly read the parquet file at c2=123 to verify the physical files are partitioned
    let partitioned_file = format!("{out_dir}/c2=123");
    let filter_df = ctx
        .read_parquet(&partitioned_file, ParquetReadOptions::default())
        .await?;

    // Check that the c2 column is gone and that c1 is abc.
    let results = filter_df.collect().await?;
    insta::allow_duplicates! {
    assert_snapshot!(
       batches_to_string(&results),
        @r"
    +-----+
    | c1  |
    +-----+
    | abc |
    +-----+
    "
    )};

    // Read the entire set of parquet files
    let df = ctx
        .read_parquet(
            &out_dir_url,
            ParquetReadOptions::default()
                .table_partition_cols(vec![(String::from("c2"), DataType::Utf8)]),
        )
        .await?;

    // Check that the df has the entire set of data
    let results = df.collect().await?;
    insta::allow_duplicates! {
        assert_snapshot!(
            batches_to_sort_string(&results),
            @r"
    +-----+-----+
    | c1  | c2  |
    +-----+-----+
    | abc | 123 |
    | def | 456 |
    +-----+-----+
    "
        )
    };

    Ok(())
}

#[tokio::test]
async fn write_parquet_results() -> Result<()> {
    // create partitioned input file and context
    let tmp_dir = TempDir::new()?;
    // let mut ctx = create_ctx(&tmp_dir, 4).await?;
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let schema = populate_csv_partitions(&tmp_dir, 4, ".csv")?;
    // register csv file with the execution context
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    // register a local file system object store for /tmp directory
    let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
    let local_url = Url::parse("file://local").unwrap();
    ctx.register_object_store(&local_url, local);

    // execute a simple query and write the results to parquet
    let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
    let out_dir_url = "file://local/out/";
    let df = ctx.sql("SELECT c1, c2 FROM test").await?;
    df.write_parquet(out_dir_url, DataFrameWriteOptions::new(), None)
        .await?;
    // write_parquet(&mut ctx, "SELECT c1, c2 FROM test", &out_dir, None).await?;

    // create a new context and verify that the results were saved to a partitioned parquet file
    let ctx = SessionContext::new();

    // get write_id
    let mut paths = fs::read_dir(&out_dir).unwrap();
    let path = paths.next();
    let name = path
        .unwrap()?
        .path()
        .file_name()
        .expect("Should be a file name")
        .to_str()
        .expect("Should be a str")
        .to_owned();
    let (parsed_id, _) = name.split_once('_').expect("File should contain _ !");
    let write_id = parsed_id.to_owned();

    // register each partition as well as the top level dir
    ctx.register_parquet(
        "part0",
        &format!("{out_dir}/{write_id}_0.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet("allparts", &out_dir, ParquetReadOptions::default())
        .await?;

    let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
    let allparts = ctx
        .sql("SELECT c1, c2 FROM allparts")
        .await?
        .collect()
        .await?;

    let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

    assert_eq!(part0[0].schema(), allparts[0].schema());

    assert_eq!(allparts_count, 40);

    Ok(())
}

fn union_fields() -> UnionFields {
    [
        (0, Arc::new(Field::new("A", DataType::Int32, true))),
        (1, Arc::new(Field::new("B", DataType::Float64, true))),
        (2, Arc::new(Field::new("C", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect()
}

#[tokio::test]
async fn union_literal_is_null_and_not_null() -> Result<()> {
    let str_array_1 = StringArray::from(vec![None::<String>]);
    let str_array_2 = StringArray::from(vec![Some("a")]);

    let batch_1 =
        RecordBatch::try_from_iter(vec![("arr", Arc::new(str_array_1) as ArrayRef)])?;
    let batch_2 =
        RecordBatch::try_from_iter(vec![("arr", Arc::new(str_array_2) as ArrayRef)])?;

    let ctx = SessionContext::new();
    ctx.register_batch("union_batch_1", batch_1)?;
    ctx.register_batch("union_batch_2", batch_2)?;

    let df1 = ctx.table("union_batch_1").await?;
    let df2 = ctx.table("union_batch_2").await?;

    let batches = df1.union(df2)?.collect().await?;
    let schema = batches[0].schema();

    for batch in batches {
        // Verify schema is the same for all batches
        if !schema.contains(&batch.schema()) {
            return Err(internal_datafusion_err!(
                "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                &schema,
                batch.schema()
            ));
        }
    }

    Ok(())
}

#[tokio::test]
async fn sparse_union_is_null() {
    // union of [{A=1}, {A=}, {B=3.2}, {B=}, {C="a"}, {C=}]
    let int_array = Int32Array::from(vec![Some(1), None, None, None, None, None]);
    let float_array = Float64Array::from(vec![None, None, Some(3.2), None, None, None]);
    let str_array = StringArray::from(vec![None, None, None, None, Some("a"), None]);
    let type_ids = [0, 0, 1, 1, 2, 2].into_iter().collect::<ScalarBuffer<i8>>();

    let children = vec![
        Arc::new(int_array) as Arc<dyn Array>,
        Arc::new(float_array),
        Arc::new(str_array),
    ];

    let array = UnionArray::try_new(union_fields(), type_ids, None, children).unwrap();

    let field = Field::new(
        "my_union",
        DataType::Union(union_fields(), UnionMode::Sparse),
        true,
    );
    let schema = Arc::new(Schema::new(vec![field]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

    let ctx = SessionContext::new();

    ctx.register_batch("union_batch", batch).unwrap();

    let df = ctx.table("union_batch").await.unwrap();

    // view_all
    assert_snapshot!(
        batches_to_sort_string(&df.clone().collect().await.unwrap()),
        @r"
    +----------+
    | my_union |
    +----------+
    | {A=1}    |
    | {A=}     |
    | {B=3.2}  |
    | {B=}     |
    | {C=a}    |
    | {C=}     |
    +----------+
    "
    );

    // filter where is null
    let result_df = df.clone().filter(col("my_union").is_null()).unwrap();
    assert_snapshot!(
        batches_to_sort_string(&result_df.collect().await.unwrap()),
        @r"
    +----------+
    | my_union |
    +----------+
    | {A=}     |
    | {B=}     |
    | {C=}     |
    +----------+
    "
    );

    // filter where is not null
    let result_df = df.filter(col("my_union").is_not_null()).unwrap();
    assert_snapshot!(
        batches_to_sort_string(&result_df.collect().await.unwrap()),
        @r"
    +----------+
    | my_union |
    +----------+
    | {A=1}    |
    | {B=3.2}  |
    | {C=a}    |
    +----------+
    "
    );
}

#[tokio::test]
async fn dense_union_is_null() {
    // union of [{A=1}, null, {B=3.2}, {A=34}]
    let int_array = Int32Array::from(vec![Some(1), None]);
    let float_array = Float64Array::from(vec![Some(3.2), None]);
    let str_array = StringArray::from(vec![Some("a"), None]);
    let type_ids = [0, 0, 1, 1, 2, 2].into_iter().collect::<ScalarBuffer<i8>>();
    let offsets = [0, 1, 0, 1, 0, 1]
        .into_iter()
        .collect::<ScalarBuffer<i32>>();

    let children = vec![
        Arc::new(int_array) as Arc<dyn Array>,
        Arc::new(float_array),
        Arc::new(str_array),
    ];

    let array =
        UnionArray::try_new(union_fields(), type_ids, Some(offsets), children).unwrap();

    let field = Field::new(
        "my_union",
        DataType::Union(union_fields(), UnionMode::Dense),
        true,
    );
    let schema = Arc::new(Schema::new(vec![field]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

    let ctx = SessionContext::new();

    ctx.register_batch("union_batch", batch).unwrap();

    let df = ctx.table("union_batch").await.unwrap();

    // view_all
    assert_snapshot!(
        batches_to_sort_string(&df.clone().collect().await.unwrap()),
        @r"
    +----------+
    | my_union |
    +----------+
    | {A=1}    |
    | {A=}     |
    | {B=3.2}  |
    | {B=}     |
    | {C=a}    |
    | {C=}     |
    +----------+
    "
    );

    // filter where is null
    let result_df = df.clone().filter(col("my_union").is_null()).unwrap();
    assert_snapshot!(
        batches_to_sort_string(&result_df.collect().await.unwrap()),
        @r"
    +----------+
    | my_union |
    +----------+
    | {A=}     |
    | {B=}     |
    | {C=}     |
    +----------+
    "
    );

    // filter where is not null
    let result_df = df.filter(col("my_union").is_not_null()).unwrap();
    assert_snapshot!(
        batches_to_sort_string(&result_df.collect().await.unwrap()),
        @r"
    +----------+
    | my_union |
    +----------+
    | {A=1}    |
    | {B=3.2}  |
    | {C=a}    |
    +----------+
    "
    );
}

#[tokio::test]
async fn boolean_dictionary_as_filter() {
    let values = vec![Some(true), Some(false), None, Some(true)];
    let keys = vec![0, 0, 1, 2, 1, 3, 1];
    let values_array = BooleanArray::from(values);
    let keys_array = Int8Array::from(keys);
    let array =
        DictionaryArray::new(keys_array, Arc::new(values_array) as Arc<dyn Array>);
    let array = Arc::new(array);

    let field = Field::new(
        "my_dict",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Boolean)),
        true,
    );
    let schema = Arc::new(Schema::new(vec![field]));

    let batch = RecordBatch::try_new(schema, vec![array.clone()]).unwrap();

    let ctx = SessionContext::new();

    ctx.register_batch("dict_batch", batch).unwrap();

    let df = ctx.table("dict_batch").await.unwrap();

    // view_all
    assert_snapshot!(
       batches_to_string(&df.clone().collect().await.unwrap()),
        @r"
    +---------+
    | my_dict |
    +---------+
    | true    |
    | true    |
    | false   |
    |         |
    | false   |
    | true    |
    | false   |
    +---------+
    "
    );

    let result_df = df.clone().filter(col("my_dict")).unwrap();

    assert_snapshot!(
       batches_to_string(&result_df.collect().await.unwrap()),
        @r"
    +---------+
    | my_dict |
    +---------+
    | true    |
    | true    |
    | true    |
    +---------+
    "
    );

    // test nested dictionary
    let keys = vec![0, 2]; // 0 -> true, 2 -> false
    let keys_array = Int8Array::from(keys);
    let nested_array = DictionaryArray::new(keys_array, array);

    let field = Field::new(
        "my_nested_dict",
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Boolean),
            )),
        ),
        true,
    );

    let schema = Arc::new(Schema::new(vec![field]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(nested_array)]).unwrap();

    ctx.register_batch("nested_dict_batch", batch).unwrap();

    let df = ctx.table("nested_dict_batch").await.unwrap();

    // view_all
    assert_snapshot!(
       batches_to_string(&df.clone().collect().await.unwrap()),
        @r"
    +----------------+
    | my_nested_dict |
    +----------------+
    | true           |
    | false          |
    +----------------+
    "
    );

    let result_df = df.clone().filter(col("my_nested_dict")).unwrap();
    assert_snapshot!(
       batches_to_string(&result_df.collect().await.unwrap()),
        @r"
    +----------------+
    | my_nested_dict |
    +----------------+
    | true           |
    +----------------+
    "
    );
}

#[tokio::test]
async fn test_union_by_name() -> Result<()> {
    let df = create_test_table("test")
        .await?
        .select(vec![col("a"), col("b"), lit(1).alias("c")])?
        .alias("table_alias")?;

    let df2 = df.clone().select_columns(&["c", "b", "a"])?;
    let result = df.union_by_name(df2)?.sort_by(vec![col("a"), col("b")])?;

    assert_snapshot!(
        batches_to_sort_string(&result.collect().await?),
        @r"
    +-----------+-----+---+
    | a         | b   | c |
    +-----------+-----+---+
    | 123AbcDef | 100 | 1 |
    | 123AbcDef | 100 | 1 |
    | CBAdef    | 10  | 1 |
    | CBAdef    | 10  | 1 |
    | abc123    | 10  | 1 |
    | abc123    | 10  | 1 |
    | abcDEF    | 1   | 1 |
    | abcDEF    | 1   | 1 |
    +-----------+-----+---+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_union_by_name_distinct() -> Result<()> {
    let df = create_test_table("test")
        .await?
        .select(vec![col("a"), col("b"), lit(1).alias("c")])?
        .alias("table_alias")?;

    let df2 = df.clone().select_columns(&["c", "b", "a"])?;
    let result = df
        .union_by_name_distinct(df2)?
        .sort_by(vec![col("a"), col("b")])?;

    assert_snapshot!(
        batches_to_sort_string(&result.collect().await?),
        @r"
    +-----------+-----+---+
    | a         | b   | c |
    +-----------+-----+---+
    | 123AbcDef | 100 | 1 |
    | CBAdef    | 10  | 1 |
    | abc123    | 10  | 1 |
    | abcDEF    | 1   | 1 |
    +-----------+-----+---+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_alias() -> Result<()> {
    let df = create_test_table("test")
        .await?
        .select(vec![col("a"), col("test.b"), lit(1).alias("one")])?
        .alias("table_alias")?;
    // All output column qualifiers are changed to "table_alias"
    df.schema().columns().iter().for_each(|c| {
        assert_eq!(c.relation, Some("table_alias".into()));
    });

    let plan = df
        .clone()
        .into_unoptimized_plan()
        .display_indent_schema()
        .to_string();
    assert_snapshot!(plan, @r"
    SubqueryAlias: table_alias [a:Utf8, b:Int32, one:Int32]
      Projection: test.a, test.b, Int32(1) AS one [a:Utf8, b:Int32, one:Int32]
        TableScan: test [a:Utf8, b:Int32]
    ");

    // Select over the aliased DataFrame
    let df = df.select(vec![
        col("table_alias.a"),
        col("b") + col("table_alias.one"),
    ])?;
    assert_snapshot!(
        batches_to_sort_string(&df.collect().await.unwrap()),
        @r"
    +-----------+---------------------------------+
    | a         | table_alias.b + table_alias.one |
    +-----------+---------------------------------+
    | 123AbcDef | 101                             |
    | CBAdef    | 11                              |
    | abc123    | 11                              |
    | abcDEF    | 2                               |
    +-----------+---------------------------------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_alias_with_metadata() -> Result<()> {
    let mut metadata = HashMap::new();
    metadata.insert(String::from("k"), String::from("v"));
    let metadata = FieldMetadata::from(metadata);
    let df = create_test_table("test")
        .await?
        .select(vec![col("a").alias_with_metadata("b", Some(metadata))])?
        .alias("table_alias")?;
    let df = df.select(vec![col("table_alias.b")])?;
    let schema = df.schema();
    let metadata = schema.field(0).metadata();
    assert_eq!(metadata.get("k"), Some(&String::from("v")));
    Ok(())
}

// Use alias to perform a self-join
// Issue: https://github.com/apache/datafusion/issues/14112
#[tokio::test]
async fn test_alias_self_join() -> Result<()> {
    let left = create_test_table("t1").await?;
    let right = left.clone().alias("t2")?;
    let joined = left.join(right, JoinType::Full, &["a"], &["a"], None)?;
    assert_snapshot!(
        batches_to_sort_string(&joined.collect().await.unwrap()),
        @r"
    +-----------+-----+-----------+-----+
    | a         | b   | a         | b   |
    +-----------+-----+-----------+-----+
    | 123AbcDef | 100 | 123AbcDef | 100 |
    | CBAdef    | 10  | CBAdef    | 10  |
    | abc123    | 10  | abc123    | 10  |
    | abcDEF    | 1   | abcDEF    | 1   |
    +-----------+-----+-----------+-----+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_alias_empty() -> Result<()> {
    let df = create_test_table("test").await?.alias("")?;
    let plan = df
        .clone()
        .into_unoptimized_plan()
        .display_indent_schema()
        .to_string();
    assert_snapshot!(plan, @r"
    SubqueryAlias:  [a:Utf8, b:Int32]
      TableScan: test [a:Utf8, b:Int32]
    ");

    assert_snapshot!(
        batches_to_sort_string(&df.select(vec![col("a"), col("b")])?.collect().await.unwrap()),
        @r"
    +-----------+-----+
    | a         | b   |
    +-----------+-----+
    | 123AbcDef | 100 |
    | CBAdef    | 10  |
    | abc123    | 10  |
    | abcDEF    | 1   |
    +-----------+-----+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_alias_nested() -> Result<()> {
    let df = create_test_table("test")
        .await?
        .select(vec![col("a"), col("test.b"), lit(1).alias("one")])?
        .alias("alias1")?
        .alias("alias2")?;

    let plan = df
        .clone()
        .into_optimized_plan()?
        .display_indent_schema()
        .to_string();
    assert_snapshot!(plan, @r"
    SubqueryAlias: alias2 [a:Utf8, b:Int32, one:Int32]
      SubqueryAlias: alias1 [a:Utf8, b:Int32, one:Int32]
        Projection: test.a, test.b, Int32(1) AS one [a:Utf8, b:Int32, one:Int32]
          TableScan: test projection=[a, b] [a:Utf8, b:Int32]
    ");

    // Select over the aliased DataFrame
    let select1 = df
        .clone()
        .select(vec![col("alias2.a"), col("b") + col("alias2.one")])?;

    assert_snapshot!(
        batches_to_sort_string(&select1.collect().await.unwrap()),
        @r"
    +-----------+-----------------------+
    | a         | alias2.b + alias2.one |
    +-----------+-----------------------+
    | 123AbcDef | 101                   |
    | CBAdef    | 11                    |
    | abc123    | 11                    |
    | abcDEF    | 2                     |
    +-----------+-----------------------+
    "
    );

    // Only the outermost alias is visible
    let select2 = df.select(vec![col("alias1.a")]);
    assert_snapshot!(
        select2.unwrap_err().strip_backtrace(),
        @"Schema error: No field named alias1.a. Valid fields are alias2.a, alias2.b, alias2.one."
    );
    Ok(())
}

#[tokio::test]
async fn register_non_json_file() {
    let ctx = SessionContext::new();
    let err = ctx
        .register_json(
            "data",
            "tests/data/test_binary.parquet",
            JsonReadOptions::default(),
        )
        .await;
    assert_contains!(
        err.unwrap_err().to_string(),
        "test_binary.parquet' does not match the expected extension '.json'"
    );
}

#[tokio::test]
async fn register_non_csv_file() {
    let ctx = SessionContext::new();
    let err = ctx
        .register_csv(
            "data",
            "tests/data/test_binary.parquet",
            CsvReadOptions::default(),
        )
        .await;
    assert_contains!(
        err.unwrap_err().to_string(),
        "test_binary.parquet' does not match the expected extension '.csv'"
    );
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn register_non_avro_file() {
    let ctx = SessionContext::new();
    let err = ctx
        .register_avro(
            "data",
            "tests/data/test_binary.parquet",
            datafusion::prelude::AvroReadOptions::default(),
        )
        .await;
    assert_contains!(
        err.unwrap_err().to_string(),
        "test_binary.parquet' does not match the expected extension '.avro'"
    );
}

#[tokio::test]
async fn register_non_parquet_file() {
    let ctx = SessionContext::new();
    let err = ctx
        .register_parquet("data", "tests/data/1.json", ParquetReadOptions::default())
        .await;
    assert_contains!(
        err.unwrap_err().to_string(),
        "1.json' does not match the expected extension '.parquet'"
    );
}

// Test inserting into checking.
#[tokio::test]
async fn test_insert_into_checking() -> Result<()> {
    // Create a new schema with one field called "a" of type Int64, and setting nullable to false
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

    let session_ctx = SessionContext::new();

    // Create and register the initial table with the provided schema and data
    let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);
    session_ctx.register_table("t", initial_table.clone())?;

    // There are two cases we need to check
    // 1. The len of the schema of the plan and the schema of the table should be the same
    // 2. The datatype of the schema of the plan and the schema of the table should be the same

    // Test case 1:
    let write_df = session_ctx.sql("values (1, 2), (3, 4)").await.unwrap();

    let e = write_df
        .write_table("t", DataFrameWriteOptions::new())
        .await
        .unwrap_err();

    assert_contains!(
        e.to_string(),
        "Inserting query must have the same schema length as the table."
    );

    // Setting nullable to true
    // Make sure the nullable check go through
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

    let session_ctx = SessionContext::new();

    // Create and register the initial table with the provided schema and data
    let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);
    session_ctx.register_table("t", initial_table.clone())?;

    // Test case 2:
    let write_df = session_ctx.sql("values ('a123'), ('b456')").await.unwrap();

    let e = write_df
        .write_table("t", DataFrameWriteOptions::new())
        .await
        .unwrap_err();

    assert_contains!(
        e.to_string(),
        "Inserting query schema mismatch: Expected table field 'a' with type Int64, but got 'column1' with type Utf8"
    );

    Ok(())
}

async fn create_null_table() -> Result<DataFrame> {
    // create a DataFrame with null values
    //    "+---+----+",
    //    "| a | b |",
    //    "+---+---+",
    //    "| 1 | x |",
    //    "|   |   |",
    //    "| 3 | z |",
    //    "+---+---+",
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let a_values = Int32Array::from(vec![Some(1), None, Some(3)]);
    let b_values = StringArray::from(vec![Some("x"), None, Some("z")]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a_values), Arc::new(b_values)],
    )?;

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    ctx.register_table("t_null", Arc::new(table))?;
    let df = ctx.table("t_null").await?;
    Ok(df)
}

#[tokio::test]
async fn test_fill_null() -> Result<()> {
    let df = create_null_table().await?;

    // Use fill_null to replace nulls on each column.
    let df_filled = df
        .fill_null(ScalarValue::Int32(Some(0)), vec!["a".to_string()])?
        .fill_null(
            ScalarValue::Utf8(Some("default".to_string())),
            vec!["b".to_string()],
        )?;

    let results = df_filled.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +---+---------+
    | a | b       |
    +---+---------+
    | 0 | default |
    | 1 | x       |
    | 3 | z       |
    +---+---------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_fill_null_all_columns() -> Result<()> {
    let df = create_null_table().await?;

    // Use fill_null to replace nulls on all columns.
    // Only column "b" will be replaced since ScalarValue::Utf8(Some("default".to_string()))
    // can be cast to Utf8.
    let df_filled =
        df.fill_null(ScalarValue::Utf8(Some("default".to_string())), vec![])?;

    let results = df_filled.clone().collect().await?;

    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +---+---------+
    | a | b       |
    +---+---------+
    |   | default |
    | 1 | x       |
    | 3 | z       |
    +---+---------+
    "
    );

    // Fill column "a" null values with a value that cannot be cast to Int32.
    let df_filled = df_filled.fill_null(ScalarValue::Int32(Some(0)), vec![])?;

    let results = df_filled.collect().await?;
    assert_snapshot!(
        batches_to_sort_string(&results),
        @r"
    +---+---------+
    | a | b       |
    +---+---------+
    | 0 | default |
    | 1 | x       |
    | 3 | z       |
    +---+---------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_insert_into_casting_support() -> Result<()> {
    // Testing case1:
    // Inserting query schema mismatch: Expected table field 'a' with type Float16, but got 'a' with type Utf8.
    // And the cast is not supported from Utf8 to Float16.

    // Create a new schema with one field called "a" of type Float16, and setting nullable to false
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float16, false)]));

    let session_ctx = SessionContext::new();

    // Create and register the initial table with the provided schema and data
    let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);
    session_ctx.register_table("t", initial_table.clone())?;

    let mut write_df = session_ctx.sql("values ('a123'), ('b456')").await.unwrap();

    write_df = write_df
        .clone()
        .with_column_renamed("column1", "a")
        .unwrap();

    let e = write_df
        .write_table("t", DataFrameWriteOptions::new())
        .await
        .unwrap_err();

    assert_contains!(
        e.to_string(),
        "Inserting query schema mismatch: Expected table field 'a' with type Float16, but got 'a' with type Utf8."
    );

    // Testing case2:
    // Inserting query schema mismatch: Expected table field 'a' with type Utf8View, but got 'a' with type Utf8.
    // And the cast is supported from Utf8 to Utf8View.

    // Create a new schema with one field called "a" of type Utf8View, and setting nullable to false
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Utf8View,
        false,
    )]));

    let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);

    session_ctx.register_table("t2", initial_table.clone())?;

    let mut write_df = session_ctx.sql("values ('a123'), ('b456')").await.unwrap();

    write_df = write_df
        .clone()
        .with_column_renamed("column1", "a")
        .unwrap();

    write_df
        .write_table("t2", DataFrameWriteOptions::new())
        .await?;

    let res = session_ctx
        .sql("select * from t2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_snapshot!(
       batches_to_string(&res),
        @r"
    +------+
    | a    |
    +------+
    | a123 |
    | b456 |
    +------+
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_dataframe_from_columns() -> Result<()> {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let b: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false]));
    let c: ArrayRef = Arc::new(StringArray::from(vec![Some("foo"), Some("bar"), None]));
    let df = DataFrame::from_columns(vec![("a", a), ("b", b), ("c", c)])?;

    assert_eq!(df.schema().fields().len(), 3);
    assert_eq!(df.clone().count().await?, 3);

    let rows = df.sort(vec![col("a").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+---+-------+-----+",
            "| a | b     | c   |",
            "+---+-------+-----+",
            "| 1 | true  | foo |",
            "| 2 | true  | bar |",
            "| 3 | false |     |",
            "+---+-------+-----+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_dataframe_macro() -> Result<()> {
    let df = dataframe!(
        "a" => [1, 2, 3],
        "b" => [true, true, false],
        "c" => [Some("foo"), Some("bar"), None]
    )?;

    assert_eq!(df.schema().fields().len(), 3);
    assert_eq!(df.clone().count().await?, 3);

    let rows = df.sort(vec![col("a").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+---+-------+-----+",
            "| a | b     | c   |",
            "+---+-------+-----+",
            "| 1 | true  | foo |",
            "| 2 | true  | bar |",
            "| 3 | false |     |",
            "+---+-------+-----+",
        ],
        &rows.collect().await?
    );

    let df_empty = dataframe!()?;
    assert_eq!(df_empty.schema().fields().len(), 0);
    assert_eq!(df_empty.count().await?, 0);

    Ok(())
}

#[tokio::test]
async fn test_copy_schema() -> Result<()> {
    let tmp_dir = TempDir::new()?;

    let session_state = SessionStateBuilder::new_with_default_features().build();

    let session_ctx = SessionContext::new_with_state(session_state);

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

    // Create and register the source table with the provided schema and data
    let source_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);
    session_ctx.register_table("source_table", source_table.clone())?;

    let target_path = tmp_dir.path().join("target.csv");

    let query = format!(
        "COPY source_table TO '{}' STORED AS csv",
        target_path.to_str().unwrap()
    );

    let result = session_ctx.sql(&query).await?;
    assert_logical_expr_schema_eq_physical_expr_schema(result).await?;
    Ok(())
}

#[tokio::test]
async fn test_copy_to_preserves_order() -> Result<()> {
    let tmp_dir = TempDir::new()?;

    let session_state = SessionStateBuilder::new_with_default_features().build();
    let session_ctx = SessionContext::new_with_state(session_state);

    let target_path = tmp_dir.path().join("target_ordered.csv");
    let csv_file_format = session_ctx
        .state()
        .get_file_format_factory("csv")
        .map(format_as_file_type)
        .unwrap();

    let ordered_select_plan = LogicalPlanBuilder::values(vec![
        vec![lit(1u64)],
        vec![lit(10u64)],
        vec![lit(20u64)],
        vec![lit(100u64)],
    ])?
    .sort(vec![SortExpr::new(col("column1"), false, true)])?
    .build()?;

    let copy_to_plan = LogicalPlanBuilder::copy_to(
        ordered_select_plan,
        target_path.to_str().unwrap().to_string(),
        csv_file_format,
        HashMap::new(),
        vec![],
    )?
    .build()?;

    let union_side_branch = LogicalPlanBuilder::values(vec![vec![lit(1u64)]])?.build()?;
    let union_plan = LogicalPlanBuilder::from(copy_to_plan)
        .union(union_side_branch)?
        .build()?;

    let frame = session_ctx.execute_logical_plan(union_plan).await?;
    let physical_plan = frame.create_physical_plan().await?;

    let physical_plan_format =
        displayable(physical_plan.as_ref()).indent(true).to_string();

    // Expect that input to the DataSinkExec is sorted correctly
    assert_snapshot!(
        physical_plan_format,
        @r"
    UnionExec
      DataSinkExec: sink=CsvSink(file_groups=[])
        SortExec: expr=[column1@0 DESC], preserve_partitioning=[false]
          DataSourceExec: partitions=1, partition_sizes=[1]
      DataSourceExec: partitions=1, partition_sizes=[1]
    "
    );
    Ok(())
}

#[tokio::test]
async fn test_duplicate_state_fields_for_dfschema_construct() -> Result<()> {
    let ctx = SessionContext::new();

    // Simple schema with just the fields we need
    let file_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("ticker", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("date", DataType::Utf8, false),
    ]));

    let df_schema = DFSchema::try_from(file_schema.clone())?;

    let timestamp = col("timestamp");
    let value = col("value");
    let ticker = col("ticker");
    let date = col("date");

    let mock_exec = Arc::new(EmptyExec::new(file_schema.clone()));

    // Build first_value aggregate
    let first_value = Arc::new(
        AggregateExprBuilder::new(
            datafusion_functions_aggregate::first_last::first_value_udaf(),
            vec![ctx.create_physical_expr(value.clone(), &df_schema)?],
        )
        .alias("first_value(value)")
        .order_by(vec![PhysicalSortExpr::new(
            ctx.create_physical_expr(timestamp.clone(), &df_schema)?,
            SortOptions::new(false, false),
        )])
        .schema(file_schema.clone())
        .build()
        .expect("Failed to build first_value"),
    );

    // Build last_value aggregate
    let last_value = Arc::new(
        AggregateExprBuilder::new(
            datafusion_functions_aggregate::first_last::last_value_udaf(),
            vec![ctx.create_physical_expr(value.clone(), &df_schema)?],
        )
        .alias("last_value(value)")
        .order_by(vec![PhysicalSortExpr::new(
            ctx.create_physical_expr(timestamp.clone(), &df_schema)?,
            SortOptions::new(false, false),
        )])
        .schema(file_schema.clone())
        .build()
        .expect("Failed to build last_value"),
    );

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::new_single(vec![
            (
                ctx.create_physical_expr(date.clone(), &df_schema)?,
                "date".to_string(),
            ),
            (
                ctx.create_physical_expr(ticker.clone(), &df_schema)?,
                "ticker".to_string(),
            ),
        ]),
        vec![first_value, last_value],
        vec![None, None],
        mock_exec,
        file_schema,
    )
    .expect("Failed to build partial agg");

    // Assert that the schema field names match the expected names
    let expected_field_names = vec![
        "date",
        "ticker",
        "first_value(value)[first_value]",
        "timestamp@0",
        "first_value(value)[first_value_is_set]",
        "last_value(value)[last_value]",
        "timestamp@0",
        "last_value(value)[last_value_is_set]",
    ];

    let binding = partial_agg.schema();
    let actual_field_names: Vec<_> = binding.fields().iter().map(|f| f.name()).collect();
    assert_eq!(actual_field_names, expected_field_names);

    // Ensure that DFSchema::try_from does not fail
    let partial_agg_exec_schema = DFSchema::try_from(partial_agg.schema());
    assert!(
        partial_agg_exec_schema.is_ok(),
        "Expected get AggregateExec schema to succeed with duplicate state fields"
    );

    Ok(())
}

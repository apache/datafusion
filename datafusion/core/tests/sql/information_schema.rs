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

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::{
    catalog::{
        catalog::{CatalogProvider, MemoryCatalogProvider},
        schema::{MemorySchemaProvider, SchemaProvider},
    },
    datasource::{TableProvider, TableType},
    logical_plan::Expr,
};

use super::*;

#[tokio::test]
async fn information_schema_tables_not_exist_by_default() {
    let ctx = SessionContext::new();

    let err = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        // Error propagates from SessionState::schema_for_ref
        "Error during planning: failed to resolve schema: information_schema"
    );
}

#[tokio::test]
async fn information_schema_tables_no_tables() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------------+-------------+------------+",
        "| table_catalog | table_schema       | table_name  | table_type |",
        "+---------------+--------------------+-------------+------------+",
        "| datafusion    | information_schema | columns     | VIEW       |",
        "| datafusion    | information_schema | df_settings | VIEW       |",
        "| datafusion    | information_schema | tables      | VIEW       |",
        "| datafusion    | information_schema | views       | VIEW       |",
        "+---------------+--------------------+-------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn information_schema_tables_tables_default_catalog() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    // Now, register an empty table
    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------------+-------------+------------+",
        "| table_catalog | table_schema       | table_name  | table_type |",
        "+---------------+--------------------+-------------+------------+",
        "| datafusion    | information_schema | columns     | VIEW       |",
        "| datafusion    | information_schema | df_settings | VIEW       |",
        "| datafusion    | information_schema | tables      | VIEW       |",
        "| datafusion    | information_schema | views       | VIEW       |",
        "| datafusion    | public             | t           | BASE TABLE |",
        "+---------------+--------------------+-------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Newly added tables should appear
    ctx.register_table("t2", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------------+-------------+------------+",
        "| table_catalog | table_schema       | table_name  | table_type |",
        "+---------------+--------------------+-------------+------------+",
        "| datafusion    | information_schema | columns     | VIEW       |",
        "| datafusion    | information_schema | df_settings | VIEW       |",
        "| datafusion    | information_schema | tables      | VIEW       |",
        "| datafusion    | information_schema | views       | VIEW       |",
        "| datafusion    | public             | t           | BASE TABLE |",
        "| datafusion    | public             | t2          | BASE TABLE |",
        "+---------------+--------------------+-------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn information_schema_tables_tables_with_multiple_catalogs() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();
    schema
        .register_table("t1".to_owned(), table_with_sequence(1, 1).unwrap())
        .unwrap();
    schema
        .register_table("t2".to_owned(), table_with_sequence(1, 1).unwrap())
        .unwrap();
    catalog
        .register_schema("my_schema", Arc::new(schema))
        .unwrap();
    ctx.register_catalog("my_catalog", Arc::new(catalog));

    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();
    schema
        .register_table("t3".to_owned(), table_with_sequence(1, 1).unwrap())
        .unwrap();
    catalog
        .register_schema("my_other_schema", Arc::new(schema))
        .unwrap();
    ctx.register_catalog("my_other_catalog", Arc::new(catalog));

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap();

    let expected = vec![
        "+------------------+--------------------+-------------+------------+",
        "| table_catalog    | table_schema       | table_name  | table_type |",
        "+------------------+--------------------+-------------+------------+",
        "| datafusion       | information_schema | columns     | VIEW       |",
        "| datafusion       | information_schema | df_settings | VIEW       |",
        "| datafusion       | information_schema | tables      | VIEW       |",
        "| datafusion       | information_schema | views       | VIEW       |",
        "| my_catalog       | information_schema | columns     | VIEW       |",
        "| my_catalog       | information_schema | df_settings | VIEW       |",
        "| my_catalog       | information_schema | tables      | VIEW       |",
        "| my_catalog       | information_schema | views       | VIEW       |",
        "| my_catalog       | my_schema          | t1          | BASE TABLE |",
        "| my_catalog       | my_schema          | t2          | BASE TABLE |",
        "| my_other_catalog | information_schema | columns     | VIEW       |",
        "| my_other_catalog | information_schema | df_settings | VIEW       |",
        "| my_other_catalog | information_schema | tables      | VIEW       |",
        "| my_other_catalog | information_schema | views       | VIEW       |",
        "| my_other_catalog | my_other_schema    | t3          | BASE TABLE |",
        "+------------------+--------------------+-------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn information_schema_tables_table_types() {
    struct TestTable(TableType);

    #[async_trait]
    impl TableProvider for TestTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn table_type(&self) -> TableType {
            self.0
        }

        fn schema(&self) -> SchemaRef {
            unimplemented!()
        }

        async fn scan(
            &self,
            _ctx: &SessionState,
            _: &Option<Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }
    }

    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.register_table("physical", Arc::new(TestTable(TableType::Base)))
        .unwrap();
    ctx.register_table("query", Arc::new(TestTable(TableType::View)))
        .unwrap();
    ctx.register_table("temp", Arc::new(TestTable(TableType::Temporary)))
        .unwrap();

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------------+-------------+-----------------+",
        "| table_catalog | table_schema       | table_name  | table_type      |",
        "+---------------+--------------------+-------------+-----------------+",
        "| datafusion    | information_schema | columns     | VIEW            |",
        "| datafusion    | information_schema | df_settings | VIEW            |",
        "| datafusion    | information_schema | tables      | VIEW            |",
        "| datafusion    | information_schema | views       | VIEW            |",
        "| datafusion    | public             | physical    | BASE TABLE      |",
        "| datafusion    | public             | query       | VIEW            |",
        "| datafusion    | public             | temp        | LOCAL TEMPORARY |",
        "+---------------+--------------------+-------------+-----------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn information_schema_show_tables_no_information_schema() {
    let ctx = SessionContext::with_config(SessionConfig::new());

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    // use show tables alias
    let err = plan_and_collect(&ctx, "SHOW TABLES").await.unwrap_err();

    assert_eq!(err.to_string(), "Error during planning: SHOW TABLES is not supported unless information_schema is enabled");
}

#[tokio::test]
async fn information_schema_describe_table() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let sql = "CREATE OR REPLACE TABLE y AS VALUES (1,2),(3,4);";
    ctx.sql(sql).await.unwrap();

    let sql_all = "describe y;";
    let results_all = execute_to_batches(&ctx, sql_all).await;

    let expected = vec![
        "+-------------+-----------+-------------+",
        "| column_name | data_type | is_nullable |",
        "+-------------+-----------+-------------+",
        "| column1     | Int64     | YES         |",
        "| column2     | Int64     | YES         |",
        "+-------------+-----------+-------------+",
    ];

    assert_batches_eq!(expected, &results_all);
}

#[tokio::test]
async fn information_schema_describe_table_not_exists() {
    let ctx = SessionContext::with_config(SessionConfig::new());

    let sql_all = "describe table;";
    let err = plan_and_collect(&ctx, sql_all).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: 'datafusion.public.table' not found"
    );
}

#[tokio::test]
async fn information_schema_show_tables() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    // use show tables alias
    let result = plan_and_collect(&ctx, "SHOW TABLES").await.unwrap();

    let expected = vec![
        "+---------------+--------------------+-------------+------------+",
        "| table_catalog | table_schema       | table_name  | table_type |",
        "+---------------+--------------------+-------------+------------+",
        "| datafusion    | information_schema | columns     | VIEW       |",
        "| datafusion    | information_schema | df_settings | VIEW       |",
        "| datafusion    | information_schema | tables      | VIEW       |",
        "| datafusion    | information_schema | views       | VIEW       |",
        "| datafusion    | public             | t           | BASE TABLE |",
        "+---------------+--------------------+-------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "SHOW tables").await.unwrap();

    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn information_schema_show_columns_no_information_schema() {
    let ctx = SessionContext::with_config(SessionConfig::new());

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let err = plan_and_collect(&ctx, "SHOW COLUMNS FROM t")
        .await
        .unwrap_err();

    assert_eq!(err.to_string(), "Error during planning: SHOW COLUMNS is not supported unless information_schema is enabled");
}

#[tokio::test]
async fn information_schema_show_columns_like_where() {
    let ctx = SessionContext::with_config(SessionConfig::new());

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let expected =
        "Error during planning: SHOW COLUMNS with WHERE or LIKE is not supported";

    let err = plan_and_collect(&ctx, "SHOW COLUMNS FROM t LIKE 'f'")
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), expected);

    let err = plan_and_collect(&ctx, "SHOW COLUMNS FROM t WHERE column_name = 'bar'")
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), expected);
}

#[tokio::test]
async fn information_schema_show_columns() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let result = plan_and_collect(&ctx, "SHOW COLUMNS FROM t").await.unwrap();

    let expected = vec![
        "+---------------+--------------+------------+-------------+-----------+-------------+",
        "| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |",
        "+---------------+--------------+------------+-------------+-----------+-------------+",
        "| datafusion    | public       | t          | i           | Int32     | YES         |",
        "+---------------+--------------+------------+-------------+-----------+-------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "SHOW columns from t").await.unwrap();
    assert_batches_sorted_eq!(expected, &result);

    // This isn't ideal but it is consistent behavior for `SELECT * from "T"`
    let err = plan_and_collect(&ctx, "SHOW columns from \"T\"")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        // Error propagates from SessionState::get_table_provider
        "Error during planning: 'datafusion.public.T' not found"
    );
}

// test errors with WHERE and LIKE
#[tokio::test]
async fn information_schema_show_columns_full_extended() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let result = plan_and_collect(&ctx, "SHOW FULL COLUMNS FROM t")
        .await
        .unwrap();
    let expected = vec![
        "+---------------+--------------+------------+-------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
        "| table_catalog | table_schema | table_name | column_name | ordinal_position | column_default | is_nullable | data_type | character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale | datetime_precision | interval_type |",
        "+---------------+--------------+------------+-------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
        "| datafusion    | public       | t          | i           | 0                |                | YES         | Int32     |                          |                        | 32                | 2                       |               |                    |               |",
        "+---------------+--------------+------------+-------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "SHOW EXTENDED COLUMNS FROM t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn information_schema_show_table_table_names() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let result = plan_and_collect(&ctx, "SHOW COLUMNS FROM public.t")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------+------------+-------------+-----------+-------------+",
        "| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |",
        "+---------------+--------------+------------+-------------+-----------+-------------+",
        "| datafusion    | public       | t          | i           | Int32     | YES         |",
        "+---------------+--------------+------------+-------------+-----------+-------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let result = plan_and_collect(&ctx, "SHOW columns from datafusion.public.t")
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &result);

    let err = plan_and_collect(&ctx, "SHOW columns from t2")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        // Error propagates from SessionState::get_table_provider
        "Error during planning: 'datafusion.public.t2' not found"
    );

    let err = plan_and_collect(&ctx, "SHOW columns from datafusion.public.t2")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        // Error propagates from SessionState::get_table_provider
        "Error during planning: 'datafusion.public.t2' not found"
    );
}

// FIXME once we support raise error while show non-existing variable, we could add this back
//#[tokio::test]
//async fn show_unsupported() {
//    let ctx = SessionContext::with_config(SessionConfig::new());
//
//    let err = plan_and_collect(&ctx, "SHOW SOMETHING_UNKNOWN")
//        .await
//        .unwrap_err();
//
//    assert_eq!(err.to_string(), "This feature is not implemented: SHOW SOMETHING_UNKNOWN not implemented. Supported syntax: SHOW <TABLES>");
//}

// FIXME
// currently we cannot know whether a variable exists, this will output 0 row instead
// one way to fix this is to generate a ConfigOptions and get options' key to compare
// however config.rs is currently in core lib, could not be used by datafusion_sql due to the dependency cycle
#[tokio::test]
async fn show_non_existing_variable() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let result = plan_and_collect(&ctx, "SHOW SOMETHING_UNKNOWN")
        .await
        .unwrap();

    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn show_unsupported_when_information_schema_off() {
    let ctx = SessionContext::with_config(SessionConfig::new());

    let err = plan_and_collect(&ctx, "SHOW SOMETHING").await.unwrap_err();

    assert_eq!(err.to_string(), "Error during planning: SHOW [VARIABLE] is not supported unless information_schema is enabled");
}

#[tokio::test]
async fn information_schema_columns_not_exist_by_default() {
    let ctx = SessionContext::new();

    let err = plan_and_collect(&ctx, "SELECT * from information_schema.columns")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        // Error propagates from SessionState::schema_for_ref
        "Error during planning: failed to resolve schema: information_schema"
    );
}

fn table_with_many_types() -> Arc<dyn TableProvider> {
    let schema = Schema::new(vec![
        Field::new("int32_col", DataType::Int32, false),
        Field::new("float64_col", DataType::Float64, true),
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("large_utf8_col", DataType::LargeUtf8, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("large_binary_col", DataType::LargeBinary, false),
        Field::new(
            "timestamp_nanos",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from_slice(&[1])),
            Arc::new(Float64Array::from_slice(&[1.0])),
            Arc::new(StringArray::from(vec![Some("foo")])),
            Arc::new(LargeStringArray::from(vec![Some("bar")])),
            Arc::new(BinaryArray::from_slice(&[b"foo" as &[u8]])),
            Arc::new(LargeBinaryArray::from_slice(&[b"foo" as &[u8]])),
            Arc::new(TimestampNanosecondArray::from_opt_vec(
                vec![Some(123)],
                None,
            )),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

#[tokio::test]
async fn information_schema_columns() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    schema
        .register_table("t1".to_owned(), table_with_sequence(1, 1).unwrap())
        .unwrap();

    schema
        .register_table("t2".to_owned(), table_with_many_types())
        .unwrap();
    catalog
        .register_schema("my_schema", Arc::new(schema))
        .unwrap();
    ctx.register_catalog("my_catalog", Arc::new(catalog));

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.columns")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------+------------+------------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
        "| table_catalog | table_schema | table_name | column_name      | ordinal_position | column_default | is_nullable | data_type                   | character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale | datetime_precision | interval_type |",
        "+---------------+--------------+------------+------------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
        "| my_catalog    | my_schema    | t1         | i                | 0                |                | YES         | Int32                       |                          |                        | 32                | 2                       |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | binary_col       | 4                |                | NO          | Binary                      |                          | 2147483647             |                   |                         |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | float64_col      | 1                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | int32_col        | 0                |                | NO          | Int32                       |                          |                        | 32                | 2                       |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | large_binary_col | 5                |                | NO          | LargeBinary                 |                          | 9223372036854775807    |                   |                         |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | large_utf8_col   | 3                |                | NO          | LargeUtf8                   |                          | 9223372036854775807    |                   |                         |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | timestamp_nanos  | 6                |                | NO          | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
        "| my_catalog    | my_schema    | t2         | utf8_col         | 2                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
        "+---------------+--------------+------------+------------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn show_create_view() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
    plan_and_collect(&ctx, table_sql).await.unwrap();
    let view_sql = "CREATE VIEW xyz AS SELECT * FROM abc";
    plan_and_collect(&ctx, view_sql).await.unwrap();

    let results_sql = "SHOW CREATE TABLE xyz";
    let results = plan_and_collect(&ctx, results_sql).await.unwrap();
    assert_eq!(results[0].num_rows(), 1);

    let expected = vec![
        "+---------------+--------------+------------+--------------------------------------+",
        "| table_catalog | table_schema | table_name | definition                           |",
        "+---------------+--------------+------------+--------------------------------------+",
        "| datafusion    | public       | xyz        | CREATE VIEW xyz AS SELECT * FROM abc |",
        "+---------------+--------------+------------+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn show_create_view_in_catalog() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
    plan_and_collect(&ctx, table_sql).await.unwrap();
    let schema_sql = "CREATE SCHEMA test";
    plan_and_collect(&ctx, schema_sql).await.unwrap();
    let view_sql = "CREATE VIEW test.xyz AS SELECT * FROM abc";
    plan_and_collect(&ctx, view_sql).await.unwrap();

    let result_sql = "SHOW CREATE TABLE test.xyz";
    let results = plan_and_collect(&ctx, result_sql).await.unwrap();
    assert_eq!(results[0].num_rows(), 1);

    let expected = vec![
        "+---------------+--------------+------------+-------------------------------------------+",
        "| table_catalog | table_schema | table_name | definition                                |",
        "+---------------+--------------+------------+-------------------------------------------+",
        "| datafusion    | test         | xyz        | CREATE VIEW test.xyz AS SELECT * FROM abc |",
        "+---------------+--------------+------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn show_create_table() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let table_sql = "CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)";
    plan_and_collect(&ctx, table_sql).await.unwrap();

    let result_sql = "SHOW CREATE TABLE abc";
    let results = plan_and_collect(&ctx, result_sql).await.unwrap();

    let expected = vec![
        "+---------------+--------------+------------+------------+",
        "| table_catalog | table_schema | table_name | definition |",
        "+---------------+--------------+------------+------------+",
        "| datafusion    | public       | abc        |            |",
        "+---------------+--------------+------------+------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn show_external_create_table() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let table_sql =
        "CREATE EXTERNAL TABLE abc STORED AS CSV WITH HEADER ROW LOCATION '../../testing/data/csv/aggregate_test_100.csv'";
    plan_and_collect(&ctx, table_sql).await.unwrap();

    let result_sql = "SHOW CREATE TABLE abc";
    let results = plan_and_collect(&ctx, result_sql).await.unwrap();

    let expected = vec![
        "+---------------+--------------+------------+-------------------------------------------------------------------------------------------------+",
        "| table_catalog | table_schema | table_name | definition                                                                                      |",
        "+---------------+--------------+------------+-------------------------------------------------------------------------------------------------+",
        "| datafusion    | public       | abc        | CREATE EXTERNAL TABLE abc STORED AS CSV LOCATION ../../testing/data/csv/aggregate_test_100.csv  |",
        "+---------------+--------------+------------+-------------------------------------------------------------------------------------------------+",
    ];

    assert_batches_eq!(expected, &results);
}

/// Execute SQL and return results
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

#[tokio::test]
async fn show_variable_in_config_options() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let sql = "SHOW datafusion.execution.batch_size";
    let results = plan_and_collect(&ctx, sql).await.unwrap();

    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 8192    |",
        "+---------------------------------+---------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn show_all() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let sql = "SHOW ALL";

    let results = plan_and_collect(&ctx, sql).await.unwrap();

    let expected_length = ctx
        .state
        .read()
        .config
        .config_options
        .read()
        .options()
        .len();
    assert_eq!(expected_length, results[0].num_rows());
}

#[tokio::test]
async fn show_time_zone_default_utc() {
    // https://github.com/apache/arrow-datafusion/issues/3255
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let sql = "SHOW TIME ZONE";
    let results = plan_and_collect(&ctx, sql).await.unwrap();

    let expected = vec![
        "+--------------------------------+---------+",
        "| name                           | setting |",
        "+--------------------------------+---------+",
        "| datafusion.execution.time_zone | UTC     |",
        "+--------------------------------+---------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn show_timezone_default_utc() {
    // https://github.com/apache/arrow-datafusion/issues/3255
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let sql = "SHOW TIMEZONE";
    let results = plan_and_collect(&ctx, sql).await.unwrap();

    let expected = vec![
        "+--------------------------------+---------+",
        "| name                           | setting |",
        "+--------------------------------+---------+",
        "| datafusion.execution.time_zone | UTC     |",
        "+--------------------------------+---------+",
    ];

    assert_batches_eq!(expected, &results);
}

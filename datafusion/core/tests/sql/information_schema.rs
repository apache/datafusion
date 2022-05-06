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
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| datafusion    | information_schema | columns    | VIEW       |",
        "| datafusion    | information_schema | tables     | VIEW       |",
        "+---------------+--------------------+------------+------------+",
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
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| datafusion    | information_schema | tables     | VIEW       |",
        "| datafusion    | information_schema | columns    | VIEW       |",
        "| datafusion    | public             | t          | BASE TABLE |",
        "+---------------+--------------------+------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Newly added tables should appear
    ctx.register_table("t2", table_with_sequence(1, 1).unwrap())
        .unwrap();

    let result = plan_and_collect(&ctx, "SELECT * from information_schema.tables")
        .await
        .unwrap();

    let expected = vec![
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| datafusion    | information_schema | columns    | VIEW       |",
        "| datafusion    | information_schema | tables     | VIEW       |",
        "| datafusion    | public             | t          | BASE TABLE |",
        "| datafusion    | public             | t2         | BASE TABLE |",
        "+---------------+--------------------+------------+------------+",
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
        "+------------------+--------------------+------------+------------+",
        "| table_catalog    | table_schema       | table_name | table_type |",
        "+------------------+--------------------+------------+------------+",
        "| datafusion       | information_schema | columns    | VIEW       |",
        "| datafusion       | information_schema | tables     | VIEW       |",
        "| my_catalog       | information_schema | columns    | VIEW       |",
        "| my_catalog       | information_schema | tables     | VIEW       |",
        "| my_catalog       | my_schema          | t1         | BASE TABLE |",
        "| my_catalog       | my_schema          | t2         | BASE TABLE |",
        "| my_other_catalog | information_schema | columns    | VIEW       |",
        "| my_other_catalog | information_schema | tables     | VIEW       |",
        "| my_other_catalog | my_other_schema    | t3         | BASE TABLE |",
        "+------------------+--------------------+------------+------------+",
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
        "+---------------+--------------------+------------+-----------------+",
        "| table_catalog | table_schema       | table_name | table_type      |",
        "+---------------+--------------------+------------+-----------------+",
        "| datafusion    | information_schema | tables     | VIEW            |",
        "| datafusion    | information_schema | columns    | VIEW            |",
        "| datafusion    | public             | physical   | BASE TABLE      |",
        "| datafusion    | public             | query      | VIEW            |",
        "| datafusion    | public             | temp       | LOCAL TEMPORARY |",
        "+---------------+--------------------+------------+-----------------+",
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
async fn information_schema_show_tables() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.register_table("t", table_with_sequence(1, 1).unwrap())
        .unwrap();

    // use show tables alias
    let result = plan_and_collect(&ctx, "SHOW TABLES").await.unwrap();

    let expected = vec![
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| datafusion    | information_schema | columns    | VIEW       |",
        "| datafusion    | information_schema | tables     | VIEW       |",
        "| datafusion    | public             | t          | BASE TABLE |",
        "+---------------+--------------------+------------+------------+",
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

#[tokio::test]
async fn show_unsupported() {
    let ctx = SessionContext::with_config(SessionConfig::new());

    let err = plan_and_collect(&ctx, "SHOW SOMETHING_UNKNOWN")
        .await
        .unwrap_err();

    assert_eq!(err.to_string(), "This feature is not implemented: SHOW SOMETHING_UNKNOWN not implemented. Supported syntax: SHOW <TABLES>");
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

/// Execute SQL and return results
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

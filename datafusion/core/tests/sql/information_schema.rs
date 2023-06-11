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
};
use datafusion_expr::Expr;

use super::*;

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
            _state: &SessionState,
            _: Option<&Vec<usize>>,
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
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec![Some("foo")])),
            Arc::new(LargeStringArray::from(vec![Some("bar")])),
            Arc::new(BinaryArray::from(vec![b"foo" as &[u8]])),
            Arc::new(LargeBinaryArray::from(vec![b"foo" as &[u8]])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(123)])),
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

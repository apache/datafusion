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
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::{
    arrow::{
        array::{
            BinaryArray, Float64Array, Int32Array, LargeBinaryArray, LargeStringArray,
            StringArray, TimestampNanosecondArray,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    catalog::{schema::MemorySchemaProvider, CatalogProvider, MemoryCatalogProvider},
    datasource::{MemTable, TableProvider, TableType},
    prelude::{CsvReadOptions, SessionContext},
};
use datafusion_common::DataFusionError;
use log::info;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
    /// Temporary directory created and cleared at the end of the test
    test_dir: Option<TempDir>,
}

impl TestContext {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            test_dir: None,
        }
    }

    /// Create a SessionContext, configured for the specific sqllogictest
    /// test(.slt file) , if possible.
    ///
    /// If `None` is returned (e.g. because some needed feature is not
    /// enabled), the file should be skipped
    pub async fn try_new_for_test_file(relative_path: &Path) -> Option<Self> {
        let config = SessionConfig::new()
            // hardcode target partitions so plans are deterministic
            .with_target_partitions(4);

        let mut test_ctx = TestContext::new(SessionContext::new_with_config(config));

        let file_name = relative_path.file_name().unwrap().to_str().unwrap();
        match file_name {
            "scalar.slt" => {
                info!("Registering scalar tables");
                register_scalar_tables(test_ctx.session_ctx()).await;
            }
            "information_schema_table_types.slt" => {
                info!("Registering local temporary table");
                register_temp_table(test_ctx.session_ctx()).await;
            }
            "information_schema_columns.slt" => {
                info!("Registering table with many types");
                register_table_with_many_types(test_ctx.session_ctx()).await;
            }
            "avro.slt" => {
                #[cfg(feature = "avro")]
                {
                    info!("Registering avro tables");
                    register_avro_tables(&mut test_ctx).await;
                }
                #[cfg(not(feature = "avro"))]
                {
                    info!("Skipping {file_name} because avro feature is not enabled");
                    return None;
                }
            }
            "joins.slt" => {
                info!("Registering partition table tables");
                register_partition_table(&mut test_ctx).await;
            }
            "metadata.slt" => {
                info!("Registering metadata table tables");
                register_metadata_tables(test_ctx.session_ctx()).await;
            }
            "csv_files.slt" => {
                info!("Registering metadata table tables");
                register_csv_custom_tables(&mut test_ctx).await;
            }
            _ => {
                info!("Using default SessionContext");
            }
        };
        Some(test_ctx)
    }

    /// Enables the test directory feature. If not enabled,
    /// calling `testdir_path` will result in a panic.
    pub fn enable_testdir(&mut self) {
        if self.test_dir.is_none() {
            self.test_dir = Some(TempDir::new().expect("failed to create testdir"));
        }
    }

    /// Returns the path to the test directory. Panics if the test
    /// directory feature is not enabled via `enable_testdir`.
    pub fn testdir_path(&self) -> &Path {
        self.test_dir.as_ref().expect("testdir not enabled").path()
    }

    /// Returns a reference to the internal SessionContext
    pub fn session_ctx(&self) -> &SessionContext {
        &self.ctx
    }
}

#[cfg(feature = "avro")]
pub async fn register_avro_tables(ctx: &mut crate::TestContext) {
    use datafusion::prelude::AvroReadOptions;

    ctx.enable_testdir();

    let table_path = ctx.testdir_path().join("avro");
    std::fs::create_dir(&table_path).expect("failed to create avro table path");

    let testdata = datafusion::test_util::arrow_test_data();
    let alltypes_plain_file = format!("{testdata}/avro/alltypes_plain.avro");
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain1.avro", table_path.display()),
    )
    .unwrap();
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain2.avro", table_path.display()),
    )
    .unwrap();

    ctx.session_ctx()
        .register_avro(
            "alltypes_plain_multi_files",
            table_path.display().to_string().as_str(),
            AvroReadOptions::default(),
        )
        .await
        .unwrap();
}

pub async fn register_scalar_tables(ctx: &SessionContext) {
    register_nan_table(ctx)
}

/// Register a table with a NaN value (different than NULL, and can
/// not be created via SQL)
fn register_nan_table(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Float64, true)]));

    let data = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(f64::NAN),
        ]))],
    )
    .unwrap();
    ctx.register_batch("test_float", data).unwrap();
}

/// Generate a partitioned CSV file and register it with an execution context
pub async fn register_partition_table(test_ctx: &mut TestContext) {
    test_ctx.enable_testdir();
    let partition_count = 1;
    let file_extension = "csv";
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));
    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = test_ctx.testdir_path().join(filename);
        let mut file = File::create(file_path).unwrap();

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes()).unwrap()
        }
    }

    // register csv file with the execution context
    test_ctx
        .ctx
        .register_csv(
            "test_partition_table",
            test_ctx.testdir_path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await
        .unwrap();
}

pub async fn register_csv_custom_tables(test_ctx: &mut TestContext) {
    test_ctx.enable_testdir();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Utf8, false),
    ]));
    let filename = format!("quote_escape.{}", "csv");
    let file_path = test_ctx.testdir_path().join(filename);
    let mut file = File::create(file_path.clone()).unwrap();

    // generate some data
    for index in 0..10 {
        let text1 = format!("id{index:}");
        let text2 = format!("value{index:}");
        let data = format!("~{text1}~,~{text2}~\r\n");
        file.write_all(data.as_bytes()).unwrap();
    }
    test_ctx
        .ctx
        .register_csv(
            "test_custom_quote_escape",
            file_path.to_str().unwrap(),
            CsvReadOptions::new()
                .schema(&schema)
                .has_header(false)
                .quote(b'~')
                .escape(b'\\'),
        )
        .await
        .unwrap();
}

// registers a LOCAL TEMPORARY table.
pub async fn register_temp_table(ctx: &SessionContext) {
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
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            unimplemented!()
        }
    }

    ctx.register_table(
        "datafusion.public.temp",
        Arc::new(TestTable(TableType::Temporary)),
    )
    .unwrap();
}

pub async fn register_table_with_many_types(ctx: &SessionContext) {
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    catalog
        .register_schema("my_schema", Arc::new(schema))
        .unwrap();
    ctx.register_catalog("my_catalog", Arc::new(catalog));

    ctx.register_table("my_catalog.my_schema.t2", table_with_many_types())
        .unwrap();
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

/// Registers a table_with_metadata that contains both field level and Table level metadata
pub async fn register_metadata_tables(ctx: &SessionContext) {
    let id = Field::new("id", DataType::Int32, true).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the id field"),
    )]));
    let name = Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the name field"),
    )]));

    let schema = Schema::new(vec![id, name]).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the entire schema"),
    )]));

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as _,
            Arc::new(StringArray::from(vec![None, Some("bar"), Some("baz")])) as _,
        ],
    )
    .unwrap();

    ctx.register_batch("table_with_metadata", batch).unwrap();
}

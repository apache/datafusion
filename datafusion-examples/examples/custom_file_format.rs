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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{AsArray, RecordBatch, StringArray, UInt8Array},
    datatypes::UInt64Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::LexRequirement;
use datafusion::{
    datasource::{
        file_format::{
            csv::CsvFormatFactory, file_compression_type::FileCompressionType,
            FileFormat, FileFormatFactory,
        },
        physical_plan::{FileScanConfig, FileSinkConfig},
        MemTable,
    },
    error::Result,
    execution::context::SessionState,
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use datafusion_common::{GetExt, Statistics};
use datafusion_physical_expr::PhysicalExpr;
use object_store::{ObjectMeta, ObjectStore};
use tempfile::tempdir;

/// Example of a custom file format that reads and writes TSV files.
///
/// TSVFileFormatFactory is responsible for creating instances of TSVFileFormat.
/// The former, once registered with the SessionState, will then be used
/// to facilitate SQL operations on TSV files, such as `COPY TO` shown here.

#[derive(Debug)]
/// Custom file format that reads and writes TSV files
///
/// This file format is a wrapper around the CSV file format
/// for demonstration purposes.
struct TSVFileFormat {
    csv_file_format: Arc<dyn FileFormat>,
}

impl TSVFileFormat {
    pub fn new(csv_file_format: Arc<dyn FileFormat>) -> Self {
        Self { csv_file_format }
    }
}

#[async_trait::async_trait]
impl FileFormat for TSVFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "tsv".to_string()
    }

    fn get_ext_with_compression(
        &self,
        c: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        if c == &FileCompressionType::UNCOMPRESSED {
            Ok("tsv".to_string())
        } else {
            todo!("Compression not supported")
        }
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.csv_file_format
            .infer_schema(state, store, objects)
            .await
    }

    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.csv_file_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.csv_file_format
            .create_physical_plan(state, conf, filters)
            .await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.csv_file_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }
}

#[derive(Default, Debug)]
/// Factory for creating TSV file formats
///
/// This factory is a wrapper around the CSV file format factory
/// for demonstration purposes.
pub struct TSVFileFactory {
    csv_file_factory: CsvFormatFactory,
}

impl TSVFileFactory {
    pub fn new() -> Self {
        Self {
            csv_file_factory: CsvFormatFactory::new(),
        }
    }
}

impl FileFormatFactory for TSVFileFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<std::sync::Arc<dyn FileFormat>> {
        let mut new_options = format_options.clone();
        new_options.insert("format.delimiter".to_string(), "\t".to_string());

        let csv_file_format = self.csv_file_factory.create(state, &new_options)?;
        let tsv_file_format = Arc::new(TSVFileFormat::new(csv_file_format));

        Ok(tsv_file_format)
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for TSVFileFactory {
    fn get_ext(&self) -> String {
        "tsv".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new context with the default configuration
    let mut state = SessionStateBuilder::new().with_default_features().build();

    // Register the custom file format
    let file_format = Arc::new(TSVFileFactory::new());
    state.register_file_format(file_format, true).unwrap();

    // Create a new context with the custom file format
    let ctx = SessionContext::new_with_state(state);

    let mem_table = create_mem_table();
    ctx.register_table("mem_table", mem_table).unwrap();

    let temp_dir = tempdir().unwrap();
    let table_save_path = temp_dir.path().join("mem_table.tsv");

    let d = ctx
        .sql(&format!(
            "COPY mem_table TO '{}' STORED AS TSV;",
            table_save_path.display(),
        ))
        .await?;

    let results = d.collect().await?;
    println!(
        "Number of inserted rows: {:?}",
        (results[0]
            .column_by_name("count")
            .unwrap()
            .as_primitive::<UInt64Type>()
            .value(0))
    );

    Ok(())
}

// create a simple mem table
fn create_mem_table() -> Arc<MemTable> {
    let fields = vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("data", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(fields));

    let partitions = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt8Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["foo", "bar"])),
        ],
    )
    .unwrap();

    Arc::new(MemTable::try_new(schema, vec![vec![partitions]]).unwrap())
}

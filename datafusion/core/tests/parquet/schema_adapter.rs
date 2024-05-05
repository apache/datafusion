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

use std::fs;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, SchemaRef};
use object_store::ObjectMeta;
use object_store::path::Path;
use datafusion::assert_batches_sorted_eq;

use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec, SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use datafusion::physical_plan::{collect, Statistics};
use datafusion::prelude::SessionContext;

use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use datafusion::datasource::listing::PartitionedFile;

#[tokio::test]
async fn can_override_schema_adapter() {
    // Create several parquet files in same directoty / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");
    fs::DirBuilder::new()
        .create(table_dir.as_path())
        .unwrap();
    let f1 = Field::new("id", DataType::Int32, true);

    let file_schema = Arc::new(Schema::new(vec![f1.clone()]));
    let filename = format!("part.parquet");
    let path = table_dir.as_path().join(filename.clone());
    let file = fs::File::create(path.clone()).unwrap();
    let mut writer = ArrowWriter::try_new(file, file_schema.clone(), None).unwrap();

    let ids = Arc::new(Int32Array::from(vec![1 as i32]));
    let rec_batch = RecordBatch::try_new(file_schema.clone(), vec![ids]).unwrap();

    writer.write(&rec_batch).unwrap();
    writer.close().unwrap();

    let location = Path::parse(path.to_str().unwrap()).unwrap();
    let metadata = std::fs::metadata(path.as_path()).expect("Local file metadata");
    let meta = ObjectMeta {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len() as usize,
        e_tag: None,
        version: None,
    };

    let partitioned_file = PartitionedFile {
        object_meta: meta,
        partition_values: vec![],
        range: None,
        statistics: None,
        extensions: None,
    };

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("extra_column", DataType::Utf8, true);

    let schema = Arc::new(Schema::new(vec![f1.clone(), f2.clone()]));

    // prepare the scan
    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![vec![partitioned_file]],
            statistics: Statistics::new_unknown(&schema),
            file_schema: schema,
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        },
        None,
        None,
        Default::default(),
    )
   .with_schema_adapter_factory(Arc::new(TestSchemaAdapterFactory {}));

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

    let expected = [
        "+----+--------------+",
        "| id | extra_column |",
        "+----+--------------+",
        "| 1  | foo          |",
        "+----+--------------+",
    ];

    assert_batches_sorted_eq!(expected, &read);
}

#[derive(Debug)]
struct TestSchemaAdapterFactory {}

impl SchemaAdapterFactory for TestSchemaAdapterFactory {
    fn create(&self, schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Box::new(TestSchemaAdapter {
            table_schema: schema,
        })
    }
}

struct TestSchemaAdapter {
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter for TestSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some(_) = self.table_schema.fields().find(file_field.name())
            {
                projection.push(file_idx);
            }
        }

        Ok((
            Arc::new(TestSchemaMapping {}),
            projection,
        ))
    }
}

#[derive(Debug)]
struct TestSchemaMapping {
}

impl SchemaMapper for TestSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let f1 = Field::new("id", DataType::Int32, true);
        let f2 = Field::new("extra_column", DataType::Utf8, true);

        let schema = Arc::new(Schema::new(vec![f1.clone(), f2.clone()]));

        let extra_column = Arc::new(StringArray::from(vec!["foo"]));
        let mut new_columns = batch.columns().to_vec();
        new_columns.push(extra_column);

        Ok(RecordBatch::try_new(schema, new_columns).unwrap())
    }
}



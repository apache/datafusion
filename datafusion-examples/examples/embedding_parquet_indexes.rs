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

//! Example: embedding a "distinct values" index in a Parquet file's metadata
//!
//! 1. Read an existing Parquet file
//! 2. Compute distinct values for a target column using DataFusion
//! 3. Serialize the distinct list to bytes and write a new Parquet file
//!    with these bytes appended as a custom metadata entry
//! 4. Read the new file, extract and deserialize the index from footer
//! 5. Use the index to answer membership queries without scanning data pages

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use base64::engine::general_purpose;
use base64::Engine;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::fs::{create_dir_all, read_dir, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

/// Example creating parquet file that
/// contains specialized indexes that
/// are ignored by other readers
///
/// ```text
///         ┌──────────────────────┐
///         │┌───────────────────┐ │
///         ││     DataPage      │ │      Standard Parquet
///         │└───────────────────┘ │      Data / pages
///         │┌───────────────────┐ │
///         ││     DataPage      │ │
///         │└───────────────────┘ │
///         │        ...           │
///         │                      │
///         │┌───────────────────┐ │
///         ││     DataPage      │ │
///         │└───────────────────┘ │
///         │┏━━━━━━━━━━━━━━━━━━━┓ │
///         │┃                   ┃ │        key/value metadata
///         │┃   Special Index   ┃◀┼────    that points at the
///         │┃                   ┃ │     │  special index
///         │┗━━━━━━━━━━━━━━━━━━━┛ │
///         │╔═══════════════════╗ │     │
///         │║                   ║ │
///         │║  Parquet Footer   ║ │     │  Footer includes
///         │║                   ║ ┼──────  thrift-encoded
///         │║                   ║ │        ParquetMetadata
///         │╚═══════════════════╝ │
///         └──────────────────────┘
///
///               Parquet File
/// ```
/// DistinctIndexTable is a custom TableProvider that reads Parquet files
#[derive(Debug)]
struct DistinctIndexTable {
    schema: SchemaRef,
    index: HashMap<String, HashSet<String>>,
    dir: PathBuf,
}

impl DistinctIndexTable {
    /// Scan a directory, read each file's footer metadata into a map
    fn try_new(dir: impl Into<PathBuf>, schema: SchemaRef) -> Result<Self> {
        let dir = dir.into();
        let mut index = HashMap::new();
        for entry in read_dir(&dir)? {
            let p = entry?.path();
            if p.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            let reader = SerializedFileReader::new(File::open(&p)?)?;
            if let Some(kv) = reader.metadata().file_metadata().key_value_metadata() {
                if let Some(e) = kv.iter().find(|kv| kv.key == "distinct_index_data") {
                    let raw = general_purpose::STANDARD_NO_PAD
                        .decode(e.value.as_deref().unwrap())
                        .unwrap();
                    let s = String::from_utf8(raw).unwrap();
                    let set = s.lines().map(|l| l.to_string()).collect();
                    index.insert(name, set);
                }
            }
        }
        Ok(Self { schema, index, dir })
    }
}

// Write a Parquet file and embed its distinct "category" values in footer metadata
fn write_file_with_index(path: &Path, values: &[&str]) -> Result<()> {
    let field = Field::new("category", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let arr: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])?;

    // Compute distinct values, serialize & Base64‑encode
    let distinct: HashSet<_> = values.iter().copied().collect();
    let serialized = distinct.iter().cloned().collect::<Vec<_>>().join("\n");
    let b64 = general_purpose::STANDARD_NO_PAD.encode(serialized.as_bytes());

    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "distinct_index_data".into(),
            b64,
        )]))
        .build();

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

/// Implement TableProvider for DistinctIndexTable, using the distinct index to prune files
#[async_trait]
impl TableProvider for DistinctIndexTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Prune files before reading: only keep files whose distinct set contains the filter value
    async fn scan(
        &self,
        _ctx: &dyn Session,
        _proj: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Look for a single `category = 'X'` filter
        let mut target: Option<String> = None;

        if filters.len() == 1 {
            if let Expr::BinaryExpr(expr) = &filters[0] {
                if expr.op == Operator::Eq {
                    if let (
                        Expr::Column(c),
                        Expr::Literal(ScalarValue::Utf8(Some(v)), _),
                    ) = (&*expr.left, &*expr.right)
                    {
                        if c.name == "category" {
                            target = Some(v.clone());
                        }
                    }
                }
            }
        }
        // Determine which files to scan
        let keep: Vec<String> = self
            .index
            .iter()
            .filter(|(_f, set)| target.as_ref().is_none_or(|v| set.contains(v)))
            .map(|(f, _)| f.clone())
            .collect();

        println!("Pruned files: {:?}", keep.clone());

        // Build ParquetSource for kept files
        let url = ObjectStoreUrl::parse("file://")?;
        let source = Arc::new(ParquetSource::default());
        let mut builder = FileScanConfigBuilder::new(url, self.schema.clone(), source);
        for file in keep {
            let path = self.dir.join(&file);
            let len = std::fs::metadata(&path)?.len();
            builder = builder.with_file(PartitionedFile::new(
                path.to_string_lossy().into_owned(),
                len,
            ));
        }
        Ok(DataSourceExec::from_data_source(builder.build()))
    }

    fn supports_filters_pushdown(
        &self,
        fs: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Mark as inexact since pruning is file‑granular
        Ok(vec![TableProviderFilterPushDown::Inexact; fs.len()])
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create temp dir and write 3 Parquet files with different category sets
    let tmp = TempDir::new()?;
    let dir = tmp.path();
    create_dir_all(dir)?;
    write_file_with_index(&dir.join("a.parquet"), &["foo", "bar", "foo"])?;
    write_file_with_index(&dir.join("b.parquet"), &["baz", "qux"])?;
    write_file_with_index(&dir.join("c.parquet"), &["foo", "quux"])?;

    // 2. Register our custom TableProvider
    let field = Field::new("category", DataType::Utf8, false);
    let schema_ref = Arc::new(Schema::new(vec![field]));
    let provider = Arc::new(DistinctIndexTable::try_new(dir, schema_ref.clone())?);
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    // 3. Run a query: only files containing 'foo' get scanned
    let df = ctx.sql("SELECT * FROM t WHERE category = 'foo'").await?;
    df.show().await?;

    Ok(())
}

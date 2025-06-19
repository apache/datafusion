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

//! Example: embedding and using a custom “distinct values” index in Parquet files
//!
//! This example shows how to build and leverage a file‑level distinct‑values index
//! for pruning in DataFusion’s Parquet scans.
//!
//! Steps:
//! 1. Compute the distinct values for a target column and serialize them into bytes.
//! 2. Write each Parquet file with:
//!    - regular data pages for your column
//!    - the magic marker `IDX1` and a little‑endian length, to identify our custom index format
//!    - the serialized distinct‑values bytes
//!    - footer key/value metadata entries (`distinct_index_offset` and `distinct_index_length`)
//! 3. Read back each file’s footer metadata to locate and deserialize the index.
//! 4. Build a `DistinctIndexTable` (a custom `TableProvider`) that scans footers
//!    into a map of filename → `HashSet<String>` of distinct values.
//! 5. In `scan()`, prune out any Parquet files whose distinct set doesn’t match the
//!    `category = 'X'` filter, then only read data from the remaining files.
//!
//! This technique embeds a lightweight, application‑specific index directly in Parquet
//! metadata to achieve efficient file‑level pruning without modifying the Parquet format.
//!
//! And it's very efficient, since we don't add any additional info to the metadata, we write the custom index
//! after the data pages, and we only read it when needed.

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::parquet::arrow::ArrowSchemaConverter;
use datafusion::parquet::data_type::{ByteArray, ByteArrayType};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::fs::{create_dir_all, read_dir, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

///
/// Example creating the Parquet file that
/// contains specialized indexes and a page‑index offset
///
/// Note: the page index offset will after the custom index, which
/// is originally after the data pages.
///
/// ```text
///         ┌──────────────────────┐
///         │┌───────────────────┐ │
///         ││     DataPage      │ │      Standard Parquet
///         │└───────────────────┘ │      Data pages
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
///         │┃   Special Index   ┃◀┼────    that points to the
///         │┃                   ┃ │     │  custom index blob
///         │┗━━━━━━━━━━━━━━━━━━━┛ │
///         │┏───────────────────┓ │
///         │┃ Page Index Offset ┃◀┼────    little‑endian u64
///         │┗───────────────────┛ │     │  sitting after the custom index
///         │╔═══════════════════╗ │     │
///         │║                   ║ │
///         │║  Parquet Footer   ║ │     │  thrift‑encoded
///         │║                   ║ ┼──────  ParquetMetadata
///         │║                   ║ │
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
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();

            let distinct_set = read_distinct_index(&path)?;

            println!("Read distinct index for {file_name}: {file_name:?}");
            index.insert(file_name, distinct_set);
        }

        Ok(Self { schema, index, dir })
    }
}

pub struct IndexedParquetWriter<W: Write + Seek> {
    writer: SerializedFileWriter<W>,
}

impl<W: Write + Seek + Send> IndexedParquetWriter<W> {
    pub fn try_new(
        sink: W,
        schema: Arc<Schema>,
        props: WriterProperties,
    ) -> Result<Self> {
        let schema_desc = ArrowSchemaConverter::new().convert(schema.as_ref())?;
        let props_ptr = Arc::new(props);
        let writer =
            SerializedFileWriter::new(sink, schema_desc.root_schema_ptr(), props_ptr)?;
        Ok(Self { writer })
    }
}

/// Magic bytes to identify our custom index format
const INDEX_MAGIC: &[u8] = b"IDX1";

fn write_file_with_index(path: &Path, values: &[&str]) -> Result<()> {
    let field = Field::new("category", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let arr: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])?;

    let distinct: HashSet<_> = values.iter().copied().collect();
    let serialized = distinct.into_iter().collect::<Vec<_>>().join("\n");
    let index_bytes = serialized.into_bytes();

    let props = WriterProperties::builder().build();
    let file = File::create(path)?;

    let mut writer = IndexedParquetWriter::try_new(file, schema.clone(), props)?;

    // Write data to the Parquet file, we only write one column since our schema has one field
    {
        let mut rg_writer = writer.writer.next_row_group()?;
        let mut ser_col_writer = rg_writer
            .next_column()?
            .ok_or_else(|| ParquetError::General("No column writer".into()))?;

        let col_writer = ser_col_writer.typed::<ByteArrayType>();
        let values_bytes: Vec<ByteArray> = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|opt| ByteArray::from(opt.unwrap()))
            .collect();

        println!("Writing values: {values_bytes:?}");
        col_writer.write_batch(&values_bytes, None, None)?;
        ser_col_writer.close()?;
        rg_writer.close()?;
    }

    let offset = writer.writer.inner().stream_position()?;
    let index_len = index_bytes.len() as u64;

    println!("Writing custom index at offset: {offset}, length: {index_len}");
    // Write the index magic and length to the file
    writer.writer.write_all(b"IDX1")?;
    writer.writer.write_all(&index_len.to_le_bytes())?;

    // Write the index bytes
    writer.writer.write_all(&index_bytes)?;

    // Append metadata about the index to the Parquet file footer
    writer.writer.append_key_value_metadata(KeyValue::new(
        "distinct_index_offset".to_string(),
        offset.to_string(),
    ));
    writer.writer.append_key_value_metadata(KeyValue::new(
        "distinct_index_length".to_string(),
        index_bytes.len().to_string(),
    ));

    writer.writer.close()?;

    println!("Finished writing file to {}", path.display());
    Ok(())
}

fn read_distinct_index(path: &Path) -> Result<HashSet<String>, ParquetError> {
    let mut file = File::open(path)?;

    let file_size = file.metadata()?.len();
    println!(
        "Reading index from {} (size: {})",
        path.display(),
        file_size
    );

    let reader = SerializedFileReader::new(file.try_clone()?)?;
    let meta = reader.metadata().file_metadata();

    let offset = meta
        .key_value_metadata()
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == "distinct_index_offset"))
        .and_then(|kv| kv.value.as_ref())
        .ok_or_else(|| ParquetError::General("Missing index offset".into()))?
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))?;

    let length = meta
        .key_value_metadata()
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == "distinct_index_length"))
        .and_then(|kv| kv.value.as_ref())
        .ok_or_else(|| ParquetError::General("Missing index length".into()))?
        .parse::<usize>()
        .map_err(|e| ParquetError::General(e.to_string()))?;

    println!("Reading index at offset: {offset}, length: {length}");

    file.seek(SeekFrom::Start(offset))?;

    let mut magic_buf = [0u8; 4];
    file.read_exact(&mut magic_buf)?;
    if magic_buf != INDEX_MAGIC {
        return Err(ParquetError::General("Invalid index magic".into()));
    }

    let mut len_buf = [0u8; 8];
    file.read_exact(&mut len_buf)?;
    let stored_len = u64::from_le_bytes(len_buf) as usize;

    if stored_len != length {
        return Err(ParquetError::General("Index length mismatch".into()));
    }

    let mut index_buf = vec![0u8; length];
    file.read_exact(&mut index_buf)?;

    let s =
        String::from_utf8(index_buf).map_err(|e| ParquetError::General(e.to_string()))?;

    Ok(s.lines().map(|s| s.to_string()).collect())
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
                            println!("Filtering for category: {v}");
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
        let source = Arc::new(ParquetSource::default().with_enable_page_index(true));
        let mut builder = FileScanConfigBuilder::new(url, self.schema.clone(), source);
        for file in keep {
            let path = self.dir.join(&file);
            let len = std::fs::metadata(&path)?.len();
            builder = builder.with_file(PartitionedFile::new(
                path.to_str().unwrap().to_string(),
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
    write_file_with_index(&dir.join("c.parquet"), &["foo", "quux", "quux"])?;

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

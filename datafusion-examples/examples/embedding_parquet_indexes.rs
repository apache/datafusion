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
//! 1. Read existing Parquet files
//! 2. Compute distinct values for a target column using DataFusion
//! 3. Serialize the distinct index to bytes and write to the new Parquet file
//!    with these encoded bytes appended as a custom metadata entry
//! 4. Read each new parquet file, extract and deserialize the index from footer
//! 5. Use the distinct index to prune files when querying

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
use datafusion::parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::fs::{create_dir_all, read_dir, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use futures::AsyncWriteExt;
use tempfile::TempDir;
use datafusion::parquet::column::writer::ColumnWriter;
use datafusion::parquet::data_type::{ByteArray, ByteArrayType};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use datafusion_proto::protobuf::FromProtoError::DataFusionError;

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
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();

            // 直接用工具函数读取该文件的 distinct index
            let distinct_set = read_distinct_index(&path)?;

            println!("Read distinct index for {}: {:?}", file_name, distinct_set);
            index.insert(file_name, distinct_set);
        }

        Ok(Self { schema, index, dir })
    }
}

pub struct IndexedParquetWriter<W: Write + Seek> {
    writer: SerializedFileWriter<W>,
}

impl<W: Write + Seek + Send> IndexedParquetWriter<W> {
    /// 构造：传入已经创建好的文件、schema 和普通的 WriterProperties（不要在 metadata 里放 Base64）
    pub fn try_new(
        sink: W,
        schema: Arc<Schema>,
        props: WriterProperties,
    ) -> Result<Self> {
        let schema_desc = ArrowSchemaConverter::new().convert(schema.as_ref())?;
        let props_ptr = Arc::new(props);
        let writer = SerializedFileWriter::new(sink, schema_desc.root_schema_ptr(), props_ptr)?;
        Ok(Self { writer })
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
    let serialized = distinct.into_iter().collect::<Vec<_>>().join("\n");
    let index_bytes = serialized.into_bytes();

    let props = WriterProperties::builder()
        .build();

    let file = File::create(path)?;

    let mut writer = IndexedParquetWriter::try_new(file, schema.clone(), props)?;
    // {
    //     // 1) next_row_group
    //     let mut rg_writer = writer.writer.next_row_group()?;
    //
    //     // 2) 拿到 SerializedColumnWriter
    //     let mut ser_col_writer: SerializedColumnWriter<'_> =
    //         rg_writer
    //             .next_column()?
    //             .ok_or_else(|| ParquetError::General("No column writer".into()))?;
    //
    //     // 3) 通过 typed 拿到具体的 ByteArrayColumnWriter 引用
    //     let col_writer = ser_col_writer.typed::<ByteArrayType>();
    //
    //     // 4) 写入数据
    //     let values_bytes: Vec<ByteArray> = batch
    //         .column(0)
    //         .as_any()
    //         .downcast_ref::<StringArray>()
    //         .unwrap()
    //         .iter()
    //         .map(|opt| ByteArray::from(opt.unwrap()))
    //         .collect();
    //
    //     println!("Writing values: {:?}", values_bytes);
    //
    //     col_writer.write_batch(&values_bytes, None, None)?;
    //
    //     // 5) 关闭这个 column writer（.close(self) 会消费 ser_col_writer）
    //     ser_col_writer.close()?;
    //
    //     // 6) 关闭 row‑group
    //     rg_writer.close()?;
    // }

    let offset = writer.writer
        .inner()
        .seek(SeekFrom::Current(0))?;

    println!("Writing distinct index at offset: {} path: {}", offset, path.display());

    writer.writer.inner().write_all(&index_bytes)?;


    writer.writer.append_key_value_metadata(
        KeyValue::new(
            "distinct_index_offset".into(),
            offset.to_string(),
        ),
    );

    writer.writer.append_key_value_metadata(
        KeyValue::new(
            "distinct_index_length".into(),
            index_bytes.len().to_string(),
        ),
    );

    // let final_props = WriterProperties::builder()
    //     .set_key_value_metadata(Some(vec![
    //         KeyValue::new("distinct_index_offset".into(), offset.to_string()),
    //         KeyValue::new("distinct_index_length".into(), index_bytes.len().to_string()),
    //     ]))
    //     .build();
    //
    // let mut footer_writer =
    //     SerializedFileWriter::new(
    //         writer.writer.inner(),
    //         ArrowSchemaConverter::new().convert(schema.as_ref())?.root_schema_ptr(),
    //         Arc::new(final_props),
    //     )?;

    writer.writer.close()?;

    println!("Finished writing file");

    Ok(())
}


fn read_distinct_index(path: &Path) -> Result<HashSet<String>> {
    // 1. Open reader for metadata
    let reader = SerializedFileReader::new(
        File::open(path)
            .map_err(|e| ParquetError::General(e.to_string()))?
    )?;
    let meta = reader.metadata().file_metadata();
    let (off_kv, len_kv) = meta
        .key_value_metadata()
        .and_then(|vec| {
            let off = vec.iter().find(|kv| kv.key == "distinct_index_offset")?;
            let len = vec.iter().find(|kv| kv.key == "distinct_index_length")?;
            Some((off, len))
        })
        .ok_or_else(|| ParquetError::General("missing index offset/length metadata".into()))?;

    // 2. Parse offset and length, converting any ParseIntError to String
    let offset: u64 = off_kv
        .value
        .as_ref()
        .ok_or_else(|| ParquetError::General("empty offset".into()))?
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))?;
    let length: usize = len_kv
        .value
        .as_ref()
        .ok_or_else(|| ParquetError::General("empty length".into()))?
        .parse::<usize>()
        .map_err(|e| ParquetError::General(e.to_string()))?;

    // 3. Seek & read exactly `length` bytes at `offset`
    let mut file = File::open(path)
        .map_err(|e| ParquetError::General(e.to_string()))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| ParquetError::General(e.to_string()))?;
    let mut buf = vec![0u8; length];
    file.read_exact(&mut buf)
        .map_err(|e| ParquetError::General(e.to_string()))?;

    // 4. Decode UTF-8 & split into lines
    let s = String::from_utf8(buf)
        .map_err(|e| ParquetError::General(e.to_string()))?;
    Ok(s.lines().map(|l| l.to_string()).collect())
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
        let source = Arc::new(ParquetSource::default());
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

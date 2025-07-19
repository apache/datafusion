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

//! Embedding and using a custom index in Parquet files
//!
//! # Background
//!
//! This example shows how to add an application‑specific index to an Apache
//! Parquet file without modifying the Parquet format itself. The resulting
//! files can be read by any standard Parquet reader, which will simply
//! ignore the extra index data.
//!
//! A “distinct value” index, similar to a  ["set" Skip Index in ClickHouse],
//! is stored in a custom binary format within the parquet file. Only the
//! location of index is stored in Parquet footer key/value metadata.
//! This approach is more efficient than storing the index itself in the footer
//! metadata because the footer must be read and parsed by all readers,
//! even those that do not use the index.
//!
//! This example uses a file level index for skipping entire files, but any
//! index can be stored using the same techniques and used skip row groups,
//! data pages, or rows using the APIs on [`TableProvider`] and [`ParquetSource`].
//!
//! The resulting Parquet file layout is as follows:
//!
//! ```text
//!                   ┌──────────────────────┐
//!                   │┌───────────────────┐ │
//!                   ││     DataPage      │ │
//!                   │└───────────────────┘ │
//!  Standard Parquet │┌───────────────────┐ │
//!  Data Pages       ││     DataPage      │ │
//!                   │└───────────────────┘ │
//!                   │        ...           │
//!                   │┌───────────────────┐ │
//!                   ││     DataPage      │ │
//!                   │└───────────────────┘ │
//!                   │┏━━━━━━━━━━━━━━━━━━━┓ │
//! Non standard      │┃                   ┃ │
//! index (ignored by │┃Custom Binary Index┃ │
//! other Parquet     │┃ (Distinct Values) ┃◀│─ ─ ─
//! readers)          │┃                   ┃ │     │
//!                   │┗━━━━━━━━━━━━━━━━━━━┛ │
//! Standard Parquet  │┏━━━━━━━━━━━━━━━━━━━┓ │     │  key/value metadata
//! Page Index        │┃    Page Index     ┃ │        contains location
//!                   │┗━━━━━━━━━━━━━━━━━━━┛ │     │  of special index
//!                   │╔═══════════════════╗ │
//!                   │║ Parquet Footer w/ ║ │     │
//!                   │║     Metadata      ║ ┼ ─ ─
//!                   │║ (Thrift Encoded)  ║ │
//!                   │╚═══════════════════╝ │
//!                   └──────────────────────┘
//!
//!                         Parquet File
//!
//! # High Level Flow
//!
//! To create a custom Parquet index:
//!
//! 1. Compute the index and serialize it to a binary format.
//!
//! 2. Write the Parquet file with:
//!    - regular data pages
//!    - the serialized index inline
//!    - footer key/value metadata entry to locate the index
//!
//! To read and use the index are:
//!
//! 1. Read and deserialize the file’s footer to locate the index.
//!
//! 2. Read and deserialize the index.
//!
//! 3. Create a `TableProvider` that knows how to use the index to quickly find
//!   the relevant files, row groups, data pages or rows based on on pushed down
//!   filters.
//!
//! # FAQ: Why do other Parquet readers skip over the custom index?
//!
//! The flow for reading a parquet file is:
//!
//! 1. Seek to the end of the file and read the last 8 bytes (a 4‑byte
//!    little‑endian footer length followed by the `PAR1` magic bytes).
//!
//! 2. Seek backwards by that length to parse the Thrift‑encoded footer
//!    metadata (including key/value pairs).
//!
//! 3. Read data required for decoding such as data pages based on the offsets
//!    encoded in the metadata.
//!
//! Since parquet readers do not scan from the start of the file they will read
//! data in the file unless it is explicitly referenced in the footer metadata.
//!
//! Thus other readers will encounter and ignore an unknown key
//! (`distinct_index_offset`) in the footer key/value metadata. Unless they
//! know how to use that information, they will not attempt to read or
//! the bytes that make up the index.
//!
//! ["set" Skip Index in ClickHouse]: https://clickhouse.com/docs/optimize/skipping-indexes#set

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{exec_err, HashMap, HashSet, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::{FileMetaData, KeyValue};
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::fs::{read_dir, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

/// An index of distinct values for a single column
///
/// In this example the index is a simple set of strings, but in a real
/// application it could be any arbitrary data structure.
///
/// Also, this example indexes the distinct values for an entire file
/// but a real application could create multiple indexes for multiple
/// row groups and/or columns, depending on the use case.
#[derive(Debug, Clone)]
struct DistinctIndex {
    inner: HashSet<String>,
}

impl DistinctIndex {
    /// Create a DistinctIndex from an iterator of strings
    pub fn new<I: IntoIterator<Item = String>>(iter: I) -> Self {
        Self {
            inner: iter.into_iter().collect(),
        }
    }

    /// Returns true if the index contains the given value
    pub fn contains(&self, value: &str) -> bool {
        self.inner.contains(value)
    }

    /// Serialize the distinct index to a writer as bytes
    ///
    /// In this example, we use a simple newline-separated format,
    /// but a real application can use any arbitrary binary format.
    ///
    /// Note that we must use the ArrowWriter to write the index so that its
    /// internal accounting of offsets can correctly track the actual size of
    /// the file. If we wrote directly to the underlying writer, the PageIndex
    /// written right before the would be incorrect as they would not account
    /// for the extra bytes written.
    fn serialize<W: Write + Send>(
        &self,
        arrow_writer: &mut ArrowWriter<W>,
    ) -> Result<()> {
        let serialized = self
            .inner
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let index_bytes = serialized.into_bytes();

        // Set the offset for the index
        let offset = arrow_writer.bytes_written();
        let index_len = index_bytes.len() as u64;

        println!("Writing custom index at offset: {offset}, length: {index_len}");
        // Write the index magic and length to the file
        arrow_writer.write_all(INDEX_MAGIC)?;
        arrow_writer.write_all(&index_len.to_le_bytes())?;

        // Write the index bytes
        arrow_writer.write_all(&index_bytes)?;

        // Append metadata about the index to the Parquet file footer
        arrow_writer.append_key_value_metadata(KeyValue::new(
            "distinct_index_offset".to_string(),
            offset.to_string(),
        ));
        Ok(())
    }

    /// Read the distinct values index from a reader at the given offset and length
    pub fn new_from_reader<R: Read + Seek>(mut reader: R, offset: u64) -> Result<Self> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut magic_buf = [0u8; 4];
        reader.read_exact(&mut magic_buf)?;
        if magic_buf != INDEX_MAGIC {
            return exec_err!("Invalid index magic number at offset {offset}");
        }

        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let stored_len = u64::from_le_bytes(len_buf) as usize;

        let mut index_buf = vec![0u8; stored_len];
        reader.read_exact(&mut index_buf)?;

        let Ok(s) = String::from_utf8(index_buf) else {
            return exec_err!("Invalid UTF-8 in index data");
        };

        Ok(Self {
            inner: s.lines().map(|s| s.to_string()).collect(),
        })
    }
}

/// DataFusion [`TableProvider]` that reads Parquet files and uses a
/// `DistinctIndex` to prune files based on pushed down filters.
#[derive(Debug)]
struct DistinctIndexTable {
    /// The schema of the table
    schema: SchemaRef,
    /// Key is file name, value is DistinctIndex for that file
    files_and_index: HashMap<String, DistinctIndex>,
    /// Directory containing the Parquet files
    dir: PathBuf,
}

impl DistinctIndexTable {
    /// Create a new DistinctIndexTable for files in the given directory
    ///
    /// Scans the directory, reading the `DistinctIndex` from each file
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

        Ok(Self {
            schema,
            files_and_index: index,
            dir,
        })
    }
}

/// Wrapper around ArrowWriter to write Parquet files with an embedded index
struct IndexedParquetWriter<W: Write + Seek> {
    writer: ArrowWriter<W>,
}

/// Magic bytes to identify our custom index format
const INDEX_MAGIC: &[u8] = b"IDX1";

impl<W: Write + Seek + Send> IndexedParquetWriter<W> {
    pub fn try_new(sink: W, schema: Arc<Schema>) -> Result<Self> {
        let writer = ArrowWriter::try_new(sink, schema, None)?;
        Ok(Self { writer })
    }

    /// Write a RecordBatch to the Parquet file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    /// Flush the current row group
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Close the Parquet file, flushing any remaining data
    pub fn close(self) -> Result<()> {
        self.writer.close()?;
        Ok(())
    }

    /// write the DistinctIndex to the Parquet file
    pub fn write_index(&mut self, index: &DistinctIndex) -> Result<()> {
        index.serialize(&mut self.writer)
    }
}

/// Write a Parquet file with a single column "category" containing the
/// strings in `values` and a DistinctIndex for that column.
fn write_file_with_index(path: &Path, values: &[&str]) -> Result<()> {
    // form an input RecordBatch with the string values
    let field = Field::new("category", DataType::Utf8, false);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let arr: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])?;

    // compute the distinct index
    let distinct_index: DistinctIndex =
        DistinctIndex::new(values.iter().map(|s| s.to_string()));

    let file = File::create(path)?;

    let mut writer = IndexedParquetWriter::try_new(file, schema.clone())?;
    writer.write(&batch)?;
    writer.flush()?;
    writer.write_index(&distinct_index)?;
    writer.close()?;

    println!("Finished writing file to {}", path.display());
    Ok(())
}

/// Read a `DistinctIndex` from a Parquet file
fn read_distinct_index(path: &Path) -> Result<DistinctIndex> {
    let file = File::open(path)?;

    let file_size = file.metadata()?.len();
    println!("Reading index from {} (size: {file_size})", path.display(),);

    let reader = SerializedFileReader::new(file.try_clone()?)?;
    let meta = reader.metadata().file_metadata();

    let offset = get_key_value(meta, "distinct_index_offset")
        .ok_or_else(|| ParquetError::General("Missing index offset".into()))?
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))?;

    println!("Reading index at offset: {offset}, length");
    DistinctIndex::new_from_reader(file, offset)
}

/// Returns the value of a named key from the Parquet file metadata
///
/// Returns None if the key is not found
fn get_key_value<'a>(file_meta_data: &'a FileMetaData, key: &'_ str) -> Option<&'a str> {
    let kvs = file_meta_data.key_value_metadata()?;
    let kv = kvs.iter().find(|kv| kv.key == key)?;
    kv.value.as_deref()
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

    /// Prune files before reading: only keep files whose distinct set
    /// contains the filter value
    async fn scan(
        &self,
        _ctx: &dyn Session,
        _proj: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // This example only handles filters of the form
        // `category = 'X'` where X is a string literal
        //
        // You can use `PruningPredicate` for much more general range and
        // equality analysis or write your own custom logic.
        let mut target: Option<&str> = None;

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
                            target = Some(v);
                        }
                    }
                }
            }
        }
        // Determine which files to scan
        let files_to_scan: Vec<_> = self
            .files_and_index
            .iter()
            .filter_map(|(f, distinct_index)| {
                // keep file if no target or target is in the distinct set
                if target.is_none() || distinct_index.contains(target?) {
                    Some(f)
                } else {
                    None
                }
            })
            .collect();

        println!("Scanning only files: {files_to_scan:?}");

        // Build ParquetSource to actually read the files
        let url = ObjectStoreUrl::parse("file://")?;
        let source = Arc::new(ParquetSource::default().with_enable_page_index(true));
        let mut builder = FileScanConfigBuilder::new(url, self.schema.clone(), source);
        for file in files_to_scan {
            let path = self.dir.join(file);
            let len = std::fs::metadata(&path)?.len();
            // If the index contained information about row groups or pages,
            // you could also pass that information here to further prune
            // the data read from the file.
            let partitioned_file =
                PartitionedFile::new(path.to_str().unwrap().to_string(), len);
            builder = builder.with_file(partitioned_file);
        }
        Ok(DataSourceExec::from_data_source(builder.build()))
    }

    /// Tell DataFusion that we can handle filters on the "category" column
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
    write_file_with_index(&dir.join("a.parquet"), &["foo", "bar", "foo"])?;
    write_file_with_index(&dir.join("b.parquet"), &["baz", "qux"])?;
    write_file_with_index(&dir.join("c.parquet"), &["foo", "quux", "quux"])?;

    // 2. Register our custom TableProvider
    let field = Field::new("category", DataType::Utf8, false);
    let schema_ref = Arc::new(Schema::new(vec![field]));
    let provider = Arc::new(DistinctIndexTable::try_new(dir, schema_ref.clone())?);

    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    // 3. Run a query: only files containing 'foo' get scanned. The rest are pruned.
    // based on the distinct index.
    let df = ctx.sql("SELECT * FROM t WHERE category = 'foo'").await?;
    df.show().await?;

    Ok(())
}

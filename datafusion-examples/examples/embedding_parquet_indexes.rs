
//! Example: embedding a "distinct values" index in a Parquet file's metadata
//!
//! 1. Read an existing Parquet file
//! 2. Compute distinct values for a target column using DataFusion
//! 3. Serialize the distinct list to bytes and write a new Parquet file
//!    with these bytes appended as a custom metadata entry
//! 4. Read the new file, extract and deserialize the index from footer
//! 5. Use the index to answer membership queries without scanning data pages

use arrow::array::{ArrayRef, StringArray, StringBuilder, StringViewArray};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemTable;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::writer;
use std::fs::{File, read_dir};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use arrow_schema::{DataType, Field, Schema};
use tempfile::TempDir;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::main]
async fn main() -> Result<()> {

    let tmpdir = TempDir::new()?;

    let input_path = tmpdir.path().join("input.parquet");
    let indexed_path = tmpdir.path().join("output_with_index.parquet");
    let column = "category";


    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
    ]));

    let mut builder = StringBuilder::new();
    builder.append_value("foo");
    builder.append_value("bar");
    builder.append_value("baz");
    builder.append_value("foo");
    builder.append_value("qux");
    builder.append_value("baz");
    builder.append_value("bar");
    builder.append_value("quux");
    builder.append_value("corge");
    builder.append_value("grault");

    let array = Arc::new(builder.finish()) as ArrayRef;

    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;

    let input_file = File::create(&input_path)?;
    let mut writer = ArrowWriter::try_new(input_file, schema, None)?;
    writer.write(&batch)?;
    writer.finish()?;


    // 1. Compute distinct values for `column`
    let ctx = SessionContext::new();
    ctx.register_parquet("t", input_path.to_str().unwrap().to_string(), ParquetReadOptions::default()).await?;

    let df = ctx.sql(&format!("SELECT DISTINCT {col} FROM t", col=column)).await?;
    let batches = df.collect().await?;

    println!("batches: {:#?}", batches);

    // Flatten distinct strings
    let mut distinct: Vec<String> = Vec::new();
    for batch in &batches {
        // ut8 default read to ut8view
        let col_array = batch.column(0)
            .as_any().downcast_ref::<StringViewArray>().unwrap();
        for i in col_array.iter() {
            distinct.push(i.unwrap().to_string());
        }
    }

    // 2. Serialize distinct list (simple newline-delimited)
    let serialized = distinct.join("\n");
    let bytes = serialized.as_bytes();


    let mut props = WriterProperties::builder();
    // store index length and bytes in metadata
    props = props.set_key_value_metadata(Some(vec![
        KeyValue::new("distinct_index_size".to_string(), bytes.len().to_string()),
        KeyValue::new("distinct_index_data".to_string(), base64::encode(bytes)),
    ]));
    let props = props.build();

    // use ArrowWriter to copy data pages
    let file = File::open(&input_path).map_err(|e| {
        DataFusionError::from(e).context(format!("Error opening file {input_path:?}"))
    })?;

    let write_file = File::create(&indexed_path).map_err(|e| {
        DataFusionError::from(e).context(format!("Error creating file {indexed_path:?}"))
    })?;


    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let schema = reader.schema().clone();
    let mut writer = ArrowWriter::try_new(
        write_file,
        schema,
        Some(props),
    )?;

    let file = File::open(&input_path).map_err(|e| {
        DataFusionError::from(e).context(format!("Error opening file {input_path:?}"))
    })?;

    // stream record batches using a fresh reader
    let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    while let Some(Ok(batch)) = batch_reader.next() {
        println!("batch = {:#?}", batch);
        writer.write(&batch)?;
    }
    writer.finish()?;


    // 4. Open new file and extract index bytes
    let file = File::open(&indexed_path).map_err(|e| {
        DataFusionError::from(e).context(format!("Error opening file {input_path:?}"))
    })?;

    let reader2 = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let meta = reader2.metadata().file_metadata();
    let kv = meta.key_value_metadata().unwrap();
    let map = kv.iter().map(|kv| (kv.key.as_str(), kv.value.as_deref().unwrap_or(""))).collect::<std::collections::HashMap<_,_>>();
    let size: usize = map.get("distinct_index_size").unwrap().parse().unwrap();
    let data_b64 = map.get("distinct_index_data").unwrap();
    let data = base64::decode(data_b64).unwrap();

    // 5. Demonstrate using the index: membership check
    let loaded = String::from_utf8(data).unwrap();
    let loaded_set: std::collections::HashSet<&str> = loaded.lines().collect();
    println!("Index contains {} distinct values", loaded_set.len());
    println!("Contains 'foo'? {}", loaded_set.contains("foo"));

    Ok(())
}


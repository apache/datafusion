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

//! See `main.rs` for how to run it.
//!
//! This follows the recommended approach: implement the `ObjectStore` trait
//! (or use an existing implementation), register it with DataFusion, and then
//! read a URL "path" from that store.
//! See the in-memory reference implementation:
//! https://docs.rs/object_store/0.6.1/object_store/memory/struct.InMemory.html

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
use datafusion::common::Result;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{
    CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionContext,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use url::Url;

/// Demonstrates reading CSV/JSON/Parquet data from an in-memory object store.
pub async fn in_memory_object_store() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ctx = SessionContext::new();
    let object_store_url = Url::parse("mem://").unwrap();
    ctx.register_object_store(&object_store_url, Arc::clone(&store));

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    println!("=== CSV from memory ===");
    let csv_path = Path::from("/people.csv");
    let csv_data = b"id,name\n1,Alice\n2,Bob\n";
    store
        .put(&csv_path, PutPayload::from_static(csv_data))
        .await?;
    let csv = ctx
        .read_csv("mem:///people.csv", CsvReadOptions::new().schema(&schema))
        .await?
        .collect()
        .await?;
    #[rustfmt::skip]
    let expected = [
        "+----+-------+",
        "| id | name  |",
        "+----+-------+",
        "| 1  | Alice |",
        "| 2  | Bob   |",
        "+----+-------+",
    ];
    assert_batches_eq!(expected, &csv);

    println!("=== JSON (NDJSON) from memory ===");
    let json_path = Path::from("/people.json");
    let json_data = br#"{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}
"#;
    store
        .put(&json_path, PutPayload::from_static(json_data))
        .await?;
    let json = ctx
        .read_json(
            "mem:///people.json",
            JsonReadOptions::default().schema(&schema),
        )
        .await?
        .collect()
        .await?;
    assert_batches_eq!(expected, &json);

    println!("=== Parquet from memory ===");
    let parquet_path = Path::from("/people.parquet");
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;
    let mut buf = vec![];
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    store
        .put(&parquet_path, PutPayload::from_bytes(buf.into()))
        .await?;
    let parquet = ctx
        .read_parquet("mem:///people.parquet", ParquetReadOptions::default())
        .await?
        .collect()
        .await?;
    assert_batches_eq!(expected, &parquet);

    Ok(())
}

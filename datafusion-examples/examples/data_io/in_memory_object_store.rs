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
//! https://docs.rs/object_store/latest/object_store/memory/struct.InMemory.html

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
use datafusion::common::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

/// Demonstrates reading CSV data from an in-memory object store.
///
/// The same pattern applies to JSON/Parquet: register a store for a URL
/// prefix, write bytes into the store, then read via that URL.
pub async fn in_memory_object_store() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ctx = SessionContext::new();
    let object_store_url = ObjectStoreUrl::parse("memory://")?;
    // Register a URL prefix to route reads through this object store.
    ctx.register_object_store(object_store_url.as_ref(), Arc::clone(&store));

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    println!("=== CSV from memory ===");
    let csv_path = Path::from("/people.csv");
    let csv_data = b"id,name\n1,Alice\n2,Bob\n";
    // Write bytes into the in-memory object store.
    store
        .put(&csv_path, PutPayload::from_static(csv_data))
        .await?;
    // Read using the URL that matches the registered prefix.
    let csv = ctx
        .read_csv(
            "memory:///people.csv",
            CsvReadOptions::new().schema(&schema),
        )
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

    Ok(())
}

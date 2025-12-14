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

use std::path::PathBuf;

use arrow::array::{AsArray, PrimitiveArray};
use arrow::datatypes::{Float64Type, Int32Type};
use datafusion::common::assert_batches_eq;
use datafusion::error::Result;
use datafusion::prelude::*;
use futures::StreamExt;

/// This example shows how to convert query results into Rust structs by using
/// the Arrow APIs to convert the results into Rust native types.
///
/// This is a bit tricky initially as the results are returned as columns stored
/// as [ArrayRef]
///
/// [ArrayRef]: arrow::array::ArrayRef
pub async fn deserialize_to_struct() -> Result<()> {
    // Run a query that returns two columns of data
    let ctx = SessionContext::new();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("parquet")
        .join("alltypes_plain.parquet");

    ctx.register_parquet(
        "alltypes_plain",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx
        .sql("SELECT int_col, double_col FROM alltypes_plain")
        .await?;

    // print out the results showing we have an int32 and a float64 column
    let results = df.clone().collect().await?;
    assert_batches_eq!(
        [
            "+---------+------------+",
            "| int_col | double_col |",
            "+---------+------------+",
            "| 0       | 0.0        |",
            "| 1       | 10.1       |",
            "| 0       | 0.0        |",
            "| 1       | 10.1       |",
            "| 0       | 0.0        |",
            "| 1       | 10.1       |",
            "| 0       | 0.0        |",
            "| 1       | 10.1       |",
            "+---------+------------+",
        ],
        &results
    );

    // We will now convert the query results into a Rust struct
    let mut stream = df.execute_stream().await?;
    let mut list = vec![];

    // DataFusion produces data in chunks called `RecordBatch`es which are
    // typically 8000 rows each. This loop processes each `RecordBatch` as it is
    // produced by the query plan and adds it to the list
    while let Some(b) = stream.next().await.transpose()? {
        // Each `RecordBatch` has one or more columns. Each column is stored as
        // an `ArrayRef`. To interact with data using Rust native types we need to
        // convert these `ArrayRef`s into concrete array types using APIs from
        // the arrow crate.

        // In this case, we know that each batch has two columns of the  Arrow
        // types Int32 and Float64, so first we cast the two columns to the
        // appropriate Arrow PrimitiveArray (this is a fast / zero-copy cast).:
        let int_col: &PrimitiveArray<Int32Type> = b.column(0).as_primitive();
        let float_col: &PrimitiveArray<Float64Type> = b.column(1).as_primitive();

        // With PrimitiveArrays, we can access to the values as native Rust
        // types i32 and f64, and forming the desired `Data` structs
        for (i, f) in int_col.values().iter().zip(float_col.values()) {
            list.push(Data {
                int_col: *i,
                double_col: *f,
            })
        }
    }

    // Finally, we have the results in the list of Rust structs
    let res = format!("{list:#?}");
    assert_eq!(
        res,
        r#"[
    Data {
        int_col: 0,
        double_col: 0.0,
    },
    Data {
        int_col: 1,
        double_col: 10.1,
    },
    Data {
        int_col: 0,
        double_col: 0.0,
    },
    Data {
        int_col: 1,
        double_col: 10.1,
    },
    Data {
        int_col: 0,
        double_col: 0.0,
    },
    Data {
        int_col: 1,
        double_col: 10.1,
    },
    Data {
        int_col: 0,
        double_col: 0.0,
    },
    Data {
        int_col: 1,
        double_col: 10.1,
    },
]"#
    );

    // Use the fields in the struct to avoid clippy complaints
    let int_sum = list.iter().fold(0, |acc, x| acc + x.int_col);
    let double_sum = list.iter().fold(0.0, |acc, x| acc + x.double_col);
    assert_eq!(int_sum, 4);
    assert_eq!(double_sum, 40.4);

    Ok(())
}

/// This is target struct where we want the query results.
#[derive(Debug)]
struct Data {
    int_col: i32,
    double_col: f64,
}

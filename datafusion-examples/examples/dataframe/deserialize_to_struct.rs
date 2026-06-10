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

use arrow::array::{Array, Float64Array, StringViewArray};
use datafusion::common::assert_batches_eq;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_examples::utils::{datasets::ExampleDataset, write_csv_to_parquet};
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

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    ctx.register_parquet(
        "cars",
        parquet_temp.path_str()?,
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx
        .sql("SELECT car, speed FROM cars ORDER BY speed LIMIT 50")
        .await?;

    // print out the results showing we have car and speed columns and a deterministic ordering
    let results = df.clone().collect().await?;
    assert_batches_eq!(
        [
            "+-------+-------+",
            "| car   | speed |",
            "+-------+-------+",
            "| red   | 0.0   |",
            "| red   | 1.0   |",
            "| green | 2.0   |",
            "| red   | 3.0   |",
            "| red   | 7.0   |",
            "| red   | 7.1   |",
            "| red   | 7.2   |",
            "| green | 8.0   |",
            "| green | 10.0  |",
            "| green | 10.3  |",
            "| green | 10.4  |",
            "| green | 10.5  |",
            "| green | 11.0  |",
            "| green | 12.0  |",
            "| green | 14.0  |",
            "| green | 15.0  |",
            "| green | 15.1  |",
            "| green | 15.2  |",
            "| red   | 17.0  |",
            "| red   | 18.0  |",
            "| red   | 19.0  |",
            "| red   | 20.0  |",
            "| red   | 20.3  |",
            "| red   | 21.4  |",
            "| red   | 21.5  |",
            "+-------+-------+",
        ],
        &results
    );

    // We will now convert the query results into a Rust struct
    let mut stream = df.execute_stream().await?;
    let mut list: Vec<Data> = vec![];

    // DataFusion produces data in chunks called `RecordBatch`es which are
    // typically 8000 rows each. This loop processes each `RecordBatch` as it is
    // produced by the query plan and adds it to the list
    while let Some(batch) = stream.next().await.transpose()? {
        // Each `RecordBatch` has one or more columns. Each column is stored as
        // an `ArrayRef`. To interact with data using Rust native types we need to
        // convert these `ArrayRef`s into concrete array types using APIs from
        // the arrow crate.

        // In this case, we know that each batch has two columns of the  Arrow
        // types StringView and Float64, so first we cast the two columns to the
        // appropriate Arrow PrimitiveArray (this is a fast / zero-copy cast).:
        let car_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("car column must be Utf8View");

        let speed_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("speed column must be Float64");

        // With PrimitiveArrays, we can access to the values as native Rust
        // types String and f64, and forming the desired `Data` structs
        for i in 0..batch.num_rows() {
            let car = if car_col.is_null(i) {
                None
            } else {
                Some(car_col.value(i).to_string())
            };

            let speed = if speed_col.is_null(i) {
                None
            } else {
                Some(speed_col.value(i))
            };

            list.push(Data { car, speed });
        }
    }

    // Finally, we have the results in the list of Rust structs
    let res = format!("{list:#?}");
    assert_eq!(
        res,
        r#"[
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            0.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            1.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            2.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            3.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            7.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            7.1,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            7.2,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            8.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            10.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            10.3,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            10.4,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            10.5,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            11.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            12.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            14.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            15.0,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            15.1,
        ),
    },
    Data {
        car: Some(
            "green",
        ),
        speed: Some(
            15.2,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            17.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            18.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            19.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            20.0,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            20.3,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            21.4,
        ),
    },
    Data {
        car: Some(
            "red",
        ),
        speed: Some(
            21.5,
        ),
    },
]"#
    );

    let speed_green_sum: f64 = list
        .iter()
        .filter(|data| data.car.as_deref() == Some("green"))
        .filter_map(|data| data.speed)
        .sum();
    let speed_red_sum: f64 = list
        .iter()
        .filter(|data| data.car.as_deref() == Some("red"))
        .filter_map(|data| data.speed)
        .sum();
    assert_eq!(speed_green_sum, 133.5);
    assert_eq!(speed_red_sum, 162.5);

    Ok(())
}

/// This is target struct where we want the query results.
#[derive(Debug)]
struct Data {
    car: Option<String>,
    speed: Option<f64>,
}

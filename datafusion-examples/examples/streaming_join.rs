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

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::{any::Any, sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, AsArray, Float64Array},
    datatypes::Float64Type,
};
//use arrow::infer_arrow_schema_from_json_value;
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use datafusion::{
    config::ConfigOptions,
    dataframe::DataFrame,
    datasource::{continuous::KafkaSource, provider_as_source, TableProvider},
    execution::{
        context::{SessionContext, SessionState},
        RecordBatchStream,
    },
    franz_sinks::{FranzSink, PrettyPrinter},
    physical_plan::{
        display::DisplayableExecutionPlan,
        kafka_source::{KafkaStreamConfig, KafkaStreamRead, StreamEncoding},
        streaming::StreamingTableExec,
        time::TimestampUnit,
        ExecutionPlan,
    },
};
use datafusion_common::{
    franz_arrow::infer_arrow_schema_from_json_value, plan_err, ScalarValue,
};
use datafusion_expr::{
    col, count, create_udwf, ident, max, min, Expr, LogicalPlanBuilder,
    PartitionEvaluator, TableType, Volatility, WindowFrame,
};

use datafusion::execution::SendableRecordBatchStream;

use datafusion::{dataframe::DataFrameWriteOptions, prelude::*};
use datafusion_common::config::CsvOptions;
use datafusion_common::Result;
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};

use futures::StreamExt;
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_log::LogTracer::init().expect("Failed to set up log tracer");

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let sample_event = r#"
    {
        "trip_id": 1,
        "action": "trip_started",
        "timestamp": 1718655296
    }"#;

    // register the window function with DataFusion so we can call it
    let sample_value: serde_json::Value = serde_json::from_str(sample_event).unwrap();
    let inferred_schema = infer_arrow_schema_from_json_value(&sample_value).unwrap();
    let mut fields = inferred_schema.fields().to_vec();

    // Add a new column to the dataset that should mirror the occured_at_ms field
    fields.insert(
        fields.len(),
        Arc::new(Field::new(
            String::from("franz_canonical_timestamp"),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )),
    );

    let bootstrap_servers =
        String::from("localhost:19092,localhost:29092,localhost:39092");
    let canonical_schema = Arc::new(Schema::new(fields));
    let _config = KafkaStreamConfig {
        bootstrap_servers: bootstrap_servers.clone(),
        topic: String::from("trip_actions"),
        consumer_group_id: String::from("trip_actions_reader"),
        original_schema: Arc::new(inferred_schema.clone()),
        schema: canonical_schema.clone(),
        batch_size: 10,
        encoding: StreamEncoding::Json,
        order: vec![],
        partitions: 1_i32,
        timestamp_column: String::from("timestamp"),
        timestamp_unit: TimestampUnit::Int64Seconds,
        offset_reset: String::from("earliest"),
    };

    // Create a new streaming table
    let trip_stream: KafkaSource = KafkaSource(Arc::new(_config));

    let _config = KafkaStreamConfig {
        bootstrap_servers: bootstrap_servers.clone(),
        topic: String::from("driver_actions"),
        consumer_group_id: String::from("driver_actions_reader"),
        original_schema: Arc::new(inferred_schema),
        schema: canonical_schema,
        batch_size: 10,
        encoding: StreamEncoding::Json,
        order: vec![],
        partitions: 1_i32,
        timestamp_column: String::from("timestamp"),
        timestamp_unit: TimestampUnit::Int64Seconds,
        offset_reset: String::from("earliest"),
    };

    // Create a new streaming table
    let driver_stream: KafkaSource = KafkaSource(Arc::new(_config));
    let mut config = ConfigOptions::default();
    let _ = config.set("datafusion.execution.batch_size", "32");

    // Create the context object with a source from kafka
    let ctx = SessionContext::new_with_config(config.into());
    let _ = ctx.register_table("trip_actions", Arc::new(trip_stream));
    let _ = ctx.register_table("driver_actions", Arc::new(driver_stream));

    let df = ctx.sql("SELECT *
                FROM trip_actions 
                JOIN driver_actions ON trip_actions.trip_id = driver_actions.trip_id 
                WHERE trip_actions.franz_canonical_timestamp >= driver_actions.franz_canonical_timestamp - INTERVAL '5 SECONDS' 
                AND trip_actions.franz_canonical_timestamp <= driver_actions.franz_canonical_timestamp + INTERVAL '5 SECONDS'",).await.unwrap();

    //let plan = df.create_physical_plan().await.unwrap();
    println!("{}", df.logical_plan().display());

    let writer = PrettyPrinter::new().unwrap();
    let sink = Box::new(writer) as Box<dyn FranzSink>;
    let _ = df.sink(sink).await;
}

async fn print_stream(windowed_df: &DataFrame) {
    let mut stream: std::pin::Pin<Box<dyn RecordBatchStream + Send>> =
        windowed_df.clone().execute_stream().await.unwrap();

    // for _ in 1..100 {
    loop {
        let rb = stream.next().await.transpose();
        // println!("{:?}", rb);
        if let Ok(Some(batch)) = rb {
            if batch.num_rows() > 0 {
                println!(
                    "{}",
                    arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
                );
            }
        }
    }
}

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
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::{
    config::ConfigOptions,
    dataframe::DataFrame,
    datasource::{continuous::KafkaSource, provider_as_source, TableProvider},
    execution::{
        context::{SessionContext, SessionState},
        RecordBatchStream,
    },
    franz_sinks::{FileSink, FranzSink, PrettyPrinter},
    physical_plan::{
        display::DisplayableExecutionPlan,
        kafka_source::{KafkaStreamConfig, KafkaStreamRead, StreamEncoding},
        streaming::StreamingTableExec,
        time::TimestampUnit,
        ExecutionPlan,
    },
};
use datafusion::{dataframe::DataFrameWriteOptions, prelude::*};
use datafusion_common::{
    franz_arrow::infer_arrow_schema_from_json_value, plan_err, JoinType, ScalarValue,
};
use datafusion_expr::{
    col, create_udwf, ident, lit, max, min, Expr, LogicalPlanBuilder, PartitionEvaluator,
    TableType, Volatility, WindowFrame,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_aggregate::count::count;
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};

use futures::StreamExt;
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_log::LogTracer::init().expect("Failed to set up log tracer");

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let bootstrap_servers =
        String::from("localhost:19092,localhost:29092,localhost:39092");

    // Configure Kafka source for IMU data
    let imu_stream = create_kafka_source(
        &r#"{
            "driver_id": "690c119e-63c9-479b-b822-872ee7d89165",
            "occurred_at_ms": 1715201766763,
            "imu_measurement": {
                "timestamp": "2024-05-08T20:56:06.763260Z",
                "accelerometer": {
                    "x": 1.4187794,
                    "y": -0.13967037,
                    "z": 0.5483732
                },
                "gyroscope": {
                    "x": 0.005840948,
                    "y": 0.0035944171,
                    "z": 0.0041645765
                },
                "gps": {
                    "latitude": 72.3492587464122,
                    "longitude": 144.85596244550095,
                    "altitude": 2.9088259,
                    "speed": 57.96137
                }
            },
            "meta": {
                "nonsense": "MMMMMMMMMM"
            }
        }"#
        .to_string(),
        bootstrap_servers.clone(),
        "driver-imu-data".to_string(),
        "kafka_rideshare".to_string(),
    );

    // Configure Kafka source for Trip data
    let trip_stream = create_kafka_source(
        &r#"{
            "event_name": "TRIP_START",
            "trip_id": "b005922a-4ba5-4678-b0e6-bcb5ca2abe3e",
            "driver_id": "788fb395-96d0-4bc8-8ed9-bcf4e11e7543",
            "occurred_at_ms": 1718752555452,
            "meta": {
                "nonsense": "MMMMMMMMMM"
            }
        }"#
        .to_string(),
        bootstrap_servers.clone(),
        "trips".to_string(),
        "kafka_rideshare".to_string(),
    );

    let mut config = ConfigOptions::default();
    let _ = config.set("datafusion.execution.batch_size", "32");

    // Create the context object with a source from kafka
    let ctx = SessionContext::new_with_config(config.into());

    let imu_stream_plan = LogicalPlanBuilder::scan_with_filters(
        "imu_data",
        provider_as_source(Arc::new(imu_stream)),
        None,
        vec![],
    )
    .unwrap()
    .build()
    .unwrap();

    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "trips",
        provider_as_source(Arc::new(trip_stream)),
        None,
        vec![],
    )
    .unwrap()
    .join_on(
        imu_stream_plan,
        JoinType::Left,
        vec![col("trips.driver_id").eq(col("imu_data.driver_id"))],
    )
    .unwrap()
    .build()
    .unwrap();

    let df = DataFrame::new(ctx.state(), logical_plan);
    let windowed_df = df
        .clone()
        .select(vec![
            col("trips.trip_id"),
            col("trips.driver_id"),
            col("trips.event_name"),
            col("imu_measurement").field("gps").field("speed"),
        ])
        .unwrap();

    let writer = PrettyPrinter::new().unwrap();
    let sink = Box::new(writer) as Box<dyn FranzSink>;
    let _ = windowed_df.sink(sink).await;
}

fn create_kafka_source(
    sample_event: &String,
    bootstrap_servers: String,
    topic: String,
    consumer_group_id: String,
) -> KafkaSource {
    // register the window function with DataFusion so we can call it
    let sample_value: serde_json::Value = serde_json::from_str(sample_event).unwrap();
    let inferred_schema = infer_arrow_schema_from_json_value(&sample_value).unwrap();
    let mut fields = inferred_schema.fields().to_vec();

    // Add a new column to the dataset that should mirror the occurred_at_ms field
    fields.insert(
        fields.len(),
        Arc::new(Field::new(
            String::from("franz_canonical_timestamp"),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )),
    );

    let canonical_schema = Arc::new(Schema::new(fields));
    let _config = KafkaStreamConfig {
        bootstrap_servers,
        topic,
        consumer_group_id,
        original_schema: Arc::new(inferred_schema.clone()),
        schema: canonical_schema.clone(),
        batch_size: 10,
        encoding: StreamEncoding::Json,
        order: vec![],
        partitions: 1_i32,
        timestamp_column: String::from("occurred_at_ms"),
        timestamp_unit: TimestampUnit::Int64Millis,
        offset_reset: String::from("latest"),
    };

    // Create a new streaming table
    KafkaSource(Arc::new(_config))
}

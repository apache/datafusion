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

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::{
    config::ConfigOptions,
    dataframe::DataFrame,
    datasource::{continuous::KafkaSource, provider_as_source},
    execution::context::SessionContext,
    physical_plan::{
        kafka_source::{KafkaStreamConfig, StreamEncoding},
        time::TimestampUnit,
    },
};
use datafusion_common::franz_arrow::infer_arrow_schema_from_json_value;
use datafusion_expr::{col, max, min, LogicalPlanBuilder};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_aggregate::count::count;
use std::{sync::Arc, time::Duration};
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

    let sample_event = r#"
        {
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
        }"#;

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

    let bootstrap_servers =
        String::from("localhost:19092,localhost:29092,localhost:39092");
    let canonical_schema = Arc::new(Schema::new(fields));
    let _config = KafkaStreamConfig {
        bootstrap_servers: bootstrap_servers.clone(),
        topic: String::from("driver-imu-data"),
        consumer_group_id: String::from("kafka_rideshare"),
        original_schema: Arc::new(inferred_schema),
        schema: canonical_schema,
        batch_size: 10,
        encoding: StreamEncoding::Json,
        order: vec![],
        partitions: 1_i32,
        timestamp_column: String::from("occurred_at_ms"),
        timestamp_unit: TimestampUnit::Int64Millis,
        offset_reset: String::from("earliest"),
    };

    // Create a new streaming table
    let kafka_source = KafkaSource(Arc::new(_config));
    let mut config = ConfigOptions::default();
    let _ = config.set("datafusion.execution.batch_size", "32");

    // Create the context object with a source from kafka
    let ctx = SessionContext::new_with_config(config.into());

    // create logical plan composed of a single TableScan
    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "kafka_imu_data",
        provider_as_source(Arc::new(kafka_source)),
        None,
        vec![],
    )
    .unwrap()
    .build()
    .unwrap();

    let df = DataFrame::new(ctx.state(), logical_plan);
    let windowed_df = df
        .clone()
        .streaming_window(
            vec![],
            vec![
                max(col("imu_measurement").field("gps").field("speed")),
                min(col("imu_measurement").field("gps").field("altitude")),
                count(col("imu_measurement")).alias("count"),
            ],
            Duration::from_millis(5_000), // 5 second window
            Some(Duration::from_millis(1_000)), // 1 second slide
        )
        .unwrap();

    use datafusion::franz_sinks::{
        FileSink, FranzSink, KafkaSink, KafkaSinkSettings, PrettyPrinter, StdoutSink,
    };

    use datafusion_franz::{RocksDBBackend, StreamMonitor, StreamMonitorConfig};

    // let fname = "/tmp/out.jsonl";
    // println!("Writing results to file {}", fname);
    // let writer = FileSink::new(fname).unwrap();
    // let file_writer = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(file_writer).await;

    // let writer = StdoutSink::new().unwrap();
    // let sink = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(sink).await;

    //// Write pretty output to the terminal
    let writer = PrettyPrinter::new().unwrap();
    let sink = Box::new(writer) as Box<dyn FranzSink>;
    let _ = windowed_df.sink(sink).await;

    //// Write Messages to Kafka topic
    // let config = KafkaSinkSettings {
    //     topic: "out_topic".to_string(),
    //     bootstrap_servers: bootstrap_servers.clone(),
    // };
    // let writer = KafkaSink::new(&config).unwrap();
    // let sink = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(sink).await;

    // let kafka_sink_config = KafkaSinkSettings {
    //     topic: "out_topic_monitored".to_string(),
    //     bootstrap_servers: bootstrap_servers.clone(),
    // };
    // let kafka_writer = KafkaSink::new(&kafka_sink_config).unwrap();
    // let rocksdb_backend = RocksDBBackend::new("./state_store.rocksdb").unwrap();
    //
    // let stream_monitor_config = StreamMonitorConfig::new();
    // let stream_monitor = StreamMonitor::new(
    //     &stream_monitor_config,
    //     Arc::new(tokio::sync::Mutex::new(kafka_writer)),
    //     Arc::new(tokio::sync::Mutex::new(rocksdb_backend)),
    // )
    // .await
    // .unwrap();
    // stream_monitor.start_server().await;
    // let sink = Box::new(stream_monitor) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(sink).await;
}

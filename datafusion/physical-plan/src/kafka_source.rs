use std::collections::HashMap;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::TimestampMillisecondType;
use arrow_array::{Array, PrimitiveArray, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion_common::franz_arrow::json_records_to_arrow_record_batch;
use tracing::{debug, error, info, instrument};
//use datafusion::datasource::TableProvider;
//use datafusion_execution::context::SessionState;
use futures::StreamExt;
use serde_json::Value;

use crate::stream::RecordBatchReceiverStreamBuilder;
use crate::streaming::PartitionStream;
use arrow::compute::{max, min};
use datafusion_common::{plan_err, DataFusionError};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Expr;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Timestamp, TopicPartitionList};
// Import min_max function

use crate::time::{array_to_timestamp_array, TimestampUnit};

/// The data encoding for [`StreamTable`]
#[derive(Debug, Clone)]
pub enum StreamEncoding {
    Avro,
    Json,
}

impl FromStr for StreamEncoding {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "json" => Ok(Self::Json),
            _ => plan_err!("Unrecognised StreamEncoding {}", s),
        }
    }
}

/// The configuration for a [`StreamTable`]
#[derive(Debug)]
pub struct KafkaStreamConfig {
    pub original_schema: SchemaRef,
    pub schema: SchemaRef,
    pub topic: String,
    pub batch_size: usize,
    pub encoding: StreamEncoding,
    pub order: Vec<Vec<Expr>>,
    pub partitions: i32,
    pub timestamp_column: String,
    pub timestamp_unit: TimestampUnit,
    //constraints: Constraints,
}

impl KafkaStreamConfig {
    /// Specify the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Specify an encoding for the stream
    pub fn with_encoding(mut self, encoding: StreamEncoding) -> Self {
        self.encoding = encoding;
        self
    }
}

pub struct KafkaStreamRead {
    pub config: Arc<KafkaStreamConfig>,
    pub assigned_partitions: Vec<i32>,
}

impl PartitionStream for KafkaStreamRead {
    fn schema(&self) -> &SchemaRef {
        &self.config.schema
    }

    #[instrument(name = "KafkaStreamRead::execute", skip(self, _ctx))]
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut assigned_partitions = TopicPartitionList::new();
        for partition in self.assigned_partitions.clone() {
            assigned_partitions.add_partition(self.config.topic.as_str(), partition);
        }
        info!("Reading partition {:?}", assigned_partitions);
        // Create Kafka consumer with rebalance callback
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "my_consumer_group") // Replace with your group ID
            .set("bootstrap.servers", "localhost:9092") // Replace with your Kafka bootstrap servers
            .set("enable.auto.commit", "false") // Disable auto-commit for manual offset control
            .create()
            .expect("Consumer creation failed");

        // Subscribe to the topic
        consumer
            .assign(&assigned_partitions)
            .expect("Partition assignment failed.");

        //let schema = self.config.schema.clone();

        let mut builder =
            RecordBatchReceiverStreamBuilder::new(self.config.schema.clone(), 1);
        let tx = builder.tx();
        let canonical_schema = self.config.schema.clone();
        let json_schema = self.config.original_schema.clone();
        let timestamp_column: String = self.config.timestamp_column.clone();
        let timestamp_unit = self.config.timestamp_unit.clone();

        let _ = builder.spawn(async move {
            // should this be blocking?
            loop {
                let batch: Vec<serde_json::Value> = consumer
                    .stream()
                    .take_until(tokio::time::sleep(Duration::from_secs(1)))
                    .map(|message| match message {
                        Ok(m) => {
                            let timestamp = match m.timestamp() {
                                Timestamp::NotAvailable => -1_i64,
                                Timestamp::CreateTime(ts) => ts,
                                Timestamp::LogAppendTime(ts) => ts,
                            };
                            let key = m.key();

                            let payload = m.payload().expect("Message payload is empty");
                            let mut deserialized_record: HashMap<String, Value> =
                                serde_json::from_slice(payload).unwrap();
                            deserialized_record.insert(
                                "kafka_timestamp".to_string(),
                                Value::from(timestamp),
                            );
                            if let Some(key) = key {
                                deserialized_record.insert(
                                    "kafka_key".to_string(),
                                    Value::from(String::from_utf8_lossy(key)),
                                );
                            } else {
                                deserialized_record.insert(
                                    "kafka_key".to_string(),
                                    Value::from(String::from("")),
                                );
                            }
                            let new_payload =
                                serde_json::to_value(deserialized_record).unwrap();
                            new_payload
                        }
                        Err(err) => {
                            error!("Error reading from Kafka {:?}", err);
                            panic!("Error reading from Kafka {:?}", err)
                        }
                    })
                    .collect()
                    .await;

                let record_batch: RecordBatch =
                    json_records_to_arrow_record_batch(batch, json_schema.clone());

                let ts_column = record_batch
                    .column_by_name(timestamp_column.as_str())
                    .map(|ts_col| {
                        Arc::new(array_to_timestamp_array(ts_col, timestamp_unit.clone()))
                    })
                    .unwrap();

                let ts_array = ts_column
                    .as_any()
                    .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                    .unwrap();

                let max_timestamp: Option<_> = max::<TimestampMillisecondType>(&ts_array);
                let min_timestamp: Option<_> = min::<TimestampMillisecondType>(&ts_array);
                debug!("min: {:?}, max: {:?}", min_timestamp, max_timestamp);
                let mut columns: Vec<Arc<dyn Array>> = record_batch.columns().to_vec();
                columns.push(ts_column);

                let timestamped_record_batch: RecordBatch =
                    RecordBatch::try_new(canonical_schema.clone(), columns).unwrap();
                let tx_result = tx.send(Ok(timestamped_record_batch)).await;
                match tx_result {
                    Ok(m) => println!("result ok {:?}", m),
                    Err(err) => println!("result err {:?}", err),
                }
            }
            Ok(())
        });
        builder.build()
    }
}

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

use async_trait::async_trait;
use std::time::Duration;

use crate::error::{DataFusionError, Result};
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

use super::sink::FranzSink;

#[derive(Debug, Clone)]
pub struct KafkaSinkSettings {
    pub bootstrap_servers: String,
    pub topic: String,
}

impl Default for KafkaSinkSettings {
    fn default() -> Self {
        KafkaSinkSettings {
            bootstrap_servers: "".to_string(),
            topic: "".to_string(),
        }
    }
}

impl KafkaSinkSettings {
    pub fn new() -> Self {
        KafkaSinkSettings {
            ..Default::default()
        }
    }
}

pub struct KafkaSink {
    config: KafkaSinkSettings,
    producer: FutureProducer,
}

impl KafkaSink {
    pub fn new(config: &KafkaSinkSettings) -> Result<Self> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", config.bootstrap_servers.as_str())
            .set("message.timeout.ms", "5000");

        let producer: FutureProducer =
            client_config.create().expect("Producer creation error");

        log::info!(
            "kafka connection established bootstrap_servers: {}, topic: {}",
            config.bootstrap_servers,
            config.topic
        );

        Ok(Self {
            config: config.clone(),
            producer,
        })
    }
}

#[async_trait]
impl FranzSink for KafkaSink {
    async fn write_records(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        let topic = self.config.topic.as_str();

        let buf = Vec::new();
        let mut writer = LineDelimitedWriter::new(buf);
        writer.write_batches(&vec![&batch]).unwrap();
        writer.finish().unwrap();
        let buf = writer.into_inner();

        let record = FutureRecord::<[u8], _>::to(topic).payload(&buf);
        // .key(key.as_str()),

        let _delivery_status = self
            .producer
            .send(record, Duration::from_secs(0))
            .await
            .expect("Message not delivered");

        Ok(())
    }
}

#[cfg(test)]
mod tests {}

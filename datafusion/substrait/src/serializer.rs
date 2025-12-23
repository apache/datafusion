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

use crate::logical_plan::producer;

use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::prelude::*;

use prost::Message;
use std::path::Path;
use substrait::proto::Plan;
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt},
};

/// Plans a sql and serializes the generated logical plan to bytes.
/// The bytes are then written into a file at `path`.
///
/// Returns an error if the file already exists.
pub async fn serialize(
    sql: &str,
    ctx: &SessionContext,
    path: impl AsRef<Path>,
) -> Result<()> {
    let protobuf_out = serialize_bytes(sql, ctx).await?;

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(&protobuf_out).await?;
    file.flush().await?;
    Ok(())
}

/// Plans a sql and serializes the generated logical plan to bytes.
pub async fn serialize_bytes(sql: &str, ctx: &SessionContext) -> Result<Vec<u8>> {
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = producer::to_substrait_plan(&plan, &ctx.state())?;

    let mut protobuf_out = Vec::<u8>::new();
    proto
        .encode(&mut protobuf_out)
        .map_err(|e| DataFusionError::Substrait(format!("Failed to encode plan: {e}")))?;
    Ok(protobuf_out)
}

/// Reads the file at `path` and deserializes a plan from the bytes.
pub async fn deserialize(path: impl AsRef<Path>) -> Result<Box<Plan>> {
    let mut protobuf_in = Vec::<u8>::new();

    let mut file = OpenOptions::new().read(true).open(path).await?;
    file.read_to_end(&mut protobuf_in).await?;

    deserialize_bytes(protobuf_in).await
}

/// Deserializes a plan from the bytes.
pub async fn deserialize_bytes(proto_bytes: Vec<u8>) -> Result<Box<Plan>> {
    Ok(Box::new(Message::decode(&*proto_bytes).map_err(|e| {
        DataFusionError::Substrait(format!("Failed to decode plan: {e}"))
    })?))
}

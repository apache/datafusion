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

//! `ExchangeCodec`: a `PhysicalExtensionCodec` that serializes/deserializes
//! [`ExchangeSinkExec`] and [`ExchangeSourceExec`] so a stage plan containing
//! them survives a `datafusion-proto` round-trip.
//!
//! The codec holds the shared [`InMemoryExchange`] and injects it on decode —
//! the exchange itself is not carried in the serialized bytes. This mirrors how
//! an executor knows its own transport endpoint independently of the plan it
//! receives (exactly as the old `ShuffleCodec` injected the shuffle dir).

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_common::not_impl_err;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use prost::Message;

use super::InMemoryExchange;
use super::sink::ExchangeSinkExec;
use super::source::ExchangeSourceExec;

const SINK_TAG: u8 = 0;
const SOURCE_TAG: u8 = 1;

/// `PhysicalExtensionCodec` for [`ExchangeSinkExec`] and [`ExchangeSourceExec`].
///
/// Holds the shared [`InMemoryExchange`] that gets injected into decoded
/// operators; the exchange itself is not part of the serialized bytes — it
/// mirrors an executor knowing its own transport endpoint independently of the
/// plan it receives.
pub struct ExchangeCodec {
    pub exchange: Arc<InMemoryExchange>,
}

impl std::fmt::Debug for ExchangeCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // The exchange holds live channels and is intentionally opaque here.
        f.debug_struct("ExchangeCodec").finish_non_exhaustive()
    }
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn read_u64(buf: &[u8], offset: &mut usize) -> Result<u64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataFusionError::Internal(
            "ExchangeCodec: buffer too short reading u64".to_string(),
        )
    })?;
    let bytes: [u8; 8] = buf
        .get(*offset..end)
        .ok_or_else(|| {
            DataFusionError::Internal(
                "ExchangeCodec: buffer too short reading u64".to_string(),
            )
        })?
        .try_into()
        .unwrap();
    *offset = end;
    Ok(u64::from_le_bytes(bytes))
}

fn write_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_u64(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

fn read_len_prefixed<'a>(buf: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let len = read_u64(buf, offset)? as usize;
    let end = offset.checked_add(len).ok_or_else(|| {
        DataFusionError::Internal(
            "ExchangeCodec: buffer too short reading length-prefixed bytes".to_string(),
        )
    })?;
    let bytes = buf.get(*offset..end).ok_or_else(|| {
        DataFusionError::Internal(
            "ExchangeCodec: buffer too short reading length-prefixed bytes".to_string(),
        )
    })?;
    *offset = end;
    Ok(bytes)
}

impl PhysicalExtensionCodec for ExchangeCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if let Some(sink) = node.downcast_ref::<ExchangeSinkExec>() {
            buf.push(SINK_TAG);
            write_u64(buf, sink.stage_id() as u64);
            let part_proto = serialize_partitioning(
                sink.output_partitioning_spec(),
                &DefaultPhysicalExtensionCodec {},
                proto_converter,
            )?;
            write_len_prefixed(buf, &part_proto.encode_to_vec());
            return Ok(());
        }

        if let Some(source) = node.downcast_ref::<ExchangeSourceExec>() {
            buf.push(SOURCE_TAG);
            write_u64(buf, source.from_stage_id() as u64);
            write_u64(buf, source.num_producer_tasks() as u64);
            write_u64(buf, source.output_partition_count() as u64);
            let schema_proto: protobuf::Schema = source.schema().as_ref().try_into()?;
            write_len_prefixed(buf, &schema_proto.encode_to_vec());
            return Ok(());
        }

        not_impl_err!("ExchangeCodec: unsupported plan node {}", node.name())
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (tag, rest) = buf.split_first().ok_or_else(|| {
            DataFusionError::Internal("ExchangeCodec: empty buffer".to_string())
        })?;

        match *tag {
            SINK_TAG => {
                let mut offset = 0usize;
                let stage_id = read_u64(rest, &mut offset)? as usize;
                let part_bytes = read_len_prefixed(rest, &mut offset)?;
                let part_proto =
                    protobuf::Partitioning::decode(part_bytes).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "ExchangeCodec: failed to decode Partitioning: {e}"
                        ))
                    })?;

                let input = inputs.first().cloned().ok_or_else(|| {
                    DataFusionError::Internal(
                        "ExchangeCodec: ExchangeSinkExec requires exactly one input"
                            .to_string(),
                    )
                })?;

                let default_codec = DefaultPhysicalExtensionCodec {};
                let decode_ctx = PhysicalPlanDecodeContext::new(ctx, &default_codec);
                let partitioning: Partitioning = parse_protobuf_partitioning(
                    Some(&part_proto),
                    &decode_ctx,
                    input.schema().as_ref(),
                    proto_converter,
                )?
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "ExchangeCodec: missing Partitioning".to_string(),
                    )
                })?;

                let sink = ExchangeSinkExec::try_new(
                    stage_id,
                    input,
                    partitioning,
                    self.exchange.clone(),
                )?;
                Ok(Arc::new(sink))
            }
            SOURCE_TAG => {
                let mut offset = 0usize;
                let from_stage_id = read_u64(rest, &mut offset)? as usize;
                let num_producer_tasks = read_u64(rest, &mut offset)? as usize;
                let output_partition_count = read_u64(rest, &mut offset)? as usize;
                let schema_bytes = read_len_prefixed(rest, &mut offset)?;
                let schema_proto =
                    protobuf::Schema::decode(schema_bytes).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "ExchangeCodec: failed to decode Schema: {e}"
                        ))
                    })?;
                let schema: datafusion::arrow::datatypes::Schema =
                    (&schema_proto).try_into()?;

                let source = ExchangeSourceExec::try_new(
                    from_stage_id,
                    Arc::new(schema),
                    num_producer_tasks,
                    output_partition_count,
                    self.exchange.clone(),
                )?;
                Ok(Arc::new(source))
            }
            other => not_impl_err!("ExchangeCodec: unknown node tag {other}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{Column, col};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;

    use super::{ExchangeCodec, read_len_prefixed, write_u64};
    use crate::exchange::{ExchangeSinkExec, ExchangeSourceExec, InMemoryExchange};
    use crate::serde::{decode_plan, encode_plan};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    #[tokio::test]
    async fn sink_round_trips_through_exchange_codec() {
        let schema = schema();
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let partitioning = Partitioning::Hash(vec![col("a", &schema).unwrap()], 3);

        let exchange = InMemoryExchange::new();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ExchangeSinkExec::try_new(1, empty, partitioning, exchange.clone()).unwrap(),
        );

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let codec = ExchangeCodec {
            exchange: exchange.clone(),
        };
        let bytes = encode_plan(&plan, &codec).unwrap();
        let decoded = decode_plan(&bytes, &task_ctx, &codec).unwrap();

        let sink = decoded
            .downcast_ref::<ExchangeSinkExec>()
            .expect("decoded plan should be an ExchangeSinkExec");
        assert_eq!(sink.stage_id(), 1);
        match sink.output_partitioning_spec() {
            Partitioning::Hash(exprs, n) => {
                assert_eq!(*n, 3);
                assert_eq!(exprs.len(), 1);
                let col = exprs[0]
                    .downcast_ref::<Column>()
                    .expect("hash partitioning expr should be a Column");
                assert_eq!(col.name(), "a");
            }
            other => panic!(
                "expected Partitioning::Hash([a], 3) after round-trip, got {other:?}"
            ),
        }
        assert!(
            sink.input().downcast_ref::<EmptyExec>().is_some(),
            "child should round-trip as EmptyExec"
        );
    }

    #[tokio::test]
    async fn source_round_trips_through_exchange_codec() {
        let schema = schema();
        let exchange = InMemoryExchange::new();

        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ExchangeSourceExec::try_new(1, schema.clone(), 2, 3, exchange.clone())
                .unwrap(),
        );

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let codec = ExchangeCodec {
            exchange: exchange.clone(),
        };
        let bytes = encode_plan(&plan, &codec).unwrap();
        let decoded = decode_plan(&bytes, &task_ctx, &codec).unwrap();

        let source = decoded
            .downcast_ref::<ExchangeSourceExec>()
            .expect("decoded plan should be an ExchangeSourceExec");
        assert_eq!(source.from_stage_id(), 1);
        assert_eq!(source.num_producer_tasks(), 2);
        assert_eq!(source.output_partition_count(), 3);
        assert_eq!(source.schema(), schema);
    }

    #[test]
    fn read_len_prefixed_rejects_malformed_length_without_panicking() {
        // Length prefix claims far more bytes than remain in the buffer (and
        // would overflow `offset + len` if computed unchecked). This must
        // return an `Err`, not panic, even in debug builds.
        let mut buf = Vec::new();
        write_u64(&mut buf, u64::MAX - 1);
        buf.extend_from_slice(&[1, 2, 3]);

        let mut offset = 0usize;
        let result = read_len_prefixed(&buf, &mut offset);
        assert!(
            result.is_err(),
            "expected Err for malformed length prefix, got {result:?}"
        );
    }
}

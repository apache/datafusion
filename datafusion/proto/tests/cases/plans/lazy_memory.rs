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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion_common::{Result, assert_contains, not_impl_err};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{self, PhysicalPlanNode};
use parking_lot::RwLock;
use prost::Message;

fn plan_node(node: protobuf::GenerateSeriesNode) -> PhysicalPlanNode {
    PhysicalPlanNode {
        physical_plan_type: Some(
            protobuf::physical_plan_node::PhysicalPlanType::GenerateSeries(node),
        ),
    }
}

fn int_node() -> Result<protobuf::GenerateSeriesNode> {
    let schema = Schema::new(vec![Field::new("v", DataType::Int64, false)]);
    Ok(protobuf::GenerateSeriesNode {
        schema: Some((&schema).try_into()?),
        target_batch_size: 2,
        args: Some(protobuf::generate_series_node::Args::Int64Args(
            protobuf::GenerateSeriesArgsInt64 {
                start: 3,
                end: 1,
                step: -1,
                include_end: true,
                name: protobuf::GenerateSeriesName::GsGenerateSeries as i32,
            },
        )),
    })
}

/// Construct the legacy wire messages independently of the generator hooks,
/// then check exact re-encoding and actual execution, including batch sizes.
#[tokio::test]
async fn lazy_memory_legacy_wire_roundtrip() -> Result<()> {
    use protobuf::generate_series_node::Args;
    for (name, include_end) in [
        (protobuf::GenerateSeriesName::GsGenerateSeries, true),
        (protobuf::GenerateSeriesName::GsRange, false),
    ] {
        let mut integer = int_node()?;
        let Some(Args::Int64Args(args)) = integer.args.as_mut() else {
            unreachable!()
        };
        args.name = name as i32;
        args.include_end = include_end;
        let mut empty = integer.clone();
        empty.target_batch_size = 8192;
        empty.args = Some(Args::ContainsNull(
            protobuf::GenerateSeriesArgsContainsNull { name: name as i32 },
        ));
        let expected = if include_end {
            vec![3, 2, 1]
        } else {
            vec![3, 2]
        };
        let mut cases = vec![(integer, expected.clone()), (empty, vec![])];
        for tz in [None, Some("+08:00")] {
            let schema = Schema::new(vec![Field::new(
                "v",
                DataType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)),
                false,
            )]);
            let step = Some(datafusion_proto_common::IntervalMonthDayNanoValue {
                months: 0,
                days: 0,
                nanos: -1,
            });
            let args = match tz {
                Some(tz) => Args::TimestampArgs(protobuf::GenerateSeriesArgsTimestamp {
                    start: 3,
                    end: 1,
                    step,
                    include_end,
                    name: name as i32,
                    tz: Some(tz.to_string()),
                }),
                None => Args::DateArgs(protobuf::GenerateSeriesArgsDate {
                    start: 3,
                    end: 1,
                    step,
                    include_end,
                    name: name as i32,
                }),
            };
            cases.push((
                protobuf::GenerateSeriesNode {
                    schema: Some((&schema).try_into()?),
                    target_batch_size: 2,
                    args: Some(args),
                },
                expected.clone(),
            ));
        }
        for (node, expected) in cases {
            let legacy = plan_node(node);
            let bytes = legacy.encode_to_vec();
            let decoded = PhysicalPlanNode::decode(bytes.as_slice()).unwrap();
            let ctx = Arc::new(TaskContext::default());
            let plan = decoded
                .try_into_physical_plan(&ctx, &DefaultPhysicalExtensionCodec {})?;
            let encoded = PhysicalPlanNode::try_from_physical_plan(
                Arc::clone(&plan),
                &DefaultPhysicalExtensionCodec {},
            )?;
            assert_eq!(encoded.encode_to_vec(), bytes);
            // Repeated execute calls must still produce fresh independent streams.
            for _ in 0..2 {
                let batches = collect(plan.execute(0, Arc::clone(&ctx))?).await?;
                let sizes: Vec<_> = batches.iter().map(RecordBatch::num_rows).collect();
                let expected_sizes: Vec<_> =
                    expected.chunks(2).map(<[i64]>::len).collect();
                assert_eq!(sizes, expected_sizes);
                let actual: Vec<i64> = batches
                    .iter()
                    .flat_map(|batch| {
                        let column = batch.column(0);
                        if let Some(array) = column.as_any().downcast_ref::<Int64Array>()
                        {
                            array.values().to_vec()
                        } else {
                            column
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap()
                                .values()
                                .to_vec()
                        }
                    })
                    .collect();
                assert_eq!(actual, expected);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct HookGenerator(protobuf::GenerateSeriesNode);

impl fmt::Display for HookGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HookGenerator")
    }
}

impl LazyBatchGenerator for HookGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(self.clone()))
    }
    fn try_to_proto(&self) -> Result<Option<protobuf::GenerateSeriesNode>> {
        Ok(Some(self.0.clone()))
    }
}

#[test]
fn lazy_memory_dispatches_to_generator_hook() -> Result<()> {
    let mut payload = int_node()?;
    // The plan, rather than the generator, supplies the authoritative schema.
    payload.schema = None;
    let generator = Arc::new(RwLock::new(HookGenerator(payload)));
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let plan = Arc::new(LazyMemoryExec::try_new(schema, vec![generator])?);
    let encoded = PhysicalPlanNode::try_from_physical_plan(
        plan,
        &DefaultPhysicalExtensionCodec {},
    )?;
    assert_eq!(encoded, plan_node(int_node()?));
    Ok(())
}

#[derive(Debug, Clone)]
struct OpaqueGenerator;

impl fmt::Display for OpaqueGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OpaqueGenerator")
    }
}

// Deliberately use the default hook to cover existing downstream generators.
impl LazyBatchGenerator for OpaqueGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(Self))
    }
}

#[derive(Debug)]
struct FallbackCodec;

impl PhysicalExtensionCodec for FallbackCodec {
    fn try_encode(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        assert!(plan.downcast_ref::<LazyMemoryExec>().is_some());
        buf.extend_from_slice(b"lazy-extension");
        Ok(())
    }
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("encoding-only test codec")
    }
}

#[test]
fn lazy_memory_preserves_extension_fallback() -> Result<()> {
    let opaque: Arc<RwLock<dyn LazyBatchGenerator>> =
        Arc::new(RwLock::new(OpaqueGenerator));
    let supported: Arc<RwLock<dyn LazyBatchGenerator>> =
        Arc::new(RwLock::new(HookGenerator(int_node()?)));
    for generators in [
        vec![],
        vec![opaque],
        vec![Arc::clone(&supported), supported],
    ] {
        let schema = Arc::new(Schema::empty());
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(LazyMemoryExec::try_new(schema, generators)?);
        assert!(
            PhysicalPlanNode::try_from_physical_plan(
                Arc::clone(&plan),
                &DefaultPhysicalExtensionCodec {},
            )
            .is_err()
        );
        let node = PhysicalPlanNode::try_from_physical_plan(plan, &FallbackCodec)?;
        let Some(protobuf::physical_plan_node::PhysicalPlanType::Extension(extension)) =
            node.physical_plan_type
        else {
            panic!("expected extension fallback")
        };
        assert_eq!(extension.node, b"lazy-extension");
    }
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn lazy_memory_preserves_encode_errors() -> Result<()> {
    use datafusion_functions_table::generate_series::{
        GenSeriesArgs, GenerateSeriesTable,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let table = GenerateSeriesTable::new(
        Arc::clone(&schema),
        GenSeriesArgs::Int64Args {
            start: 1,
            end: 3,
            step: 1,
            include_end: true,
            name: "generate_series",
        },
    );
    let generator = table.as_generator(usize::MAX)?;
    let plan = Arc::new(LazyMemoryExec::try_new(schema, vec![generator])?);
    // A real encode error must propagate even if an extension codec is available.
    let error =
        PhysicalPlanNode::try_from_physical_plan(plan, &FallbackCodec).unwrap_err();
    assert_contains!(error.to_string(), "target_batch_size");
    assert_contains!(error.to_string(), "out of range for the plan wire format");
    Ok(())
}

#[test]
fn lazy_memory_rejects_malformed_series_payloads() -> Result<()> {
    let mut missing_schema = int_node()?;
    missing_schema.schema = None;
    let mut missing_args = int_node()?;
    missing_args.args = None;
    let mut zero_batch_size = int_node()?;
    zero_batch_size.target_batch_size = 0;
    let mut missing_step = int_node()?;
    missing_step.args = Some(protobuf::generate_series_node::Args::TimestampArgs(
        protobuf::GenerateSeriesArgsTimestamp {
            step: None,
            ..Default::default()
        },
    ));
    for (node, expected) in [
        (missing_schema, "schema"),
        (missing_args, "Missing args"),
        (zero_batch_size, "target_batch_size must be greater than 0"),
        (missing_step, "Missing step in TimestampArgs"),
    ] {
        let error = plan_node(node)
            .try_into_physical_plan(
                &TaskContext::default(),
                &DefaultPhysicalExtensionCodec {},
            )
            .unwrap_err();
        assert_contains!(error.to_string(), expected);
    }
    Ok(())
}

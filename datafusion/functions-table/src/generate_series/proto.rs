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

//! The built-in generate-series wire format. Encoding is dispatched through
//! the generator hook; decoding stays with the types that own the payload.

use std::sync::Arc;

use arrow::datatypes::{IntervalMonthDayNanoType, Schema};
use datafusion_common::utils::usize_from_wire;
use datafusion_common::{Result, internal_datafusion_err, internal_err, plan_err};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::memory::LazyMemoryExec;
use datafusion_proto_models::protobuf;

use super::{GenSeriesArgs, GenerateSeriesTable};

pub(super) fn encode_name(name: &str) -> Result<protobuf::GenerateSeriesName> {
    match name {
        "generate_series" => Ok(protobuf::GenerateSeriesName::GsGenerateSeries),
        "range" => Ok(protobuf::GenerateSeriesName::GsRange),
        _ => internal_err!("unknown name: {name}"),
    }
}

fn decode_name(name: protobuf::GenerateSeriesName) -> &'static str {
    match name {
        protobuf::GenerateSeriesName::GsGenerateSeries => "generate_series",
        protobuf::GenerateSeriesName::GsRange => "range",
    }
}

/// Reconstruct a single-generator [`LazyMemoryExec`] from its built-in payload.
///
/// This is the fixed dispatch entry point used by `datafusion-proto`; no
/// registry or extension codec is required for the built-in series types.
pub fn try_from_proto(
    node: &protobuf::GenerateSeriesNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = node.schema.as_ref().ok_or_else(|| {
        internal_datafusion_err!("GenerateSeriesNode is missing required field 'schema'")
    })?;
    let schema = Arc::new(Schema::try_from(schema)?);
    let args = match &node.args {
        Some(protobuf::generate_series_node::Args::ContainsNull(args)) => {
            GenSeriesArgs::ContainsNull {
                name: decode_name(args.name()),
            }
        }
        Some(protobuf::generate_series_node::Args::Int64Args(args)) => {
            GenSeriesArgs::Int64Args {
                start: args.start,
                end: args.end,
                step: args.step,
                include_end: args.include_end,
                name: decode_name(args.name()),
            }
        }
        Some(protobuf::generate_series_node::Args::TimestampArgs(args)) => {
            let step = args.step.as_ref().ok_or_else(|| {
                internal_datafusion_err!("Missing step in TimestampArgs")
            })?;
            GenSeriesArgs::TimestampArgs {
                start: args.start,
                end: args.end,
                step: IntervalMonthDayNanoType::make_value(
                    step.months,
                    step.days,
                    step.nanos,
                ),
                tz: args.tz.as_ref().map(|s| Arc::from(s.as_str())),
                include_end: args.include_end,
                name: decode_name(args.name()),
            }
        }
        Some(protobuf::generate_series_node::Args::DateArgs(args)) => {
            let step = args
                .step
                .as_ref()
                .ok_or_else(|| internal_datafusion_err!("Missing step in DateArgs"))?;
            GenSeriesArgs::DateArgs {
                start: args.start,
                end: args.end,
                step: IntervalMonthDayNanoType::make_value(
                    step.months,
                    step.days,
                    step.nanos,
                ),
                include_end: args.include_end,
                name: decode_name(args.name()),
            }
        }
        None => return internal_err!("Missing args in GenerateSeriesNode"),
    };
    let target_batch_size = usize_from_wire(
        node.target_batch_size,
        "GenerateSeriesNode",
        "target_batch_size",
    )?;
    if target_batch_size == 0 {
        return plan_err!("GenerateSeriesNode: target_batch_size must be greater than 0");
    }
    let table = GenerateSeriesTable::new(Arc::clone(&schema), args);
    let generator = table.as_generator(target_batch_size)?;
    Ok(Arc::new(LazyMemoryExec::try_new(schema, vec![generator])?))
}

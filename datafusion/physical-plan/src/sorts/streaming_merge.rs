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

//! Merge that deals with an arbitrary size of streaming inputs.
//! This is an order-preserving merge.

use crate::metrics::BaselineMetrics;
use crate::sorts::{
    merge::SortPreservingMergeStream,
    stream::{FieldCursorStream, RowCursorStream},
};
use crate::{PhysicalSortExpr, SendableRecordBatchStream};
use arrow::datatypes::{DataType, SchemaRef};
use arrow_array::*;
use datafusion_common::{internal_err, Result};
use datafusion_execution::memory_pool::MemoryReservation;

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $sort:ident, $config:ident) => {{
        let streams = FieldCursorStream::<$t>::new($sort, $config.streams);
        return Ok(Box::pin(SortPreservingMergeStream::new(
            Box::new(streams),
            $config.schema,
            $config.metrics,
            $config.batch_size,
            $config.fetch,
            $config.reservation,
        )));
    }};
}

/// Configuration parameters to initialize a `SortPreservingMergeStream`
pub struct StreamingMergeConfig<'a> {
    pub streams: Vec<SendableRecordBatchStream>,
    pub schema: SchemaRef,
    pub expressions: &'a [PhysicalSortExpr],
    pub metrics: BaselineMetrics,
    pub batch_size: usize,
    pub fetch: Option<usize>,
    pub reservation: MemoryReservation,
}

/// Perform a streaming merge of [`SendableRecordBatchStream`] based on provided sort expressions
/// while preserving order.
pub fn streaming_merge(
    config: StreamingMergeConfig,
) -> Result<SendableRecordBatchStream> {
    // If there are no sort expressions, preserving the order
    // doesn't mean anything (and result in infinite loops)
    if config.expressions.is_empty() {
        return internal_err!("Sort expressions cannot be empty for streaming merge");
    }
    // Special case single column comparisons with optimized cursor implementations
    if config.expressions.len() == 1 {
        let sort = config.expressions[0].clone();
        let data_type = sort.expr.data_type(config.schema.as_ref())?;
        downcast_primitive! {
            data_type => (primitive_merge_helper, sort, config),
            DataType::Utf8 => merge_helper!(StringArray, sort, config)
            DataType::LargeUtf8 => merge_helper!(LargeStringArray, sort, config)
            DataType::Binary => merge_helper!(BinaryArray, sort, config)
            DataType::LargeBinary => merge_helper!(LargeBinaryArray, sort, config)
            _ => {}
        }
    }

    let streams = RowCursorStream::try_new(
        config.schema.as_ref(),
        config.expressions,
        config.streams,
        config.reservation.new_empty(),
    )?;

    Ok(Box::pin(SortPreservingMergeStream::new(
        Box::new(streams),
        config.schema,
        config.metrics,
        config.batch_size,
        config.fetch,
        config.reservation,
    )))
}

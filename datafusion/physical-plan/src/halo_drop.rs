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

//! Drop halo rows above a `BoundedWindowAggExec` running per-partition
//! over `RangeRepartitionExec`-routed input.
//!
//! Each partition reads its *intended primary range* from
//! `input.runtime_partition_extremes(partition)` — which `RangeRepartitionExec`
//! exposes as a "useful lie" — and filters rows whose leading sort key
//! falls outside that range. Halo rows (rows duplicated into this
//! partition for the window's frame context at boundaries) sit *outside*
//! the primary range by construction, so the filter drops them.
//!
//! Reads extremes lazily on the first batch, because
//! `RangeRepartitionExec`'s coordinator populates ranges before routing
//! any data — so any batch arriving at us implies ranges are ready.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::filter_record_batch;
use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::expressions::Column;

use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct HaloDropExec {
    input: Arc<dyn ExecutionPlan>,
    /// Column index of the leading sort key in the input schema. We
    /// resolve it at construction from a `LexOrdering`'s first key,
    /// which must be a `Column` (the same constraint `ParallelWindow`
    /// applies to candidate windows).
    sort_col: usize,
    cache: Arc<PlanProperties>,
}

impl HaloDropExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        ordering: &LexOrdering,
    ) -> Result<Self> {
        // PhysicalExpr: Any, so downcast_ref via Any works directly on
        // the Arc<dyn PhysicalExpr> through auto-deref.
        let sort_col = ordering
            .first()
            .expr
            .downcast_ref::<Column>()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "HaloDropExec: leading sort key must be a Column"
                )
            })?
            .index();
        let cache = Arc::clone(input.properties());
        Ok(Self {
            input,
            sort_col,
            cache,
        })
    }
}

impl DisplayAs for HaloDropExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "HaloDropExec")
    }
}

impl ExecutionPlan for HaloDropExec {
    fn name(&self) -> &'static str {
        "HaloDropExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.swap_remove(0);
        let cache = Arc::clone(input.properties());
        Ok(Arc::new(Self {
            input,
            sort_col: self.sort_col,
            cache,
        }))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let extremes_provider = Arc::clone(&self.input);
        let sort_col = self.sort_col;
        let stream = filter_stream(input, extremes_provider, partition, sort_col);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Drains `input`, lazy-initializing the `[min, max]` filter range on
/// the first batch by calling
/// `extremes_provider.runtime_partition_extremes(partition)`. Subsequent
/// batches reuse the cached range.
fn filter_stream(
    input: SendableRecordBatchStream,
    extremes_provider: Arc<dyn ExecutionPlan>,
    partition: usize,
    sort_col: usize,
) -> impl futures::Stream<Item = Result<RecordBatch>> + Send {
    struct St {
        input: SendableRecordBatchStream,
        range: Option<(i64, i64)>,
        provider: Arc<dyn ExecutionPlan>,
        partition: usize,
        sort_col: usize,
    }
    let st = St {
        input,
        range: None,
        provider: extremes_provider,
        partition,
        sort_col,
    };
    futures::stream::try_unfold(st, |mut st| async move {
        use futures::StreamExt;
        loop {
            let batch = match st.input.next().await {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(e),
                None => return Ok(None),
            };
            if st.range.is_none() {
                let extremes = st
                    .provider
                    .runtime_partition_extremes(st.partition)?
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "HaloDropExec: extremes unavailable on first batch \
                             — RangeRepartitionExec coordinator should have \
                             populated them before routing any rows"
                        )
                    })?;
                let lo = scalar_to_i64(extremes.min.first())?;
                let hi = scalar_to_i64(extremes.max.first())?;
                st.range = Some((lo, hi));
            }
            let (lo, hi) = st.range.unwrap();
            let filtered = filter_batch(&batch, st.sort_col, lo, hi)?;
            if filtered.num_rows() == 0 {
                continue; // skip empty filtered batches
            }
            return Ok(Some((filtered, st)));
        }
    })
}

fn scalar_to_i64(s: Option<&ScalarValue>) -> Result<i64> {
    match s {
        Some(ScalarValue::Int64(Some(v))) => Ok(*v),
        _ => Err(internal_datafusion_err!(
            "HaloDropExec: leading extreme must be non-null Int64"
        )),
    }
}

fn filter_batch(
    batch: &RecordBatch,
    sort_col: usize,
    lo: i64,
    hi: i64,
) -> Result<RecordBatch> {
    let col = batch
        .column(sort_col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            internal_datafusion_err!("HaloDropExec: leading sort column must be Int64")
        })?;
    let mask: BooleanArray = (0..col.len())
        .map(|i| {
            if col.is_null(i) {
                Some(false)
            } else {
                let v = col.value(i);
                Some(v >= lo && v <= hi)
            }
        })
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
}

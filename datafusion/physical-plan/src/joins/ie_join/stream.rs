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

use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use arrow::array::{UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow_ord::ord::{DynComparator, make_comparator};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{JoinSide, JoinType, NullEquality, Result};
use datafusion_expr::Operator;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};

use crate::RecordBatchStream;
use crate::joins::JoinOn;
use crate::joins::ie_join::algorithm::{ActiveBitmap, IEJoinData, RightGroup};
use crate::joins::ie_join::exec::IEJoinCondition;
use crate::joins::utils::{
    ColumnIndex, JoinFilter, apply_join_filter_to_indices, build_batch_from_indices,
    equal_rows_arr,
};
use crate::metrics::{Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time};

#[derive(Clone)]
pub(super) struct IEJoinMetrics {
    pub load_time: Time,
    pub join_time: Time,
    pub left_input_batches: Count,
    pub right_input_batches: Count,
    pub left_input_rows: Count,
    pub right_input_rows: Count,
    pub candidate_rows: Count,
    pub output_batches: Count,
    pub output_rows: Count,
    pub peak_mem_used: Gauge,
}

impl IEJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            load_time: MetricBuilder::new(metrics).subset_time("load_time", partition),
            join_time: MetricBuilder::new(metrics).subset_time("join_time", partition),
            left_input_batches: MetricBuilder::new(metrics)
                .counter("left_input_batches", partition),
            right_input_batches: MetricBuilder::new(metrics)
                .counter("right_input_batches", partition),
            left_input_rows: MetricBuilder::new(metrics)
                .counter("left_input_rows", partition),
            right_input_rows: MetricBuilder::new(metrics)
                .counter("right_input_rows", partition),
            candidate_rows: MetricBuilder::new(metrics)
                .counter("candidate_rows", partition),
            output_batches: MetricBuilder::new(metrics)
                .counter("output_batches", partition),
            output_rows: MetricBuilder::new(metrics).counter("output_rows", partition),
            peak_mem_used: MetricBuilder::new(metrics)
                .peak_memory_usage("peak_mem_used", partition),
        }
    }
}

pub(super) struct IEJoinStream {
    schema: SchemaRef,
    column_indices: Vec<ColumnIndex>,
    conditions: [IEJoinCondition; 2],
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    null_equality: NullEquality,
    batch_size: usize,
    state: StreamState,
    metrics: IEJoinMetrics,
}

enum StreamState {
    Loading(BoxFuture<'static, Result<IEJoinData>>),
    Joining(Box<JoinState>),
    Done,
}

struct CurrentLeft {
    row: u32,
    boundary: usize,
    scan_position: usize,
}

struct JoinState {
    data: IEJoinData,
    first_comparator: DynComparator,
    second_comparator: DynComparator,
    active: ActiveBitmap,
    active_memory: usize,
    current_hash: Option<u64>,
    current_group: Option<RightGroup>,
    right_group_index: usize,
    right_second_cursor: usize,
    left_position: usize,
    current_left: Option<CurrentLeft>,
}

impl IEJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        schema: SchemaRef,
        column_indices: Vec<ColumnIndex>,
        conditions: [IEJoinCondition; 2],
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        null_equality: NullEquality,
        batch_size: usize,
        load: BoxFuture<'static, Result<IEJoinData>>,
        metrics: IEJoinMetrics,
    ) -> Self {
        Self {
            schema,
            column_indices,
            conditions,
            on,
            filter,
            join_type,
            null_equality,
            batch_size: batch_size.max(1),
            state: StreamState::Loading(load),
            metrics,
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Loading(load) => {
                    let data = match ready!(load.poll_unpin(cx)) {
                        Ok(data) => data,
                        Err(error) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    match JoinState::try_new(data, &self.metrics.peak_mem_used) {
                        Ok(state) => self.state = StreamState::Joining(Box::new(state)),
                        Err(error) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
                StreamState::Joining(state) => {
                    let timer = self.metrics.join_time.timer();
                    let result = state.next_batch(
                        &self.schema,
                        &self.column_indices,
                        &self.conditions,
                        &self.on,
                        self.filter.as_ref(),
                        self.join_type,
                        self.null_equality,
                        self.batch_size,
                        &self.metrics,
                    );
                    timer.done();
                    match result {
                        Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Ok(None) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(None);
                        }
                        Err(error) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl Stream for IEJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for IEJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl JoinState {
    fn try_new(data: IEJoinData, peak_mem_used: &Gauge) -> Result<Self> {
        let first_comparator = make_comparator(
            data.left_keys[0].as_ref(),
            data.right_keys[0].as_ref(),
            SortOptions::default(),
        )?;
        let second_comparator = make_comparator(
            data.left_keys[1].as_ref(),
            data.right_keys[1].as_ref(),
            SortOptions::default(),
        )?;
        let max_group = data
            .right_groups
            .iter()
            .map(|group| group.first.len())
            .max()
            .unwrap_or(0);
        let active = ActiveBitmap::new(max_group);
        let active_memory = active.memory_size();
        data.reservation.try_grow(active_memory)?;
        peak_mem_used.set_max(data.reservation.size());

        Ok(Self {
            data,
            first_comparator,
            second_comparator,
            active,
            active_memory,
            current_hash: None,
            current_group: None,
            right_group_index: 0,
            right_second_cursor: 0,
            left_position: 0,
            current_left: None,
        })
    }

    #[expect(clippy::too_many_arguments)]
    fn next_batch(
        &mut self,
        schema: &SchemaRef,
        column_indices: &[ColumnIndex],
        conditions: &[IEJoinCondition; 2],
        on: &JoinOn,
        filter: Option<&JoinFilter>,
        join_type: JoinType,
        null_equality: NullEquality,
        batch_size: usize,
        metrics: &IEJoinMetrics,
    ) -> Result<Option<RecordBatch>> {
        loop {
            let mut left_indices = Vec::with_capacity(batch_size);
            let mut right_indices = Vec::with_capacity(batch_size);

            while left_indices.len() < batch_size {
                if self.current_left.is_none() && !self.start_next_left(conditions) {
                    break;
                }
                let current = self.current_left.as_mut().expect("set above");
                let Some(position) = self
                    .active
                    .next_set(current.scan_position, current.boundary)
                else {
                    self.current_left = None;
                    continue;
                };
                current.scan_position = position + 1;
                let group = self
                    .current_group
                    .as_ref()
                    .expect("group set with left row");
                let right_row = self.data.right_by_first[group.first.start + position];
                left_indices.push(current.row as u64);
                right_indices.push(right_row);
            }

            if left_indices.is_empty() {
                return Ok(None);
            }
            metrics.candidate_rows.add(left_indices.len());
            let mut left_indices = UInt64Array::from(left_indices);
            let mut right_indices = UInt32Array::from(right_indices);

            if !on.is_empty() {
                (left_indices, right_indices) = equal_rows_arr(
                    &left_indices,
                    &right_indices,
                    &self.data.left_equality_keys,
                    &self.data.right_equality_keys,
                    null_equality,
                )?;
            }
            if let Some(filter) = filter {
                (left_indices, right_indices) = apply_join_filter_to_indices(
                    &self.data.left_batch,
                    &self.data.right_batch,
                    left_indices,
                    right_indices,
                    filter,
                    JoinSide::Left,
                    Some(batch_size),
                    join_type,
                )?;
            }
            if left_indices.is_empty() {
                continue;
            }

            let batch = build_batch_from_indices(
                schema,
                &self.data.left_batch,
                &self.data.right_batch,
                &left_indices,
                &right_indices,
                column_indices,
                JoinSide::Left,
                join_type,
            )?;
            metrics.output_batches.add(1);
            metrics.output_rows.add(batch.num_rows());
            return Ok(Some(batch));
        }
    }

    fn start_next_left(&mut self, conditions: &[IEJoinCondition; 2]) -> bool {
        while self.left_position < self.data.left_by_second.len() {
            let left_row = self.data.left_by_second[self.left_position];
            self.left_position += 1;
            let hash = self
                .data
                .left_hashes
                .as_ref()
                .map_or(0, |hashes| hashes.value(left_row as usize));
            while self.right_group_index < self.data.right_groups.len()
                && self.data.right_groups[self.right_group_index].hash < hash
            {
                self.right_group_index += 1;
            }
            let Some(group) = self
                .data
                .right_groups
                .get(self.right_group_index)
                .filter(|group| group.hash == hash)
                .cloned()
            else {
                continue;
            };

            if self.current_hash != Some(hash) {
                self.current_hash = Some(hash);
                self.active.resize_and_clear(group.first.len());
                self.right_second_cursor = group.second.start;
            }

            while self.right_second_cursor < group.second.end {
                let right_row =
                    self.data.right_by_second[self.right_second_cursor] as usize;
                if !matches_operator(
                    conditions[1].operator(),
                    (self.second_comparator)(left_row as usize, right_row),
                ) {
                    break;
                }
                let global_position = self.data.right_first_position[right_row] as usize;
                self.active.insert(global_position - group.first.start);
                self.right_second_cursor += 1;
            }

            let mut low = group.first.start;
            let mut high = group.first.end;
            while low < high {
                let middle = low + (high - low) / 2;
                let right_row = self.data.right_by_first[middle] as usize;
                if matches_operator(
                    conditions[0].operator(),
                    (self.first_comparator)(left_row as usize, right_row),
                ) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            let boundary = low - group.first.start;
            self.current_group = Some(group);
            self.current_left = Some(CurrentLeft {
                row: left_row,
                boundary,
                scan_position: 0,
            });
            return true;
        }
        false
    }
}

impl Drop for JoinState {
    fn drop(&mut self) {
        self.data.reservation.shrink(self.active_memory);
    }
}

#[inline]
fn matches_operator(operator: Operator, ordering: Ordering) -> bool {
    match operator {
        Operator::Lt => ordering == Ordering::Less,
        Operator::LtEq => ordering != Ordering::Greater,
        Operator::Gt => ordering == Ordering::Greater,
        Operator::GtEq => ordering != Ordering::Less,
        _ => unreachable!("IEJoin condition validation rejects non-range operators"),
    }
}

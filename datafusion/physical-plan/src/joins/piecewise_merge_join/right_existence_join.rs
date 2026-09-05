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

//! PiecewiseMergeJoin stream specialized for right existence joins.
//!
//! Instantiated by [`PiecewiseMergeJoinExec`] when the join type is `RightSemi` or
//! `RightAnti`. `LeftSemi`/`LeftAnti` are served by `ExistencePWMJStream` (see
//! `existence_join.rs`); the Mark joins are still rejected in
//! `PiecewiseMergeJoinExec::try_new`.
//!
//! # Algorithm
//!
//! Left and right existence joins mark opposite sides, and for a single range predicate
//! that difference is not symmetric — it collapses the work.
//!
//! `LeftSemi`/`LeftAnti` ask, for each *buffered* row, whether any streamed row matches, so
//! the answer depends on the whole streamed side and can only be emitted once it has all
//! been read. `RightSemi`/`RightAnti` ask the mirror question — for each *streamed* row,
//! does any buffered row match? — and with only `buffered_key OP streamed_key` to satisfy,
//! that is decided by a single buffered key:
//!
//! ```text
//!   ∃b. b <  s   ⟺   min(b) <  s          ∃b. b >  s   ⟺   max(b) >  s
//!   ∃b. b <= s   ⟺   min(b) <= s          ∃b. b >= s   ⟺   max(b) >= s
//! ```
//!
//! The buffered side is therefore reduced to that one key -- `min_batch`/`max_batch`, folded
//! batch by batch as it streams in -- and each batch is dropped once folded. It is never
//! concatenated or retained, so the state this join holds is a single row however large that
//! side is. The reduction is shared, so every streamed partition reads the same key rather
//! than repeating the pass:
//!
//! ```text
//!   operator `<`, buffered keys [NULL, 5, 9, 7]  ->  min = 5
//!   `b < s` holds for some b   iff   `5 < s`
//! ```
//!
//! A min/max is `O(B)` from any order, so `required_input_ordering` returns nothing for either
//! child and no `SortExec` is planned.
//!
//! Every streamed row is then decided by comparing it against that one key, which is a
//! vectorized `cmp` kernel per batch and a filter. Nothing about a streamed row depends on
//! any other, so:
//!
//! * output is produced per batch as it arrives — no watermark, no final pass, and no
//!   election among the streamed partitions,
//! * all N streamed partitions produce output, rather than one non-empty partition,
//! * the streamed side is never sorted, not even per batch.
//!
//! Rows whose join key is NULL never satisfy a comparison predicate. `min_batch`/`max_batch`
//! ignore NULLs, so the reduced key is null only when *every* buffered key is (or the buffered
//! side is empty) — and then no streamed row can match at all, which makes `RightSemi` empty
//! without reading the streamed side and `RightAnti` a passthrough of it. A NULL streamed key
//! makes its comparison NULL rather than false, which is "no match" — dropped by `RightSemi`,
//! kept by `RightAnti`.
//!
//! Picking the extreme and comparing against it must agree on ordering. `min_batch`/`max_batch`
//! and arrow's `lt`/`lt_eq`/`gt`/`gt_eq` kernels both order floats by `total_cmp`
//! (`arrow-arith`'s `MinAccumulator` compares with `ArrowNativeTypeOp::is_lt`, seeded from
//! `MAX_TOTAL_ORDER`), which puts `-0.0` strictly below `+0.0` -- but SQL comparisons treat
//! them as equal, and that is what a real `k < r` predicate actually evaluates to: any
//! `BinaryExpr` comparison, including the one the `NestedLoopJoinExec` oracle in the
//! differential fuzz test builds its filter from, normalizes `-0.0` to `+0.0` first (see
//! `apply_cmp` in `datafusion-physical-expr-common`). Both the extreme and the streamed key
//! array are normalized with [`normalize_float_zero`] before comparing,
//! after the reduction: normalizing first would not change which value the reduction picks
//! (`-0.0` and `+0.0` are numerically equal either way), and normalizing only where the
//! comparison happens keeps the reduction itself agreeing with the unnormalized `min`/`max`
//! semantics its docs above describe. `NaN` needs no such fix-up: every kernel involved orders
//! it as the maximum, so it is treated identically on both sides already.
//!
//! # Cost
//!
//! Let `B` be the buffered rows and `S` the streamed rows: `O(B)` to reduce the buffered side
//! plus `O(S)` to filter, and no sort on either side. The state retained across batches is one
//! row; while folding, each buffered partition also holds the key array of the batch it is
//! reducing, which it accounts against the memory pool for that long.
//!
//! This is why the shared state is [`BufferedExtreme`] and not `BufferedSideData`: every field
//! of the latter -- the concatenated batch, the key array, the visited-indices bitmap, the
//! final-pass counter -- would be dead here.
//!
//! [`PiecewiseMergeJoinExec`]: super::PiecewiseMergeJoinExec

use std::sync::Arc;
use std::task::{Poll, ready};

use arrow::array::{Array, ArrayRef, RecordBatch, Scalar};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::not;
use arrow::compute::kernels::cmp::{gt, gt_eq, lt, lt_eq};
use arrow_schema::SchemaRef;
use datafusion_common::utils::normalize_float_zero;
use datafusion_common::{Result, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};

use crate::handle_state;
use crate::joins::piecewise_merge_join::exec::BufferedExtreme;
use crate::joins::utils::{
    BuildProbeJoinMetrics, OnceFut, StatefulStreamResult, boolean_mask_from_filter,
};
use crate::stream::EmptyRecordBatchStream;

pub(super) enum RightExistencePWMJStreamState {
    /// Await the buffered side's reduction to a single key.
    WaitBufferedExtreme,
    /// Fetch streamed batches and emit the rows that do (`RightSemi`) or do not
    /// (`RightAnti`) have a buffered match.
    ScanStreamBatches,
    Completed,
}

pub(super) struct RightExistencePWMJStream {
    /// Output schema, which for `RightSemi`/`RightAnti` is the streamed side's schema
    schema: SchemaRef,
    /// Physical expression evaluated on the streamed side. The buffered side's
    /// equivalent is already evaluated when the buffered side is collected.
    on_streamed: PhysicalExprRef,
    /// `RightSemi` or `RightAnti`
    join_type: JoinType,
    /// Comparison operator
    operator: Operator,
    streamed: SendableRecordBatchStream,
    /// Resolves to the whole buffered side reduced to one key. Shared with the other streamed
    /// partitions, so that reduction happens exactly once.
    buffered_extreme_fut: OnceFut<BufferedExtreme>,
    state: RightExistencePWMJStreamState,
    /// That key, held as a one-element [`Scalar`] so the comparison against a streamed batch is
    /// one kernel call. `None` when the buffered side has no non-null key, i.e. nothing can ever
    /// match. Only populated once `buffered_extreme_fut` has resolved.
    buffered_extreme: Option<Scalar<ArrayRef>>,
    join_metrics: BuildProbeJoinMetrics,
}

impl RightExistencePWMJStream {
    pub(super) fn try_new(
        schema: SchemaRef,
        on_streamed: PhysicalExprRef,
        join_type: JoinType,
        operator: Operator,
        streamed: SendableRecordBatchStream,
        buffered_extreme_fut: OnceFut<BufferedExtreme>,
        join_metrics: BuildProbeJoinMetrics,
    ) -> Self {
        Self {
            schema,
            on_streamed,
            join_type,
            operator,
            streamed,
            buffered_extreme_fut,
            state: RightExistencePWMJStreamState::WaitBufferedExtreme,
            buffered_extreme: None,
            join_metrics,
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                RightExistencePWMJStreamState::WaitBufferedExtreme => {
                    handle_state!(ready!(self.collect_buffered_extreme(cx)))
                }
                RightExistencePWMJStreamState::ScanStreamBatches => {
                    handle_state!(ready!(self.scan_stream_batch(cx)))
                }
                RightExistencePWMJStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    /// Picks up the buffered side's reduced key, which is all this join type needs from it.
    fn collect_buffered_extreme(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let buffered_extreme = ready!(self.buffered_extreme_fut.get_shared(cx))?;
        build_timer.done();

        // Null exactly when no buffered key is non-null, and NULLs match nothing. Cloned out by
        // value -- it is one row -- so the shared state is not kept alive by this stream.
        //
        // Normalized because the `lt`/`lt_eq`/`gt`/`gt_eq` kernels called in
        // `filter_streamed_batch` order `-0.0` strictly below `+0.0`, but SQL comparisons --
        // including `apply_cmp`, which every other `k < r` predicate in the plan goes through
        // -- treat them as equal. The streamed key array is normalized the same way, right
        // before that comparison. `min`/`max_batch`, which reduced this extreme, order `-0.0`
        // below `+0.0` too, so normalizing after the reduction rather than before leaves the
        // reduction itself agreeing with the unnormalized ordering its own docs describe.
        let extreme = normalize_float_zero(buffered_extreme.extreme());
        self.buffered_extreme = (extreme.null_count() == 0).then(|| Scalar::new(extreme));

        // With no non-null buffered key nothing matches, so `RightSemi` outputs nothing
        // and does not need to read a single streamed batch. `RightAnti` still has to,
        // since it outputs all of them.
        self.state = match (&self.buffered_extreme, self.join_type) {
            (None, JoinType::RightSemi) => {
                let streamed_schema = self.streamed.schema();
                self.streamed = Box::pin(EmptyRecordBatchStream::new(streamed_schema));
                RightExistencePWMJStreamState::Completed
            }
            _ => RightExistencePWMJStreamState::ScanStreamBatches,
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Fetches one streamed batch and emits the rows it contributes, if any.
    fn scan_stream_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.streamed.poll_next_unpin(cx)) {
            None => self.state = RightExistencePWMJStreamState::Completed,
            Some(Ok(batch)) => {
                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                let output = self.filter_streamed_batch(&batch)?;
                if output.num_rows() > 0 {
                    return Poll::Ready(Ok(StatefulStreamResult::Ready(Some(output))));
                }
                // Nothing survived; take the next batch rather than yielding an empty one.
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        }

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Keeps the streamed rows that have a buffered match (`RightSemi`) or that have none
    /// (`RightAnti`), by comparing each against the single buffered extreme.
    fn filter_streamed_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = match &self.buffered_extreme {
            // No non-null buffered key, so no streamed row matches and `RightAnti` keeps
            // the batch whole. `RightSemi` never gets here: it completed without reading
            // the streamed side.
            None => batch.columns().to_vec(),
            Some(extreme) => {
                let stream_values = normalize_float_zero(
                    &self
                        .on_streamed
                        .evaluate(batch)?
                        .into_array(batch.num_rows())?,
                );

                // `extreme` is the buffered key, so it goes on the left of the operator,
                // matching the `buffered OP streamed` orientation of the predicate.
                let matched = match self.operator {
                    Operator::Lt => lt(extreme, &stream_values),
                    Operator::LtEq => lt_eq(extreme, &stream_values),
                    Operator::Gt => gt(extreme, &stream_values),
                    Operator::GtEq => gt_eq(extreme, &stream_values),
                    other => {
                        return internal_err!(
                            "PiecewiseMergeJoin should not contain operator, {other}"
                        );
                    }
                }?;

                let predicate = match self.join_type {
                    // A NULL streamed key compares NULL rather than false, and `filter`
                    // already treats NULL as "not selected" -- which is what a
                    // non-matching row is.
                    JoinType::RightSemi => matched,
                    // Anti needs the complement, so those NULLs have to be folded into
                    // false first: `not(NULL)` is NULL, which would drop a row that
                    // matched nothing.
                    _ => not(&boolean_mask_from_filter(&matched))?,
                };

                filter_record_batch(batch, &predicate)?.columns().to_vec()
            }
        };

        // Right existence joins output the streamed columns only. The streamed child's
        // schema is field-for-field equal to the join's own, but rebuild against the
        // latter so the stream's declared schema is what it yields.
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

impl RecordBatchStream for RightExistencePWMJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for RightExistencePWMJStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // `record_poll` fills in `output_rows` and `end_time`; `elapsed_compute` is handled
        // by `BuildProbeJoinMetrics::drop`.
        let poll = self.poll_next_impl(cx);
        self.join_metrics.baseline.record_poll(poll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sorts::sort::SortExec;
    use crate::{
        ExecutionPlan, ExecutionPlanProperties, common, joins::PiecewiseMergeJoinExec,
        test::TestMemoryExec,
    };
    use arrow::array::Int32Array;
    use arrow::compute::SortOptions;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::TaskContext;
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
    use insta::assert_snapshot;

    // Coverage for right existence joins also lives in `pwmj.slt` (both correlation
    // orientations, all four operators, NULLs, key types) and in the differential fuzz test
    // `fuzz_pwmj_matches_nested_loop`, which checks them against
    // `NestedLoopJoinExec` over randomized inputs. The tests here pin the parts SQL cannot
    // observe: which streamed batches were read, and which partition emitted them.

    fn kv_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("k", DataType::Int32, true),
        ]))
    }

    fn kv_batch(rows: &[(i32, Option<i32>)]) -> RecordBatch {
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        let keys: Vec<Option<i32>> = rows.iter().map(|(_, k)| *k).collect();
        RecordBatch::try_new(
            kv_schema(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(keys)),
            ],
        )
        .unwrap()
    }

    fn kv_exec(partitions: &[Vec<RecordBatch>]) -> Arc<dyn ExecutionPlan> {
        TestMemoryExec::try_new_exec(partitions, kv_schema(), None).unwrap()
    }

    /// Right existence joins declare no input ordering requirement, so the buffered sides
    /// below are deliberately **unsorted**: only their min/max matters. That is also why
    /// building the exec directly is faithful here, where the other streams would need a
    /// `SortExec` the tests have to supply by hand.
    fn join(
        buffered: Arc<dyn ExecutionPlan>,
        streamed: Arc<dyn ExecutionPlan>,
        operator: Operator,
        join_type: JoinType,
    ) -> Result<PiecewiseMergeJoinExec> {
        let on = (
            Arc::new(Column::new_with_schema("k", &buffered.schema())?) as _,
            Arc::new(Column::new_with_schema("k", &streamed.schema())?) as _,
        );
        let probe = PiecewiseMergeJoinExec::try_new(
            Arc::clone(&buffered),
            Arc::clone(&streamed),
            on.clone(),
            operator,
            join_type,
            1,
        )?;
        assert!(
            probe.required_input_ordering().iter().all(Option::is_none),
            "right existence joins must not require an input ordering"
        );
        assert!(
            probe
                .input_distribution_requirements()
                .per_child_distributions()
                .all(|d| matches!(
                    d,
                    datafusion_physical_expr::Distribution::UnspecifiedDistribution
                )),
            "right existence joins must not require the buffered side coalesced"
        );
        assert_eq!(
            probe.benefits_from_input_partitioning(),
            vec![false, true],
            "the buffered side must not be fanned out just to fold it"
        );
        PiecewiseMergeJoinExec::try_new(buffered, streamed, on, operator, join_type, 1)
    }

    fn input_batches(join: &PiecewiseMergeJoinExec) -> usize {
        join.metrics()
            .unwrap()
            .sum_by_name("input_batches")
            .expect("input_batches metric")
            .as_usize()
    }

    /// `RightSemi` keeps the streamed rows with at least one buffered match, and outputs
    /// only the streamed columns. `>` needs the buffered maximum, 5, so of the streamed keys
    /// {4, 5, 6} only 4 has some buffered key above it.
    #[tokio::test]
    async fn join_right_semi() -> Result<()> {
        let join = join(
            kv_exec(&[vec![kv_batch(&[(1, Some(5)), (2, Some(1)), (3, Some(2))])]]),
            kv_exec(&[vec![kv_batch(&[
                (10, Some(4)),
                (20, Some(5)),
                (30, Some(6)),
            ])]]),
            Operator::Gt,
            JoinType::RightSemi,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+---+
        | id | k |
        +----+---+
        | 10 | 4 |
        +----+---+
        ");
        Ok(())
    }

    /// `RightAnti` is the complement: the streamed rows with no buffered match.
    #[tokio::test]
    async fn join_right_anti() -> Result<()> {
        let join = join(
            kv_exec(&[vec![kv_batch(&[(1, Some(5)), (2, Some(1)), (3, Some(2))])]]),
            kv_exec(&[vec![kv_batch(&[
                (10, Some(4)),
                (20, Some(5)),
                (30, Some(6)),
            ])]]),
            Operator::Gt,
            JoinType::RightAnti,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+---+
        | id | k |
        +----+---+
        | 20 | 5 |
        | 30 | 6 |
        +----+---+
        ");
        Ok(())
    }

    /// A NULL key satisfies no comparison predicate, on either side.
    ///
    /// The buffered side is sorted descending with NULLs first, as `<` requires, so the key
    /// this operator needs -- the minimum, 5 -- is the last row and the NULL is nowhere near
    /// it. On the streamed side the NULL-keyed row matches nothing, so `RightSemi` drops it
    /// and `RightAnti` keeps it.
    #[tokio::test]
    async fn null_keys_match_nothing() -> Result<()> {
        let buffered =
            || kv_exec(&[vec![kv_batch(&[(1, Some(9)), (2, None), (3, Some(5))])]]);
        let streamed = || {
            kv_exec(&[vec![kv_batch(&[
                (10, Some(5)),
                (20, Some(6)),
                (30, None),
                (40, Some(100)),
            ])]])
        };

        let semi = join(buffered(), streamed(), Operator::Lt, JoinType::RightSemi)?;
        let batches =
            common::collect(semi.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+-----+
        | id | k   |
        +----+-----+
        | 20 | 6   |
        | 40 | 100 |
        +----+-----+
        ");

        let anti = join(buffered(), streamed(), Operator::Lt, JoinType::RightAnti)?;
        let batches =
            common::collect(anti.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+---+
        | id | k |
        +----+---+
        | 10 | 5 |
        | 30 |   |
        +----+---+
        ");
        Ok(())
    }

    /// With no non-null buffered key nothing can match, so `RightSemi` is empty however many
    /// streamed rows there are -- and it does not read a single one of them. Asserted through
    /// the `input_batches` metric, which SQL cannot observe.
    #[tokio::test]
    async fn all_null_buffered_side_makes_right_semi_read_nothing() -> Result<()> {
        let join = join(
            kv_exec(&[vec![kv_batch(&[(1, None), (2, None)])]]),
            kv_exec(&[vec![
                kv_batch(&[(10, Some(4))]),
                kv_batch(&[(20, Some(5))]),
                kv_batch(&[(30, Some(6))]),
            ]]),
            Operator::Gt,
            JoinType::RightSemi,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(input_batches(&join), 0, "no streamed batch should be read");
        Ok(())
    }

    /// The same buffered side, here empty rather than all-NULL, sends every streamed row to
    /// `RightAnti` instead. It has to read them all: they are the output.
    #[tokio::test]
    async fn empty_buffered_side_passes_right_anti_through() -> Result<()> {
        let join = join(
            kv_exec(&[vec![kv_batch(&[])]]),
            kv_exec(&[vec![
                kv_batch(&[(10, Some(4))]),
                kv_batch(&[(20, None)]),
                kv_batch(&[(30, Some(6))]),
            ]]),
            Operator::Gt,
            JoinType::RightAnti,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+---+
        | id | k |
        +----+---+
        | 10 | 4 |
        | 20 |   |
        | 30 | 6 |
        +----+---+
        ");
        assert_eq!(input_batches(&join), 3);
        Ok(())
    }

    /// The streamed side's ordering survives this join -- one output batch per streamed batch, in
    /// order, with rows only removed -- so `maintains_input_order` claims it and the operator
    /// advertises the streamed child's ordering as its own. That lets a downstream operator skip
    /// a re-sort, which means a wrong claim here would produce wrong results, not just a slow
    /// plan. The runtime side of it is pinned by the row-order snapshots above.
    #[test]
    fn output_ordering_follows_the_streamed_side() -> Result<()> {
        let streamed_input = kv_exec(&[vec![kv_batch(&[(20, Some(4)), (10, Some(9))])]]);
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new_with_schema("id", &streamed_input.schema())?),
            SortOptions::new(true, false),
        )])
        .unwrap();
        let streamed = Arc::new(SortExec::new(ordering.clone(), streamed_input));

        for join_type in [JoinType::RightSemi, JoinType::RightAnti] {
            let join = join(
                kv_exec(&[vec![kv_batch(&[(1, Some(5))])]]),
                Arc::clone(&streamed) as _,
                Operator::Gt,
                join_type,
            )?;
            assert_eq!(
                join.properties().output_ordering(),
                Some(&ordering),
                "{join_type} should advertise the streamed side's ordering"
            );
        }

        // The buffered side contributes no column, so its ordering must not leak out: a buffered
        // child sorted on `k` cannot make the join claim anything.
        let buffered_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 1)),
            SortOptions::new(false, true),
        )])
        .unwrap();
        let sorted_buffered = Arc::new(SortExec::new(
            buffered_ordering,
            kv_exec(&[vec![kv_batch(&[(1, Some(5))])]]),
        ));
        let join = join(
            sorted_buffered as _,
            kv_exec(&[vec![kv_batch(&[(20, Some(4))])]]),
            Operator::Gt,
            JoinType::RightSemi,
        )?;
        assert_eq!(join.properties().output_ordering(), None);
        Ok(())
    }

    /// A zero-row streamed batch still reaches the comparison kernel, which yields an empty
    /// mask rather than erroring, so the batch is filtered away and the surrounding batches
    /// are unaffected. Covered for both join types since only anti also runs `not` over it.
    #[tokio::test]
    async fn empty_streamed_batch_is_skipped() -> Result<()> {
        for (join_type, expected) in [(JoinType::RightSemi, 1), (JoinType::RightAnti, 1)]
        {
            let join = join(
                kv_exec(&[vec![kv_batch(&[(1, Some(5)), (2, Some(1))])]]),
                kv_exec(&[vec![
                    kv_batch(&[]),
                    kv_batch(&[(10, Some(4)), (20, Some(9))]),
                    kv_batch(&[]),
                ]]),
                Operator::Gt,
                join_type,
            )?;

            let stream = join.execute(0, Arc::new(TaskContext::default()))?;
            let batches = common::collect(stream).await?;

            // Buffered maximum is 5, so `>` keeps 4 for semi and 9 for anti -- one row each,
            // and no empty batch in between.
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                expected,
                "{join_type}"
            );
            assert!(
                batches.iter().all(|b| b.num_rows() > 0),
                "{join_type} should not yield an empty batch"
            );
            assert_eq!(input_batches(&join), 3, "{join_type}");
        }
        Ok(())
    }

    /// The buffered side is folded to one key as it streams in and never materialized, so this
    /// join runs in a memory pool far smaller than that side. 20k buffered rows is >80 KB of
    /// Int32 keys alone and ~160 KB of batches; the pool here is 64 KB, which the collecting
    /// path cannot fit -- it reserves every batch, then the concatenation, then the key array.
    /// The fold needs only the one batch's key array it is reducing (~8 KB), which it does
    /// reserve, so the pool is sized above that and well below the collecting path.
    ///
    /// This is the assertion that pins the retained buffered state at one row. `build_mem_used`
    /// is checked too, since a regression to collecting would show up there even under a
    /// generous pool.
    #[tokio::test]
    async fn buffered_side_is_folded_not_collected() -> Result<()> {
        let buffered: Vec<RecordBatch> = (0..10)
            .map(|b| {
                kv_batch(
                    &(0..2000)
                        .map(|i| (i, Some(b * 2000 + i)))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();

        let join = join(
            kv_exec(&[buffered]),
            kv_exec(&[vec![kv_batch(&[(1, Some(0)), (2, Some(19_999))])]]),
            Operator::Gt,
            JoinType::RightSemi,
        )?;

        let task_ctx = Arc::new(
            TaskContext::default().with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(64 * 1024)))
                    .build()?,
            )),
        );
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // Buffered maximum is 19999, so `>` keeps only the streamed 0.
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+---+
        | id | k |
        +----+---+
        | 1  | 0 |
        +----+---+
        ");

        // What is held is one row, not 20k.
        let build_mem_used = join
            .metrics()
            .unwrap()
            .sum_by_name("build_mem_used")
            .expect("build_mem_used metric")
            .as_usize();
        assert!(
            build_mem_used < 1024,
            "buffered state should be a single key, got {build_mem_used} bytes"
        );
        Ok(())
    }

    /// The key array a partition is reducing is transient but as wide as the batch, and it is
    /// reserved for as long as it is held. A pool that cannot fit one of them fails the join
    /// rather than allocating outside the accounting -- which is what keeps the memory claim
    /// honest, since without the reservation this fold would simply succeed.
    #[tokio::test]
    async fn folding_reserves_the_batch_key_array() -> Result<()> {
        let join = join(
            kv_exec(&[vec![kv_batch(
                &(0..2000).map(|i| (i, Some(i))).collect::<Vec<_>>(),
            )]]),
            kv_exec(&[vec![kv_batch(&[(1, Some(0))])]]),
            Operator::Gt,
            JoinType::RightSemi,
        )?;

        let task_ctx = Arc::new(
            TaskContext::default().with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024)))
                    .build()?,
            )),
        );
        let err = common::collect(join.execute(0, task_ctx)?)
            .await
            .expect_err("a 2000-row key array should not fit a 1 KB pool");
        assert!(
            err.to_string().contains("PiecewiseMergeJoinBufferedFold"),
            "expected the fold's reservation to fail, got: {err}"
        );
        Ok(())
    }

    /// A min/max combines across partitions, so the buffered side is not required to be
    /// coalesced -- and every partition of it has to be consumed. The deciding key here is in
    /// the **last** buffered partition, so reading only partition 0 (which the other streams do,
    /// since they require `SinglePartition`) silently loses it and changes the answer.
    #[tokio::test]
    async fn every_buffered_partition_is_folded() -> Result<()> {
        // Buffered maxima per partition: 1, 3, 50. Only the last admits the streamed 40.
        let buffered = vec![
            vec![kv_batch(&[(1, Some(0)), (2, Some(1))])],
            vec![kv_batch(&[(3, Some(3))])],
            vec![kv_batch(&[(4, Some(2)), (5, Some(50))])],
        ];
        let streamed = || kv_exec(&[vec![kv_batch(&[(10, Some(2)), (20, Some(40))])]]);

        let semi = join(
            kv_exec(&buffered),
            streamed(),
            Operator::Gt,
            JoinType::RightSemi,
        )?;
        assert_eq!(semi.buffered().output_partitioning().partition_count(), 3);
        let batches =
            common::collect(semi.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+
        | id | k  |
        +----+----+
        | 10 | 2  |
        | 20 | 40 |
        +----+----+
        ");

        let anti = join(
            kv_exec(&buffered),
            streamed(),
            Operator::Gt,
            JoinType::RightAnti,
        )?;
        let batches =
            common::collect(anti.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        // All three partitions were read, not just the first.
        assert_eq!(input_batches(&semi), 1, "streamed batches");
        let build_batches = semi
            .metrics()
            .unwrap()
            .sum_by_name("build_input_batches")
            .expect("build_input_batches metric")
            .as_usize();
        assert_eq!(
            build_batches, 3,
            "every buffered partition should be folded"
        );
        Ok(())
    }

    /// Unlike `LeftSemi`/`LeftAnti`, where one elected partition emits everything at the end,
    /// each streamed partition here emits its own rows as it reads them -- so the join's N
    /// advertised output partitions are all live.
    #[tokio::test]
    async fn every_streamed_partition_emits() -> Result<()> {
        let join = join(
            kv_exec(&[vec![kv_batch(&[(1, Some(5)), (2, Some(1))])]]),
            kv_exec(&[
                vec![kv_batch(&[(10, Some(4)), (20, Some(9))])],
                vec![kv_batch(&[(30, Some(0)), (40, Some(7))])],
            ]),
            Operator::Gt,
            JoinType::RightSemi,
        )?;

        assert_eq!(join.properties().output_partitioning().partition_count(), 2);

        let task_ctx = Arc::new(TaskContext::default());
        let mut per_partition = Vec::new();
        for partition in 0..2 {
            let stream = join.execute(partition, Arc::clone(&task_ctx))?;
            per_partition.push(common::collect(stream).await?);
        }

        // Buffered maximum is 5: partition 0 keeps 4, partition 1 keeps 0.
        for (partition, batches) in per_partition.iter().enumerate() {
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                1,
                "partition {partition} should have emitted its own row"
            );
        }
        let out = arrow::compute::concat_batches(
            &join.schema(),
            per_partition.iter().flatten(),
        )?;
        assert_snapshot!(batches_to_string(&[out]), @r"
        +----+---+
        | id | k |
        +----+---+
        | 10 | 4 |
        | 30 | 0 |
        +----+---+
        ");
        Ok(())
    }
}

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

//! Contains code to filter entire pages

use std::collections::HashSet;
use std::sync::Arc;

use super::metrics::ParquetFileMetrics;
use crate::ParquetAccessPlan;

use arrow::array::BooleanArray;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
};
use datafusion_common::ScalarValue;
use datafusion_common::pruning::PruningStatistics;
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};
use datafusion_pruning::{NoopObserver, PruningObserver, PruningPredicate, Tag};

use log::{debug, trace};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex};
use parquet::file::page_index::offset_index::PageLocation;
use parquet::schema::types::SchemaDescriptor;
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    file::metadata::{ParquetMetaData, RowGroupMetaData},
};

/// Filters a [`ParquetAccessPlan`] based on the [Parquet PageIndex], if present
///
/// It does so by evaluating statistics from the [`ParquetColumnIndex`] and
/// [`ParquetOffsetIndex`] and converting them to [`RowSelection`].
///
/// [Parquet PageIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
///
/// For example, given a row group with two column (chunks) for `A`
/// and `B` with the following with page level statistics:
///
/// ```text
/// ┏━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━
///    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ┃
/// ┃     ┌──────────────┐  │     ┌──────────────┐  │  ┃
/// ┃  │  │              │     │  │              │     ┃
/// ┃     │              │  │     │     Page     │  │
///    │  │              │     │  │      3       │     ┃
/// ┃     │              │  │     │   min: "A"   │  │  ┃
/// ┃  │  │              │     │  │   max: "C"   │     ┃
/// ┃     │     Page     │  │     │ first_row: 0 │  │
///    │  │      1       │     │  │              │     ┃
/// ┃     │   min: 10    │  │     └──────────────┘  │  ┃
/// ┃  │  │   max: 20    │     │  ┌──────────────┐     ┃
/// ┃     │ first_row: 0 │  │     │              │  │
///    │  │              │     │  │     Page     │     ┃
/// ┃     │              │  │     │      4       │  │  ┃
/// ┃  │  │              │     │  │   min: "D"   │     ┃
/// ┃     │              │  │     │   max: "G"   │  │
///    │  │              │     │  │first_row: 100│     ┃
/// ┃     └──────────────┘  │     │              │  │  ┃
/// ┃  │  ┌──────────────┐     │  │              │     ┃
/// ┃     │              │  │     └──────────────┘  │
///    │  │     Page     │     │  ┌──────────────┐     ┃
/// ┃     │      2       │  │     │              │  │  ┃
/// ┃  │  │   min: 30    │     │  │     Page     │     ┃
/// ┃     │   max: 40    │  │     │      5       │  │
///    │  │first_row: 200│     │  │   min: "H"   │     ┃
/// ┃     │              │  │     │   max: "Z"   │  │  ┃
/// ┃  │  │              │     │  │first_row: 250│     ┃
/// ┃     └──────────────┘  │     │              │  │
///    │                       │  └──────────────┘     ┃
/// ┃   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ┃
/// ┃       ColumnChunk            ColumnChunk         ┃
/// ┃            A                      B
///  ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━┛
///
///   Total rows: 300
/// ```
///
/// Given the predicate `A > 35 AND B = 'F'`:
///
/// Using `A > 35`: can rule out all of values in Page 1 (rows 0 -> 199)
///
/// Using `B = 'F'`: can rule out all values in Page 3 and Page 5 (rows 0 -> 99, and 250 -> 299)
///
/// So we can entirely skip rows 0->199 and 250->299 as we know they
/// can not contain rows that match the predicate.
///
/// # Implementation notes
///
/// Single column predicates are evaluated using the PageIndex information
/// for that column to determine which row ranges can be skipped based.
///
/// The resulting [`RowSelection`]'s are combined into a final
/// row selection that is added to the [`ParquetAccessPlan`].
#[derive(Debug)]
pub struct PagePruningAccessPlanFilter {
    /// Single-column conjuncts split from the top-level `AND` of the
    /// overall predicate (via `split_conjunction`), keeping only those
    /// that build into a non-trivial single-column `PruningPredicate`
    /// (e.g. `col = 5`, `col < 10`, or a same-column `col < 5 OR col > 100`).
    /// Multi-column, always-true, and non-buildable conjuncts are
    /// dropped, so this is a *necessary-but-not-sufficient* subset of
    /// the filter: every entry must hold for a row to be included, but
    /// satisfying all of them does not by itself guarantee inclusion.
    /// Page pruning ANDs each entry's per-page selection — a page is
    /// kept only if it may satisfy all of them.
    ///
    /// Each carries an optional caller [`Tag`]; when present, the
    /// page-index evaluation loop fires `observer.on_leaf(tag, ..)`
    /// after evaluating that predicate. Untagged predicates produce
    /// `None` tags and no-op against `NoopObserver`.
    predicates: Vec<TaggedPagePredicate>,
}

/// Single-column predicate paired with an optional caller tag.
#[derive(Debug)]
struct TaggedPagePredicate {
    tag: Option<Tag>,
    predicate: PruningPredicate,
}

/// Builder for a tagged [`PagePruningAccessPlanFilter`]. Each
/// [`push`](Self::push) runs the same single-column / non-trivial
/// filtering as [`PagePruningAccessPlanFilter::new`]; conjuncts that
/// fail it are dropped (their tags will not appear in observer
/// output). [`build`](Self::build) always succeeds — an empty filter
/// is a valid no-op pruner.
#[derive(Debug)]
pub struct PagePruningAccessPlanFilterBuilder {
    schema: SchemaRef,
    predicates: Vec<TaggedPagePredicate>,
}

impl PagePruningAccessPlanFilterBuilder {
    /// Add a tagged conjunct. Mutates and returns `&mut self`, so this
    /// works in both chains and loops.
    pub fn push(&mut self, tag: Tag, expr: Arc<dyn PhysicalExpr>) -> &mut Self {
        if let Some(p) =
            PagePruningAccessPlanFilter::build_one(expr, &self.schema, Some(tag))
        {
            self.predicates.push(p);
        }
        self
    }

    /// Finish building.
    pub fn build(self) -> PagePruningAccessPlanFilter {
        PagePruningAccessPlanFilter {
            predicates: self.predicates,
        }
    }
}

/// Result of applying page-index pruning to a [`ParquetAccessPlan`].
pub(crate) struct PagePruningResult {
    pub(crate) access_plan: ParquetAccessPlan,
    /// Pages skipped because the containing row group was fully matched by
    /// row-group statistics.
    pub(crate) pages_skipped_by_fully_matched: usize,
}

impl PagePruningResult {
    fn new(
        access_plan: ParquetAccessPlan,
        pages_skipped_by_fully_matched: usize,
    ) -> Self {
        Self {
            access_plan,
            pages_skipped_by_fully_matched,
        }
    }
}

/// The always-together context for a page-index prune. Bundled so the
/// prune entry points don't carry four loose refs.
#[derive(Clone, Copy)]
pub(crate) struct PagePruningContext<'a> {
    pub arrow_schema: &'a Schema,
    pub parquet_schema: &'a SchemaDescriptor,
    pub parquet_metadata: &'a ParquetMetaData,
    pub file_metrics: &'a ParquetFileMetrics,
}

/// A configured-but-not-yet-run page-index prune, produced by
/// [`PagePruningAccessPlanFilter::prune_pages`]. Optionally attach an
/// observer with [`with_observer`](Self::with_observer), then execute
/// with [`prune`](Self::prune).
pub(crate) struct PagePrune<'a> {
    filter: &'a PagePruningAccessPlanFilter,
    access_plan: ParquetAccessPlan,
    ctx: &'a PagePruningContext<'a>,
    observer: Option<&'a mut dyn PruningObserver>,
}

impl<'a> PagePrune<'a> {
    /// Attach a per-leaf observer. Omit to prune without collecting
    /// per-conjunct stats (zero overhead).
    // Consumed by the adaptive parquet scan (later in the stack); the
    // untagged `prune_plan_with_page_index` path never sets an observer.
    #[expect(
        dead_code,
        reason = "tagged page-prune consumer lands later in the stack"
    )]
    pub fn with_observer(mut self, observer: &'a mut dyn PruningObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Run the prune, returning the updated access plan and metrics.
    pub fn prune(self) -> PagePruningResult {
        let PagePrune {
            filter,
            access_plan,
            ctx,
            observer,
        } = self;
        let mut noop = NoopObserver;
        let observer: &mut dyn PruningObserver = match observer {
            Some(o) => o,
            None => &mut noop,
        };
        filter.run_prune(access_plan, ctx, observer)
    }
}

impl PagePruningAccessPlanFilter {
    /// Create a new [`PagePruningAccessPlanFilter`] from a physical
    /// expression. Predicates created this way have no caller tag.
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(expr: &Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Self {
        let predicates = split_conjunction(expr)
            .into_iter()
            .filter_map(|predicate| Self::build_one(Arc::clone(predicate), &schema, None))
            .collect::<Vec<_>>();
        Self { predicates }
    }

    /// Start building a filter whose conjuncts each carry a
    /// caller-supplied [`Tag`]. Mirrors [`PruningConjunction::builder`]:
    ///
    /// ```ignore
    /// let mut b = PagePruningAccessPlanFilter::builder(schema);
    /// for (id, expr) in conjuncts {
    ///     b.push(id, expr);
    /// }
    /// let filter = b.build();
    /// ```
    ///
    /// During pruning the page-index loop fires `observer.on_leaf(tag,
    /// ..)` once per leaf actually evaluated; leaves cut off by the
    /// AND short-circuit on row selection (`!selects_any`) are not
    /// observed.
    ///
    /// [`PruningConjunction::builder`]: datafusion_pruning::PruningConjunction::builder
    pub fn builder(schema: SchemaRef) -> PagePruningAccessPlanFilterBuilder {
        PagePruningAccessPlanFilterBuilder {
            schema,
            predicates: Vec::new(),
        }
    }

    fn build_one(
        expr: Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
        tag: Option<Tag>,
    ) -> Option<TaggedPagePredicate> {
        let pp = match PruningPredicate::try_new(expr, Arc::clone(schema)) {
            Ok(pp) => pp,
            Err(e) => {
                debug!("Ignoring error creating page pruning predicate: {e}");
                return None;
            }
        };
        if pp.always_true() {
            debug!(
                "Ignoring always true page pruning predicate: {}",
                pp.orig_expr()
            );
            return None;
        }
        if pp.required_columns().single_column().is_none() {
            debug!(
                "Ignoring multi-column page pruning predicate: {}",
                pp.orig_expr()
            );
            return None;
        }
        Some(TaggedPagePredicate { tag, predicate: pp })
    }

    /// Returns an updated [`ParquetAccessPlan`] by applying predicates to the
    /// parquet page index, if any.
    ///
    /// Thin shim over the fluent `prune_pages` op (no observer).
    /// Could be `#[deprecated]` in favor of the fluent form once the
    /// adaptive scan migrates.
    pub fn prune_plan_with_page_index(
        &self,
        access_plan: ParquetAccessPlan,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        parquet_metadata: &ParquetMetaData,
        file_metrics: &ParquetFileMetrics,
    ) -> ParquetAccessPlan {
        let ctx = PagePruningContext {
            arrow_schema,
            parquet_schema,
            parquet_metadata,
            file_metrics,
        };
        self.prune_pages(access_plan, &ctx).prune().access_plan
    }

    /// Begin a page-index prune. Returns a [`PagePrune`] op that can
    /// optionally take an observer before running:
    ///
    /// ```ignore
    /// let result = filter.prune_pages(access_plan, &ctx)
    ///     .with_observer(&mut obs)   // optional
    ///     .prune();
    /// ```
    pub(crate) fn prune_pages<'a>(
        &'a self,
        access_plan: ParquetAccessPlan,
        ctx: &'a PagePruningContext<'a>,
    ) -> PagePrune<'a> {
        PagePrune {
            filter: self,
            access_plan,
            ctx,
            observer: None,
        }
    }

    /// Workhorse for page-index pruning. The observer fires
    /// `on_leaf(tag, mask)` once per predicate that is actually
    /// evaluated; `mask[i] = true` means row group `i` still has at
    /// least one page that may match that leaf. Predicates skipped by
    /// the per-row-group `!selects_any` short-circuit are not observed,
    /// so per-conjunct stats are not biased by predicates that never
    /// ran (resolves the reviewer's Q2 concern on PR #22235).
    fn run_prune(
        &self,
        mut access_plan: ParquetAccessPlan,
        ctx: &PagePruningContext<'_>,
        observer: &mut dyn PruningObserver,
    ) -> PagePruningResult {
        let PagePruningContext {
            arrow_schema,
            parquet_schema,
            parquet_metadata,
            file_metrics,
        } = *ctx;
        // scoped timer updates on drop
        let _timer_guard = file_metrics.page_index_eval_time.timer();
        if self.predicates.is_empty() {
            return PagePruningResult::new(access_plan, 0);
        }

        let page_index_predicates = &self.predicates;
        let groups = parquet_metadata.row_groups();

        // Per-leaf "did this row group still have any matching pages
        // after the leaf alone?" mask. Built across all row groups,
        // emitted via `observer.on_leaf` at the end so each leaf gets
        // exactly one observation per call to this function. Leaves
        // never evaluated (because an earlier conjunct emptied the
        // running row selection — `!selects_any` break below) end up
        // with `None` here and are intentionally not observed.
        let mut per_leaf_mask: Vec<Option<Vec<bool>>> =
            (0..page_index_predicates.len()).map(|_| None).collect();

        if groups.is_empty() {
            return PagePruningResult::new(access_plan, 0);
        }

        if parquet_metadata.offset_index().is_none()
            || parquet_metadata.column_index().is_none()
        {
            debug!(
                "Can not prune pages due to lack of indexes. Have offset: {}, column index: {}",
                parquet_metadata.offset_index().is_some(),
                parquet_metadata.column_index().is_some()
            );
            return PagePruningResult::new(access_plan, 0);
        };

        // track the total number of rows that should be skipped
        let mut total_skip = 0;
        // track the total number of rows that should not be skipped
        let mut total_select = 0;
        // track the total number of pages that should be skipped
        let mut total_pages_skip = 0;
        // track the total number of pages that should not be skipped
        let mut total_pages_select = 0;
        // track pages for which page-index pruning was skipped because the
        // containing row group was already proven fully matched by statistics
        let mut total_pages_skipped_by_fully_matched = 0;

        // for each row group specified in the access plan
        let row_group_indexes = access_plan.row_group_indexes();
        for row_group_index in row_group_indexes {
            // Skip page pruning for fully matched row groups: all rows are
            // known to satisfy the predicate, so page-level pruning is wasted work.
            if access_plan.is_fully_matched(row_group_index) {
                let page_count =
                    fully_matched_page_count(row_group_index, parquet_metadata);
                total_pages_skipped_by_fully_matched += page_count;

                continue;
            }
            // The selection for this particular row group
            let mut overall_selection = None;

            let total_pages_in_group =
                parquet_metadata.offset_index().map_or(0, |offset_index| {
                    offset_index[row_group_index]
                        .first()
                        .map_or(0, |column| column.page_locations.len())
                });
            // stores the indexes of the matched pages
            let mut matched_pages_in_group: HashSet<usize> =
                HashSet::from_iter(0..total_pages_in_group);

            for (leaf_idx, tagged) in page_index_predicates.iter().enumerate() {
                let predicate = &tagged.predicate;
                let Some(column) = predicate.required_columns().single_column() else {
                    debug!(
                        "Ignoring multi-column page pruning predicate: {:?}",
                        predicate.predicate_expr()
                    );
                    continue;
                };

                let converter = StatisticsConverter::try_new(
                    column.name(),
                    arrow_schema,
                    parquet_schema,
                );

                let converter = match converter {
                    Ok(converter) => converter,
                    Err(e) => {
                        debug!(
                            "Could not create statistics converter for column {}: {e}",
                            column.name()
                        );
                        continue;
                    }
                };

                let selection = prune_pages_in_one_row_group(
                    row_group_index,
                    predicate,
                    converter,
                    parquet_metadata,
                    file_metrics,
                );

                let Some((selection, pages)) = selection else {
                    trace!("No pages pruned in prune_pages_in_one_row_group");
                    continue;
                };

                debug!(
                    "Use filter and page index to create RowSelection {:?} from predicate: {:?}",
                    &selection,
                    predicate.predicate_expr(),
                );

                // Per-leaf observation: this leaf ran for `row_group_index`
                // and produced `selection`. Whether this row group is
                // "kept" by the leaf is `selection.selects_any()`. The
                // entry is created lazily so leaves never observed (due
                // to short-circuit) stay `None`.
                let mask = per_leaf_mask[leaf_idx]
                    .get_or_insert_with(|| vec![false; groups.len()]);
                mask[row_group_index] = selection.selects_any();

                let matched_pages_indexes: HashSet<_> = pages
                    .into_iter()
                    .enumerate()
                    .filter(|x| x.1)
                    .map(|x| x.0)
                    .collect();
                matched_pages_in_group.retain(|x| matched_pages_indexes.contains(x));

                overall_selection = update_selection(overall_selection, selection);

                // if the overall selection has ruled out all rows, no need to
                // continue with the other predicates
                let selects_any = overall_selection
                    .as_ref()
                    .map(|selection| selection.selects_any())
                    .unwrap_or(true);

                if !selects_any {
                    break;
                }
            }

            if let Some(overall_selection) = overall_selection {
                let rows_selected = overall_selection.row_count();
                if rows_selected > 0 {
                    let rows_skipped = overall_selection.skipped_row_count();
                    trace!(
                        "Overall selection from predicate skipped {rows_skipped}, selected {rows_selected}: {overall_selection:?}"
                    );
                    total_skip += rows_skipped;
                    total_select += rows_selected;
                    access_plan.scan_selection(row_group_index, overall_selection)
                } else {
                    // Selection skips all rows, so skip the entire row group
                    let rows_skipped = groups[row_group_index].num_rows() as usize;
                    access_plan.skip(row_group_index);
                    total_skip += rows_skipped;
                    trace!(
                        "Overall selection from predicate is empty, \
                        skipping all {rows_skipped} rows in row group {row_group_index}"
                    );
                }
            } else {
                total_select +=
                    parquet_metadata.row_group(row_group_index).num_rows() as usize;
            }

            let pages_matched = matched_pages_in_group.len();
            total_pages_select += pages_matched;
            total_pages_skip += total_pages_in_group - pages_matched;
        }

        file_metrics.page_index_rows_pruned.add_pruned(total_skip);
        file_metrics
            .page_index_rows_pruned
            .add_matched(total_select);
        file_metrics
            .page_index_pages_pruned
            .add_pruned(total_pages_skip);
        file_metrics
            .page_index_pages_pruned
            .add_matched(total_pages_select);

        // Emit one observer event per leaf that was actually
        // evaluated against at least one row group. Leaves that the
        // outer `!selects_any` short-circuit prevented from running
        // stay `None` here and are correctly absent from the stats.
        for (leaf_idx, mask_opt) in per_leaf_mask.into_iter().enumerate() {
            if let Some(mask) = mask_opt {
                let tag = page_index_predicates[leaf_idx].tag;
                observer.on_leaf(tag, &mask);
            }
        }

        PagePruningResult::new(access_plan, total_pages_skipped_by_fully_matched)
    }

    /// Returns the number of filters in the [`PagePruningAccessPlanFilter`]
    pub fn filter_number(&self) -> usize {
        self.predicates.len()
    }
}

fn update_selection(
    current_selection: Option<RowSelection>,
    row_selection: RowSelection,
) -> Option<RowSelection> {
    match current_selection {
        None => Some(row_selection),
        Some(current_selection) => Some(current_selection.intersection(&row_selection)),
    }
}

/// Returns the number of pages for which page-index pruning is skipped because
/// the containing row group is fully matched by row-group statistics.
fn fully_matched_page_count(
    row_group_index: usize,
    parquet_metadata: &ParquetMetaData,
) -> usize {
    parquet_metadata.offset_index().map_or(0, |offset_index| {
        offset_index[row_group_index]
            .first()
            .map_or(0, |column| column.page_locations.len())
    })
}

/// Returns a [`RowSelection`] for the rows in this row group to scan, in addition to a vec of
/// booleans that state if each page was matched (true) or not (false).
///
/// This Row Selection is formed from the page index and the predicate skips row
/// ranges that can be ruled out based on the predicate.
///
/// Returns `None` if there is an error evaluating the predicate or the required
/// page information is not present.
fn prune_pages_in_one_row_group(
    row_group_index: usize,
    pruning_predicate: &PruningPredicate,
    converter: StatisticsConverter<'_>,
    parquet_metadata: &ParquetMetaData,
    metrics: &ParquetFileMetrics,
) -> Option<(RowSelection, Vec<bool>)> {
    let pruning_stats =
        PagesPruningStatistics::try_new(row_group_index, converter, parquet_metadata)?;

    // Each element in values is a boolean indicating whether the page may have
    // values that match the predicate (true) or could not possibly have values
    // that match the predicate (false).
    let values = match pruning_predicate.prune(&pruning_stats) {
        Ok(values) => values,
        Err(e) => {
            debug!("Error evaluating page index predicate values {e}");
            metrics.predicate_evaluation_errors.add(1);
            return None;
        }
    };

    // Convert the information of which pages to skip into a RowSelection
    // that describes the ranges of rows to skip.
    let Some(page_row_counts) = pruning_stats.page_row_counts() else {
        debug!(
            "Can not determine page row counts for row group {row_group_index}, skipping"
        );
        metrics.predicate_evaluation_errors.add(1);
        return None;
    };

    let mut vec = Vec::with_capacity(values.len());
    assert_eq!(page_row_counts.len(), values.len());
    let mut sum_row = *page_row_counts.first().unwrap();
    let mut selected = *values.first().unwrap();
    trace!("Pruned to {values:?} using {pruning_stats:?}");
    for (i, &f) in values.iter().enumerate().skip(1) {
        if f == selected {
            sum_row += *page_row_counts.get(i).unwrap();
        } else {
            let selector = if selected {
                RowSelector::select(sum_row)
            } else {
                RowSelector::skip(sum_row)
            };
            vec.push(selector);
            sum_row = *page_row_counts.get(i).unwrap();
            selected = f;
        }
    }

    let selector = if selected {
        RowSelector::select(sum_row)
    } else {
        RowSelector::skip(sum_row)
    };
    vec.push(selector);

    Some((RowSelection::from(vec), values))
}

/// Implement [`PruningStatistics`] for one column's PageIndex (column_index + offset_index)
#[derive(Debug)]
struct PagesPruningStatistics<'a> {
    row_group_index: usize,
    row_group_metadatas: &'a [RowGroupMetaData],
    converter: StatisticsConverter<'a>,
    column_index: &'a ParquetColumnIndex,
    offset_index: &'a ParquetOffsetIndex,
    page_offsets: &'a Vec<PageLocation>,
}

impl<'a> PagesPruningStatistics<'a> {
    /// Creates a new [`PagesPruningStatistics`] for a column in a row group, if
    /// possible.
    ///
    /// Returns None if the `parquet_metadata` does not have sufficient
    /// information to create the statistics.
    fn try_new(
        row_group_index: usize,
        converter: StatisticsConverter<'a>,
        parquet_metadata: &'a ParquetMetaData,
    ) -> Option<Self> {
        let Some(parquet_column_index) = converter.parquet_column_index() else {
            trace!(
                "Column {:?} not in parquet file, skipping",
                converter.arrow_field()
            );
            return None;
        };

        let column_index = parquet_metadata.column_index()?;
        let offset_index = parquet_metadata.offset_index()?;
        let row_group_metadatas = parquet_metadata.row_groups();

        let Some(row_group_page_offsets) = offset_index.get(row_group_index) else {
            trace!("No page offsets for row group {row_group_index}, skipping");
            return None;
        };
        let Some(offset_index_metadata) =
            row_group_page_offsets.get(parquet_column_index)
        else {
            trace!(
                "No page offsets for column {:?} in row group {row_group_index}, skipping",
                converter.arrow_field()
            );
            return None;
        };
        let page_offsets = offset_index_metadata.page_locations();

        Some(Self {
            row_group_index,
            row_group_metadatas,
            converter,
            column_index,
            offset_index,
            page_offsets,
        })
    }

    /// return the row counts in each data page, if possible.
    fn page_row_counts(&self) -> Option<Vec<usize>> {
        let row_group_metadata = self
            .row_group_metadatas
            .get(self.row_group_index)
            // fail fast/panic if row_group_index is out of bounds
            .unwrap();

        let num_rows_in_row_group = row_group_metadata.num_rows() as usize;

        let page_offsets = self.page_offsets;
        let mut vec = Vec::with_capacity(page_offsets.len());
        page_offsets.windows(2).for_each(|x| {
            let start = x[0].first_row_index as usize;
            let end = x[1].first_row_index as usize;
            vec.push(end - start);
        });
        vec.push(num_rows_in_row_group - page_offsets.last()?.first_row_index as usize);
        Some(vec)
    }
}
impl PruningStatistics for PagesPruningStatistics<'_> {
    fn min_values(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        match self.converter.data_page_mins(
            self.column_index,
            self.offset_index,
            [&self.row_group_index],
        ) {
            Ok(min_values) => Some(min_values),
            Err(e) => {
                debug!("Error evaluating data page min values {e}");
                None
            }
        }
    }

    fn max_values(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        match self.converter.data_page_maxes(
            self.column_index,
            self.offset_index,
            [&self.row_group_index],
        ) {
            Ok(min_values) => Some(min_values),
            Err(e) => {
                debug!("Error evaluating data page max values {e}");
                None
            }
        }
    }

    fn num_containers(&self) -> usize {
        self.page_offsets.len()
    }

    fn null_counts(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        match self.converter.data_page_null_counts(
            self.column_index,
            self.offset_index,
            [&self.row_group_index],
        ) {
            Ok(null_counts) => Some(Arc::new(null_counts)),
            Err(e) => {
                debug!("Error evaluating data page null counts {e}");
                None
            }
        }
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        match self.converter.data_page_row_counts(
            self.offset_index,
            self.row_group_metadatas,
            [&self.row_group_index],
        ) {
            Ok(row_counts) => row_counts.map(|a| Arc::new(a) as ArrayRef),
            Err(e) => {
                debug!("Error evaluating data page row counts {e}");
                None
            }
        }
    }

    fn contained(
        &self,
        _column: &datafusion_common::Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

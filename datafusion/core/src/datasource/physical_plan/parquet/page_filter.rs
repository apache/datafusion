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
use crate::datasource::physical_plan::parquet::ParquetAccessPlan;

use arrow::array::BooleanArray;
use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::{split_conjunction, PhysicalExpr};
use datafusion_physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

use log::{debug, trace};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex};
use parquet::format::PageLocation;
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
///
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
    /// single column predicates (e.g. (`col = 5`) extracted from the overall
    /// predicate. Must all be true for a row to be included in the result.
    predicates: Vec<PruningPredicate>,
}

impl PagePruningAccessPlanFilter {
    /// Create a new [`PagePruningAccessPlanFilter`] from a physical
    /// expression.
    pub fn new(expr: &Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Self {
        // extract any single column predicates
        let predicates = split_conjunction(expr)
            .into_iter()
            .filter_map(|predicate| {
                let pp = match PruningPredicate::try_new(
                    Arc::clone(predicate),
                    Arc::clone(&schema),
                ) {
                    Ok(pp) => pp,
                    Err(e) => {
                        debug!("Ignoring error creating page pruning predicate: {e}");
                        return None;
                    }
                };

                if pp.always_true() {
                    debug!("Ignoring always true page pruning predicate: {predicate}");
                    return None;
                }

                if pp.required_columns().single_column().is_none() {
                    debug!("Ignoring multi-column page pruning predicate: {predicate}");
                    return None;
                }

                Some(pp)
            })
            .collect::<Vec<_>>();
        Self { predicates }
    }

    /// Returns an updated [`ParquetAccessPlan`] by applying predicates to the
    /// parquet page index, if any
    pub fn prune_plan_with_page_index(
        &self,
        mut access_plan: ParquetAccessPlan,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        parquet_metadata: &ParquetMetaData,
        file_metrics: &ParquetFileMetrics,
    ) -> ParquetAccessPlan {
        // scoped timer updates on drop
        let _timer_guard = file_metrics.page_index_eval_time.timer();
        if self.predicates.is_empty() {
            return access_plan;
        }

        let page_index_predicates = &self.predicates;
        let groups = parquet_metadata.row_groups();

        if groups.is_empty() {
            return access_plan;
        }

        if parquet_metadata.offset_index().is_none()
            || parquet_metadata.column_index().is_none()
        {
            debug!(
                    "Can not prune pages due to lack of indexes. Have offset: {}, column index: {}",
                    parquet_metadata.offset_index().is_some(), parquet_metadata.column_index().is_some()
                );
            return access_plan;
        };

        // track the total number of rows that should be skipped
        let mut total_skip = 0;
        // track the total number of rows that should not be skipped
        let mut total_select = 0;

        // for each row group specified in the access plan
        let row_group_indexes = access_plan.row_group_indexes();
        for row_group_index in row_group_indexes {
            // The selection for this particular row group
            let mut overall_selection = None;
            for predicate in page_index_predicates {
                let column = predicate
                    .required_columns()
                    .single_column()
                    .expect("Page pruning requires single column predicates");

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

                let Some(selection) = selection else {
                    trace!("No pages pruned in prune_pages_in_one_row_group");
                    continue;
                };

                debug!("Use filter and page index to create RowSelection {:?} from predicate: {:?}",
                    &selection,
                    predicate.predicate_expr(),
                );

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
                if overall_selection.selects_any() {
                    let rows_skipped = rows_skipped(&overall_selection);
                    let rows_selected = rows_selected(&overall_selection);
                    trace!("Overall selection from predicate skipped {rows_skipped}, selected {rows_selected}: {overall_selection:?}");
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
            }
        }

        file_metrics.page_index_rows_pruned.add(total_skip);
        file_metrics.page_index_rows_matched.add(total_select);
        access_plan
    }

    /// Returns the number of filters in the [`PagePruningAccessPlanFilter`]
    pub fn filter_number(&self) -> usize {
        self.predicates.len()
    }
}

/// returns the number of rows skipped in the selection
/// TODO should this be upstreamed to RowSelection?
fn rows_skipped(selection: &RowSelection) -> usize {
    selection
        .iter()
        .fold(0, |acc, x| if x.skip { acc + x.row_count } else { acc })
}

/// returns the number of rows not skipped in the selection
/// TODO should this be upstreamed to RowSelection?
fn rows_selected(selection: &RowSelection) -> usize {
    selection
        .iter()
        .fold(0, |acc, x| if x.skip { acc } else { acc + x.row_count })
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

/// Returns a [`RowSelection`] for the rows in this row group to scan.
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
) -> Option<RowSelection> {
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
    trace!("Pruned to {:?} using {:?}", values, pruning_stats);
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
    Some(RowSelection::from(vec))
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

    fn row_counts(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
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

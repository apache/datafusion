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

//! Sort-related utilities for Parquet scanning

use arrow::datatypes::Schema;
use datafusion_common::ScalarValue;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

/// Reorder a file list so the most "promising" files are read first,
/// matching `PreparedAccessPlan::reorder_by_statistics` at the
/// row-group level: key lexicographically off the file's per-column
/// `min` for the longest plain-`Column` prefix of the sort order, and
/// let the leading sort direction follow the request (ASC by `min`
/// for ASC requests, DESC by `min` for DESC requests).
///
/// Secondary sort keys break ties when the leading column's `min` is
/// equal across files (e.g. `ORDER BY low_cardinality_col, ts LIMIT k`),
/// mirroring the row-group level lexicographic reorder.
///
/// Keeping both layers consistent matters because they share the same
/// convergence story for TopK's dynamic filter: file `i`'s `min` is a
/// lower bound on every row group inside it, so the order chosen here
/// is a natural prefix of the order `reorder_by_statistics` will
/// produce within each file.
///
/// No-op when:
/// * `sort_order` is `None` (sort pushdown didn't fire);
/// * the leading sort expression is not a plain `Column`; or
/// * the column is not in `table_schema`.
///
/// Files missing statistics sort to the end so present-stats files
/// run first.
pub(crate) fn reorder_files_by_min_statistics(
    mut files: Vec<PartitionedFile>,
    sort_order: Option<&LexOrdering>,
    reverse_row_groups: bool,
    table_schema: &Schema,
) -> Vec<PartitionedFile> {
    let sort_keys = extract_topk_sort_info(sort_order, reverse_row_groups);
    if sort_keys.is_empty() {
        return files;
    }

    // Resolve names to column indexes; the leading key is required, later
    // keys are best-effort (stop at the first unresolvable one).
    let mut keys: Vec<(usize, bool)> = Vec::with_capacity(sort_keys.len());
    for (col_name, descending) in &sort_keys {
        match table_schema.index_of(col_name) {
            Ok(idx) => keys.push((idx, *descending)),
            Err(_) if keys.is_empty() => return files,
            Err(_) => break,
        }
    }

    files.sort_by(|a, b| {
        for &(col_idx, descending) in &keys {
            let key_a = file_min_value(a, col_idx);
            let key_b = file_min_value(b, col_idx);
            let ord = match (key_a, key_b) {
                (Some(va), Some(vb)) => {
                    let cmp = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                    if descending { cmp.reverse() } else { cmp }
                }
                // Missing stats always sort last, regardless of direction.
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    log::debug!(
        "Reordered {} files by lexicographic min of {:?} for TopK optimization",
        files.len(),
        sort_keys,
    );

    files
}

/// Extract the `(column name, descending)` keys used by file-level
/// reordering: the longest prefix of the sort order made of plain
/// `Column` expressions. Returns an empty vec when the sort order isn't
/// set or the leading sort expression isn't a plain `Column`.
///
/// The leading key's direction is `reverse_row_groups` (the pushdown's
/// authoritative flip decision, which may differ from the raw
/// expression's `descending` in the `reversed_satisfies` case);
/// subsequent keys apply their direction *relative to the leading
/// expression* on top of that flag, so a request like
/// `[a DESC, b ASC]` with `reverse_row_groups=true` sorts by
/// `(min(a) DESC, min(b) ASC)`.
fn extract_topk_sort_info(
    sort_order: Option<&LexOrdering>,
    reverse_row_groups: bool,
) -> Vec<(String, bool)> {
    let Some(sort_order) = sort_order else {
        return vec![];
    };
    let leading_descending = sort_order.first().options.descending;
    let mut keys = Vec::new();
    for sort_expr in sort_order.iter() {
        let Some(col) = sort_expr.expr.downcast_ref::<Column>() else {
            break;
        };
        let relative_desc = sort_expr.options.descending != leading_descending;
        keys.push((col.name().to_string(), reverse_row_groups != relative_desc));
    }
    keys
}

/// File's per-column `min` for the reorder key.
fn file_min_value(file: &PartitionedFile, col_idx: usize) -> Option<ScalarValue> {
    let stats = file.statistics.as_ref()?;
    stats
        .column_statistics
        .get(col_idx)?
        .min_value
        .get_value()
        .cloned()
}

#[cfg(test)]
mod tests {
    use crate::ParquetAccessPlan;
    use crate::RowGroupAccess;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::sync::Arc;

    /// Helper function to create a ParquetMetaData with specified row group sizes
    /// by actually writing a parquet file in memory
    fn create_test_metadata(
        row_group_sizes: Vec<i64>,
    ) -> parquet::file::metadata::ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let mut buffer = Vec::new();
        {
            let props = parquet::file::properties::WriterProperties::builder().build();
            let mut writer =
                ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

            for &size in &row_group_sizes {
                let array = arrow::array::Int32Array::from(vec![1; size as usize]);
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(array)],
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.flush().unwrap();
            }
            writer.close().unwrap();
        }

        let bytes = Bytes::from(buffer);
        let reader = SerializedFileReader::new(bytes).unwrap();
        reader.metadata().clone()
    }

    #[test]
    fn test_prepared_access_plan_reverse() {
        use RowGroupAccess::{Scan, Selection, Skip};
        let selection = |selectors: Vec<RowSelector>| Selection(selectors.into());
        // Exercise empty, single-group, non-contiguous, unequal-size, and
        // fragmented selections. Reverse group order, never local row order.
        let cases = vec![
            (vec![100, 100, 100], vec![Scan, Scan, Scan], vec![2, 1, 0]),
            (vec![100, 100, 100], vec![Skip, Skip, Skip], vec![]),
            (
                vec![100, 100, 100],
                vec![selection(vec![RowSelector::skip(100)]); 3],
                vec![],
            ),
            (
                vec![100],
                vec![selection(vec![
                    RowSelector::select(50),
                    RowSelector::skip(50),
                ])],
                vec![0],
            ),
            (
                vec![100, 100, 100],
                vec![
                    selection(vec![RowSelector::skip(50), RowSelector::select(50)]),
                    selection(vec![RowSelector::select(50), RowSelector::skip(50)]),
                    Scan,
                ],
                vec![2, 1, 0],
            ),
            (
                vec![50, 150, 100],
                vec![
                    selection(vec![RowSelector::skip(25), RowSelector::select(25)]),
                    Scan,
                    selection(vec![RowSelector::select(50), RowSelector::skip(50)]),
                ],
                vec![2, 1, 0],
            ),
            (
                vec![100, 100, 100, 100],
                vec![
                    Scan,
                    Skip,
                    selection(vec![RowSelector::select(25), RowSelector::skip(75)]),
                    Scan,
                ],
                vec![3, 2, 0],
            ),
            (
                vec![100, 100, 100, 100],
                vec![
                    selection(vec![RowSelector::select(30), RowSelector::skip(70)]),
                    Skip,
                    selection(vec![RowSelector::skip(20), RowSelector::select(80)]),
                    Skip,
                ],
                vec![2, 0],
            ),
            (vec![100, 100, 100], vec![Skip, Scan, Skip], vec![1]),
            (
                vec![100, 100, 100],
                vec![
                    selection(vec![
                        RowSelector::select(30),
                        RowSelector::skip(40),
                        RowSelector::select(30),
                    ]),
                    selection(vec![RowSelector::skip(50), RowSelector::select(50)]),
                    Scan,
                ],
                vec![2, 1, 0],
            ),
        ];
        for (sizes, accesses, expected_indexes) in cases {
            let metadata = create_test_metadata(sizes);
            let mut plan = ParquetAccessPlan::new(accesses.clone());
            for index in (0..accesses.len()).step_by(2) {
                plan.mark_fully_matched(index);
            }
            let reversed = plan.prepare(metadata.row_groups()).unwrap().reverse();
            assert_eq!(reversed.row_group_indexes(), expected_indexes);
            for rg in reversed.row_groups {
                let index = rg.selection.row_group_index();
                let expected = match &accesses[index] {
                    Scan => None,
                    Selection(selection) => Some(selection),
                    Skip => panic!("skipped group must not be read"),
                };
                assert_eq!(rg.selection.selection(), expected);
                assert_eq!(rg.fully_matched, index % 2 == 0);
            }
        }
    }

    #[test]
    fn test_prepared_access_plan_preserves_bitmap_selection() {
        let metadata = create_test_metadata(vec![4, 4]);
        let mask = arrow::buffer::BooleanBuffer::from(vec![true, false, true, false]);
        let plan = ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(RowSelection::from(mask.clone())),
            RowGroupAccess::Selection(RowSelection::from(vec![
                RowSelector::skip(1),
                RowSelector::select(3),
            ])),
        ]);
        let reversed = plan.prepare(metadata.row_groups()).unwrap().reverse();
        assert_eq!(reversed.row_group_indexes(), vec![1, 0]);
        assert_eq!(
            reversed.row_groups[1]
                .selection
                .selection()
                .unwrap()
                .as_mask(),
            Some(&mask)
        );
        assert_eq!(
            reversed.row_groups[0].selection.selection(),
            Some(&RowSelection::from(vec![
                RowSelector::skip(1),
                RowSelector::select(3)
            ]))
        );
    }
}

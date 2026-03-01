use std::cmp::Ordering;
use std::future;
use crate::joins::sort_merge_join::filter::get_filter_columns;
use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute;
use arrow::compute::{filter_record_batch, is_not_null, take_arrays};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{StreamExt, TryStreamExt};
use itertools::{Chunks, Itertools, Product};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::task::{Context, Poll};
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use crate::joins::sort_merge_join::stream::is_join_arrays_equal;
use crate::joins::utils::compare_join_arrays;
// pub(super) fn compare_join_batch(
//     left: &RecordBatch,
//     left_index: usize,
//     right: &RecordBatch,
//     right_index: usize,
// ) -> Ordering {
//     todo!()
// }
//
// /// `source[source_index]` is the value to compare to
// /// `batch[start_index]` is where to start comparing
// ///
// /// batch is ordered
// ///
// /// If nothing equals it will return `start_index`
// /// If everything is equal from `start_index` to end of the `batch` None will be returned
// pub(super) fn get_index_of_first_not_equal(
//     source_join_key: JoinKey,
//     target_join_key: &mut JoinKey,
// ) {
//     while !target_join_key.reached_end() && source_join_key == target_join_key {
//         target_join_key.advance();
//     }
//     if target_join_key.reached_end() {
//         return None;
//     }
//
//     return Some(target_join_key);
// }

pub(super) trait SendableRecordBatchStreamExt {
    /// Return the next batch that is not empty
    ///
    /// if all batches are empty and consumed the entire stream, None will be returned
    /// if in any of the results there was an error, return that.
    async fn next_non_empty(&mut self) -> Option<Result<RecordBatch>>;
}

impl SendableRecordBatchStreamExt for SendableRecordBatchStream {
    async fn next_non_empty(&mut self) -> Option<Result<RecordBatch>> {
        self.by_ref().skip_while(|b| match b {
            Ok(b) if b.num_rows() == 0 => future::ready(true),
            _ => future::ready(false)
        }).next().await
    }
}

pub(crate) async fn next_non_empty(stream: &mut SendableRecordBatchStream) -> Option<Result<RecordBatch>> {
    stream.by_ref().skip_while(|b| match b {
        Ok(b) if b.num_rows() == 0 => future::ready(true),
        _ => future::ready(false)
    }).next().await
}

pub(super) fn cartesian_product(
    left: (&RecordBatch, &Range<usize>),
    right: (&RecordBatch, &Range<usize>),
) -> Vec<RecordBatch> {
    todo!()
}

pub(super) struct RecordBatchJoin {
    schema: SchemaRef,
    max_output_batch_size: usize,
}

pub(super) struct SideInput {
    pub(super) batch: RecordBatch,
    pub(super) range: Range<usize>,
}

impl RecordBatchJoin {
    pub(super) fn new(output_schema: SchemaRef, max_output_batch_size: usize) -> Self {
        Self {
            schema: output_schema,
            max_output_batch_size,
        }
    }

    fn create_indices_to_cartesian_product(
        left: &Range<usize>,
        right: &Range<usize>,
        max_num_rows: usize,
    ) -> impl Iterator<Item = (UInt64Array, UInt64Array)> {
        IndicesChunkedCartesianProductIterator::new(
            left.clone(),
            right.clone(),
            max_num_rows,
        )
    }

    fn create_cartesian_product(
        left: SideInput,
        right: SideInput,
        max_num_rows: usize,
    ) -> impl Iterator<Item = Result<(RecordBatch, RecordBatch)>> {
        let left = left.batch.slice(left.range.start, left.range.len());

        let right_take_indices = UInt64Array::from_value(0, left.num_rows());

        right.range.into_iter().map(move |index| {
            let single_row_batch = right.batch.slice(index, 1);

            let right_batch_repeated = arrow::compute::take_record_batch(
                &single_row_batch,
                &right_take_indices,
            )?;

            Ok((left.clone(), right_batch_repeated))
        })
    }

    pub(super) fn create_output_batches<'a>(
        &'a self,
        left: SideInput,
        right: SideInput,
    ) -> impl Iterator<Item = Result<RecordBatch>> + 'a {
        assert_ne!(left.range.len(), 0, "left range must not be 0");
        assert_ne!(right.range.len(), 0, "right range must not be 0");
        Self::create_cartesian_product(left, right, self.max_output_batch_size).map(
            |chunk| {
                let (left, right) = chunk?;
                let left_columns = left.columns().to_vec();
                let right_columns = right.columns().to_vec();

                // TODO - on join type right right columns should be the first columns
                let columns = {
                    let mut output_columns = left_columns;
                    output_columns.extend(right_columns);

                    output_columns
                };

                RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(Into::into)
            },
        )
    }
}

#[derive(Clone)]
pub(super) struct JoinKey {
    pub(super) batch: RecordBatch,
    pub(super) join_keys: Vec<ArrayRef>,
    pub(super) index: usize,
}

impl JoinKey {
    pub(super) fn new(batch: RecordBatch, on_column: &[PhysicalExprRef]) -> Result<Self> {
        let join_keys = crate::joins::sort_merge_join::stream::join_arrays(&batch, on_column);

        Ok(Self {
            batch,
            join_keys,
            index: 0,
        })
    }

    pub(super) fn advance(&mut self) {
        self.index += 1;
    }

    pub(super) fn reached_end(&self) -> bool {
        self.index >= self.batch.num_rows()
    }

    pub(super) fn try_skip_while<F: FnMut(&Self) -> Result<bool, E>, E>(
        &mut self,
        mut skip_while_fn: F,
    ) -> Result<Range<usize>, E> {
        let start = self.index;
        let mut end = self.index + 1;
        while !self.reached_end() && skip_while_fn(self)? {
            self.advance();
            end += 1;
        }

        Ok(start..end)
    }

    pub(super) async fn replace_with_next(
        maybe_join_key: &mut Option<Self>,
        on_columns: &[PhysicalExprRef],
        stream: &mut SendableRecordBatchStream,
    ) -> Result<()> {
        let Some(join_key) = maybe_join_key.as_mut() else {
            return Ok(());
        };

        if join_key.reached_end() {
            let new_batch = stream.next_non_empty().await.transpose()?;

            // Finished
            if let Some(new_batch) = new_batch {
                *join_key = JoinKey::new(
                    new_batch,
                    on_columns
                )?;
            } else {
                *maybe_join_key = None;
            }
        }

        Ok(())
    }

    pub(super) fn get_ord(&self, other: &Self, null_equality: NullEquality, sort_options: &[SortOptions]) -> Result<Ordering> {

        compare_join_arrays(
            self.join_keys.as_slice(),
            self.index,
            &other.join_keys.as_slice(),
            other.index,
            sort_options,
            null_equality,
        )
    }

    pub(super) fn is_join_arrays_equal(&self, other: &Self) -> Result<bool> {
        is_join_arrays_equal(self.join_keys.as_slice(), self.index, other.join_keys.as_slice(), other.index)
    }
}

//
// pub(super) async fn progress_side_until_not_equal(
//     stream: &mut SendableRecordBatchStream,
//     start_join_key: JoinKey,
// ) -> Option<JoinKey> {
//     // TODO - avoid clone for single equal
//     output_builder.start_equal();
//     let mut join_key = start_join_key.clone();
//     let mut current_join_key_start_index = start_join_key.index;
//     // Advance index in left batch to get when not matched
//     // index_in_left_batch += 1;
//     // TODO - continue left until it is no longer equal to the last left
//     loop {
//         let start_index = join_key.index;
//         join_key.skip_while(|key| key == start_join_key);
//
//         if start_index != join_key.index {
//             output_builder.add_new(&join_key, start_index);
//         }
//
//         if !join_key.reached_end() {
//             output_builder.finish_equal();
//
//             return Some(join_key);
//         }
//
//         // Everything matched go to next batch
//         if let Some(new_batch) = stream.next_non_empty().await.transpose()? {
//             join_key = JoinKey {
//                 batch: new_batch,
//                 index: 0,
//             };
//         } else {
//             // Left is finished so we can wrap up as we finished everything
//             output_builder.finish_equal();
//
//             // since this is inner join we can discard the right side
//             // TODO - what if the prev left was equal?
//             return None;
//         }
//     }
// }
//
// pub(super) async fn progress_side_custom<P: ProgressSide>(
//     stream: &mut SendableRecordBatchStream,
//     start_join_key: JoinKey,
//     p: &mut P,
// ) -> Result<Option<JoinKey>> {
//     let mut join_key = start_join_key.clone();
//     let mut current_join_key_start_index = start_join_key.index;
//     // Advance index in left batch to get when not matched
//     // index_in_left_batch += 1;
//     // TODO - continue left until it is no longer equal to the last left
//     loop {
//         let start_index = join_key.index;
//         join_key.skip_while(|key| P::skip_predicate(key, &start_join_key));
//
//         if start_index != join_key.index {
//             p.add_matched(&join_key, start_index);
//         }
//
//         if !join_key.reached_end() {
//             return Ok(Some(join_key));
//         }
//
//         // Everything matched go to next batch
//         if let Some(new_batch) = stream.next_non_empty().await.transpose()? {
//             join_key = JoinKey {
//                 batch: new_batch,
//                 index: 0,
//             };
//         } else {
//             // since this is inner join we can discard the right side
//             // TODO - what if the prev left was equal?
//             return Ok(None);
//         }
//     }
// }

pub(super) trait ProgressSide {
    fn add_matched(&mut self, join_key: &JoinKey, start_index: usize);

    fn skip_predicate(start_join_key: &JoinKey, current_join_key: &JoinKey) -> bool;
}

struct IndicesChunkedCartesianProductIterator {
    num_rows: usize,
    cartesian_product_iter: Product<Range<u64>, Range<u64>>,
}

impl IndicesChunkedCartesianProductIterator {
    fn new(left: Range<usize>, right: Range<usize>, max_num_rows: usize) -> Self {
        Self {
            cartesian_product_iter: ((left.start as u64)..(left.end as u64))
                .cartesian_product((right.start as u64)..(right.end as u64)),
            num_rows: max_num_rows,
        }
    }
}

impl Iterator for IndicesChunkedCartesianProductIterator {
    type Item = (UInt64Array, UInt64Array);

    fn next(&mut self) -> Option<Self::Item> {
        let (left, right): (Vec<u64>, Vec<u64>) = self
            .cartesian_product_iter
            .by_ref()
            .take(self.num_rows)
            .unzip();

        assert_eq!(left.len(), right.len());

        if left.is_empty() {
            return None;
        }

        let left = UInt64Array::from(left);
        let right = UInt64Array::from(right);

        Some((left, right))
    }
}

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use futures::Stream;
use std::cmp::Ordering;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::helpers::*;

#[derive(Default)]
struct InnerJoinOutputBuilder {
    output_schema: SchemaRef,
    output_batches: Vec<RecordBatch>,
}

impl InnerJoinOutputBuilder {
    fn process(
        &mut self,
        left_output_builder: &mut InnerJoinSingleSideOutputBuilder,
        right_output_builder: &mut InnerJoinSingleSideOutputBuilder,
    ) -> _ {
        assert_ne!(left_output_builder.batches.len(), 0);
        assert_ne!(right_output_builder.batches.len(), 0);

        // Cartesian project between each index
        let interleave_indices = vec![];
        for (left_batch, left_range) in &left_output_builder.batches {
            assert_ne!(left_range.len(), 0);
            for (right_batch, right_range) in &right_output_builder.batches {
                assert_ne!(right_range.len(), 0);
                let new_output_batches = cartesian_product(
                    (left_batch, left_range),
                    (right_batch, right_range),
                );
                self.output_batches.extend_from_slice(&new_output_batches);
            }
        }
    }

    fn add_left_batch_with_null_rights(
        &self,
        batch: RecordBatch,
        index_in_left_batch: usize,
    ) {
        todo!()
    }

    fn add_rest_of_left_stream(&self, left_stream: SendableRecordBatchStream) {
        todo!()
    }
}

#[derive(Default)]
struct InnerJoinSingleSideOutputBuilder {
    /// Vec<(batch, range of equal values)>
    batches: Vec<(RecordBatch, Range<usize>)>,
}
impl InnerJoinSingleSideOutputBuilder {
    fn start_equal(&mut self) {
        todo!()
    }

    fn finish_equal(&mut self) {
        todo!()
    }

    fn add_new_batch_to_current_equal(
        &mut self,
        batch: &RecordBatch,
        start_index_equal: usize,
        end_index_equal: usize,
    ) {
        if start_index_equal == end_index_equal {
            // Nothing equal in this batch, skip
            return;
        }
        todo!()
    }
}

pub async fn left_join(
    left_stream: SendableRecordBatchStream,
    right_stream: SendableRecordBatchStream,
) -> Result<Vec<RecordBatch>> {
    let mut output_builder = InnerJoinOutputBuilder::default();
    let mut left_output_builder = InnerJoinSingleSideOutputBuilder::default();
    let mut right_output_builder = InnerJoinSingleSideOutputBuilder::default();

    let mut right_batch = right_stream.next_non_empty().await.transpose()?;

    if right_batch.is_none() {
        // If right stream is empty return left stream
        return left_stream;
    }

    let mut left_batch = left_stream.next_non_empty().await.transpose()?;

    if left_batch.is_none() {
        // No matches
        return Ok(create_empty_stream());
    }

    let mut index_in_left_batch = 0;
    let mut index_in_right_batch = 0;

    loop {
        if left_batch.is_none() || right_batch.is_none() {
            break;
        }

        match compare_join_batch(
            left_batch.as_ref().unwrap(),
            index_in_left_batch,
            right_batch.as_ref().unwrap(),
            index_in_right_batch,
        ) {
            // left < right
            Ordering::Less => {
                // TODO - don't do slice every time
                output_builder.add_left_batch_with_null_right(
                    left_batch.as_ref().unwrap().slice(index_in_left_batch, 1),
                );
                // If left is less than right
                index_in_left_batch += 1;
            }
            // left > right
            Ordering::Greater => {
                index_in_right_batch += 1;
            }
            // left == right
            Ordering::Equal => {
                match progress_side_until_not_equal(
                    &mut left_stream,
                    left_batch.unwrap(),
                    index_in_left_batch,
                    &mut left_output_builder,
                ) {
                    ProgressSideUntilNotEqualResult::ContinueFrom { batch, index } => {
                        left_batch = Some(batch);
                        index_in_left_batch = index;
                    }
                    ProgressSideUntilNotEqualResult::StreamFinished => {
                        left_batch = None;
                    }
                }

                match progress_side_until_not_equal(
                    &mut right_stream,
                    right_batch.unwrap(),
                    index_in_right_batch,
                    &mut right_output_builder,
                ) {
                    ProgressSideUntilNotEqualResult::ContinueFrom { batch, index } => {
                        right_batch = Some(batch);
                        index_in_right_batch = index;
                    }
                    ProgressSideUntilNotEqualResult::StreamFinished => {
                        right_batch = None;
                    }
                }

                output_builder
                    .process(&mut left_output_builder, &mut right_output_builder);
            }
        }

        if index_in_left_batch >= left_batch.num_rows() {
            left_batch = left_stream.next_non_empty().await.transpose()?;
            index_in_left_batch = 0;
        }

        if index_in_right_batch >= right_batch.num_rows() {
            right_batch = right_stream.next_non_empty().await.transpose()?;
            index_in_right_batch = 0;
        }
    }

    match (left_batch, right_batch) {
        (Some(_), Some(_)) => unreachable!(),
        (None, _) => {
            // Nothing more to add
        }
        (Some(batch), None) => {
            // If left batch was done in the middle add the rest
            output_builder.add_left_batch_with_null_rights(batch, index_in_left_batch);
            output_builder.add_rest_of_left_stream(left_stream);
        }
    }

    Ok(output_builder.output_batches)
}

enum ProgressSideUntilNotEqualResult {
    /// From where the join should continue from,
    ContinueFrom {
        batch: RecordBatch,
        index: usize,
    },
    StreamFinished,
}

async fn progress_side_until_not_equal(
    stream: &mut SendableRecordBatchStream,
    start_batch: RecordBatch,
    starting_batch_index: usize,
    output_builder: &mut InnerJoinSingleSideOutputBuilder,
) -> ProgressSideUntilNotEqualResult {
    let mut index_in_batch = starting_batch_index;
    // TODO - avoid clone for single equal
    output_builder.start_equal();
    let batch = start_batch.clone();
    // Advance index in left batch to get when not matched
    // index_in_left_batch += 1;
    // TODO - continue left until it is no longer equal to the last left
    loop {
        if let Some(next_index) = get_index_of_first_not_equal(
            &start_batch,
            starting_batch_index,
            &batch,
            index_in_batch,
        ) {
            output_builder.add_new_batch_to_current_equal(
                &batch,
                index_in_batch,
                next_index,
            );
            output_builder.finish_equal();
            index_in_batch = next_index;
            return ProgressSideUntilNotEqualResult::ContinueFrom {
                batch,
                index: index_in_batch,
            };
        }
        output_builder.add_new_batch_to_current_equal(
            &batch,
            index_in_batch,
            batch.num_rows(),
        );
        // Everything matched go to next batch
        //
        if let Some(new_batch) = stream.next_non_empty().await.transpose()? {
            index_in_batch = 0;
            batch = new_batch;
        } else {
            // Left is finished so we can wrap up as we finished everything
            output_builder.finish_equal();

            // since this is inner join we can discard the right side
            // TODO - what if the prev left was equal?
            return ProgressSideUntilNotEqualResult::StreamFinished;
        }
    }
}

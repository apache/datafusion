use arrow::array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, NullEquality, Result};
use datafusion_execution::SendableRecordBatchStream;
use futures::Stream;
use std::cmp::Ordering;
use std::ops::Range;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Sender;
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use super::helpers::*;

struct InnerJoinOutputBuilder {
    output_join: RecordBatchJoin,
    // output_batches: Vec<RecordBatch>,
}

impl InnerJoinOutputBuilder {

    fn new(schema: SchemaRef,
           max_output_batch_size: usize) -> Self {
        Self {
            output_join: RecordBatchJoin::new(schema, max_output_batch_size),
            // output_batches: vec![],
        }
    }

    fn process(
        &self,
        left_output_builder: InnerJoinSingleSideOutputBuilder,
        right_output_builder: InnerJoinSingleSideOutputBuilder,
    ) -> impl Iterator<Item=Result<RecordBatch>> {
        assert_ne!(left_output_builder.batches.len(), 0);
        assert_ne!(right_output_builder.batches.len(), 0);

        left_output_builder.batches.into_iter()
          .flat_map(move |(left_batch, left_range)| {
              let right_batches = right_output_builder.batches.clone();
              right_batches.into_iter().flat_map(move |(right_batch, right_range)| {
                  // TODO - if we have multiple equal batches we will have all first left cartesian product with all right and then next left batch and so on
                  self.output_join.create_output_batches(
                      SideInput {
                          batch: left_batch.clone(),
                          range: left_range.clone()
                      },
                      SideInput {
                          batch: right_batch.clone(),
                          range: right_range.clone()
                      },
                  )
              })
          })
    }
}

#[derive(Default)]
struct InnerJoinSingleSideOutputBuilder {
    /// Vec<(batch, range of equal values)>
    batches: Vec<(RecordBatch, Range<usize>)>,
}
impl InnerJoinSingleSideOutputBuilder {
    fn start_equal(&mut self) {
        // todo!()
    }

    fn finish_equal(&mut self) {

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

    fn add_new(&mut self, batch: &RecordBatch, equal_range: Range<usize>) {
        self.batches.push((batch.clone(), equal_range));
    }
}

pub(crate) async fn inner_join(
    output_schema: SchemaRef,
    max_output_batch_size: usize,
    mut left_stream: SendableRecordBatchStream,
    on_left: Vec<PhysicalExprRef>,
    mut right_stream: SendableRecordBatchStream,
    on_right: Vec<PhysicalExprRef>,
    sender: Sender<Result<RecordBatch>>,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
) -> Result<()> {
    let mut output_builder = InnerJoinOutputBuilder::new(output_schema, max_output_batch_size);

    let left_batch = left_stream.next_non_empty().await.transpose()?;
    let Some(left_batch) = left_batch else {
        // No matches, empty stream
        return Ok(());
    };

    let right_batch = right_stream.next_non_empty().await.transpose()?;
    let Some(right_batch) = right_batch else {
        // No matches, empty stream
        return Ok(());
    };

    let mut maybe_left_join_key = Some(JoinKey::new(left_batch, &on_left)?);
    let mut maybe_right_join_key = Some(JoinKey::new(right_batch, &on_right)?);

    loop {

        let Some(left_join_key) = maybe_left_join_key.as_mut() else {
            break;
        };
        let Some(right_join_key) = maybe_right_join_key.as_mut() else {
            break;
        };

        let mut left_output_builder = InnerJoinSingleSideOutputBuilder::default();
        let mut right_output_builder = InnerJoinSingleSideOutputBuilder::default();

        match left_join_key.get_ord(right_join_key, null_equality, sort_options.as_slice())? {
            // left < right
            Ordering::Less => {
                // If left is less than right
                // TODO - optimization - advance left until not less than right join key
                left_join_key.try_skip_while(|left| {
                    left.get_ord(right_join_key, null_equality, sort_options.as_slice()).map(|ord| ord == Ordering::Less)
                })?;
            }
            // left > right
            Ordering::Greater => {
                // TODO - optimization - advance right until not less than left join key
                right_join_key.try_skip_while(|right| {
                    left_join_key.get_ord(right, null_equality, sort_options.as_slice()).map(|ord| ord == Ordering::Greater)
                })?;
            }
            // left == right
            Ordering::Equal => {
                match progress_side_until_not_equal(
                    &mut left_stream,
                    left_join_key,
                    &mut left_output_builder,
                    on_left.as_slice(),
                ).await? {
                    ProgressSideUntilNotEqualResult::ContinueFrom(new_join_key) => {
                        maybe_left_join_key = Some(new_join_key);
                    }
                    ProgressSideUntilNotEqualResult::StreamFinished => {
                        maybe_left_join_key = None;
                    }
                }

                match progress_side_until_not_equal(
                    &mut right_stream,
                    right_join_key,
                    &mut right_output_builder,
                    on_right.as_slice(),
                ).await? {
                    ProgressSideUntilNotEqualResult::ContinueFrom(new_join_key) => {
                        maybe_right_join_key = Some(new_join_key);
                    }
                    ProgressSideUntilNotEqualResult::StreamFinished => {
                        maybe_right_join_key = None;
                    }
                }

                let output_batches = output_builder
                    .process(left_output_builder, right_output_builder);

                for output_batch in output_batches {
                    sender.send(output_batch).await.map_err(|e| DataFusionError::External(Box::new(e)))?;
                }
            }
        }

        let Some(left_join_key) = maybe_left_join_key.as_mut() else {
            break;
        };

        if left_join_key.reached_end() {
            let new_batch = left_stream.next_non_empty().await.transpose()?;

            // Finished
            if let Some(new_batch) = new_batch {
                *left_join_key = JoinKey::new(new_batch, &on_left)?;
            } else {
                break;
            }
        }


        let Some(right_join_key) = maybe_right_join_key.as_mut() else {
            break;
        };

        if right_join_key.reached_end() {
            let new_batch = right_stream.next_non_empty().await.transpose()?;

            if let Some(new_batch) = new_batch {
                *right_join_key = JoinKey::new(new_batch, &on_right)?;
            } else {
                break;
            }
        }
    }

    Ok(())
}

enum ProgressSideUntilNotEqualResult {
    /// From where the join should continue from,
    ContinueFrom(JoinKey),
    StreamFinished,
}

async fn progress_side_until_not_equal(
    stream: &mut SendableRecordBatchStream,
    start_join_key: &JoinKey,
    output_builder: &mut InnerJoinSingleSideOutputBuilder,
    on_column: &[PhysicalExprRef],
) -> Result<ProgressSideUntilNotEqualResult> {
    // TODO - avoid clone for single equal
    output_builder.start_equal();
    let mut join_key = start_join_key.clone();
    let mut current_join_key_start_index = start_join_key.index;
    // Advance index in left batch to get when not matched
    // index_in_left_batch += 1;
    // TODO - continue left until it is no longer equal to the last left
    loop {
        let skipped_range = join_key.try_skip_while(|key| key.is_join_arrays_equal(start_join_key))?;

        output_builder.add_new(&join_key.batch, skipped_range);

        if !join_key.reached_end() {
            output_builder.finish_equal();

            return Ok(ProgressSideUntilNotEqualResult::ContinueFrom(join_key));
        }

        // Everything matched go to next batch
        if let Some(new_batch) = stream.next_non_empty().await.transpose()? {
            join_key = JoinKey::new(new_batch, on_column)?;
        } else {
            // Left is finished so we can wrap up as we finished everything
            output_builder.finish_equal();

            // since this is inner join we can discard the right side
            // TODO - what if the prev left was equal?
            return Ok(ProgressSideUntilNotEqualResult::StreamFinished);
        }
    }
}

trait ProgressSide {
    fn add_matched(&mut self, join_key: &JoinKey, start_index: usize);

    fn finish(&mut self, join_key: Option<JoinKey>);

    fn skip_predicate(start_join_key: &JoinKey, current_join_key: &JoinKey);
}


pub(crate) struct InnerJoinStream {

}

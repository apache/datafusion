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

//! Tracking the rows of an input through an output by a column of unique row
//! ids, such as the one `SourceSpec::with_row_ids` adds.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::array::{AsArray, RecordBatch, UInt64Array};
use arrow::datatypes::UInt64Type;
use datafusion_common::{Result, plan_datafusion_err};

/// Where a row is in a set of batches split into partitions: its partition,
/// and its position among the rows of that partition, counted across its
/// batches
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowPosition {
    /// The partition
    pub partition: usize,
    /// The position of the row in its partition
    pub row: usize,
}

/// Returns, for each row of each partition of `output`, the position in
/// `input` of the input row it copies, both given as the batches of each
/// partition. Each input row is identified by the id in column `input_ids`
/// of the input batches, which the output copies into column `output_ids` of
/// its batches. Both columns must be `UInt64`. An output row without an id,
/// such as a row an outer join pads with nulls, or with an id that no input
/// row has, such as a row of another input, has no position.
///
/// Returns `None` if the input ids are not unique or an input row has no
/// id, so that rows cannot be tracked, and an error if a batch does not have
/// such a column.
pub fn input_positions(
    input: &[Vec<RecordBatch>],
    input_ids: usize,
    output: &[Vec<RecordBatch>],
    output_ids: usize,
) -> Result<Option<Vec<Vec<Option<RowPosition>>>>> {
    // The position of every input row, by id
    let mut positions: HashMap<u64, RowPosition> = HashMap::new();
    for (partition, batches) in input.iter().enumerate() {
        let mut row = 0;
        for batch in batches {
            for id in row_ids(batch, input_ids)? {
                let Some(id) = id else {
                    return Ok(None);
                };
                if positions
                    .insert(id, RowPosition { partition, row })
                    .is_some()
                {
                    return Ok(None);
                }
                row += 1;
            }
        }
    }
    output
        .iter()
        .map(|batches| {
            let mut rows = vec![];
            for batch in batches {
                let ids = row_ids(batch, output_ids)?;
                rows.extend(ids.map(|id| id.and_then(|id| positions.get(&id).copied())));
            }
            Ok(rows)
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

/// A row of an output partition that copies an input row which comes before
/// the input row copied by an earlier row of the same output partition, from
/// the same input partition (see [`order_breaks`])
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBreak {
    /// The output partition
    pub output_partition: usize,
    /// The position in the output partition of the row that breaks the order
    pub output_row: usize,
    /// The input row it copies
    pub input_row: RowPosition,
    /// The position in the output partition of the earlier row
    pub earlier_output_row: usize,
    /// The input row the earlier row copies, which comes after `input_row`
    /// in the same input partition
    pub earlier_input_row: RowPosition,
}

/// Returns where the output partitions described by `positions` (see
/// [`input_positions`]) do not keep the rows of each input partition in
/// their input order: for each output partition and each input partition,
/// the first output row that copies an input row which comes before the
/// input row of an earlier output row from the same input partition, with
/// the latest such earlier row. Rows of other input partitions, and rows
/// without a position, are ignored, so rows of several input partitions may
/// be interleaved in any way. A row may repeat, as long as no row of the same
/// input partition that comes later in the input is between its copies.
pub fn order_breaks(positions: &[Vec<Option<RowPosition>>]) -> Vec<OrderBreak> {
    let mut breaks = vec![];
    for (output_partition, rows) in positions.iter().enumerate() {
        // The latest input row seen so far of each input partition, and the
        // output row that copies it
        let mut latest: BTreeMap<usize, (usize, RowPosition)> = BTreeMap::new();
        let mut broken: HashSet<usize> = HashSet::new();
        for (output_row, position) in rows.iter().enumerate() {
            let Some(position) = *position else {
                continue;
            };
            if broken.contains(&position.partition) {
                continue;
            }
            match latest.get(&position.partition) {
                Some(&(earlier_output_row, earlier_input_row))
                    if position.row < earlier_input_row.row =>
                {
                    broken.insert(position.partition);
                    breaks.push(OrderBreak {
                        output_partition,
                        output_row,
                        input_row: position,
                        earlier_output_row,
                        earlier_input_row,
                    });
                }
                _ => {
                    latest.insert(position.partition, (output_row, position));
                }
            }
        }
    }
    breaks
}

/// Where the rows of an input appear in an output, see [`input_order`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputOrder {
    /// The rows cannot be tracked: the input ids are not unique, or no output
    /// row has an id of the input
    Untracked,
    /// Every row of the output is a row of the input, each output partition
    /// has rows of one input partition only, and they appear in the order of
    /// that partition. A row can repeat, as long as its copies are next to
    /// each other.
    Kept {
        /// The number of input partitions whose rows are in the output
        input_partitions: usize,
        /// The largest number of distinct input rows in one output partition
        max_partition_rows: usize,
    },
    /// Row `row` of output partition `partition` breaks the order: it is not
    /// a row of the input, it is a row of another input partition than the
    /// rows before it, or it comes before the row before it in its input
    /// partition
    Broken { partition: usize, row: usize },
}

/// Returns where the rows of `input` appear in `output`, both given as the
/// batches of each partition. Each input row is identified by the id in
/// column `input_ids` of the input batches, which the output copies into
/// column `output_ids` of its batches. Both columns must be `UInt64`. Positions
/// count rows across the batches of a partition.
///
/// Returns an error if a batch does not have such a column.
pub fn input_order(
    input: &[Vec<RecordBatch>],
    input_ids: usize,
    output: &[Vec<RecordBatch>],
    output_ids: usize,
) -> Result<InputOrder> {
    Ok(
        match input_positions(input, input_ids, output, output_ids)? {
            Some(positions) => input_order_of(&positions),
            None => InputOrder::Untracked,
        },
    )
}

/// Returns where the rows of an input appear in an output, from the position
/// of the input row each output row copies (see [`input_positions`]), as
/// [`input_order`] does
pub fn input_order_of(positions: &[Vec<Option<RowPosition>>]) -> InputOrder {
    if positions.iter().flatten().all(Option::is_none) {
        return InputOrder::Untracked;
    }
    let mut input_partitions = HashSet::new();
    let mut max_partition_rows = 0;
    for (partition, rows) in positions.iter().enumerate() {
        let mut previous: Option<RowPosition> = None;
        let mut distinct = 0;
        for (row, current) in rows.iter().enumerate() {
            let Some(current) = *current else {
                return InputOrder::Broken { partition, row };
            };
            match previous {
                Some(previous) if current == previous => continue,
                Some(previous)
                    if current.partition != previous.partition
                        || current.row < previous.row =>
                {
                    return InputOrder::Broken { partition, row };
                }
                _ => {}
            }
            previous = Some(current);
            distinct += 1;
            input_partitions.insert(current.partition);
        }
        max_partition_rows = max_partition_rows.max(distinct);
    }
    InputOrder::Kept {
        input_partitions: input_partitions.len(),
        max_partition_rows,
    }
}

/// The values of the `UInt64` column `column` of `batch`
fn row_ids(
    batch: &RecordBatch,
    column: usize,
) -> Result<impl Iterator<Item = Option<u64>> + '_> {
    let ids: &UInt64Array = batch
        .columns()
        .get(column)
        .and_then(|column| column.as_primitive_opt::<UInt64Type>())
        .ok_or_else(|| {
            plan_datafusion_err!("the batch has no UInt64 row id column at {column}")
        })?;
    Ok(ids.iter())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    /// A batch with an `Int32` column and a row id column
    fn batch(ids: &[Option<u64>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            Field::new("id", DataType::UInt64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0; ids.len()])),
                Arc::new(UInt64Array::from(ids.to_vec())),
            ],
        )
        .unwrap()
    }

    /// The input: ids 0 to 4 in partition 0, and 10 to 12 in partition 1
    fn input() -> Vec<Vec<RecordBatch>> {
        let ids = |ids: std::ops::Range<u64>| ids.map(Some).collect::<Vec<_>>();
        vec![
            vec![batch(&ids(0..2)), batch(&[]), batch(&ids(2..5))],
            vec![batch(&ids(10..13))],
        ]
    }

    fn order(output: &[&[Option<u64>]]) -> InputOrder {
        let output: Vec<Vec<RecordBatch>> =
            output.iter().map(|ids| vec![batch(ids)]).collect();
        input_order(&input(), 1, &output, 1).unwrap()
    }

    #[test]
    fn kept_order() {
        // Subsequences of the input partitions, with repeated rows
        assert_eq!(
            order(&[&[Some(0), Some(2), Some(2), Some(4)], &[], &[Some(11)]]),
            InputOrder::Kept {
                input_partitions: 2,
                max_partition_rows: 3,
            }
        );
    }

    #[test]
    fn broken_order() {
        // Out of order
        assert_eq!(
            order(&[&[Some(1), Some(0)]]),
            InputOrder::Broken {
                partition: 0,
                row: 1
            }
        );
        // A repeated row that is not next to its copy
        assert_eq!(
            order(&[&[Some(0), Some(1), Some(0)]]),
            InputOrder::Broken {
                partition: 0,
                row: 2
            }
        );
        // Rows of two input partitions in one output partition
        assert_eq!(
            order(&[&[Some(12)], &[Some(3), Some(10)]]),
            InputOrder::Broken {
                partition: 1,
                row: 1
            }
        );
        // A row that is not a row of the input, or has no id
        assert_eq!(
            order(&[&[Some(0), Some(99)]]),
            InputOrder::Broken {
                partition: 0,
                row: 1
            }
        );
        assert_eq!(
            order(&[&[Some(0)], &[None]]),
            InputOrder::Broken {
                partition: 1,
                row: 0
            }
        );
    }

    fn breaks(output: &[&[Option<u64>]]) -> Vec<OrderBreak> {
        let output: Vec<Vec<RecordBatch>> =
            output.iter().map(|ids| vec![batch(ids)]).collect();
        let positions = input_positions(&input(), 1, &output, 1).unwrap().unwrap();
        order_breaks(&positions)
    }

    fn at(partition: usize, row: usize) -> RowPosition {
        RowPosition { partition, row }
    }

    #[test]
    fn positions_of_output_rows() {
        let output = vec![vec![batch(&[Some(11), None, Some(99), Some(2)])]];
        assert_eq!(
            input_positions(&input(), 1, &output, 1).unwrap(),
            Some(vec![vec![Some(at(1, 1)), None, None, Some(at(0, 2))]])
        );
        // Input ids that are not unique
        let input = vec![vec![batch(&[Some(0), Some(0)])]];
        assert_eq!(input_positions(&input, 1, &output, 1).unwrap(), None);
    }

    #[test]
    fn interleaved_partitions_in_order() {
        // Rows of both input partitions interleaved, with repeated rows, rows
        // without an id and rows of another input
        assert_eq!(
            breaks(&[
                &[
                    Some(10),
                    Some(0),
                    Some(0),
                    Some(11),
                    None,
                    Some(2),
                    Some(99)
                ],
                &[Some(1), Some(1), Some(12)]
            ]),
            vec![]
        );
    }

    #[test]
    fn interleaved_partitions_out_of_order() {
        assert_eq!(
            breaks(&[
                &[Some(10), Some(2), Some(0), Some(12), Some(11), Some(1)],
                &[Some(3), Some(4), Some(3)]
            ]),
            vec![
                // Only the first break of each input partition
                OrderBreak {
                    output_partition: 0,
                    output_row: 2,
                    input_row: at(0, 0),
                    earlier_output_row: 1,
                    earlier_input_row: at(0, 2),
                },
                OrderBreak {
                    output_partition: 0,
                    output_row: 4,
                    input_row: at(1, 1),
                    earlier_output_row: 3,
                    earlier_input_row: at(1, 2),
                },
                // A repeated row that is not next to its copy
                OrderBreak {
                    output_partition: 1,
                    output_row: 2,
                    input_row: at(0, 3),
                    earlier_output_row: 1,
                    earlier_input_row: at(0, 4),
                },
            ]
        );
    }

    #[test]
    fn untracked_rows() {
        // No output row is a row of the input
        assert_eq!(order(&[&[Some(99)], &[None]]), InputOrder::Untracked);
        assert_eq!(order(&[&[]]), InputOrder::Untracked);
        // Input ids that are not unique
        let input = vec![vec![batch(&[Some(0), Some(0)])]];
        let output = vec![vec![batch(&[Some(0)])]];
        assert_eq!(
            input_order(&input, 1, &output, 1).unwrap(),
            InputOrder::Untracked
        );
        // A column that is not a row id column is an error
        assert!(input_order(&input, 0, &output, 1).is_err());
    }
}

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

//! `nth_value` window function implementation

use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::OnceLock;

use crate::utils::{get_scalar_value_from_args, get_signed_integer};
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::window_doc_sections::DOC_SECTION_RANKING;
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::{
    Documentation, PartitionEvaluator, ReversedUDWF, Signature, TypeSignature,
    Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use field::WindowUDFFieldArgs;

define_udwf_and_expr!(
    First,
    first_value,
    "returns the first value in the window frame",
    NthValue::first
);
define_udwf_and_expr!(
    Last,
    last_value,
    "returns the last value in the window frame",
    NthValue::last
);
define_udwf_and_expr!(
    NthValue,
    nth_value,
    "returns the nth value in the window frame",
    NthValue::nth
);

/// Tag to differentiate special use cases of the NTH_VALUE built-in window function.
#[derive(Debug, Copy, Clone)]
pub enum NthValueKind {
    First,
    Last,
    Nth,
}

impl NthValueKind {
    fn name(&self) -> &'static str {
        match self {
            NthValueKind::First => "first",
            NthValueKind::Last => "last",
            NthValueKind::Nth => "nth",
        }
    }
}

#[derive(Debug)]
pub struct NthValue {
    signature: Signature,
    kind: NthValueKind,
}

impl NthValue {
    /// Create a new `nth_value` function
    pub fn new(kind: NthValueKind) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(0),
                    TypeSignature::Exact(vec![DataType::UInt64]),
                ],
                Volatility::Immutable,
            ),
            kind,
        }
    }

    pub fn first() -> Self {
        Self::new(NthValueKind::First)
    }

    pub fn last() -> Self {
        Self::new(NthValueKind::Last)
    }
    pub fn nth() -> Self {
        Self::new(NthValueKind::Nth)
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_ntile_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_RANKING)
            .with_description(
                "Integer ranging from 1 to the argument value, dividing the partition as equally as possible",
            )
            .build()
            .unwrap()
    })
}

impl WindowUDFImpl for NthValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let state = NthValueState {
            finalized_result: None,
            kind: self.kind,
        };

        let n: i64 = if matches!(self.kind, NthValueKind::Nth) {
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 1)?
                .map(get_signed_integer)
                .map_or(Ok(0), |v| v)
                .map(|val| {
                    if partition_evaluator_args.is_reversed() {
                        -val
                    } else {
                        val
                    }
                })?
        } else {
            0
        };

        Ok(Box::new(NthValueEvaluator {
            state,
            ignore_nulls: partition_evaluator_args.ignore_nulls(),
            n,
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        let nullable = false;

        Ok(Field::new(field_args.name(), DataType::UInt64, nullable))
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        match self.kind {
            NthValueKind::First => ReversedUDWF::Reversed(last_value_udwf()),
            NthValueKind::Last => ReversedUDWF::Reversed(first_value_udwf()),
            NthValueKind::Nth => ReversedUDWF::Reversed(nth_value_udwf()),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_ntile_doc())
    }
}

#[derive(Debug, Clone)]
pub struct NthValueState {
    // In certain cases, we can finalize the result early. Consider this usage:
    // ```
    //  FIRST_VALUE(increasing_col) OVER window AS my_first_value
    //  WINDOW (ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS window
    // ```
    // The result will always be the first entry in the table. We can store such
    // early-finalizing results and then just reuse them as necessary. This opens
    // opportunities to prune our datasets.
    pub finalized_result: Option<ScalarValue>,
    pub kind: NthValueKind,
}

#[derive(Debug)]
pub(crate) struct NthValueEvaluator {
    state: NthValueState,
    ignore_nulls: bool,
    n: i64,
}

impl PartitionEvaluator for NthValueEvaluator {
    /// When the window frame has a fixed beginning (e.g UNBOUNDED PRECEDING),
    /// for some functions such as FIRST_VALUE, LAST_VALUE and NTH_VALUE, we
    /// can memoize the result.  Once result is calculated, it will always stay
    /// same. Hence, we do not need to keep past data as we process the entire
    /// dataset.
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let out = &state.out_col;
        let size = out.len();
        let mut buffer_size = 1;
        // Decide if we arrived at a final result yet:
        let (is_prunable, is_reverse_direction) = match self.state.kind {
            NthValueKind::First => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                (n_range > 0 && size > 0, false)
            }
            NthValueKind::Last => (true, true),
            NthValueKind::Nth => {
                let n_range =
                    state.window_frame_range.end - state.window_frame_range.start;
                match self.n.cmp(&0) {
                    Ordering::Greater => (
                        n_range >= (self.n as usize) && size > (self.n as usize),
                        false,
                    ),
                    Ordering::Less => {
                        let reverse_index = (-self.n) as usize;
                        buffer_size = reverse_index;
                        // Negative index represents reverse direction.
                        (n_range >= reverse_index, true)
                    }
                    Ordering::Equal => (false, false),
                }
            }
        };
        // Do not memoize results when nulls are ignored.
        if is_prunable && !self.ignore_nulls {
            if self.state.finalized_result.is_none() && !is_reverse_direction {
                let result = ScalarValue::try_from_array(out, size - 1)?;
                self.state.finalized_result = Some(result);
            }
            state.window_frame_range.start =
                state.window_frame_range.end.saturating_sub(buffer_size);
        }
        Ok(())
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        if let Some(ref result) = self.state.finalized_result {
            Ok(result.clone())
        } else {
            // FIRST_VALUE, LAST_VALUE, NTH_VALUE window functions take a single column, values will have size 1.
            let arr = &values[0];
            let n_range = range.end - range.start;
            if n_range == 0 {
                // We produce None if the window is empty.
                return ScalarValue::try_from(arr.data_type());
            }

            // Extract valid indices if ignoring nulls.
            let valid_indices = if self.ignore_nulls {
                // Calculate valid indices, inside the window frame boundaries
                let slice = arr.slice(range.start, n_range);
                let valid_indices = slice
                    .nulls()
                    .map(|nulls| {
                        nulls
                            .valid_indices()
                            // Add offset `range.start` to valid indices, to point correct index in the original arr.
                            .map(|idx| idx + range.start)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if valid_indices.is_empty() {
                    return ScalarValue::try_from(arr.data_type());
                }
                Some(valid_indices)
            } else {
                None
            };
            match self.state.kind {
                NthValueKind::First => {
                    if let Some(valid_indices) = &valid_indices {
                        ScalarValue::try_from_array(arr, valid_indices[0])
                    } else {
                        ScalarValue::try_from_array(arr, range.start)
                    }
                }
                NthValueKind::Last => {
                    if let Some(valid_indices) = &valid_indices {
                        ScalarValue::try_from_array(
                            arr,
                            valid_indices[valid_indices.len() - 1],
                        )
                    } else {
                        ScalarValue::try_from_array(arr, range.end - 1)
                    }
                }
                NthValueKind::Nth => {
                    match self.n.cmp(&0) {
                        Ordering::Greater => {
                            // SQL indices are not 0-based.
                            let index = (self.n as usize) - 1;
                            if index >= n_range {
                                // Outside the range, return NULL:
                                ScalarValue::try_from(arr.data_type())
                            } else if let Some(valid_indices) = valid_indices {
                                if index >= valid_indices.len() {
                                    return ScalarValue::try_from(arr.data_type());
                                }
                                ScalarValue::try_from_array(&arr, valid_indices[index])
                            } else {
                                ScalarValue::try_from_array(arr, range.start + index)
                            }
                        }
                        Ordering::Less => {
                            let reverse_index = (-self.n) as usize;
                            if n_range < reverse_index {
                                // Outside the range, return NULL:
                                ScalarValue::try_from(arr.data_type())
                            } else if let Some(valid_indices) = valid_indices {
                                if reverse_index > valid_indices.len() {
                                    return ScalarValue::try_from(arr.data_type());
                                }
                                let new_index =
                                    valid_indices[valid_indices.len() - reverse_index];
                                ScalarValue::try_from_array(&arr, new_index)
                            } else {
                                ScalarValue::try_from_array(
                                    arr,
                                    range.start + n_range - reverse_index,
                                )
                            }
                        }
                        Ordering::Equal => ScalarValue::try_from(arr.data_type()),
                    }
                }
            }
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn uses_window_frame(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {

    // fn test_i32_result(expr: NthValue, expected: Int32Array) -> Result<()> {
    //     let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
    //     let values = vec![arr];
    //     let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
    //     let batch = RecordBatch::try_new(Arc::new(schema), values.clone())?;
    //     let mut ranges: Vec<Range<usize>> = vec![];
    //     for i in 0..8 {
    //         ranges.push(Range {
    //             start: 0,
    //             end: i + 1,
    //         })
    //     }
    //     let mut evaluator = expr.create_evaluator()?;
    //     let values = expr.evaluate_args(&batch)?;
    //     let result = ranges
    //         .iter()
    //         .map(|range| evaluator.evaluate(&values, range))
    //         .collect::<Result<Vec<ScalarValue>>>()?;
    //     let result = ScalarValue::iter_to_array(result.into_iter())?;
    //     let result = as_int32_array(&result)?;
    //     assert_eq!(expected, *result);
    //     Ok(())
    // }
    //
    // // #[test]
    // // fn first_value() -> Result<()> {
    // //     let first_value = NthValue::first(
    // //         "first_value".to_owned(),
    // //         Arc::new(Column::new("arr", 0)),
    // //         DataType::Int32,
    // //         false,
    // //     );
    // //     test_i32_result(first_value, Int32Array::from(vec![1; 8]))?;
    // //     Ok(())
    // // }
    // //
    // // #[test]
    // // fn last_value() -> Result<()> {
    // //     let last_value = NthValue::last(
    // //         "last_value".to_owned(),
    // //         Arc::new(Column::new("arr", 0)),
    // //         DataType::Int32,
    // //         false,
    // //     );
    // //     test_i32_result(
    // //         last_value,
    // //         Int32Array::from(vec![
    // //             Some(1),
    // //             Some(-2),
    // //             Some(3),
    // //             Some(-4),
    // //             Some(5),
    // //             Some(-6),
    // //             Some(7),
    // //             Some(8),
    // //         ]),
    // //     )?;
    // //     Ok(())
    // // }
    // //
    // // #[test]
    // // fn nth_value_1() -> Result<()> {
    // //     let nth_value = NthValue::nth(
    // //         "nth_value".to_owned(),
    // //         Arc::new(Column::new("arr", 0)),
    // //         DataType::Int32,
    // //         1,
    // //         false,
    // //     )?;
    // //     test_i32_result(nth_value, Int32Array::from(vec![1; 8]))?;
    // //     Ok(())
    // // }
    // //
    // // #[test]
    // // fn nth_value_2() -> Result<()> {
    // //     let nth_value = NthValue::nth(
    // //         "nth_value".to_owned(),
    // //         Arc::new(Column::new("arr", 0)),
    // //         DataType::Int32,
    // //         2,
    // //         false,
    // //     )?;
    // //     test_i32_result(
    // //         nth_value,
    // //         Int32Array::from(vec![
    // //             None,
    // //             Some(-2),
    // //             Some(-2),
    // //             Some(-2),
    // //             Some(-2),
    // //             Some(-2),
    // //             Some(-2),
    // //             Some(-2),
    // //         ]),
    // //     )?;
    // //     Ok(())
    // // }
}

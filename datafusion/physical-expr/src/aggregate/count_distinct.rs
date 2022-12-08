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

use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, AggregateState};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DistinctScalarValues(Vec<ScalarValue>);

/// Expression for a COUNT(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctCount {
    /// Column name
    name: String,
    /// The DataType for the final count
    data_type: DataType,
    /// The DataType used to hold the state for each input
    state_data_types: Vec<DataType>,
    /// The input arguments
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl DistinctCount {
    /// Create a new COUNT(DISTINCT) aggregate function.
    pub fn new(
        input_data_types: Vec<DataType>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: String,
        data_type: DataType,
    ) -> Self {
        let state_data_types = input_data_types;

        Self {
            name,
            data_type,
            state_data_types,
            exprs,
        }
    }
}

impl AggregateExpr for DistinctCount {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(self
            .state_data_types
            .iter()
            .map(|state_data_type| {
                Field::new(
                    &format_state_name(&self.name, "count distinct"),
                    DataType::List(Box::new(Field::new(
                        "item",
                        state_data_type.clone(),
                        true,
                    ))),
                    false,
                )
            })
            .collect::<Vec<_>>())
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctCountAccumulator {
            values: HashSet::default(),
            state_data_types: self.state_data_types.clone(),
            count_data_type: self.data_type.clone(),
        }))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<DistinctScalarValues, RandomState>,
    state_data_types: Vec<DataType>,
    count_data_type: DataType,
}
impl DistinctCountAccumulator {
    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        // If a row has a NULL, it is not included in the final count.
        if !values.iter().any(|v| v.is_null()) {
            self.values.insert(DistinctScalarValues(values.to_vec()));
        }

        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let col_values = states
            .iter()
            .map(|state| match state {
                ScalarValue::List(Some(values), _) => Ok(values),
                _ => Err(DataFusionError::Internal(format!(
                    "Unexpected accumulator state {:?}",
                    state
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        (0..col_values[0].len()).try_for_each(|row_index| {
            let row_values = col_values
                .iter()
                .map(|col| col[row_index].clone())
                .collect::<Vec<_>>();
            self.update(&row_values)
        })
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.update(&v)
        })
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.merge(&v)
        })
    }
    fn state(&self) -> Result<Vec<AggregateState>> {
        let mut cols_out = self
            .state_data_types
            .iter()
            .map(|state_data_type| {
                ScalarValue::new_list(Some(Vec::new()), state_data_type.clone())
            })
            .collect::<Vec<_>>();

        let mut cols_vec = cols_out
            .iter_mut()
            .map(|c| match c {
                ScalarValue::List(Some(ref mut v), _) => Ok(v),
                t => Err(DataFusionError::Internal(format!(
                    "cols_out should only consist of ScalarValue::List. {:?} is found",
                    t
                ))),
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.values.iter().for_each(|distinct_values| {
            distinct_values.0.iter().enumerate().for_each(
                |(col_index, distinct_value)| {
                    cols_vec[col_index].push(distinct_value.clone());
                },
            )
        });

        Ok(cols_out.into_iter().map(AggregateState::Scalar).collect())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match &self.count_data_type {
            DataType::Int64 => Ok(ScalarValue::Int64(Some(self.values.len() as i64))),
            t => Err(DataFusionError::Internal(format!(
                "Invalid data type {:?} for count distinct aggregation",
                t
            ))),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<DistinctScalarValues>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| {
                    ScalarValue::size_of_vec(&vals.0) - std::mem::size_of_val(&vals.0)
                })
                .sum::<usize>()
            + (std::mem::size_of::<DataType>() * self.state_data_types.capacity())
            + self
                .state_data_types
                .iter()
                .map(|dt| dt.size() - std::mem::size_of_val(dt))
                .sum::<usize>()
            + self.count_data_type.size()
            - std::mem::size_of_val(&self.count_data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::utils::get_accum_scalar_values;
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::array::{Int32Builder, ListBuilder, UInt64Builder};
    use arrow::datatypes::DataType;
    use datafusion_common::cast::as_list_array;

    macro_rules! state_to_vec {
        ($LIST:expr, $DATA_TYPE:ident, $PRIM_TY:ty) => {{
            match $LIST {
                ScalarValue::List(_, field) => match field.data_type() {
                    &DataType::$DATA_TYPE => (),
                    _ => panic!("Unexpected DataType for list"),
                },
                _ => panic!("Expected a ScalarValue::List"),
            }

            match $LIST {
                ScalarValue::List(None, _) => None,
                ScalarValue::List(Some(scalar_values), _) => {
                    let vec = scalar_values
                        .iter()
                        .map(|scalar_value| match scalar_value {
                            ScalarValue::$DATA_TYPE(value) => *value,
                            _ => panic!("Unexpected ScalarValue variant"),
                        })
                        .collect::<Vec<Option<$PRIM_TY>>>();

                    Some(vec)
                }
                _ => unreachable!(),
            }
        }};
    }

    macro_rules! build_list {
        ($LISTS:expr, $BUILDER_TYPE:ident) => {{
            let mut builder = ListBuilder::new($BUILDER_TYPE::with_capacity(0));
            for list in $LISTS.iter() {
                match list {
                    Some(values) => {
                        for value in values.iter() {
                            match value {
                                Some(v) => builder.values().append_value((*v).into()),
                                None => builder.values().append_null(),
                            }
                        }

                        builder.append(true);
                    }
                    None => {
                        builder.append(false);
                    }
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        }};
    }

    macro_rules! test_count_distinct_update_batch_numeric {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(1),
                Some(1),
                None,
                Some(3),
                Some(2),
                None,
                Some(2),
                Some(3),
                Some(1),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec =
                state_to_vec!(&states[0], $DATA_TYPE, $PRIM_TYPE).unwrap();
            state_vec.sort();

            assert_eq!(states.len(), 1);
            assert_eq!(state_vec, vec![Some(1), Some(2), Some(3)]);
            assert_eq!(result, ScalarValue::Int64(Some(3)));

            Ok(())
        }};
    }

    fn collect_states<T: Ord + Clone, S: Ord + Clone>(
        state1: &[Option<T>],
        state2: &[Option<S>],
    ) -> Vec<(Option<T>, Option<S>)> {
        let mut states = state1
            .iter()
            .zip(state2.iter())
            .map(|(l, r)| (l.clone(), r.clone()))
            .collect::<Vec<(Option<T>, Option<S>)>>();
        states.sort();
        states
    }

    fn run_update_batch(arrays: &[ArrayRef]) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            arrays
                .iter()
                .map(|a| a.data_type().clone())
                .collect::<Vec<_>>(),
            vec![],
            String::from("__col_name__"),
            DataType::Int64,
        );

        let mut accum = agg.create_accumulator()?;
        accum.update_batch(arrays)?;

        Ok((get_accum_scalar_values(accum.as_ref())?, accum.evaluate()?))
    }

    fn run_update(
        data_types: &[DataType],
        rows: &[Vec<ScalarValue>],
    ) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            data_types.to_vec(),
            vec![],
            String::from("__col_name__"),
            DataType::Int64,
        );

        let mut accum = agg.create_accumulator()?;

        let cols = (0..rows[0].len())
            .map(|i| {
                rows.iter()
                    .map(|inner| inner[i].clone())
                    .collect::<Vec<ScalarValue>>()
            })
            .collect::<Vec<_>>();

        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|c| ScalarValue::iter_to_array(c.clone()))
            .collect::<Result<Vec<ArrayRef>>>()?;

        accum.update_batch(&arrays)?;

        Ok((get_accum_scalar_values(accum.as_ref())?, accum.evaluate()?))
    }

    fn run_merge_batch(arrays: &[ArrayRef]) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            arrays
                .iter()
                .map(|a| as_list_array(a).unwrap())
                .map(|a| a.values().data_type().clone())
                .collect::<Vec<_>>(),
            vec![],
            String::from("__col_name__"),
            DataType::Int64,
        );

        let mut accum = agg.create_accumulator()?;
        accum.merge_batch(arrays)?;

        Ok((get_accum_scalar_values(accum.as_ref())?, accum.evaluate()?))
    }

    // Used trait to create associated constant for f32 and f64
    trait SubNormal: 'static {
        const SUBNORMAL: Self;
    }

    impl SubNormal for f64 {
        const SUBNORMAL: Self = 1.0e-308_f64;
    }

    impl SubNormal for f32 {
        const SUBNORMAL: Self = 1.0e-38_f32;
    }

    macro_rules! test_count_distinct_update_batch_floating_point {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(<$PRIM_TYPE>::INFINITY),
                Some(<$PRIM_TYPE>::NAN),
                Some(1.0),
                Some(<$PRIM_TYPE as SubNormal>::SUBNORMAL),
                Some(1.0),
                Some(<$PRIM_TYPE>::INFINITY),
                None,
                Some(3.0),
                Some(-4.5),
                Some(2.0),
                None,
                Some(2.0),
                Some(3.0),
                Some(<$PRIM_TYPE>::NEG_INFINITY),
                Some(1.0),
                Some(<$PRIM_TYPE>::NAN),
                Some(<$PRIM_TYPE>::NEG_INFINITY),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec =
                state_to_vec!(&states[0], $DATA_TYPE, $PRIM_TYPE).unwrap();

            dbg!(&state_vec);
            state_vec.sort_by(|a, b| match (a, b) {
                (Some(lhs), Some(rhs)) => lhs.total_cmp(rhs),
                _ => a.partial_cmp(b).unwrap(),
            });

            let nan_idx = state_vec.len() - 1;
            assert_eq!(states.len(), 1);
            assert_eq!(
                &state_vec[..nan_idx],
                vec![
                    Some(<$PRIM_TYPE>::NEG_INFINITY),
                    Some(-4.5),
                    Some(<$PRIM_TYPE as SubNormal>::SUBNORMAL),
                    Some(1.0),
                    Some(2.0),
                    Some(3.0),
                    Some(<$PRIM_TYPE>::INFINITY)
                ]
            );
            assert!(state_vec[nan_idx].unwrap_or_default().is_nan());
            assert_eq!(result, ScalarValue::Int64(Some(8)));

            Ok(())
        }};
    }

    #[test]
    fn count_distinct_update_batch_i8() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int8Array, Int8, i8)
    }

    #[test]
    fn count_distinct_update_batch_i16() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int16Array, Int16, i16)
    }

    #[test]
    fn count_distinct_update_batch_i32() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int32Array, Int32, i32)
    }

    #[test]
    fn count_distinct_update_batch_i64() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int64Array, Int64, i64)
    }

    #[test]
    fn count_distinct_update_batch_u8() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt8Array, UInt8, u8)
    }

    #[test]
    fn count_distinct_update_batch_u16() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt16Array, UInt16, u16)
    }

    #[test]
    fn count_distinct_update_batch_u32() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt32Array, UInt32, u32)
    }

    #[test]
    fn count_distinct_update_batch_u64() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt64Array, UInt64, u64)
    }

    #[test]
    fn count_distinct_update_batch_f32() -> Result<()> {
        test_count_distinct_update_batch_floating_point!(Float32Array, Float32, f32)
    }

    #[test]
    fn count_distinct_update_batch_f64() -> Result<()> {
        test_count_distinct_update_batch_floating_point!(Float64Array, Float64, f64)
    }

    #[test]
    fn count_distinct_update_batch_boolean() -> Result<()> {
        let get_count = |data: BooleanArray| -> Result<(Vec<Option<bool>>, i64)> {
            let arrays = vec![Arc::new(data) as ArrayRef];
            let (states, result) = run_update_batch(&arrays)?;
            let mut state_vec = state_to_vec!(&states[0], Boolean, bool).unwrap();
            state_vec.sort();
            let count = match result {
                ScalarValue::Int64(c) => c.ok_or_else(|| {
                    DataFusionError::Internal("Found None count".to_string())
                }),
                scalar => Err(DataFusionError::Internal(format!(
                    "Found non int64 scalar value from count: {}",
                    scalar
                ))),
            }?;
            Ok((state_vec, count))
        };

        let zero_count_values = BooleanArray::from(Vec::<bool>::new());

        let one_count_values = BooleanArray::from(vec![false, false]);
        let one_count_values_with_null =
            BooleanArray::from(vec![Some(true), Some(true), None, None]);

        let two_count_values = BooleanArray::from(vec![true, false, true, false, true]);
        let two_count_values_with_null = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            None,
            Some(true),
            Some(false),
        ]);

        assert_eq!(
            get_count(zero_count_values)?,
            (Vec::<Option<bool>>::new(), 0)
        );
        assert_eq!(get_count(one_count_values)?, (vec![Some(false)], 1));
        assert_eq!(
            get_count(one_count_values_with_null)?,
            (vec![Some(true)], 1)
        );
        assert_eq!(
            get_count(two_count_values)?,
            (vec![Some(false), Some(true)], 2)
        );
        assert_eq!(
            get_count(two_count_values_with_null)?,
            (vec![Some(false), Some(true)], 2)
        );
        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_all_nulls() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(
            vec![None, None, None, None] as Vec<Option<i32>>
        )) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(state_to_vec!(&states[0], Int32, i32), Some(vec![]));
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_empty() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(vec![0_i32; 0])) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(state_to_vec!(&states[0], Int32, i32), Some(vec![]));
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_multiple_columns() -> Result<()> {
        let array_int8: ArrayRef = Arc::new(Int8Array::from(vec![1, 1, 2]));
        let array_int16: ArrayRef = Arc::new(Int16Array::from(vec![3, 3, 4]));
        let arrays = vec![array_int8, array_int16];

        let (states, result) = run_update_batch(&arrays)?;

        let state_vec1 = state_to_vec!(&states[0], Int8, i8).unwrap();
        let state_vec2 = state_to_vec!(&states[1], Int16, i16).unwrap();
        let state_pairs = collect_states::<i8, i16>(&state_vec1, &state_vec2);

        assert_eq!(states.len(), 2);
        assert_eq!(
            state_pairs,
            vec![(Some(1_i8), Some(3_i16)), (Some(2_i8), Some(4_i16))]
        );

        assert_eq!(result, ScalarValue::Int64(Some(2)));

        Ok(())
    }

    #[test]
    fn count_distinct_update() -> Result<()> {
        let (states, result) = run_update(
            &[DataType::Int32, DataType::UInt64],
            &[
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::Int32(Some(5)), ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::Int32(Some(5)), ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(Some(6))],
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(Some(7))],
                vec![ScalarValue::Int32(Some(2)), ScalarValue::UInt64(Some(7))],
            ],
        )?;

        let state_vec1 = state_to_vec!(&states[0], Int32, i32).unwrap();
        let state_vec2 = state_to_vec!(&states[1], UInt64, u64).unwrap();
        let state_pairs = collect_states::<i32, u64>(&state_vec1, &state_vec2);

        assert_eq!(states.len(), 2);
        assert_eq!(
            state_pairs,
            vec![
                (Some(-1_i32), Some(5_u64)),
                (Some(-1_i32), Some(6_u64)),
                (Some(-1_i32), Some(7_u64)),
                (Some(2_i32), Some(7_u64)),
                (Some(5_i32), Some(1_u64)),
            ]
        );
        assert_eq!(result, ScalarValue::Int64(Some(5)));

        Ok(())
    }

    #[test]
    fn count_distinct_update_with_nulls() -> Result<()> {
        let (states, result) = run_update(
            &[DataType::Int32, DataType::UInt64],
            &[
                // None of these updates contains a None, so these are accumulated.
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::Int32(Some(-2)), ScalarValue::UInt64(Some(5))],
                // Each of these updates contains at least one None, so these
                // won't be accumulated.
                vec![ScalarValue::Int32(Some(-1)), ScalarValue::UInt64(None)],
                vec![ScalarValue::Int32(None), ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::Int32(None), ScalarValue::UInt64(None)],
            ],
        )?;

        let state_vec1 = state_to_vec!(&states[0], Int32, i32).unwrap();
        let state_vec2 = state_to_vec!(&states[1], UInt64, u64).unwrap();
        let state_pairs = collect_states::<i32, u64>(&state_vec1, &state_vec2);

        assert_eq!(states.len(), 2);
        assert_eq!(
            state_pairs,
            vec![(Some(-2_i32), Some(5_u64)), (Some(-1_i32), Some(5_u64))]
        );

        assert_eq!(result, ScalarValue::Int64(Some(2)));

        Ok(())
    }

    #[test]
    fn count_distinct_merge_batch() -> Result<()> {
        let state_in1 = build_list!(
            vec![
                Some(vec![Some(-1_i32), Some(-1_i32), Some(-2_i32), Some(-2_i32)]),
                Some(vec![Some(-2_i32), Some(-3_i32)]),
            ],
            Int32Builder
        );

        let state_in2 = build_list!(
            vec![
                Some(vec![Some(5_u64), Some(6_u64), Some(5_u64), Some(7_u64)]),
                Some(vec![Some(5_u64), Some(7_u64)]),
            ],
            UInt64Builder
        );

        let (states, result) = run_merge_batch(&[state_in1, state_in2])?;

        let state_out_vec1 = state_to_vec!(&states[0], Int32, i32).unwrap();
        let state_out_vec2 = state_to_vec!(&states[1], UInt64, u64).unwrap();
        let state_pairs = collect_states::<i32, u64>(&state_out_vec1, &state_out_vec2);

        assert_eq!(
            state_pairs,
            vec![
                (Some(-3_i32), Some(7_u64)),
                (Some(-2_i32), Some(5_u64)),
                (Some(-2_i32), Some(7_u64)),
                (Some(-1_i32), Some(5_u64)),
                (Some(-1_i32), Some(6_u64)),
            ]
        );

        assert_eq!(result, ScalarValue::Int64(Some(5)));

        Ok(())
    }
}

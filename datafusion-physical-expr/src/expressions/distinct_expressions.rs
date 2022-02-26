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

//! Implementations for DISTINCT expressions, e.g. `COUNT(DISTINCT c)`

use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DistinctScalarValues(Vec<ScalarValue>);

fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
}

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
        let state_data_types = input_data_types.into_iter().map(state_type).collect();

        Self {
            name,
            data_type,
            state_data_types,
            exprs,
        }
    }
}

/// return the type to use to accumulate state for the specified input type
fn state_type(data_type: DataType) -> DataType {
    match data_type {
        // when aggregating dictionary values, use the underlying value type
        DataType::Dictionary(_key_type, value_type) => *value_type,
        t => t,
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
        };
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
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.merge(&v)
        })
    }
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut cols_out = self
            .state_data_types
            .iter()
            .map(|state_data_type| {
                let values = Box::new(Vec::new());
                let data_type = Box::new(state_data_type.clone());
                ScalarValue::List(Some(values), data_type)
            })
            .collect::<Vec<_>>();

        let mut cols_vec = cols_out
            .iter_mut()
            .map(|c| match c {
                ScalarValue::List(Some(ref mut v), _) => v,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        self.values.iter().for_each(|distinct_values| {
            distinct_values.0.iter().enumerate().for_each(
                |(col_index, distinct_value)| {
                    cols_vec[col_index].push(distinct_value.clone());
                },
            )
        });

        Ok(cols_out)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match &self.count_data_type {
            DataType::UInt64 => Ok(ScalarValue::UInt64(Some(self.values.len() as u64))),
            t => Err(DataFusionError::Internal(format!(
                "Invalid data type {:?} for count distinct aggregation",
                t
            ))),
        }
    }
}

/// Expression for a ARRAY_AGG(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctArrayAgg {
    /// Column name
    name: String,
    /// The DataType for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
}

impl DistinctArrayAgg {
    /// Create a new DistinctArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Self {
        let name = name.into();
        Self {
            name,
            expr,
            input_data_type,
        }
    }
}

impl AggregateExpr for DistinctArrayAgg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            DataType::List(Box::new(Field::new(
                "item",
                self.input_data_type.clone(),
                true,
            ))),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctArrayAggAccumulator::try_new(
            &self.input_data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "distinct_array_agg"),
            DataType::List(Box::new(Field::new(
                "item",
                self.input_data_type.clone(),
                true,
            ))),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct DistinctArrayAggAccumulator {
    values: HashSet<ScalarValue>,
    datatype: DataType,
}

impl DistinctArrayAggAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: HashSet::new(),
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for DistinctArrayAggAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(
            Some(Box::new(self.values.clone().into_iter().collect())),
            Box::new(self.datatype.clone()),
        )])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1, "batch input should only include 1 column!");

        let arr = &values[0];
        for i in 0..arr.len() {
            self.values.insert(ScalarValue::try_from_array(arr, i)?);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        for array in states {
            for j in 0..array.len() {
                self.values.insert(ScalarValue::try_from_array(array, j)?);
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::List(
            Some(Box::new(self.values.clone().into_iter().collect())),
            Box::new(self.datatype.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, ListArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::array::{Int32Builder, ListBuilder, UInt64Builder};
    use arrow::datatypes::{DataType, Schema};
    use arrow::record_batch::RecordBatch;

    macro_rules! build_list {
        ($LISTS:expr, $BUILDER_TYPE:ident) => {{
            let mut builder = ListBuilder::new($BUILDER_TYPE::new(0));
            for list in $LISTS.iter() {
                match list {
                    Some(values) => {
                        for value in values.iter() {
                            match value {
                                Some(v) => builder.values().append_value((*v).into())?,
                                None => builder.values().append_null()?,
                            }
                        }

                        builder.append(true)?;
                    }
                    None => {
                        builder.append(false)?;
                    }
                }
            }

            let array = Arc::new(builder.finish()) as ArrayRef;

            Ok(array) as Result<ArrayRef>
        }};
    }

    macro_rules! state_to_vec {
        ($LIST:expr, $DATA_TYPE:ident, $PRIM_TY:ty) => {{
            match $LIST {
                ScalarValue::List(_, data_type) => match data_type.as_ref() {
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
            DataType::UInt64,
        );

        let mut accum = agg.create_accumulator()?;
        accum.update_batch(arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    fn run_update(
        data_types: &[DataType],
        rows: &[Vec<ScalarValue>],
    ) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            data_types.to_vec(),
            vec![],
            String::from("__col_name__"),
            DataType::UInt64,
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

        Ok((accum.state()?, accum.evaluate()?))
    }

    fn run_merge_batch(arrays: &[ArrayRef]) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            arrays
                .iter()
                .map(|a| a.as_any().downcast_ref::<ListArray>().unwrap())
                .map(|a| a.values().data_type().clone())
                .collect::<Vec<_>>(),
            vec![],
            String::from("__col_name__"),
            DataType::UInt64,
        );

        let mut accum = agg.create_accumulator()?;
        accum.merge_batch(arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
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
            assert_eq!(result, ScalarValue::UInt64(Some(3)));

            Ok(())
        }};
    }

    //Used trait to create associated constant for f32 and f64
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
            use ordered_float::OrderedFloat;
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
            state_vec.sort_by(|a, b| match (a, b) {
                (Some(lhs), Some(rhs)) => {
                    OrderedFloat::from(*lhs).cmp(&OrderedFloat::from(*rhs))
                }
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
            assert_eq!(result, ScalarValue::UInt64(Some(8)));

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
        let get_count = |data: BooleanArray| -> Result<(Vec<Option<bool>>, u64)> {
            let arrays = vec![Arc::new(data) as ArrayRef];
            let (states, result) = run_update_batch(&arrays)?;
            let mut state_vec = state_to_vec!(&states[0], Boolean, bool).unwrap();
            state_vec.sort();
            let count = match result {
                ScalarValue::UInt64(c) => c.ok_or_else(|| {
                    DataFusionError::Internal("Found None count".to_string())
                }),
                scalar => Err(DataFusionError::Internal(format!(
                    "Found non Uint64 scalar value from count: {}",
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
        assert_eq!(result, ScalarValue::UInt64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_empty() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(vec![0_i32; 0])) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(state_to_vec!(&states[0], Int32, i32), Some(vec![]));
        assert_eq!(result, ScalarValue::UInt64(Some(0)));

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

        assert_eq!(result, ScalarValue::UInt64(Some(2)));

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
        assert_eq!(result, ScalarValue::UInt64(Some(5)));

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

        assert_eq!(result, ScalarValue::UInt64(Some(2)));

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
        )?;

        let state_in2 = build_list!(
            vec![
                Some(vec![Some(5_u64), Some(6_u64), Some(5_u64), Some(7_u64)]),
                Some(vec![Some(5_u64), Some(7_u64)]),
            ],
            UInt64Builder
        )?;

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

        assert_eq!(result, ScalarValue::UInt64(Some(5)));

        Ok(())
    }

    fn check_distinct_array_agg(
        input: ArrayRef,
        expected: ScalarValue,
        datatype: DataType,
    ) -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", datatype.clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input])?;

        let agg = Arc::new(DistinctArrayAgg::new(
            col("a", &schema)?,
            "bla".to_string(),
            datatype,
        ));
        let actual = aggregate(&batch, agg)?;

        match (expected, actual) {
            (ScalarValue::List(Some(mut e), _), ScalarValue::List(Some(mut a), _)) => {
                // workaround lack of Ord of ScalarValue
                let cmp = |a: &ScalarValue, b: &ScalarValue| {
                    a.partial_cmp(b).expect("Can compare ScalarValues")
                };

                e.sort_by(cmp);
                a.sort_by(cmp);
                // Check that the inputs are the same
                assert_eq!(e, a);
            }
            _ => {
                unreachable!()
            }
        }

        Ok(())
    }

    #[test]
    fn distinct_array_agg_i32() -> Result<()> {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 4, 5, 2]));

        let out = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(Some(7)),
                ScalarValue::Int32(Some(4)),
                ScalarValue::Int32(Some(5)),
            ])),
            Box::new(DataType::Int32),
        );

        check_distinct_array_agg(col, out, DataType::Int32)
    }

    #[test]
    fn distinct_array_agg_nested() -> Result<()> {
        // [[1, 2, 3], [4, 5]]
        let l1 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(1i32),
                        ScalarValue::from(2i32),
                        ScalarValue::from(3i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(4i32),
                        ScalarValue::from(5i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
            ])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        // [[6], [7, 8]]
        let l2 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::List(
                    Some(Box::new(vec![ScalarValue::from(6i32)])),
                    Box::new(DataType::Int32),
                ),
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(7i32),
                        ScalarValue::from(8i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
            ])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        // [[9]]
        let l3 = ScalarValue::List(
            Some(Box::new(vec![ScalarValue::List(
                Some(Box::new(vec![ScalarValue::from(9i32)])),
                Box::new(DataType::Int32),
            )])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let list = ScalarValue::List(
            Some(Box::new(vec![l1.clone(), l2.clone(), l3.clone()])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        // Duplicate l1 in the input array and check that it is deduped in the output.
        let array = ScalarValue::iter_to_array(vec![l1.clone(), l2, l3, l1]).unwrap();

        check_distinct_array_agg(
            array,
            list,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                true,
            ))),
        )
    }
}

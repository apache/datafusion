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

//! InList expression

use std::{any::Any, collections::HashSet};
use std::{hash::Hash, sync::Arc};

use arrow::array::{ArrayRef, BooleanArray, StringOffsetSizeTrait};
use arrow::{
    array::{Array, GenericStringArray, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::error::Result;
use crate::physical_plan::{ColumnarValue, PhysicalExpr};
use crate::scalar::ScalarValue;

/// InList
#[derive(Debug)]
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
}

use std::iter::Iterator;
//This function computes the in list boolean array over an iterator
//the checks for the behavior for the negated and the contains_null are lifted outside the loops to try to avoid branches within the loop
fn compute_contains_array<
    V,
    I: Iterator<Item = Option<V>>,
    F: FnMut(Option<V>) -> Option<bool>,
>(
    it: I,
    negated: bool,
    contains_null: bool,
    contains: F,
) -> BooleanArray {
    let it_contains = it.map(contains);
    match (negated, contains_null) {
        (true, true) => it_contains
            .map(|x| match x {
                Some(true) => Some(false),
                Some(false) | None => None,
            })
            .collect::<BooleanArray>(),
        (true, false) => it_contains.map(|x| x.map(|x| !x)).collect::<BooleanArray>(),
        (false, true) => it_contains
            .map(|x| match x {
                Some(true) => Some(true),
                Some(false) | None => None,
            })
            .collect::<BooleanArray>(),
        (false, false) => it_contains.collect::<BooleanArray>(),
    }
}
//Collects the scalar values from a specified discriminant into a vector, marking if there are any null values.
fn collect_scalar_value<V, F: Fn(&ScalarValue, &mut bool) -> Option<V>>(
    vals: Vec<ColumnarValue>,
    contains_null: &mut bool,
    disc: std::mem::Discriminant<ScalarValue>,
    scalar_handler: F,
) -> Vec<V> {
    vals.iter()
        .flat_map(|expr| match expr {
            ColumnarValue::Scalar(s) => match s {
                sc if std::mem::discriminant(s) == disc => {
                    scalar_handler(sc, contains_null)
                }
                ScalarValue::Utf8(None) => {
                    *contains_null = true;
                    None
                }
                datatype => unimplemented!("Unexpected type {} for InList", datatype),
            },
            ColumnarValue::Array(_) => {
                unimplemented!("InList does not yet support nested columns.")
            }
        })
        .collect::<Vec<_>>()
}

fn contains_sorted<E: Ord>(data: &[E], item: &E)->bool{
    if data.is_empty(){
        return false;
    }
    //At this point slice is not empty so it will never panic
    let lowest = data.first().unwrap();
    let highest = data.last().unwrap();

    if item < lowest || highest < item{
        return false
    }

    for elem in data.iter(){
        if *elem == *item{
            return true;
        }else if *elem > *item{
            return false;
        }
    }
    false
}

#[allow(clippy::unnecessary_wraps)]
fn contains_ord_types_it<O, I>(
    it: I,
    mut list_values: Vec<O>,
    negated: bool,
    contains_null: bool,
    linear_search_bound: usize,
) -> Result<ColumnarValue>
where
    I: Iterator<Item = Option<O>>,
    O: Ord + Hash + Eq,
{
    let num_values = list_values.len();
    let contains_arr = if num_values == 0{
        compute_contains_array(it, negated, contains_null, |val| {
            val.map(|_x| false)
        })
    } else if num_values <= linear_search_bound {
        list_values.sort_unstable();
        compute_contains_array(it, negated, contains_null, |val| {
            val.map(|x|  contains_sorted(list_values.as_slice(), &x))
        })
    } else {
        let set = list_values.into_iter().collect::<HashSet<_>>(); // HashSet::with_capacity(values.len());
        compute_contains_array(it, negated, contains_null, |val| {
            val.map(|x| set.contains(&x))
        })
    };
    let col_val = ColumnarValue::Array(Arc::new(contains_arr));
    Ok(col_val)
}

fn contains_ord_types<A, F>(
    array: Arc<dyn Array>,
    list_values: Vec<ColumnarValue>,
    negated: bool,
    disc: std::mem::Discriminant<ScalarValue>,
    linear_search_bound: usize,
    scalar_handler: F,
) -> Result<ColumnarValue>
where
    A: ArrowPrimitiveType,
    A::Native: Ord + Hash + Eq,
    F: Fn(&ScalarValue, &mut bool) -> Option<A::Native>,
{
    let arr = array
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<A>>()
        .unwrap();
    let mut contains_null = false;
    let values =
        collect_scalar_value(list_values, &mut contains_null, disc, scalar_handler);
    contains_ord_types_it(
        arr.iter(),
        values,
        negated,
        contains_null,
        linear_search_bound,
    )
}

//Checks for values in the array of the specified ScalarValue discriminant for  partially ordered that can be totally ordered types like f32 and f64.
fn contains_partial_ord_type<A, F, V, O>(
    array: Arc<dyn Array>,
    list_values: Vec<ColumnarValue>,
    negated: bool,
    disc: std::mem::Discriminant<ScalarValue>,
    linear_search_bound: usize,
    make_ord_val: V,
    scalar_handler: F,
) -> Result<ColumnarValue>
where
    A: ArrowPrimitiveType,
    V: Copy + Fn(A::Native) -> O,
    O: Ord + Hash + Eq,
    F: Fn(&ScalarValue, &mut bool) -> Option<A::Native>,
{
    let arr = array
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<A>>()
        .unwrap();
    let mut contains_null = false;
    let handler_wrapper = |s: &ScalarValue, cont_null: &mut bool| {
        scalar_handler(s, cont_null).map(make_ord_val)
    };
    let values =
        collect_scalar_value(list_values, &mut contains_null, disc, handler_wrapper);
    contains_ord_types_it(
        arr.iter().map(|v| v.map(make_ord_val)),
        values,
        negated,
        contains_null,
        linear_search_bound,
    )
}

//The last two literals are the linear search upper bound and the binary search upper bound before the strategy switches to a HashSet.
//If a single literal is given then it is used for both search bounds and the binary search will never be performed.

macro_rules! make_ord_contains {
    ($ARRAY_TYPE:ty, $ARRAY:expr, $LIST_VALUES:expr, $NEGATED:expr, $SCALAR_VALUE:ident, $LINEAR_SEARCH_BOUND:literal) => {{
        contains_ord_types::<$ARRAY_TYPE, _>(
            $ARRAY,
            $LIST_VALUES,
            $NEGATED,
            std::mem::discriminant(&ScalarValue::$SCALAR_VALUE(None)),
            $LINEAR_SEARCH_BOUND,
            |s, contains_null| match s {
                ScalarValue::$SCALAR_VALUE(Some(v)) => Some(*v),
                ScalarValue::$SCALAR_VALUE(None) => {
                    *contains_null = true;
                    None
                }
                _ => unreachable!(),
            },
        )
    }};
}

impl InListExpr {
    /// Create a new InList expression
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
    ) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// List to search in
    pub fn list(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.list
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// Compare for specific utf8 types
    #[allow(clippy::unnecessary_wraps)]
    fn compare_utf8<T: StringOffsetSizeTrait>(
        &self,
        array: ArrayRef,
        list_values: Vec<ColumnarValue>,
    ) -> Result<ColumnarValue> {
        let array = array
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .unwrap();

        let mut contains_null = false;
        let mut values = list_values
            .iter()
            .flat_map(|expr| match expr {
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::Utf8(Some(v)) => Some(v.as_str()),
                    ScalarValue::Utf8(None) => {
                        contains_null = true;
                        None
                    }
                    ScalarValue::LargeUtf8(Some(v)) => Some(v.as_str()),
                    ScalarValue::LargeUtf8(None) => {
                        contains_null = true;
                        None
                    }
                    datatype => unimplemented!("Unexpected type {} for InList", datatype),
                },
                ColumnarValue::Array(_) => {
                    unimplemented!("InList does not yet support nested columns.")
                }
            })
            .collect::<Vec<_>>();
        //TODO: determine better bound for when to switch to hashset
        // Possibly based off of the number of characters the values in the list have combined with the total length of the list?
        const LINEAR_SEARCH_UPPER_BOUND: usize = 8;
        let contains_arr = match values.len() {
            0 =>{
                compute_contains_array(array.iter(), self.negated, contains_null, |x| {
                    x.map(|_x| false)
                })
            }
            1..=LINEAR_SEARCH_UPPER_BOUND => {
                values.sort_unstable();
                compute_contains_array(array.iter(), self.negated, contains_null, |x| {
                    x.map(|x| contains_sorted(values.as_slice(), &x))
                })
            }
            _ => {
                let set = values.into_iter().collect::<HashSet<_>>();
                compute_contains_array(array.iter(), self.negated, contains_null, |x| {
                    x.map(|s| set.contains(s))
                })
            }
        };
        Ok(ColumnarValue::Array(Arc::new(contains_arr)))
    }
}

impl std::fmt::Display for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.negated {
            write!(f, "{} NOT IN ({:?})", self.expr, self.list)
        } else {
            write!(f, "{} IN ({:?})", self.expr, self.list)
        }
    }
}

impl PhysicalExpr for InListExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        let value_data_type = value.data_type();
        let list_values = self
            .list
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        let array = match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };
        use arrow::datatypes::{
            Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
            UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        };
        match value_data_type {
            DataType::Float32 => {
                let disc = std::mem::discriminant(&ScalarValue::Float32(None));
                contains_partial_ord_type::<Float32Type, _, _, _>(
                    array,
                    list_values,
                    self.negated,
                    disc,
                    256,
                    ordered_float::OrderedFloat::from,
                    |s, contains_null| match s {
                        ScalarValue::Float32(Some(v)) => Some(*v),
                        ScalarValue::Float32(None) => {
                            *contains_null = true;
                            None
                        }
                        _ => unreachable!(),
                    },
                )
            }
            DataType::Float64 => {
                let disc = std::mem::discriminant(&ScalarValue::Float64(None));
                contains_partial_ord_type::<Float64Type, _, _, _>(
                    array,
                    list_values,
                    self.negated,
                    disc,
                    128,
                    ordered_float::OrderedFloat::from,
                    |s, contains_null| match s {
                        ScalarValue::Float64(Some(v)) => Some(*v),
                        ScalarValue::Float64(None) => {
                            *contains_null = true;
                            None
                        }
                        _ => unreachable!(),
                    },
                )
            }
            DataType::Int8 => {
                make_ord_contains!(Int8Type, array, list_values, self.negated, Int8, 256)
            }
            DataType::Int16 => {
                make_ord_contains!(
                    Int16Type,
                    array,
                    list_values,
                    self.negated,
                    Int16,
                    256
                )
            }
            DataType::Int32 => {
                make_ord_contains!(
                    Int32Type,
                    array,
                    list_values,
                    self.negated,
                    Int32,
                    256
                )
            }
            DataType::Int64 => {
                make_ord_contains!(
                    Int64Type,
                    array,
                    list_values,
                    self.negated,
                    Int64,
                    128
                )
            }
            DataType::UInt8 => {
                make_ord_contains!(
                    UInt8Type,
                    array,
                    list_values,
                    self.negated,
                    UInt8,
                    256
                )
            }
            DataType::UInt16 => {
                make_ord_contains!(
                    UInt16Type,
                    array,
                    list_values,
                    self.negated,
                    UInt16,
                    256
                )
            }
            DataType::UInt32 => {
                make_ord_contains!(
                    UInt32Type,
                    array,
                    list_values,
                    self.negated,
                    UInt32,
                    256
                )
            }
            DataType::UInt64 => {
                make_ord_contains!(
                    UInt64Type,
                    array,
                    list_values,
                    self.negated,
                    UInt64,
                    128
                )
            }

            DataType::Boolean => {
                let mut contains_null = false;
                let bool_disc = std::mem::discriminant(&ScalarValue::Boolean(None));
                let values = collect_scalar_value(
                    list_values,
                    &mut contains_null,
                    bool_disc,
                    |s, cont_null| match s {
                        ScalarValue::Boolean(Some(v)) => Some(*v),
                        ScalarValue::Boolean(None) => {
                            *cont_null = true;
                            None
                        }
                        _ => unreachable!(),
                    },
                );
                let bool_arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut found_true = false;
                let mut found_false = false;
                //Check if the list contains both true and false values
                values.iter().any(|b| {
                    match b {
                        true => {
                            found_true = true;
                        }
                        false => {
                            found_false = true;
                        }
                    }
                    found_true && found_false
                });

                let contains_arr = compute_contains_array(
                    bool_arr.iter(),
                    self.negated,
                    contains_null,
                    |o| o.map(|b| (found_true && b) || (found_false && !b)),
                );
                Ok(ColumnarValue::Array(Arc::new(contains_arr)))
            }
            DataType::Utf8 => self.compare_utf8::<i32>(array, list_values),
            DataType::LargeUtf8 => self.compare_utf8::<i64>(array, list_values),
            datatype => {
                unimplemented!("InList does not support datatype {:?}.", datatype)
            }
        }
    }
}

/// Creates a unary expression InList
pub fn in_list(
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: &bool,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(InListExpr::new(expr, list, *negated)))
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::StringArray,
        array::{Float64Array, Int64Array},
        datatypes::Field,
    };

    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::{col, lit};

    // applies the in_list expr to an input batch and list
    macro_rules! in_list {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr) => {{
            let expr = in_list(col("a"), $LIST, $NEGATED).unwrap();
            let result = expr.evaluate(&$BATCH)?.into_array($BATCH.num_rows());
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn in_list_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
        ];
        in_list!(batch, list, &false, vec![Some(true), Some(false), None]);

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
        ];
        in_list!(batch, list, &true, vec![Some(false), Some(true), None]);

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in ("a", "b")"
        let list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(Some("b".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        Ok(())
    }

    #[test]
    fn in_list_int64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(0), Some(2), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
        ];
        in_list!(batch, list, &false, vec![Some(true), Some(false), None]);

        // expression: "a not in (0, 1)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
        ];
        in_list!(batch, list, &true, vec![Some(false), Some(true), None]);

        // expression: "a in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        Ok(())
    }

    #[test]
    fn in_list_float64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let a = Float64Array::from(vec![Some(0.0), Some(0.2), None]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in (0.0, 0.2)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
        ];
        in_list!(batch, list, &false, vec![Some(true), Some(false), None]);

        // expression: "a not in (0.0, 0.2)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
        ];
        in_list!(batch, list, &true, vec![Some(false), Some(true), None]);

        // expression: "a in (0.0, 0.2, NULL)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in (0.0, 0.2, NULL)"
        let list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(Some(0.1))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        Ok(())
    }

    #[test]
    fn in_list_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a in (true)"
        let list = vec![lit(ScalarValue::Boolean(Some(true)))];
        in_list!(batch, list, &false, vec![Some(true), None, Some(false)]);

        // expression: "a not in (true)"
        let list = vec![lit(ScalarValue::Boolean(Some(true)))];
        in_list!(batch, list, &true, vec![Some(false), None, Some(true)]);

        // expression: "a in (true, NULL)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, None]);

        // expression: "a not in (true, NULL)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, None]);

        // expression: "a in (true, false)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Boolean(Some(false))),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, Some(true)]);

        //expression: "a not in (true, false)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Boolean(Some(false))),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, Some(false)]);

        // expression: "a in (true, false, NULL)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Boolean(Some(false))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &false, vec![Some(true), None, Some(true)]);

        //expression: "a not in (true, false, NULL)"
        let list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Boolean(Some(false))),
            lit(ScalarValue::Utf8(None)),
        ];
        in_list!(batch, list, &true, vec![Some(false), None, Some(false)]);
        Ok(())
    }
}

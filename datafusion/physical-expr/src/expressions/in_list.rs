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

use ahash::RandomState;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::hash_utils::HashValue;
use crate::physical_expr::down_cast_any_ref;
use crate::utils::expr_list_eq_any_order;
use crate::PhysicalExpr;
use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow::util::bit_iterator::BitIndexIterator;
use arrow::{downcast_dictionary_array, downcast_primitive_array};
use datafusion_common::{
    cast::{
        as_boolean_array, as_generic_binary_array, as_primitive_array, as_string_array,
    },
    DataFusionError, Result, ScalarValue,
};
use datafusion_expr::ColumnarValue;
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;

/// InList
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
    static_filter: Option<Box<dyn Set>>,
    input_schema: Schema,
}

impl Debug for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("InListExpr")
            .field("expr", &self.expr)
            .field("list", &self.list)
            .field("negated", &self.negated)
            .finish()
    }
}

/// A type-erased container of array elements
trait Set: Send + Sync {
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray>;
}

struct ArrayHashSet {
    state: RandomState,
    /// Used to provide a lookup from value to in list index
    ///
    /// Note: usize::hash is not used, instead the raw entry
    /// API is used to store entries w.r.t their value
    map: HashMap<usize, (), ()>,
}

struct ArraySet<T> {
    array: T,
    hash_set: ArrayHashSet,
}

impl<T> ArraySet<T>
where
    T: Array + From<ArrayData>,
{
    fn new(array: &T, hash_set: ArrayHashSet) -> Self {
        Self {
            array: T::from(array.data().clone()),
            hash_set,
        }
    }
}

impl<T> Set for ArraySet<T>
where
    T: Array + 'static,
    for<'a> &'a T: ArrayAccessor,
    for<'a> <&'a T as ArrayAccessor>::Item: PartialEq + HashValue,
{
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        downcast_dictionary_array! {
            v => {
                let values_contains = self.contains(v.values().as_ref(), negated)?;
                let result = take(&values_contains, v.keys(), None)?;
                return Ok(BooleanArray::from(result.data().clone()))
            }
            _ => {}
        }

        let v = v.as_any().downcast_ref::<T>().unwrap();
        let in_data = self.array.data();
        let in_array = &self.array;
        let has_nulls = in_data.null_count() != 0;

        Ok(ArrayIter::new(v)
            .map(|v| {
                v.and_then(|v| {
                    let hash = v.hash_one(&self.hash_set.state);
                    let contains = self
                        .hash_set
                        .map
                        .raw_entry()
                        .from_hash(hash, |idx| in_array.value(*idx) == v)
                        .is_some();

                    match contains {
                        true => Some(!negated),
                        false if has_nulls => None,
                        false => Some(negated),
                    }
                })
            })
            .collect())
    }
}

/// Computes an [`ArrayHashSet`] for the provided [`Array`] if there are nulls present
/// or there are more than [`OPTIMIZER_INSET_THRESHOLD`] values
///
/// Note: This is split into a separate function as higher-rank trait bounds currently
/// cause type inference to misbehave
fn make_hash_set<T>(array: T) -> ArrayHashSet
where
    T: ArrayAccessor,
    T::Item: PartialEq + HashValue,
{
    let data = array.data();

    let state = RandomState::new();
    let mut map: HashMap<usize, (), ()> =
        HashMap::with_capacity_and_hasher(data.len(), ());

    let insert_value = |idx| {
        let value = array.value(idx);
        let hash = value.hash_one(&state);
        if let RawEntryMut::Vacant(v) = map
            .raw_entry_mut()
            .from_hash(hash, |x| array.value(*x) == value)
        {
            v.insert_with_hasher(hash, idx, (), |x| array.value(*x).hash_one(&state));
        }
    };

    match data.null_buffer() {
        Some(buffer) => BitIndexIterator::new(buffer.as_ref(), data.offset(), data.len())
            .for_each(insert_value),
        None => (0..data.len()).for_each(insert_value),
    }

    ArrayHashSet { state, map }
}

/// Creates a `Box<dyn Set>` for the given list of `IN` expressions and `batch`
fn make_set(array: &dyn Array) -> Result<Box<dyn Set>> {
    Ok(downcast_primitive_array! {
        array => Box::new(ArraySet::new(array, make_hash_set(array))),
        DataType::Boolean => {
            let array = as_boolean_array(array)?;
            Box::new(ArraySet::new(array, make_hash_set(array)))
        },
        DataType::Decimal128(_, _) => {
            let array = as_primitive_array::<Decimal128Type>(array)?;
            Box::new(ArraySet::new(array, make_hash_set(array)))
        }
        DataType::Decimal256(_, _) => {
            let array = as_primitive_array::<Decimal256Type>(array)?;
            Box::new(ArraySet::new(array, make_hash_set(array)))
        }
        DataType::Utf8 => {
            let array = as_string_array(array)?;
            Box::new(ArraySet::new(array, make_hash_set(array)))
        }
        DataType::LargeUtf8 => {
            let array = as_largestring_array(array);
            Box::new(ArraySet::new(array, make_hash_set(array)))
        }
        DataType::Binary => {
            let array = as_generic_binary_array::<i32>(array)?;
            Box::new(ArraySet::new(array, make_hash_set(array)))
        }
        DataType::LargeBinary => {
            let array = as_generic_binary_array::<i64>(array)?;
            Box::new(ArraySet::new(array, make_hash_set(array)))
        }
        DataType::Dictionary(_, _) => unreachable!("dictionary should have been flattened"),
        d => return Err(DataFusionError::NotImplemented(format!("DataType::{} not supported in InList", d)))
    })
}

/// Evaluates the list of expressions into an array, flattening any dictionaries
fn evaluate_list(
    list: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    let scalars = list
        .iter()
        .map(|expr| {
            expr.evaluate(batch).and_then(|r| match r {
                ColumnarValue::Array(_) => Err(DataFusionError::Execution(
                    "InList expression must evaluate to a scalar".to_string(),
                )),
                // Flatten dictionary values
                ColumnarValue::Scalar(ScalarValue::Dictionary(_, v)) => Ok(*v),
                ColumnarValue::Scalar(s) => Ok(s),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    ScalarValue::iter_to_array(scalars)
}

fn try_cast_static_filter_to_set(
    list: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> Result<Box<dyn Set>> {
    let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
    make_set(evaluate_list(list, &batch)?.as_ref())
}

impl InListExpr {
    /// Create a new InList expression
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
        schema: &Schema,
    ) -> Self {
        let static_filter = try_cast_static_filter_to_set(&list, schema).ok();
        Self {
            expr,
            list,
            negated,
            static_filter,
            input_schema: schema.clone(),
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
}

impl std::fmt::Display for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.negated {
            if self.static_filter.is_some() {
                write!(f, "{} NOT IN (SET) ({:?})", self.expr, self.list)
            } else {
                write!(f, "{} NOT IN ({:?})", self.expr, self.list)
            }
        } else if self.static_filter.is_some() {
            write!(f, "Use {} IN (SET) ({:?})", self.expr, self.list)
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
        let value = self.expr.evaluate(batch)?.into_array(1);
        let r = match &self.static_filter {
            Some(f) => f.contains(value.as_ref(), self.negated)?,
            None => {
                let list = evaluate_list(&self.list, batch)?;
                make_set(list.as_ref())?.contains(value.as_ref(), self.negated)?
            }
        };
        Ok(ColumnarValue::Array(Arc::new(r)))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut children = vec![];
        children.push(self.expr.clone());
        children.extend(self.list.clone());
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        in_list(
            children[0].clone(),
            children[1..].to_vec(),
            &self.negated,
            &self.input_schema,
        )
    }
}

impl PartialEq<dyn Any> for InListExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.expr.eq(&x.expr)
                    && expr_list_eq_any_order(&self.list, &x.list)
                    && self.negated == x.negated
            })
            .unwrap_or(false)
    }
}

/// Creates a unary expression InList
pub fn in_list(
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: &bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // check the data type
    let expr_data_type = expr.data_type(schema)?;
    for list_expr in list.iter() {
        let list_expr_data_type = list_expr.data_type(schema)?;
        if !expr_data_type.eq(&list_expr_data_type) {
            return Err(DataFusionError::Internal(format!(
                "The data type inlist should be same, the value type is {}, one of list expr type is {}",
                expr_data_type, list_expr_data_type
            )));
        }
    }
    Ok(Arc::new(InListExpr::new(expr, list, *negated, schema)))
}

#[cfg(test)]
mod tests {
    use arrow::{array::StringArray, datatypes::Field};

    use super::*;
    use crate::expressions;
    use crate::expressions::{col, lit, try_cast};
    use datafusion_common::Result;
    use datafusion_expr::type_coercion::binary::comparison_coercion;

    type InListCastResult = (Arc<dyn PhysicalExpr>, Vec<Arc<dyn PhysicalExpr>>);

    // Try to do the type coercion for list physical expr.
    // It's just used in the test
    fn in_list_cast(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Result<InListCastResult> {
        let expr_type = &expr.data_type(input_schema)?;
        let list_types: Vec<DataType> = list
            .iter()
            .map(|list_expr| list_expr.data_type(input_schema).unwrap())
            .collect();
        let result_type = get_coerce_type(expr_type, &list_types);
        match result_type {
            None => Err(DataFusionError::Plan(format!(
                "Can not find compatible types to compare {:?} with {:?}",
                expr_type, list_types
            ))),
            Some(data_type) => {
                // find the coerced type
                let cast_expr = try_cast(expr, input_schema, data_type.clone())?;
                let cast_list_expr = list
                    .into_iter()
                    .map(|list_expr| {
                        try_cast(list_expr, input_schema, data_type.clone()).unwrap()
                    })
                    .collect();
                Ok((cast_expr, cast_list_expr))
            }
        }
    }

    // Attempts to coerce the types of `list_type` to be comparable with the
    // `expr_type`
    fn get_coerce_type(expr_type: &DataType, list_type: &[DataType]) -> Option<DataType> {
        list_type
            .iter()
            .fold(Some(expr_type.clone()), |left, right_type| match left {
                None => None,
                Some(left_type) => comparison_coercion(&left_type, right_type),
            })
    }

    // applies the in_list expr to an input batch and list
    macro_rules! in_list {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr, $COL:expr, $SCHEMA:expr) => {{
            let (cast_expr, cast_list_exprs) = in_list_cast($COL, $LIST, $SCHEMA)?;
            let expr = in_list(cast_expr, cast_list_exprs, $NEGATED, $SCHEMA).unwrap();
            let result = expr.evaluate(&$BATCH)?.into_array($BATCH.num_rows());
            let result =
                as_boolean_array(&result).expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn in_list_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let list = vec![lit("a"), lit("b")];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b")];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_binary() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Binary, true)]);
        let a = BinaryArray::from(vec![
            Some([1, 2, 3].as_slice()),
            Some([1, 2, 2].as_slice()),
            None,
        ]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ([1, 2, 3], [4, 5, 6])"
        let list = vec![lit([1, 2, 3].as_slice()), lit([4, 5, 6].as_slice())];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ([1, 2, 3], [4, 5, 6])"
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in ([1, 2, 3], [4, 5, 6], null)"
        let list = vec![
            lit([1, 2, 3].as_slice()),
            lit([4, 5, 6].as_slice()),
            lit(ScalarValue::Binary(None)),
        ];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a in ([1, 2, 3], [4, 5, 6], null)"
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_int64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![lit(0i64), lit(1i64)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![lit(0i64), lit(1i64)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![lit(0i64), lit(1i64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![lit(0i64), lit(1i64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_float64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let a = Float64Array::from(vec![Some(0.0), Some(0.2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0.0, 0.2)"
        let list = vec![lit(0.0f64), lit(0.1f64)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0.0, 0.2)"
        let list = vec![lit(0.0f64), lit(0.1f64)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (0.0, 0.2, NULL)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0.0, 0.2, NULL)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (true)"
        let list = vec![lit(true)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (true)"
        let list = vec![lit(true)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (true, NULL)"
        let list = vec![lit(true), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (true, NULL)"
        let list = vec![lit(true), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_date64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Date64, true)]);
        let a = Date64Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date64(Some(0))),
            lit(ScalarValue::Date64(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_date32() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Date32, true)]);
        let a = Date32Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![
            lit(ScalarValue::Date32(Some(0))),
            lit(ScalarValue::Date32(Some(1))),
            lit(ScalarValue::Null),
        ];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_decimal() -> Result<()> {
        // Now, we can check the NULL type
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);
        let array = vec![Some(100_0000_i128), None, Some(200_5000_i128)]
            .into_iter()
            .collect::<Decimal128Array>();
        let array = array.with_precision_and_scale(13, 4).unwrap();
        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;

        // expression: "a in (100,200), the data type of list is INT32
        let list = vec![lit(100i32), lit(200i32)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, Some(false)],
            col_a.clone(),
            &schema
        );
        // expression: "a not in (100,200)
        let list = vec![lit(100i32), lit(200i32)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, Some(true)],
            col_a.clone(),
            &schema
        );

        // expression: "a in (200,NULL), the data type of list is INT32 AND NULL
        let list = vec![lit(ScalarValue::Int32(Some(100))), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );
        // expression: "a not in (200,NULL), the data type of list is INT32 AND NULL
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (200.5, 100), the data type of list is FLOAT32 and INT32
        let list = vec![lit(200.50f32), lit(100i32)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, Some(true)],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (200.5, 100), the data type of list is FLOAT32 and INT32
        let list = vec![lit(200.50f32), lit(101i32)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(true), None, Some(false)],
            col_a.clone(),
            &schema
        );

        // test the optimization: set
        // expression: "a in (99..300), the data type of list is INT32
        let list = (99i32..300).into_iter().map(lit).collect::<Vec<_>>();

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, Some(false)],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, Some(true)],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn test_cast_static_filter_to_set() -> Result<()> {
        // random schema
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);

        // list of phy expr
        let mut phy_exprs = vec![
            lit(1i64),
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            expressions::try_cast(lit(3.13f32), &schema, DataType::Int64)?,
        ];
        let result = try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        let array = Int64Array::from(vec![1, 2, 3, 4]);
        let r = result.contains(&array, false).unwrap();
        assert_eq!(r, BooleanArray::from(vec![true, true, true, false]));

        try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();
        // cast(cast(lit())), but the cast to the same data type, one case will be ignored
        phy_exprs.push(expressions::cast(
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            &schema,
            DataType::Int64,
        )?);
        try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        phy_exprs.clear();

        // case(cast(lit())), the cast to the diff data type
        phy_exprs.push(expressions::cast(
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            &schema,
            DataType::Int32,
        )?);
        try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        // column
        phy_exprs.push(expressions::col("a", &schema)?);
        assert!(try_cast_static_filter_to_set(&phy_exprs, &schema).is_err());

        Ok(())
    }

    #[test]
    fn in_list_timestamp() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let a = TimestampMicrosecondArray::from(vec![
            Some(1388588401000000000),
            Some(1288588501000000000),
            None,
        ]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        let list = vec![
            lit(ScalarValue::TimestampMicrosecond(
                Some(1388588401000000000),
                None,
            )),
            lit(ScalarValue::TimestampMicrosecond(
                Some(1388588401000000001),
                None,
            )),
            lit(ScalarValue::TimestampMicrosecond(
                Some(1388588401000000002),
                None,
            )),
        ];

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list.clone(),
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );
        Ok(())
    }
}

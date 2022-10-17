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

use arrow::datatypes::{DataType, Schema, SchemaRef};

use arrow::record_batch::RecordBatch;

use datafusion_common::Result;

use datafusion_expr::{ColumnarValue, Operator};
use std::fmt::{Debug, Display};

use crate::expressions::{BinaryExpr, Column};
use crate::utils::transform;
use crate::PhysicalSortExpr;
use arrow::array::{make_array, Array, ArrayRef, BooleanArray, MutableArrayData};
use arrow::compute::{and_kleene, filter_record_batch, is_not_null, SlicesIterator};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Expression that can be evaluated against a RecordBatch
/// A Physical expression knows its type, nullability and how to evaluate itself.
pub trait PhysicalExpr: Send + Sync + Display + Debug + PartialEq<dyn Any> {
    /// Returns the physical expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
    /// Evaluate an expression against a RecordBatch after first applying a
    /// validity array
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        let tmp_batch = filter_record_batch(batch, selection)?;

        let tmp_result = self.evaluate(&tmp_batch)?;
        // All values from the `selection` filter are true.
        if batch.num_rows() == tmp_batch.num_rows() {
            return Ok(tmp_result);
        }
        if let ColumnarValue::Array(a) = tmp_result {
            let result = scatter(selection, a.as_ref())?;
            Ok(ColumnarValue::Array(result))
        } else {
            Ok(tmp_result)
        }
    }

    /// Get a list of child PhysicalExpr that provide the input for this plan.
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Returns a new PhysicalExpr where all children were replaced by new exprs.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Scatter `truthy` array by boolean mask. When the mask evaluates `true`, next values of `truthy`
/// are taken, when the mask evaluates `false` values null values are filled.
///
/// # Arguments
/// * `mask` - Boolean values used to determine where to put the `truthy` values
/// * `truthy` - All values of this array are to scatter according to `mask` into final result.
fn scatter(mask: &BooleanArray, truthy: &dyn Array) -> Result<ArrayRef> {
    let truthy = truthy.data();

    // update the mask so that any null values become false
    // (SlicesIterator doesn't respect nulls)
    let mask = and_kleene(mask, &is_not_null(mask)?)?;

    let mut mutable = MutableArrayData::new(vec![truthy], true, mask.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;
    // keep track of current position we have in truthy array
    let mut true_pos = 0;

    SlicesIterator::new(&mask).for_each(|(start, end)| {
        // the gap needs to be filled with nulls
        if start > filled {
            mutable.extend_nulls(start - filled);
        }
        // fill with truthy values
        let len = end - start;
        mutable.extend(0, true_pos, true_pos + len);
        true_pos += len;
        filled = end;
    });
    // the remaining part is falsy
    if filled < mask.len() {
        mutable.extend_nulls(mask.len() - filled);
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

/// Compare the two expr lists are equal no matter the order.
/// For example two InListExpr can be considered to be equals no matter the order:
///
/// In('a','b','c') == In('c','b','a')
///
/// Another example is for Partition Exprs, we can safely consider the below two exprs are equal:
///
/// HashPartitioned('a','b','c') == HashPartitioned('c','b','a')
pub fn expr_list_eq_any_order(
    list1: &[Arc<dyn PhysicalExpr>],
    list2: &[Arc<dyn PhysicalExpr>],
) -> bool {
    list1.len() == list2.len()
        && list1.iter().all(|e1| list2.iter().any(|e2| e2.eq(e1)))
        && list2.iter().all(|e2| list1.iter().any(|e1| e1.eq(e2)))
}

/// Strictly compare the two sort expr lists in the given order.
///
/// For Physical Sort Exprs, the order matters:
///
/// SortExpr('a','b','c') != SortExpr('c','b','a')
pub fn sort_expr_list_eq_strict_order(
    list1: &[PhysicalSortExpr],
    list2: &[PhysicalSortExpr],
) -> bool {
    list1.len() == list2.len() && list1.iter().zip(list2.iter()).all(|(e1, e2)| e1.eq(e2))
}

/// Assume the predicate is in the form of CNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 AND b1 <= b2 AND c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
///
pub fn split_predicate(predicate: &Arc<dyn PhysicalExpr>) -> Vec<&Arc<dyn PhysicalExpr>> {
    match predicate.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) => match binary.op() {
            Operator::And => {
                let mut vec1 = split_predicate(binary.left());
                let vec2 = split_predicate(binary.right());
                vec1.extend(vec2);
                vec1
            }
            _ => vec![predicate],
        },
        None => vec![],
    }
}

pub fn combine_equivalence_properties(
    eq_properties: &mut Vec<Vec<Column>>,
    new_condition: (&Column, &Column),
) {
    let mut idx1 = -1i32;
    let mut idx2 = -1i32;
    for (idx, prop) in eq_properties.iter_mut().enumerate() {
        let contains_first = prop.contains(new_condition.0);
        let contains_second = prop.contains(new_condition.1);
        if contains_first && !contains_second {
            prop.push(new_condition.1.clone());
            idx1 = idx as i32;
        } else if !contains_first && contains_second {
            prop.push(new_condition.0.clone());
            idx2 = idx as i32;
        } else if contains_first && contains_second {
            idx1 = idx as i32;
            idx2 = idx as i32;
            break;
        }
    }

    if idx1 != -1 && idx2 != -1 && idx1 != idx2 {
        // need to merge the two existing properties
        let second_properties = eq_properties.get(idx2 as usize).unwrap().clone();
        let first_properties = eq_properties.get_mut(idx1 as usize).unwrap();
        for prop in second_properties {
            first_properties.push(prop)
        }
        eq_properties.remove(idx2 as usize);
    } else if idx1 == -1 && idx2 == -1 {
        // adding new pairs
        eq_properties.push(vec![new_condition.0.clone(), new_condition.1.clone()])
    }
}

pub fn remove_equivalence_properties(
    eq_properties: &mut Vec<Vec<Column>>,
    remove_condition: (&Column, &Column),
) {
    let mut match_idx = -1i32;
    for (idx, prop) in eq_properties.iter_mut().enumerate() {
        let contains_first = prop.contains(remove_condition.0);
        let contains_second = prop.contains(remove_condition.1);
        if contains_first && contains_second {
            match_idx = idx as i32;
        }
    }
    if match_idx >= 0 {
        let matches = eq_properties.get_mut(match_idx as usize).unwrap();
        matches.retain(|e| (e != remove_condition.0 && e != remove_condition.1));
        if matches.is_empty() {
            eq_properties.remove(match_idx as usize);
        }
    }
}

pub fn merge_equivalence_properties_with_alias(
    eq_properties: &mut Vec<Vec<Column>>,
    alias_map: &HashMap<Column, Vec<Column>>,
) {
    for (column, columns) in alias_map {
        let mut find_match = false;
        for (_idx, prop) in eq_properties.iter_mut().enumerate() {
            if prop.contains(column) {
                prop.extend(columns.clone());
                find_match = true;
                break;
            }
        }
        if !find_match {
            let mut new_properties = vec![column.clone()];
            new_properties.extend(columns.clone());
            eq_properties.push(new_properties);
        }
    }
}

pub fn truncate_equivalence_properties_not_in_schema(
    eq_properties: &mut Vec<Vec<Column>>,
    schema: &SchemaRef,
) {
    for props in eq_properties.iter_mut() {
        props.retain(|column| matches!(schema.index_of(column.name()), Ok(idx) if idx == column.index()))
    }
    eq_properties.retain(|props| !props.is_empty());
}

/// Normalize the output expressions base on Alias Map and SchemaRef.
///
/// 1) If there is mapping in Alias Map, replace the Column in the output expressions with the 1st Column in Alias Map
/// 2) If the Column is invalid for the current Schema, replace the Column with a place holder Column with index = usize::MAX
///
pub fn normalize_out_expr_with_alias_schema(
    expr: Arc<dyn PhysicalExpr>,
    alias_map: &HashMap<Column, Vec<Column>>,
    schema: &SchemaRef,
) -> Arc<dyn PhysicalExpr> {
    transform(expr.clone(), &|expr| {
        let normalized_form: Option<Arc<dyn PhysicalExpr>> =
            match expr.as_any().downcast_ref::<Column>() {
                Some(column) => {
                    let out = alias_map
                        .get(column)
                        .map(|c| {
                            let out_col: Arc<dyn PhysicalExpr> = Arc::new(c[0].clone());
                            out_col
                        })
                        .or_else(|| match schema.index_of(column.name()) {
                            // Exactly matching, return None, no need to do the transform
                            Ok(idx) if column.index() == idx => None,
                            _ => {
                                let out_col: Arc<dyn PhysicalExpr> =
                                    Arc::new(Column::new(column.name(), usize::MAX));
                                Some(out_col)
                            }
                        });
                    out
                }
                None => None,
            };
        normalized_form
    })
    .unwrap_or(expr)
}

pub fn normalize_expr_with_equivalence_properties(
    expr: Arc<dyn PhysicalExpr>,
    eq_properties: &Vec<Vec<Column>>,
) -> Arc<dyn PhysicalExpr> {
    let mut normalized = expr.clone();
    match expr.as_any().downcast_ref::<Column>() {
        Some(column) => {
            for prop in eq_properties {
                if prop.contains(column) {
                    normalized = Arc::new(prop.get(0).unwrap().clone());
                    break;
                }
            }
        }
        None => {}
    }
    normalized
}

pub fn normalize_sort_expr_with_equivalence_properties(
    sort_expr: PhysicalSortExpr,
    eq_properties: &Vec<Vec<Column>>,
) -> PhysicalSortExpr {
    let mut normalized = sort_expr.clone();
    match sort_expr.expr.as_any().downcast_ref::<Column>() {
        Some(column) => {
            for prop in eq_properties {
                if prop.contains(column) {
                    normalized = PhysicalSortExpr {
                        expr: Arc::new(prop.get(0).unwrap().clone()),
                        options: sort_expr.options,
                    };
                    break;
                }
            }
        }
        None => {}
    }
    normalized
}

pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::Int32Array;
    use datafusion_common::Result;

    #[test]
    fn scatter_int() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        // the output array is expected to be the same length as the mask array
        let expected =
            Int32Array::from_iter(vec![Some(1), Some(10), None, None, Some(11)]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_int_end_with_false() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, false, true, false, false, false]);

        // output should be same length as mask
        let expected =
            Int32Array::from_iter(vec![Some(1), None, Some(10), None, None, None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_with_null_mask() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11]));
        let mask: BooleanArray = vec![Some(false), None, Some(true), Some(true), None]
            .into_iter()
            .collect();

        // output should treat nulls as though they are false
        let expected = Int32Array::from_iter(vec![None, None, Some(1), Some(10), None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_boolean() -> Result<()> {
        let truthy = Arc::new(BooleanArray::from(vec![false, false, false, true]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        // the output array is expected to be the same length as the mask array
        let expected = BooleanArray::from_iter(vec![
            Some(false),
            Some(false),
            None,
            None,
            Some(false),
        ]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(&expected, result);
        Ok(())
    }
}

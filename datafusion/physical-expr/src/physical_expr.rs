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

use std::any::Any;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::intervals::Interval;
use crate::sort_properties::SortProperties;
use crate::utils::scatter;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::DataPtr;
use datafusion_common::{internal_err, not_impl_err, DataFusionError, Result};
use datafusion_expr::ColumnarValue;

use itertools::izip;

/// Expression that can be evaluated against a RecordBatch
/// A Physical expression knows its type, nullability and how to evaluate itself.
pub trait PhysicalExpr: Send + Sync + Display + Debug + PartialEq<dyn Any> {
    /// Returns the physical expression as [`Any`] so that it can be
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

        if batch.num_rows() == tmp_batch.num_rows() {
            // All values from the `selection` filter are true.
            Ok(tmp_result)
        } else if let ColumnarValue::Array(a) = tmp_result {
            scatter(selection, a.as_ref()).map(ColumnarValue::Array)
        } else {
            Ok(tmp_result)
        }
    }

    /// Get a list of child PhysicalExpr that provide the input for this expr.
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Returns a new PhysicalExpr where all children were replaced by new exprs.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// Computes the output interval for the expression, given the input
    /// intervals.
    ///
    /// # Arguments
    ///
    /// * `children` are the intervals for the children (inputs) of this
    /// expression.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, and the input intervals are `a: [1, 2]`
    /// and `b: [3, 4]`, then the output interval would be `[4, 6]`.
    fn evaluate_bounds(&self, _children: &[&Interval]) -> Result<Interval> {
        not_impl_err!("Not implemented for {self}")
    }

    /// Updates bounds for child expressions, given a known interval for this
    /// expression.
    ///
    /// This is used to propagate constraints down through an
    /// expression tree.
    ///
    /// # Arguments
    ///
    /// * `interval` is the currently known interval for this expression.
    /// * `children` are the current intervals for the children of this expression
    ///
    /// # Returns
    ///
    /// A Vec of new intervals for the children, in order.
    ///
    /// If constraint propagation reveals an infeasibility, returns [None] for
    /// the child causing infeasibility.
    ///
    /// If none of the child intervals change as a result of propagation, may
    /// return an empty vector instead of cloning `children`.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, the current `interval` is `[4, 5] and the
    /// inputs are given [`a: [0, 2], `b: [-∞, 4]]`, then propagation would
    /// would return `[a: [0, 2], b: [2, 4]]` as `b` must be at least 2 to
    /// make the output at least `4`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _children: &[&Interval],
    ) -> Result<Vec<Option<Interval>>> {
        not_impl_err!("Not implemented for {self}")
    }

    /// Update the hash `state` with this expression requirements from
    /// [`Hash`].
    ///
    /// This method is required to support hashing [`PhysicalExpr`]s.  To
    /// implement it, typically the type implementing
    /// [`PhysicalExpr`] implements [`Hash`] and
    /// then the following boiler plate is used:
    ///
    /// # Example:
    /// ```
    /// // User defined expression that derives Hash
    /// #[derive(Hash, Debug, PartialEq, Eq)]
    /// struct MyExpr {
    ///   val: u64
    /// }
    ///
    /// // impl PhysicalExpr {
    /// // ...
    /// # impl MyExpr {
    ///   // Boiler plate to call the derived Hash impl
    ///   fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
    ///     use std::hash::Hash;
    ///     let mut s = state;
    ///     self.hash(&mut s);
    ///   }
    /// // }
    /// # }
    /// ```
    /// Note: [`PhysicalExpr`] is not constrained by [`Hash`]
    /// directly because it must remain object safe.
    fn dyn_hash(&self, _state: &mut dyn Hasher);

    /// The order information of a PhysicalExpr can be estimated from its children.
    /// This is especially helpful for projection expressions. If we can ensure that the
    /// order of a PhysicalExpr to project matches with the order of SortExec, we can
    /// eliminate that SortExecs.
    ///
    /// By recursively calling this function, we can obtain the overall order
    /// information of the PhysicalExpr. Since `SortOptions` cannot fully handle
    /// the propagation of unordered columns and literals, the `SortProperties`
    /// struct is used.
    fn get_ordering(&self, _children: &[SortProperties]) -> SortProperties {
        SortProperties::Unordered
    }
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

/// Shared [`PhysicalExpr`].
pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;

/// Returns a copy of this expr if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `PhysicalExpr::children()`.
pub fn with_new_children_if_necessary(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let old_children = expr.children();
    if children.len() != old_children.len() {
        internal_err!("PhysicalExpr: Wrong number of children")
    } else if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::data_ptr_eq(c1, c2))
    {
        expr.with_new_children(children)
    } else {
        Ok(expr)
    }
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

/// This function is similar to the `contains` method of `Vec`. It finds
/// whether `expr` is among `physical_exprs`.
pub fn physical_exprs_contains(
    physical_exprs: &[Arc<dyn PhysicalExpr>],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    physical_exprs
        .iter()
        .any(|physical_expr| physical_expr.eq(expr))
}

/// Checks whether the given slices have any common entries.
pub fn have_common_entries(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    lhs.iter().any(|expr| physical_exprs_contains(rhs, expr))
}

/// Checks whether the given physical expression slices are equal.
pub fn physical_exprs_equal(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    lhs.len() == rhs.len() && izip!(lhs, rhs).all(|(lhs, rhs)| lhs.eq(rhs))
}

/// Checks whether the given physical expression slices are equal in the sense
/// of bags (multi-sets), disregarding their orderings.
pub fn physical_exprs_bag_equal(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    // TODO: Once we can use `HashMap`s with `Arc<dyn PhysicalExpr>`, this
    //       function should use a `HashMap` to reduce computational complexity.
    if lhs.len() == rhs.len() {
        let mut rhs_vec = rhs.to_vec();
        for expr in lhs {
            if let Some(idx) = rhs_vec.iter().position(|e| expr.eq(e)) {
                rhs_vec.swap_remove(idx);
            } else {
                return false;
            }
        }
        true
    } else {
        false
    }
}

/// This utility function removes duplicates from the given `physical_exprs`
/// vector. Note that this function does not necessarily preserve its input
/// ordering.
pub fn deduplicate_physical_exprs(physical_exprs: &mut Vec<Arc<dyn PhysicalExpr>>) {
    // TODO: Once we can use `HashSet`s with `Arc<dyn PhysicalExpr>`, this
    //       function should use a `HashSet` to reduce computational complexity.
    let mut idx = 0;
    while idx < physical_exprs.len() {
        let mut rest_idx = idx + 1;
        while rest_idx < physical_exprs.len() {
            if physical_exprs[idx].eq(&physical_exprs[rest_idx]) {
                physical_exprs.swap_remove(rest_idx);
            } else {
                rest_idx += 1;
            }
        }
        idx += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expressions::{Column, Literal};
    use crate::physical_expr::{
        deduplicate_physical_exprs, have_common_entries, physical_exprs_bag_equal,
        physical_exprs_contains, physical_exprs_equal, PhysicalExpr,
    };

    use datafusion_common::ScalarValue;

    #[test]
    fn test_physical_exprs_contains() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit4 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_a_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;

        // lit(true), lit(false), lit(4), lit(2), Col(a), Col(b)
        let physical_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            lit_true.clone(),
            lit_false.clone(),
            lit4.clone(),
            lit2.clone(),
            col_a_expr.clone(),
            col_b_expr.clone(),
        ];
        // below expressions are inside physical_exprs
        assert!(physical_exprs_contains(&physical_exprs, &lit_true));
        assert!(physical_exprs_contains(&physical_exprs, &lit2));
        assert!(physical_exprs_contains(&physical_exprs, &col_b_expr));

        // below expressions are not inside physical_exprs
        assert!(!physical_exprs_contains(&physical_exprs, &col_c_expr));
        assert!(!physical_exprs_contains(&physical_exprs, &lit1));
    }

    #[test]
    fn test_have_common_entries() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let vec1 = vec![lit_true.clone(), lit_false.clone()];
        let vec2 = vec![lit_true.clone(), col_b_expr.clone()];
        let vec3 = vec![lit2.clone(), lit1.clone()];

        // lit_true is common
        assert!(have_common_entries(&vec1, &vec2));
        // there is no common entry
        assert!(!have_common_entries(&vec1, &vec3));
        assert!(!have_common_entries(&vec2, &vec3));
    }

    #[test]
    fn test_physical_exprs_equal() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let vec1 = vec![lit_true.clone(), lit_false.clone()];
        let vec2 = vec![lit_true.clone(), col_b_expr.clone()];
        let vec3 = vec![lit2.clone(), lit1.clone()];
        let vec4 = vec![lit_true.clone(), lit_false.clone()];

        // these vectors are same
        assert!(physical_exprs_equal(&vec1, &vec1));
        assert!(physical_exprs_equal(&vec1, &vec4));
        assert!(physical_exprs_bag_equal(&vec1, &vec1));
        assert!(physical_exprs_bag_equal(&vec1, &vec4));

        // these vectors are different
        assert!(!physical_exprs_equal(&vec1, &vec2));
        assert!(!physical_exprs_equal(&vec1, &vec3));
        assert!(!physical_exprs_bag_equal(&vec1, &vec2));
        assert!(!physical_exprs_bag_equal(&vec1, &vec3));
    }

    #[test]
    fn test_physical_exprs_set_equal() {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(!physical_exprs_bag_equal(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!physical_exprs_bag_equal(
            list2.as_slice(),
            list1.as_slice()
        ));
        assert!(!physical_exprs_equal(list1.as_slice(), list2.as_slice()));
        assert!(!physical_exprs_equal(list2.as_slice(), list1.as_slice()));

        let list3: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list4: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(physical_exprs_bag_equal(list3.as_slice(), list4.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list3.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list4.as_slice()));
        assert!(!physical_exprs_equal(list3.as_slice(), list4.as_slice()));
        assert!(!physical_exprs_equal(list4.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list3.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list4.as_slice()));
    }

    #[test]
    fn test_deduplicate_physical_exprs() {
        let lit_true = &(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>);
        let lit_false = &(Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>);
        let lit4 = &(Arc::new(Literal::new(ScalarValue::Int32(Some(4))))
            as Arc<dyn PhysicalExpr>);
        let lit2 = &(Arc::new(Literal::new(ScalarValue::Int32(Some(2))))
            as Arc<dyn PhysicalExpr>);
        let col_a_expr = &(Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>);
        let col_b_expr = &(Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>);

        // First vector in the tuple is arguments, second one is the expected value.
        let test_cases = vec![
            // ---------- TEST CASE 1----------//
            (
                vec![
                    lit_true, lit_false, lit4, lit2, col_a_expr, col_a_expr, col_b_expr,
                    lit_true, lit2,
                ],
                vec![lit_true, lit_false, lit4, lit2, col_a_expr, col_b_expr],
            ),
            // ---------- TEST CASE 2----------//
            (
                vec![lit_true, lit_true, lit_false, lit4],
                vec![lit_true, lit4, lit_false],
            ),
        ];
        for (physical_exprs, expected) in test_cases {
            let mut physical_exprs =
                physical_exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = expected.into_iter().cloned().collect::<Vec<_>>();
            deduplicate_physical_exprs(&mut physical_exprs);
            assert!(physical_exprs_equal(&physical_exprs, &expected));
        }
    }
}

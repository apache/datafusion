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
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::utils::scatter;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, not_impl_err, Result};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;

/// [`PhysicalExpr`]s represent expressions such as `A + 1` or `CAST(c1 AS int)`.
///
/// `PhysicalExpr` knows its type, nullability and can be evaluated directly on
/// a [`RecordBatch`] (see [`Self::evaluate`]).
///
/// `PhysicalExpr` are the physical counterpart to [`Expr`] used in logical
/// planning. They are typically created from [`Expr`] by a [`PhysicalPlanner`]
/// invoked from a higher level API
///
/// Some important examples of `PhysicalExpr` are:
/// * [`Column`]: Represents a column at a given index in a RecordBatch
///
/// To create `PhysicalExpr` from  `Expr`, see
/// * [`SessionContext::create_physical_expr`]: A high level API
/// * [`create_physical_expr`]: A low level API
///
/// [`SessionContext::create_physical_expr`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.create_physical_expr
/// [`PhysicalPlanner`]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html
/// [`Expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
/// [`create_physical_expr`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/fn.create_physical_expr.html
/// [`Column`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/expressions/struct.Column.html
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
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;

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
    ///   expression.
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
    /// This is used to propagate constraints down through an expression tree.
    ///
    /// # Arguments
    ///
    /// * `interval` is the currently known interval for this expression.
    /// * `children` are the current intervals for the children of this expression.
    ///
    /// # Returns
    ///
    /// A `Vec` of new intervals for the children, in order.
    ///
    /// If constraint propagation reveals an infeasibility for any child, returns
    /// [`None`]. If none of the children intervals change as a result of propagation,
    /// may return an empty vector instead of cloning `children`. This is the default
    /// (and conservative) return value.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, the current `interval` is `[4, 5]` and the
    /// inputs `a` and `b` are respectively given as `[0, 2]` and `[-∞, 4]`, then
    /// propagation would return `[0, 2]` and `[2, 4]` as `b` must be at least
    /// `2` to make the output at least `4`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        Ok(Some(vec![]))
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

    /// Calculates the properties of this [`PhysicalExpr`] based on its
    /// children's properties (i.e. order and range), recursively aggregating
    /// the information from its children. In cases where the [`PhysicalExpr`]
    /// has no children (e.g., `Literal` or `Column`), these properties should
    /// be specified externally, as the function defaults to unknown properties.
    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties::new_unknown())
    }
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

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
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        Ok(expr.with_new_children(children)?)
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

/// Returns [`Display`] able a list of [`PhysicalExpr`]
///
/// Example output: `[a + 1, b]`
pub fn format_physical_expr_list(exprs: &[Arc<dyn PhysicalExpr>]) -> impl Display + '_ {
    struct DisplayWrapper<'a>(&'a [Arc<dyn PhysicalExpr>]);
    impl<'a> Display for DisplayWrapper<'a> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut iter = self.0.iter();
            write!(f, "[")?;
            if let Some(expr) = iter.next() {
                write!(f, "{}", expr)?;
            }
            for expr in iter {
                write!(f, ", {}", expr)?;
            }
            write!(f, "]")?;
            Ok(())
        }
    }
    DisplayWrapper(exprs)
}

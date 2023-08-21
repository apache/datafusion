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

use crate::intervals::Interval;
use crate::utils::scatter;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::SortOptions;
use datafusion_common::utils::DataPtr;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

use std::any::Any;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::ops::Neg;
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

    /// Get a list of child PhysicalExpr that provide the input for this expr.
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Returns a new PhysicalExpr where all children were replaced by new exprs.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// Computes bounds for the expression using interval arithmetic.
    fn evaluate_bounds(&self, _children: &[&Interval]) -> Result<Interval> {
        Err(DataFusionError::NotImplemented(format!(
            "Not implemented for {self}"
        )))
    }

    /// Updates/shrinks bounds for the expression using interval arithmetic.
    /// If constraint propagation reveals an infeasibility, returns [None] for
    /// the child causing infeasibility. If none of the children intervals
    /// change, may return an empty vector instead of cloning `children`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _children: &[&Interval],
    ) -> Result<Vec<Option<Interval>>> {
        Err(DataFusionError::NotImplemented(format!(
            "Not implemented for {self}"
        )))
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
    /// the propagation of unordered columns and literals, the `ExtendedSortOptions`
    /// struct is used.
    fn get_ordering(&self, _children: &[ExtendedSortOptions]) -> ExtendedSortOptions {
        ExtendedSortOptions::Unordered
    }
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

/// To propagate [`SortOptions`] across the [`PhysicalExpr`], it is insufficient
/// to simply use `Option<SortOptions>`: There must be a differentiation between
/// unordered columns and literal values, since literals may not break the ordering
/// when they are used as a child of some binary expression when the other child has
/// some ordering. On the other hand, unordered columns cannot maintain ordering when
/// they take part in such operations.
///
/// Example: ((a_ordered + b_unordered) + c_ordered) expression cannot end up with
/// sorted data; however the ((a_ordered + 999) + c_ordered) expression can. Therefore,
/// we need two different variants for literals and unordered columns as literals are
/// often more ordering-friendly under most mathematical operations.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ExtendedSortOptions {
    /// Use the ordinary [`SortOptions`] struct to represent ordered data:
    Ordered(SortOptions),
    // This alternative represents unordered data:
    Unordered,
    // Singleton is used for single-valued literal numbers:
    Singleton,
}

impl Neg for ExtendedSortOptions {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            ExtendedSortOptions::Ordered(SortOptions {
                descending,
                nulls_first,
            }) => ExtendedSortOptions::Ordered(SortOptions {
                descending: !descending,
                nulls_first,
            }),
            ExtendedSortOptions::Singleton => ExtendedSortOptions::Singleton,
            ExtendedSortOptions::Unordered => ExtendedSortOptions::Unordered,
        }
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
        Err(DataFusionError::Internal(
            "PhysicalExpr: Wrong number of children".to_string(),
        ))
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

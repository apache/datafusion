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

//! [`AggregateExpr`] which defines the interface all aggregate expressions
//! (built-in and custom) need to satisfy.

use crate::order::AggregateOrderSensitivity;
use arrow::datatypes::Field;
use datafusion_common::exec_err;
use datafusion_common::{not_impl_err, Result};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};

pub mod count_distinct;
pub mod groups_accumulator;

/// An aggregate expression that:
/// * knows its resulting field
/// * knows how to create its accumulator
/// * knows its accumulator's state's field
/// * knows the expressions from whose its accumulator will receive values
///
/// Any implementation of this trait also needs to implement the
/// `PartialEq<dyn Any>` to allows comparing equality between the
/// trait objects.
pub trait AggregateExpr: Send + Sync + Debug + PartialEq<dyn Any> {
    /// Returns the aggregate expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// the fields that encapsulate the Accumulator's state
    /// the number of fields here equals the number of states that the accumulator contains
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Order by requirements for the aggregate function
    /// By default it is `None` (there is no requirement)
    /// Order-sensitive aggregators, such as `FIRST_VALUE(x ORDER BY y)` should implement this
    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    /// Indicates whether aggregator can produce the correct result with any
    /// arbitrary input ordering. By default, we assume that aggregate expressions
    /// are order insensitive.
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    /// Sets the indicator whether ordering requirements of the aggregator is
    /// satisfied by its input. If this is not the case, aggregators with order
    /// sensitivity `AggregateOrderSensitivity::Beneficial` can still produce
    /// the correct result with possibly more work internally.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(updated_expr))` if the process completes successfully.
    /// If the expression can benefit from existing input ordering, but does
    /// not implement the method, returns an error. Order insensitive and hard
    /// requirement aggregators return `Ok(None)`.
    fn with_beneficial_ordering(
        self: Arc<Self>,
        _requirement_satisfied: bool,
    ) -> Result<Option<Arc<dyn AggregateExpr>>> {
        if self.order_bys().is_some() && self.order_sensitivity().is_beneficial() {
            return exec_err!(
                "Should implement with satisfied for aggregator :{:?}",
                self.name()
            );
        }
        Ok(None)
    }

    /// Human readable name such as `"MIN(c2)"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "AggregateExpr: default name"
    }

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator`] will be called.
    fn groups_accumulator_supported(&self) -> bool {
        false
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("GroupsAccumulator hasn't been implemented for {self:?} yet")
    }

    /// Construct an expression that calculates the aggregate in reverse.
    /// Typically the "reverse" expression is itself (e.g. SUM, COUNT).
    /// For aggregates that do not support calculation in reverse,
    /// returns None (which is the default value).
    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        None
    }

    /// Creates accumulator implementation that supports retract
    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("Retractable Accumulator hasn't been implemented for {self:?} yet")
    }

    /// Returns all expressions used in the [`AggregateExpr`].
    /// These expressions are  (1)function arguments, (2) order by expressions.
    fn all_expressions(&self) -> AggregatePhysicalExpressions {
        let args = self.expressions();
        let order_bys = self.order_bys().unwrap_or(&[]);
        let order_by_exprs = order_bys
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();
        AggregatePhysicalExpressions {
            args,
            order_by_exprs,
        }
    }

    /// Rewrites [`AggregateExpr`], with new expressions given. The argument should be consistent
    /// with the return value of the [`AggregateExpr::all_expressions`] method.
    /// Returns `Some(Arc<dyn AggregateExpr>)` if re-write is supported, otherwise returns `None`.
    fn with_new_expressions(
        &self,
        _args: Vec<Arc<dyn PhysicalExpr>>,
        _order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn AggregateExpr>> {
        None
    }

    /// If this function is max, return (output_field, true)
    /// if the function is min, return (output_field, false)
    /// otherwise return None (the default)
    ///
    /// output_field is the name of the column produced by this aggregate
    ///
    /// Note: this is used to use special aggregate implementations in certain conditions
    fn get_minmax_desc(&self) -> Option<(Field, bool)> {
        None
    }
}

/// Stores the physical expressions used inside the `AggregateExpr`.
pub struct AggregatePhysicalExpressions {
    /// Aggregate function arguments
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// Order by expressions
    pub order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

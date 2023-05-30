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

//! This module defines the interface for logical nodes
use crate::{Expr, LogicalPlan};
use datafusion_common::{DFSchema, DFSchemaRef};
use std::hash::{Hash, Hasher};
use std::{any::Any, cmp::Eq, collections::HashSet, fmt, sync::Arc};

/// This defines the interface for [`LogicalPlan`] nodes that can be
/// used to extend DataFusion with custom relational operators.
///
/// The [`UserDefinedLogicalNodeCore`] trait is *the recommended way to implement*
/// this trait and avoids having implementing some required boiler plate code.
pub trait UserDefinedLogicalNode: fmt::Debug + Send + Sync {
    /// Return a reference to self as Any, to support dynamic downcasting
    ///
    /// Typically this will look like:
    ///
    /// ```
    /// # use std::any::Any;
    /// # struct Dummy { }
    ///
    /// # impl Dummy {
    ///   // canonical boiler plate
    ///   fn as_any(&self) -> &dyn Any {
    ///      self
    ///   }
    /// # }
    /// ```
    fn as_any(&self) -> &dyn Any;

    /// Return the plan's name.
    fn name(&self) -> &str;

    /// Return the logical plan's inputs.
    fn inputs(&self) -> Vec<&LogicalPlan>;

    /// Return the output schema of this logical plan node.
    fn schema(&self) -> &DFSchemaRef;

    /// Returns all expressions in the current logical plan node. This
    /// should not include expressions of any inputs (aka
    /// non-recursively). These expressions are used for optimizer
    /// passes and rewrites.
    fn expressions(&self) -> Vec<Expr>;

    /// A list of output columns (e.g. the names of columns in
    /// self.schema()) for which predicates can not be pushed below
    /// this node without changing the output.
    ///
    /// By default, this returns all columns and thus prevents any
    /// predicates from being pushed below this node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // default (safe) is all columns in the schema.
        get_all_columns_from_schema(self.schema())
    }

    /// Write a single line, human readable string to `f` for use in explain plan.
    ///
    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result;

    /// Create a new `ExtensionPlanNode` with the specified children
    /// and expressions. This function is used during optimization
    /// when the plan is being rewritten and a new instance of the
    /// `ExtensionPlanNode` must be created.
    ///
    /// Note that exprs and inputs are in the same order as the result
    /// of self.inputs and self.exprs.
    ///
    /// So, `self.from_template(exprs, ..).expressions() == exprs
    //
    // TODO(clippy): This should probably be renamed to use a `with_*` prefix. Something
    // like `with_template`, or `with_exprs_and_inputs`.
    //
    // Also, I think `ExtensionPlanNode` has been renamed to `UserDefinedLogicalNode`
    // but the doc comments have not been updated.
    #[allow(clippy::wrong_self_convention)]
    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode>;

    /// Update the hash `state` with this node requirements from
    /// [`Hash`].
    ///
    /// Note: consider using [`UserDefinedLogicalNodeCore`] instead of
    /// [`UserDefinedLogicalNode`] directly.
    ///
    /// This method is required to support hashing [`LogicalPlan`]s.  To
    /// implement it, typically the type implementing
    /// [`UserDefinedLogicalNode`] typically implements [`Hash`] and
    /// then the following boiler plate is used:
    ///
    /// # Example:
    /// ```
    /// // User defined node that derives Hash
    /// #[derive(Hash, Debug, PartialEq, Eq)]
    /// struct MyNode {
    ///   val: u64
    /// }
    ///
    /// // impl UserDefinedLogicalNode {
    /// // ...
    /// # impl MyNode {
    ///   // Boiler plate to call the derived Hash impl
    ///   fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
    ///     use std::hash::Hash;
    ///     let mut s = state;
    ///     self.hash(&mut s);
    ///   }
    /// // }
    /// # }
    /// ```
    /// Note: [`UserDefinedLogicalNode`] is not constrained by [`Hash`]
    /// directly because it must remain object safe.
    fn dyn_hash(&self, state: &mut dyn Hasher);

    /// Compare `other`, respecting requirements from [std::cmp::Eq].
    ///
    /// Note: consider using [`UserDefinedLogicalNodeCore`] instead of
    /// [`UserDefinedLogicalNode`] directly.
    ///
    /// When `other` has an another type than `self`, then the values
    /// are *not* equal.
    ///
    /// This method is required to support Eq on [`LogicalPlan`]s.  To
    /// implement it, typically the type implementing
    /// [`UserDefinedLogicalNode`] typically implements [`Eq`] and
    /// then the following boiler plate is used:
    ///
    /// # Example:
    /// ```
    /// # use datafusion_expr::UserDefinedLogicalNode;
    /// // User defined node that derives Eq
    /// #[derive(Hash, Debug, PartialEq, Eq)]
    /// struct MyNode {
    ///   val: u64
    /// }
    ///
    /// // impl UserDefinedLogicalNode {
    /// // ...
    /// # impl MyNode {
    ///   // Boiler plate to call the derived Eq impl
    ///   fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
    ///     match other.as_any().downcast_ref::<Self>() {
    ///       Some(o) => self == o,
    ///       None => false,
    ///     }
    ///   }
    /// // }
    /// # }
    /// ```
    /// Note: [`UserDefinedLogicalNode`] is not constrained by [`Eq`]
    /// directly because it must remain object safe.
    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool;
}

impl Hash for dyn UserDefinedLogicalNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

impl std::cmp::PartialEq for dyn UserDefinedLogicalNode {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other)
    }
}

impl Eq for dyn UserDefinedLogicalNode {}

/// This trait facilitates implementation of the [`UserDefinedLogicalNode`].
///
/// See the example in
/// [user_defined_plan.rs](../../tests/user_defined_plan.rs) for an
/// example of how to use this extension API.
pub trait UserDefinedLogicalNodeCore:
    fmt::Debug + Eq + Hash + Send + Sync + 'static
{
    /// Return the plan's name.
    fn name(&self) -> &str;

    /// Return the logical plan's inputs.
    fn inputs(&self) -> Vec<&LogicalPlan>;

    /// Return the output schema of this logical plan node.
    fn schema(&self) -> &DFSchemaRef;

    /// Returns all expressions in the current logical plan node. This
    /// should not include expressions of any inputs (aka
    /// non-recursively). These expressions are used for optimizer
    /// passes and rewrites.
    fn expressions(&self) -> Vec<Expr>;

    /// A list of output columns (e.g. the names of columns in
    /// self.schema()) for which predicates can not be pushed below
    /// this node without changing the output.
    ///
    /// By default, this returns all columns and thus prevents any
    /// predicates from being pushed below this node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // default (safe) is all columns in the schema.
        get_all_columns_from_schema(self.schema())
    }

    /// Write a single line, human readable string to `f` for use in explain plan.
    ///
    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result;

    /// Create a new `ExtensionPlanNode` with the specified children
    /// and expressions. This function is used during optimization
    /// when the plan is being rewritten and a new instance of the
    /// `ExtensionPlanNode` must be created.
    ///
    /// Note that exprs and inputs are in the same order as the result
    /// of self.inputs and self.exprs.
    ///
    /// So, `self.from_template(exprs, ..).expressions() == exprs
    //
    // TODO(clippy): This should probably be renamed to use a `with_*` prefix. Something
    // like `with_template`, or `with_exprs_and_inputs`.
    //
    // Also, I think `ExtensionPlanNode` has been renamed to `UserDefinedLogicalNode`
    // but the doc comments have not been updated.
    #[allow(clippy::wrong_self_convention)]
    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self;
}

/// Automatically derive UserDefinedLogicalNode to `UserDefinedLogicalNode`
/// to avoid boiler plate for implementing `as_any`, `Hash` and `PartialEq`
impl<T: UserDefinedLogicalNodeCore> UserDefinedLogicalNode for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expressions()
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.prevent_predicate_push_down_columns()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(self.from_template(exprs, inputs))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

fn get_all_columns_from_schema(schema: &DFSchema) -> HashSet<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

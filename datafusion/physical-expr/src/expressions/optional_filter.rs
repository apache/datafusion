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

//! [`OptionalFilterPhysicalExpr`]: a marker for filters that are not needed
//! for correctness. See the type documentation for the contract that
//! producers, consumers and rewriters must obey.

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;

/// Marks the inner filter as *optional*: it is not needed for correctness.
///
/// Some filters are only performance hints. For example, a hash join can push
/// a dynamic filter into the probe side scan, but the join itself still
/// removes the rows that do not match. Such a filter is *optional*: a
/// consumer can skip it (for example, when the filter does not remove enough
/// rows to be worth its cost) and the query result stays the same.
///
/// # Contract
///
/// * **Skip only on the root AND chain.** A consumer can skip an optional
///   filter only when the `Optional` node is a direct conjunct of the root
///   `AND` chain of its predicate. For example, in `a AND Optional(b)` the
///   consumer can skip `b`. Use [`split_optional`] to find these conjuncts.
/// * **Transparent everywhere else.** [`PhysicalExpr::evaluate`] always
///   evaluates the inner expression. Thus an `Optional` in a different
///   position (for example under `NOT`, `IS NULL`, `CASE` or `OR`) can make a
///   query slower, but it cannot make the result incorrect.
/// * **Rewriters must not move nodes across the wrapper.** A rewrite must not
///   move an expression into or out of an `Optional`. For example,
///   `NOT(Optional(x))` must not become `Optional(NOT(x))`, because that
///   would make a required filter optional.
/// * **Pruning sees through the wrapper.** [`PhysicalExpr::snapshot`]
///   returns the inner expression, so [`snapshot_physical_expr`] removes the
///   wrapper. Thus statistics pruning uses an optional filter the same as a
///   required filter.
///
/// [`split_optional`]: crate::utils::split_optional
/// [`snapshot_physical_expr`]: datafusion_physical_expr_common::physical_expr::snapshot_physical_expr
#[derive(Debug, Eq)]
pub struct OptionalFilterPhysicalExpr {
    inner: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for OptionalFilterPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Hash for OptionalFilterPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl OptionalFilterPhysicalExpr {
    /// Create a new optional filter that wraps `inner`.
    pub fn new(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { inner }
    }

    /// Get the wrapped filter expression.
    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }
}

impl fmt::Display for OptionalFilterPhysicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Optional({})", self.inner)
    }
}

impl PhysicalExpr for OptionalFilterPhysicalExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.inner.evaluate(batch)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        self.inner.evaluate_selection(batch, selection)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq_or_internal_err!(
            children.len(),
            1,
            "OptionalFilterPhysicalExpr: expected 1 child"
        );
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    // The wrapper is the identity function, so the bounds and properties of
    // the child are also the bounds and properties of the wrapper.
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        Ok(children[0].clone())
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        Ok(children[0].intersect(interval)?.map(|result| vec![result]))
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(children[0].clone())
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }

    /// Returns the inner expression, so that snapshot consumers (for example
    /// pruning) see through the wrapper.
    ///
    /// [`snapshot_physical_expr`] transforms the tree bottom up, so the inner
    /// expression is already a snapshot when this method is called. For
    /// example, `Optional(DynamicFilter)` becomes the current expression of
    /// the dynamic filter.
    ///
    /// [`snapshot_physical_expr`]: datafusion_physical_expr_common::physical_expr::snapshot_physical_expr
    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        Ok(Some(Arc::clone(&self.inner)))
    }

    fn snapshot_generation(&self) -> u64 {
        // The wrapper is not dynamic. `snapshot_generation(expr)` walks the
        // tree and adds the generation of the inner expression.
        0
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::OptionalFilter(
                Box::new(protobuf::PhysicalOptionalFilterNode {
                    inner: Some(Box::new(ctx.encode_child(&self.inner)?)),
                }),
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl OptionalFilterPhysicalExpr {
    /// Reconstruct an [`OptionalFilterPhysicalExpr`] from its protobuf
    /// representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let optional = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::OptionalFilter,
            "OptionalFilter",
        );
        let inner = ctx.decode_required_expression(
            optional.inner.as_deref(),
            "OptionalFilter",
            "inner",
        )?;

        Ok(Arc::new(Self::new(inner)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, col, lit, not};

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::Field;
    use datafusion_common::cast::as_boolean_array;
    use datafusion_expr::Operator;
    use datafusion_physical_expr_common::physical_expr::{
        fmt_sql, snapshot_generation, snapshot_physical_expr,
    };

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
    }

    /// `a > 2`
    fn a_gt_2(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            col("a", schema).unwrap(),
            Operator::Gt,
            lit(2i32),
        ))
    }

    fn optional(inner: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(OptionalFilterPhysicalExpr::new(inner))
    }

    #[test]
    fn evaluate_equals_inner() -> Result<()> {
        let schema = schema();
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;

        let inner = a_gt_2(&schema);
        let wrapped = optional(Arc::clone(&inner));

        let expected = inner.evaluate(&batch)?.into_array(batch.num_rows())?;
        let actual = wrapped.evaluate(&batch)?.into_array(batch.num_rows())?;
        assert_eq!(as_boolean_array(&actual)?, as_boolean_array(&expected)?);

        let selection = BooleanArray::from(vec![true, false, true]);
        let expected = inner
            .evaluate_selection(&batch, &selection)?
            .into_array(batch.num_rows())?;
        let actual = wrapped
            .evaluate_selection(&batch, &selection)?
            .into_array(batch.num_rows())?;
        assert_eq!(as_boolean_array(&actual)?, as_boolean_array(&expected)?);

        assert_eq!(wrapped.data_type(&schema)?, DataType::Boolean);
        assert!(wrapped.nullable(&schema)?);
        Ok(())
    }

    #[test]
    fn display_and_fmt_sql() {
        let schema = schema();
        let wrapped = optional(a_gt_2(&schema));
        assert_eq!(wrapped.to_string(), "Optional(a@0 > 2)");
        assert_eq!(fmt_sql(wrapped.as_ref()).to_string(), "a > 2");

        let negated = not(Arc::clone(&wrapped)).unwrap();
        assert_eq!(negated.to_string(), "NOT Optional(a@0 > 2)");
    }

    #[test]
    fn children_and_with_new_children() -> Result<()> {
        let schema = schema();
        let wrapped = optional(a_gt_2(&schema));
        assert_eq!(wrapped.children().len(), 1);

        let new_inner = lit(true);
        let rewrapped = Arc::clone(&wrapped).with_new_children(vec![new_inner])?;
        let rewrapped = rewrapped
            .downcast_ref::<OptionalFilterPhysicalExpr>()
            .expect("wrapper is kept");
        assert_eq!(rewrapped.inner().to_string(), "true");

        assert!(wrapped.with_new_children(vec![]).is_err());
        Ok(())
    }

    #[test]
    fn eq_and_hash_use_inner() {
        use std::collections::HashSet;

        let schema = schema();
        let a = optional(a_gt_2(&schema));
        let b = optional(a_gt_2(&schema));
        let c = optional(lit(true));
        assert_eq!(&a, &b);
        assert_ne!(&a, &c);
        // The wrapper is not equal to the inner expression.
        assert_ne!(&a, &a_gt_2(&schema));

        let set: HashSet<_> = [a, b, c].into_iter().collect();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn snapshot_sees_through_dynamic_filter() -> Result<()> {
        let schema = schema();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("a", &schema)?],
            lit(true),
        ));
        let wrapped = optional(Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>);

        assert_eq!(
            snapshot_physical_expr(Arc::clone(&wrapped))?.to_string(),
            "true"
        );

        let generation = snapshot_generation(&wrapped);
        dynamic.update(a_gt_2(&schema))?;
        assert_ne!(snapshot_generation(&wrapped), generation);
        assert_eq!(
            snapshot_physical_expr(Arc::clone(&wrapped))?.to_string(),
            "a@0 > 2"
        );

        // A static inner expression is also unwrapped.
        let wrapped = optional(a_gt_2(&schema));
        assert_eq!(wrapped.snapshot_generation(), 0);
        assert_eq!(snapshot_physical_expr(wrapped)?.to_string(), "a@0 > 2");
        Ok(())
    }
}

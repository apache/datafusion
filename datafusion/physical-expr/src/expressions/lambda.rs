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

//! Physical lambda expression: [`LambdaExpr`]

use std::hash::Hash;
use std::sync::Arc;

use crate::{
    ScalarFunctionExpr,
    expressions::{Column, LambdaVariable},
    physical_expr::PhysicalExpr,
};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{
    HashMap, plan_err,
    tree_node::{
        Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
    },
};
use datafusion_common::{HashSet, Result, internal_err};
use datafusion_expr::ColumnarValue;

/// Represents a lambda with the given parameters names and body
#[derive(Debug, Eq, Clone)]
pub struct LambdaExpr {
    params: Vec<String>,
    body: Arc<dyn PhysicalExpr>,
    projected_body: Arc<dyn PhysicalExpr>,
    projection: Vec<usize>,
    used_param_indices: Vec<usize>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808 [https://github.com/apache/datafusion/issues/13196]
impl PartialEq for LambdaExpr {
    fn eq(&self, other: &Self) -> bool {
        self.params.eq(&other.params) && self.body.eq(&other.body)
    }
}

impl Hash for LambdaExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.params.hash(state);
        self.body.hash(state);
    }
}

impl LambdaExpr {
    /// Create a new lambda expression with the given parameters and body.
    pub fn try_new(params: Vec<String>, body: Arc<dyn PhysicalExpr>) -> Result<Self> {
        if !all_unique(&params) {
            return plan_err!(
                "lambda params must be unique, got ({})",
                params.join(", ")
            );
        }

        check_async_udf(&body)?;

        Ok(Self::new(params, body))
    }

    fn new(params: Vec<String>, body: Arc<dyn PhysicalExpr>) -> Self {
        let own_params: HashSet<String> = params.iter().cloned().collect();

        let mut visitor = CollectUsedVisitor {
            own_params: &own_params,
            used_indices: HashSet::new(),
            used_param_names: HashSet::new(),
            shadow_stack: Vec::new(),
        };
        body.visit(&mut visitor).expect("visitor is infallible");
        let CollectUsedVisitor {
            used_indices,
            used_param_names,
            ..
        } = visitor;

        // Capture projection is *only* outer-batch columns (and captured
        // outer lambda variables). This lambda's own parameters are appended
        // after those captures by `LambdaArgument::new` and must not share
        // the outer-batch index space — otherwise a later projection rewrite
        // that changes the input schema width can make a stale param index
        // look like a real input column (see #24372).
        let mut projection = used_indices.into_iter().collect::<Vec<_>>();

        projection.sort();

        let column_index_map = projection
            .iter()
            .copied()
            .enumerate()
            .map(|(new_idx, original)| (original, new_idx))
            .collect::<HashMap<_, _>>();

        let used_param_indices: Vec<usize> = params
            .iter()
            .enumerate()
            .filter(|(_, name)| used_param_names.contains(*name))
            .map(|(i, _)| i)
            .collect();

        // Own params occupy slots after the captured columns:
        // `captures ++ used_params`. Bind them by name so a stale
        // planner-assigned combined-schema index cannot leak into the
        // capture projection after a schema-boundary rewrite.
        let n_captures = projection.len();
        let param_name_to_projected: HashMap<String, usize> = used_param_indices
            .iter()
            .enumerate()
            .map(|(pos, param_i)| (params[*param_i].clone(), n_captures + pos))
            .collect();

        let projected_body = Arc::clone(&body)
            .rewrite(&mut ProjectLambdaBody {
                column_index_map: &column_index_map,
                param_name_to_projected: &param_name_to_projected,
                shadow_stack: Vec::new(),
            })
            .expect("rewriter is infallible")
            .data;

        Self {
            params,
            body,
            projected_body,
            projection,
            used_param_indices,
        }
    }

    /// Get the lambda's params names
    pub fn params(&self) -> &[String] {
        &self.params
    }

    /// Get the lambda's body
    pub fn body(&self) -> &Arc<dyn PhysicalExpr> {
        &self.body
    }

    #[cfg(feature = "proto")]
    /// Reconstruct a [`LambdaExpr`] from a proto node.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let lambda = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::Lambda,
            "LambdaExpr",
        );

        Ok(Arc::new(LambdaExpr::try_new(
            lambda.params.clone(),
            ctx.decode_required_expression(lambda.body.as_deref(), "LambdaExpr", "body")?,
        )?))
    }

    pub(crate) fn projection(&self) -> &[usize] {
        &self.projection
    }

    pub(crate) fn projected_body(&self) -> &Arc<dyn PhysicalExpr> {
        &self.projected_body
    }

    /// Indices into [`params`](Self::params) of the parameters the body
    /// actually references, in declaration order. See `CollectUsedVisitor`
    /// in this module.
    ///
    /// Relies on the planner appending each lambda's own params after
    /// captures, matching the `captures ++ used_params` layout
    /// `LambdaArgument::new` builds.
    pub fn used_param_indices(&self) -> &[usize] {
        &self.used_param_indices
    }
}

/// Walks the body of a [`LambdaExpr`] and collects, on a single pass:
///
/// * `used_indices` — every outer-batch `Column` index, plus indices of
///   captured *outer* `LambdaVariable`s (not this lambda's own params).
///   This drives the `projection` used to slice the outer batch. Own
///   parameters are deliberately excluded so their planner-assigned
///   combined-schema indexes cannot be mistaken for input columns after
///   a later projection rewrite changes the input schema width (#24372).
/// * `used_param_names` — the subset of *this* lambda's `own_params` that
///   the body actually references.
///
///   A nested lambda can declare its own parameter with the same name as
///   one of `own_params` — a distinct variable that happens to reuse the
///   name (variable shadowing). E.g. in
///   `(k, v) -> func(col, (k, v2) -> k + v2 + v)`, the inner `k` is not
///   `own_params`' `k`; only `v` should flow up as used, not `k`.
///
///   `shadow_stack` holds one frame per nested `LambdaExpr` currently being
///   visited, each frame being that lambda's own parameter names. A
///   `LambdaVariable` only counts toward `used_param_names` if its name
///   isn't in any active frame (i.e. not shadowed).
///
/// The stack is maintained via `TreeNodeVisitor`'s `f_down` / `f_up`:
/// push a frame when entering a nested [`LambdaExpr`], pop it when leaving.
struct CollectUsedVisitor<'a> {
    own_params: &'a HashSet<String>,
    used_indices: HashSet<usize>,
    used_param_names: HashSet<String>,
    shadow_stack: Vec<HashSet<String>>,
}

impl TreeNodeVisitor<'_> for CollectUsedVisitor<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(col) = node.downcast_ref::<Column>() {
            self.used_indices.insert(col.index());
        } else if let Some(var) = node.downcast_ref::<LambdaVariable>() {
            let name = var.name();
            let shadowed = self.shadow_stack.iter().any(|frame| frame.contains(name));
            if !shadowed && self.own_params.contains(name) {
                self.used_param_names.insert(name.to_string());
            } else if !shadowed {
                // Captured outer lambda variable. Keep its index in the
                // outer-batch projection. Nested lambdas' own params are
                // shadowed here and remapped by the inner LambdaExpr.
                self.used_indices.insert(var.index());
            }
        } else if let Some(nested) = node.downcast_ref::<LambdaExpr>() {
            self.shadow_stack
                .push(nested.params.iter().cloned().collect());
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if node.downcast_ref::<LambdaExpr>().is_some() {
            self.shadow_stack.pop();
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Rewrites a lambda body so captured columns occupy `0..n_captures` and
/// this lambda's own parameters occupy `n_captures..`. Nested lambdas are
/// tracked on `shadow_stack` so an inner parameter that reuses a name is
/// not remapped as if it were this lambda's param.
struct ProjectLambdaBody<'a> {
    column_index_map: &'a HashMap<usize, usize>,
    param_name_to_projected: &'a HashMap<String, usize>,
    shadow_stack: Vec<HashSet<String>>,
}

impl TreeNodeRewriter for ProjectLambdaBody<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(nested) = node.downcast_ref::<LambdaExpr>() {
            self.shadow_stack
                .push(nested.params.iter().cloned().collect());
            return Ok(Transformed::no(node));
        }

        if let Some(column) = node.downcast_ref::<Column>() {
            let original = column.index();
            let projected = *self.column_index_map.get(&original).unwrap();
            if projected != original {
                return Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    projected,
                ))));
            }
            return Ok(Transformed::no(node));
        }

        if let Some(lambda_variable) = node.downcast_ref::<LambdaVariable>() {
            let name = lambda_variable.name();
            let shadowed = self.shadow_stack.iter().any(|frame| frame.contains(name));
            if shadowed {
                // Nested lambda's own param: the inner LambdaExpr remaps it.
                return Ok(Transformed::no(node));
            }
            let projected =
                if let Some(&projected) = self.param_name_to_projected.get(name) {
                    projected
                } else {
                    // Captured outer lambda variable: still lives in the same
                    // index space as the outer-batch columns.
                    *self.column_index_map.get(&lambda_variable.index()).unwrap()
                };
            if projected != lambda_variable.index() {
                return Ok(Transformed::yes(Arc::new(LambdaVariable::new(
                    projected,
                    Arc::clone(lambda_variable.field()),
                ))));
            }
        }

        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if node.downcast_ref::<LambdaExpr>().is_some() {
            self.shadow_stack.pop();
        }
        Ok(Transformed::no(node))
    }
}

impl std::fmt::Display for LambdaExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}) -> {}", self.params.join(", "), self.body)
    }
}

impl PhysicalExpr for LambdaExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("LambdaExpr::evaluate() should not be called")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.body]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [body] = children.as_slice() else {
            return internal_err!(
                "LambdaExpr expects exactly 1 child, got {}",
                children.len()
            );
        };

        check_async_udf(body)?;

        Ok(Arc::new(Self::new(self.params.clone(), Arc::clone(body))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}) -> {}", self.params.join(", "), self.body)
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Lambda(Box::new(
                protobuf::PhysicalLambdaExprNode {
                    params: self.params().to_vec(),
                    body: Some(Box::new(ctx.encode_child(self.body())?)),
                },
            ))),
        }))
    }
}

/// Create a lambda expression.
pub fn lambda(
    params: impl IntoIterator<Item = impl Into<String>>,
    body: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(LambdaExpr::try_new(
        params.into_iter().map(Into::into).collect(),
        body,
    )?))
}

fn all_unique(params: &[String]) -> bool {
    match params.len() {
        0 | 1 => true,
        2 => params[0] != params[1],
        _ => {
            let mut set = HashSet::with_capacity(params.len());

            params.iter().all(|p| set.insert(p.as_str()))
        }
    }
}

fn check_async_udf(body: &Arc<dyn PhysicalExpr>) -> Result<()> {
    if body.exists(|expr| {
        Ok(expr
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|udf| udf.fun().as_async().is_some()))
    })? {
        return plan_err!(
            "Async functions in lambdas aren't supported, see https://github.com/apache/datafusion/issues/22091"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::PhysicalExpr;
    use crate::expressions::{Column, LambdaVariable, NoOp, lambda::lambda};
    use arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;

    use super::LambdaExpr;

    #[test]
    fn test_lambda_evaluate() {
        let lambda = lambda(["a"], Arc::new(NoOp::new())).unwrap();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(lambda.evaluate(&batch).is_err());
    }

    #[test]
    fn test_lambda_duplicate_name() {
        assert!(lambda(["a", "a"], Arc::new(NoOp::new())).is_err());
    }

    /// A two-parameter lambda whose body only references the second
    /// parameter (`v`) must report only `v` as used. The higher-order
    /// function uses this set to push only `v` into the merged batch, so
    /// the body's compressed `LambdaVariable` index for `v` lines up with
    /// the batch layout.
    #[test]
    fn test_used_params_collects_only_referenced_param() {
        let v_field = Arc::new(Field::new("v", DataType::Int32, true));
        let body = Arc::new(LambdaVariable::new(1, Arc::clone(&v_field)));

        let lambda =
            LambdaExpr::try_new(vec!["k".to_string(), "v".to_string()], body).unwrap();

        // Own params are not part of the outer-batch capture projection.
        assert!(lambda.projection().is_empty());
        assert_eq!(lambda.used_param_indices(), &[1]);
    }

    /// A body that references neither declared parameter reports no used params.
    #[test]
    fn test_used_params_all_unused() {
        let body = Arc::new(NoOp::new());

        let lambda =
            LambdaExpr::try_new(vec!["k".to_string(), "v".to_string()], body).unwrap();

        assert!(lambda.projection().is_empty());
        assert!(lambda.used_param_indices().is_empty());
    }

    /// A three-parameter lambda that skips the middle parameter reports only the ends as used.
    #[test]
    fn test_used_params_three_params_middle_unused() {
        let a_field = Arc::new(Field::new("a", DataType::Int32, true));
        let c_field = Arc::new(Field::new("c", DataType::Int32, true));
        let body = Arc::new(crate::expressions::BinaryExpr::new(
            Arc::new(LambdaVariable::new(0, Arc::clone(&a_field))),
            datafusion_expr::Operator::Plus,
            Arc::new(LambdaVariable::new(2, Arc::clone(&c_field))),
        ));

        let lambda = LambdaExpr::try_new(
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            body,
        )
        .unwrap();

        assert_eq!(lambda.used_param_indices(), &[0, 2]);
    }

    /// Referencing params out of declaration order still reports both as used.
    #[test]
    fn test_used_params_both_used_in_reverse_reference_order() {
        let k_field = Arc::new(Field::new("k", DataType::Int32, true));
        let v_field = Arc::new(Field::new("v", DataType::Int32, true));
        let body = Arc::new(crate::expressions::BinaryExpr::new(
            Arc::new(LambdaVariable::new(1, Arc::clone(&v_field))),
            datafusion_expr::Operator::Plus,
            Arc::new(LambdaVariable::new(0, Arc::clone(&k_field))),
        ));

        let lambda =
            LambdaExpr::try_new(vec!["k".to_string(), "v".to_string()], body).unwrap();

        assert!(lambda.projection().is_empty());
        assert_eq!(lambda.used_param_indices(), &[0, 1]);
    }

    /// Inside a nested lambda that re-declares one of the outer parameter
    /// names, only the non-shadowed outer references should be reported as
    /// used by the outer lambda. In
    /// `(k, v) -> func(col, (k, v2) -> k + v2 + v)` the inner `k` shadows
    /// the outer `k`, so the outer lambda must only see `v` as used.
    #[test]
    fn test_used_params_handles_shadowing_inside_nested_lambda() {
        let outer_k_field = Arc::new(Field::new("k", DataType::Int32, true));
        let outer_v_field = Arc::new(Field::new("v", DataType::Int32, true));
        let inner_v2_field = Arc::new(Field::new("v2", DataType::Int32, true));

        // Inner lambda body references "k" (inner's), "v2" (inner's), and
        // "v" (outer's). Build it directly with the dense compressed
        // indices the inner LambdaExpr::new would produce: sorted referenced
        // indices, so the names alone matter here — what matters for
        // shadow tracking is the names, not the indices.
        let inner_body: Arc<dyn PhysicalExpr> =
            Arc::new(crate::expressions::BinaryExpr::new(
                Arc::new(crate::expressions::BinaryExpr::new(
                    Arc::new(LambdaVariable::new(1, Arc::clone(&outer_k_field))),
                    datafusion_expr::Operator::Plus,
                    Arc::new(LambdaVariable::new(2, Arc::clone(&inner_v2_field))),
                )),
                datafusion_expr::Operator::Plus,
                Arc::new(LambdaVariable::new(0, Arc::clone(&outer_v_field))),
            ));
        let inner_lambda = Arc::new(
            LambdaExpr::try_new(vec!["k".to_string(), "v2".to_string()], inner_body)
                .unwrap(),
        );

        // Outer body wraps the inner lambda in a binary op next to a
        // regular column reference so the walk has something non-trivial
        // to descend through. The outer body references the inner lambda
        // via `inner_lambda`.
        let outer_body: Arc<dyn PhysicalExpr> =
            Arc::new(crate::expressions::BinaryExpr::new(
                Arc::new(Column::new("col", 0)),
                datafusion_expr::Operator::Plus,
                inner_lambda,
            ));

        let outer_lambda =
            LambdaExpr::try_new(vec!["k".to_string(), "v".to_string()], outer_body)
                .unwrap();

        assert_eq!(
            outer_lambda.used_param_indices(),
            &[1],
            "only outer's `v` (index 1) should be reported as used; `k` (index 0) is \
             shadowed inside the nested lambda"
        );
    }

    /// Reconstructs nested lambdas through [`ProjectLambdaBody`] and asserts
    /// the `captures ++ used_params` index layout at both levels.
    ///
    /// `(k, v) -> col + (k, v2) -> k + v2 + v` covers:
    /// 1. an outer captured column (`col` at a non-zero planner index),
    /// 2. an outer parameter captured by the inner lambda (`v`),
    /// 3. an inner parameter that shadows an outer name (`k`).
    ///
    /// The inner body is built with stale combined-schema indexes so each
    /// `LambdaExpr::new` has to rebase, matching a projection rewrite that
    /// changes the input schema width.
    #[test]
    fn test_nested_lambda_project_lambda_body_rebasing() {
        let k_field = Arc::new(Field::new("k", DataType::Int32, true));
        let v_field = Arc::new(Field::new("v", DataType::Int32, true));
        let v2_field = Arc::new(Field::new("v2", DataType::Int32, true));

        // Planner combined schema: a@0, b@1, col@2, outer k@3, outer v@4,
        // inner k@5, inner v2@6. Inner body: k + v2 + v.
        let inner_body: Arc<dyn PhysicalExpr> =
            Arc::new(crate::expressions::BinaryExpr::new(
                Arc::new(crate::expressions::BinaryExpr::new(
                    Arc::new(LambdaVariable::new(5, Arc::clone(&k_field))),
                    datafusion_expr::Operator::Plus,
                    Arc::new(LambdaVariable::new(6, Arc::clone(&v2_field))),
                )),
                datafusion_expr::Operator::Plus,
                Arc::new(LambdaVariable::new(4, Arc::clone(&v_field))),
            ));
        let inner_lambda =
            LambdaExpr::try_new(vec!["k".to_string(), "v2".to_string()], inner_body)
                .unwrap();

        // Inner reconstruction: capture outer `v`@4, then own params `k`, `v2`.
        assert_eq!(inner_lambda.projection(), &[4]);
        assert_eq!(inner_lambda.used_param_indices(), &[0, 1]);
        assert_eq!(
            inner_lambda.projected_body().as_ref().to_string(),
            "k@1 + v2@2 + v@0",
            "inner projected body should be captures(v) ++ params(k, v2)"
        );

        let outer_body: Arc<dyn PhysicalExpr> =
            Arc::new(crate::expressions::BinaryExpr::new(
                Arc::new(Column::new("col", 2)),
                datafusion_expr::Operator::Plus,
                Arc::new(inner_lambda),
            ));
        let outer_lambda =
            LambdaExpr::try_new(vec!["k".to_string(), "v".to_string()], outer_body)
                .unwrap();

        // Outer reconstruction: capture `col`@2, then used param `v` (`k` is
        // shadowed by the inner lambda, so it is not a used outer param).
        assert_eq!(outer_lambda.projection(), &[2]);
        assert_eq!(outer_lambda.used_param_indices(), &[1]);
        assert_eq!(
            outer_lambda.projected_body().as_ref().to_string(),
            "col@0 + (k, v2) -> k@5 + v2@6 + v@1",
            "outer projected body should rebase col@2 -> col@0 and rewrite the \
             nested capture of `v` onto the outer captures++params slot"
        );

        let reconstructed_inner = outer_lambda
            .projected_body()
            .children()
            .into_iter()
            .find_map(|c| c.downcast_ref::<LambdaExpr>())
            .expect("outer projected body should still wrap the nested lambda");

        // Inner reconstruction after the outer rewrite: capture the remapped
        // outer `v`@1, then own params. Shadowed inner `k` stays an inner param.
        assert_eq!(reconstructed_inner.projection(), &[1]);
        assert_eq!(reconstructed_inner.used_param_indices(), &[0, 1]);
        assert_eq!(
            reconstructed_inner.projected_body().as_ref().to_string(),
            "k@1 + v2@2 + v@0",
            "inner projected body should rebase the outer-remapped `v`@1 back \
             onto capture slot 0, with shadowed `k` and `v2` as own params"
        );
        assert_eq!(
            reconstructed_inner.body().as_ref().to_string(),
            "k@5 + v2@6 + v@1",
            "inner original body should keep shadowed `k`/`v2` planner indexes \
             and the outer-rebased capture of `v`"
        );
    }

    /// A planner-assigned combined-schema index that would collide with a
    /// real input column after a later projection rewrite must not appear
    /// in the capture projection. See #24372.
    #[test]
    fn test_own_param_index_not_captured_from_outer_batch() {
        let x_field = Arc::new(Field::new("x", DataType::Int64, true));
        // Planned against a 1-column input, so param `x` sat at index 1.
        // After collapse the input is wider, and index 1 is a real column.
        let body = Arc::new(LambdaVariable::new(1, Arc::clone(&x_field)));

        let lambda = LambdaExpr::try_new(vec!["x".to_string()], body).unwrap();

        assert!(
            lambda.projection().is_empty(),
            "own param must not be treated as an outer-batch capture, got {:?}",
            lambda.projection()
        );
        assert_eq!(lambda.used_param_indices(), &[0]);
        assert_eq!(
            lambda.projected_body().as_ref().to_string(),
            "x@0",
            "own param should be rebased to slot 0 of the captures++params batch"
        );
    }
}

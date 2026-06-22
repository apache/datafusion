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
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
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
    /// Create a new lambda expression with the given parameters and body
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
        let mut used_column_indices = HashSet::new();

        body.apply(|node| {
            if let Some(col) = node.downcast_ref::<Column>() {
                used_column_indices.insert(col.index());
            } else if let Some(var) = node.downcast_ref::<LambdaVariable>() {
                used_column_indices.insert(var.index());
            }

            Ok(TreeNodeRecursion::Continue)
        })
        .expect("closure should be infallible");

        let mut projection = used_column_indices.into_iter().collect::<Vec<_>>();

        projection.sort();

        let column_index_map = projection
            .iter()
            .enumerate()
            .map(|(projected, original)| (*original, projected))
            .collect::<HashMap<_, _>>();

        let projected_body = Arc::clone(&body)
            .transform_down(|e| {
                if let Some(column) = e.downcast_ref::<Column>() {
                    let original = column.index();
                    let projected = *column_index_map.get(&original).unwrap();
                    if projected != original {
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            column.name(),
                            projected,
                        ))));
                    }
                } else if let Some(lambda_variable) = e.downcast_ref::<LambdaVariable>() {
                    let original = lambda_variable.index();
                    let projected = *column_index_map.get(&original).unwrap();
                    if projected != original {
                        return Ok(Transformed::yes(Arc::new(LambdaVariable::new(
                            projected,
                            Arc::clone(lambda_variable.field()),
                        ))));
                    }
                }
                Ok(Transformed::no(e))
            })
            .expect("closure should be infallible")
            .data;

        Self {
            params,
            body,
            projected_body,
            projection,
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

    pub(crate) fn projection(&self) -> &[usize] {
        &self.projection
    }

    pub(crate) fn projected_body(&self) -> &Arc<dyn PhysicalExpr> {
        &self.projected_body
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
}

/// Create a lambda expression
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
    use crate::expressions::{NoOp, lambda::lambda};
    use arrow::{array::RecordBatch, datatypes::Schema};
    use std::sync::Arc;

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
}

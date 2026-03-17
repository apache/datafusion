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

use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use crate::expressions::{Column, LambdaVariable};
use crate::physical_expr::PhysicalExpr;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{HashSet, Result, internal_err, tree_node::TreeNodeVisitor};
use datafusion_common::{
    plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::ColumnarValue;
use hashbrown::{HashMap, hash_map::EntryRef};

/// Represents a lambda with the given parameters names and body
#[derive(Debug, Eq, Clone)]
pub struct LambdaExpr {
    params: Vec<String>,
    body: Arc<dyn PhysicalExpr>,
    captured_columns: HashSet<usize>,
    captured_variables: HashSet<String>,
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
        if all_unique(&params) {
            Ok(Self::new(params, body))
        } else {
            plan_err!("lambda params must be unique, got ({})", params.join(", "))
        }
    }

    fn new(params: Vec<String>, body: Arc<dyn PhysicalExpr>) -> Self {
        let (captured_columns, captured_variables) = {
            let mut captures = Captures::new(&params);

            body.visit(&mut captures)
                .expect("visitor should be infallible");

            (captures.columns, captures.variables)
        };

        Self {
            params,
            body,
            captured_columns,
            captured_variables,
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

    pub(crate) fn captured_columns(&self) -> &HashSet<usize> {
        &self.captured_columns
    }

    /// Returns lambdas variables names that aren't of this lambda nor any other lambda down tree.
    /// Example:
    ///
    /// `array_transform([[[1, 2, 3]]], a -> array_transform(a, b -> array_transform(b, c -> length(a) + length(b) + c)))`
    ///
    /// For the outermost lambda, this would return an empty hash set
    /// For the middle one, `HashSet("a")`
    /// And for the innermost, `HashSet("a", "b")`
    pub(crate) fn captured_variables(&self) -> &HashSet<String> {
        &self.captured_variables
    }
}

impl std::fmt::Display for LambdaExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}) -> {}", self.params.join(", "), self.body)
    }
}

impl PhysicalExpr for LambdaExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.body.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.body.nullable(input_schema)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("Lambda::evaluate() should not be called")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.body]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(
            self.params.clone(),
            Arc::clone(&children[0]),
        )))
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

struct Captures<'a> {
    shadows: HashMap<&'a str, usize>,
    columns: HashSet<usize>,
    variables: HashSet<String>,
}

impl<'a> Captures<'a> {
    fn new(params: &'a [String]) -> Self {
        Self {
            shadows: params.iter().map(|p| (p.as_str(), 1)).collect(),
            columns: HashSet::new(),
            variables: HashSet::new(),
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for Captures<'n> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(lambda) = node.as_any().downcast_ref::<LambdaExpr>() {
            for param in &lambda.params {
                *self.shadows.entry(param.as_str()).or_default() += 1;
            }
        } else if let Some(lambda_variable) =
            node.as_any().downcast_ref::<LambdaVariable>()
        {
            if !self.shadows.contains_key(lambda_variable.name()) {
                self.variables.insert(lambda_variable.name().to_owned());
            }
        } else if let Some(col) = node.as_any().downcast_ref::<Column>() {
            self.columns.insert(col.index());
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(lambda) = node.as_any().downcast_ref::<LambdaExpr>() {
            for param in &lambda.params {
                match self.shadows.entry_ref(param.as_str()) {
                    EntryRef::Occupied(mut v) => {
                        if *v.get() > 1 {
                            *v.get_mut() -= 1;
                        } else {
                            v.remove();
                        }
                    }
                    EntryRef::Vacant(_v) => {
                        unreachable!(
                            "f_down should have inserted a value for every param"
                        )
                    }
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        LambdaFunctionExpr,
        expressions::{Column, LambdaExpr, NoOp, lambda::lambda, lambda_variable},
    };
    use arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, FieldRef, Schema},
    };
    use datafusion_common::{HashSet, Result};
    use datafusion_expr::{ColumnarValue, LambdaUDF};
    use std::sync::Arc;

    #[derive(Debug, Hash, Eq, PartialEq)]
    struct DummyLambdaUDF;

    impl LambdaUDF for DummyLambdaUDF {
        fn as_any(&self) -> &dyn std::any::Any {
            unimplemented!()
        }

        fn name(&self) -> &str {
            "dummy_udlf"
        }

        fn signature(&self) -> &datafusion_expr::LambdaSignature {
            unimplemented!()
        }

        fn lambdas_parameters(
            &self,
            _args: &[datafusion_expr::ValueOrLambda<FieldRef, ()>],
        ) -> Result<Vec<Option<Vec<Field>>>> {
            unimplemented!()
        }

        fn return_field_from_args(
            &self,
            _args: datafusion_expr::LambdaReturnFieldArgs,
        ) -> Result<FieldRef> {
            unimplemented!()
        }

        fn invoke_with_args(
            &self,
            _args: datafusion_expr::LambdaFunctionArgs,
        ) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn test_lambda_captures() {
        let null_field = Arc::new(Field::new("", DataType::Null, true));

        //`var_b -> dummy_udlf(var_a, var_b, column@0, var_c -> var_c))`
        let inner = LambdaExpr::try_new(
            vec![String::from("var_b")],
            Arc::new(LambdaFunctionExpr::new(
                "dummy_udlf",
                Arc::new(DummyLambdaUDF),
                vec![
                    lambda_variable("var_a", Arc::clone(&null_field)).unwrap(),
                    lambda_variable("var_b", Arc::clone(&null_field)).unwrap(),
                    Arc::new(Column::new("column", 0)),
                    lambda(
                        ["var_c"],
                        lambda_variable("var_c", Arc::clone(&null_field)).unwrap(),
                    )
                    .unwrap(),
                ],
                Arc::clone(&null_field),
                Arc::new(Default::default()),
            )),
        )
        .unwrap();

        assert_eq!(inner.captured_columns(), &HashSet::from([0]));
        assert_eq!(
            inner.captured_variables(),
            &HashSet::from([String::from("var_a")])
        );
    }

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

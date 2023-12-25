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

use std::sync::Arc;

use crate::expressions::Column;
use crate::PhysicalExpr;

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;

/// Stores the mapping between source expressions and target expressions for a
/// projection.
#[derive(Debug, Clone)]
pub struct ProjectionMapping {
    /// Mapping between source expressions and target expressions.
    /// Vector indices correspond to the indices after projection.
    pub map: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
}

impl ProjectionMapping {
    /// Constructs the mapping between a projection's input and output
    /// expressions.
    ///
    /// For example, given the input projection expressions (`a + b`, `c + d`)
    /// and an output schema with two columns `"c + d"` and `"a + b"`, the
    /// projection mapping would be:
    ///
    /// ```text
    ///  [0]: (c + d, col("c + d"))
    ///  [1]: (a + b, col("a + b"))
    /// ```
    ///
    /// where `col("c + d")` means the column named `"c + d"`.
    pub fn try_new(
        expr: &[(Arc<dyn PhysicalExpr>, String)],
        input_schema: &SchemaRef,
    ) -> Result<Self> {
        // Construct a map from the input expressions to the output expression of the projection:
        expr.iter()
            .enumerate()
            .map(|(expr_idx, (expression, name))| {
                let target_expr = Arc::new(Column::new(name, expr_idx)) as _;
                expression
                    .clone()
                    .transform_down(&|e| match e.as_any().downcast_ref::<Column>() {
                        Some(col) => {
                            // Sometimes, an expression and its name in the input_schema
                            // doesn't match. This can cause problems, so we make sure
                            // that the expression name matches with the name in `input_schema`.
                            // Conceptually, `source_expr` and `expression` should be the same.
                            let idx = col.index();
                            let matching_input_field = input_schema.field(idx);
                            let matching_input_column =
                                Column::new(matching_input_field.name(), idx);
                            Ok(Transformed::Yes(Arc::new(matching_input_column)))
                        }
                        None => Ok(Transformed::No(e)),
                    })
                    .map(|source_expr| (source_expr, target_expr))
            })
            .collect::<Result<Vec<_>>>()
            .map(|map| Self { map })
    }

    /// Iterate over pairs of (source, target) expressions
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = &(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> + '_ {
        self.map.iter()
    }

    /// This function returns the target expression for a given source expression.
    ///
    /// # Arguments
    ///
    /// * `expr` - Source physical expression.
    ///
    /// # Returns
    ///
    /// An `Option` containing the target for the given source expression,
    /// where a `None` value means that `expr` is not inside the mapping.
    pub fn target_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        self.map
            .iter()
            .find(|(source, _)| source.eq(expr))
            .map(|(_, target)| target.clone())
    }
}

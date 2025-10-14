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

use crate::expressions::Column;
use crate::utils::collect_column_indices;
use arrow::array::RecordBatch;
use arrow::datatypes::{FieldRef, Schema};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{HashMap, HashSet, Result};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::Itertools;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// A wrapper around a physical expression that optimizes evaluation by projecting only
/// the columns that are actually needed from the input batch.
///
/// This expression analyzes the wrapped expression to determine which columns it references
/// and creates a "projected" version that operates on a subset of the input record batch containing
/// only those columns.
///
/// For most expressions this projection step is not useful. For conditional expressions like `case`
/// which make extensive use of [PhysicalExpr::evaluate_selection] though this can make a
/// significant difference due to the reduction in the number of arrays that need to be filtered.
#[derive(Debug, Clone, Eq)]
pub struct ProjectedExpr {
    /// The original expression before projection
    original: Arc<dyn PhysicalExpr>,
    /// The rewritten expression that operates on projected column indices
    projected: Arc<dyn PhysicalExpr>,
    projection: Vec<usize>,
}

impl ProjectedExpr {
    /// Create a new projected expression wrapper if needed.
    ///
    /// # Arguments
    /// * `inner` - The expression to potentially wrap
    /// * `input_schema` - The schema of the input data
    ///
    /// # Returns
    /// Returns the original expression if it uses all columns from the input schema,
    /// otherwise returns a new [ProjectedExpr] that only references the required columns.
    pub fn maybe_wrap(
        inner: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let column_indices = collect_column_indices(&inner);
        if column_indices.len() == input_schema.fields().len() {
            Ok(inner)
        } else {
            Ok(Arc::new(Self::try_new_with_column_indices(
                inner,
                &column_indices,
            )?))
        }
    }

    pub fn try_new(inner: Arc<dyn PhysicalExpr>) -> Result<Self> {
        let column_indices = collect_column_indices(&inner);
        Self::try_new_with_column_indices(inner, &column_indices)
    }

    fn try_new_with_column_indices(
        inner: Arc<dyn PhysicalExpr>,
        column_indices: &HashSet<usize>,
    ) -> Result<Self> {
        let mut column_index_map = HashMap::<usize, usize>::new();
        column_indices.iter().enumerate().for_each(|(i, c)| {
            column_index_map.insert(*c, i);
        });

        let projected = Arc::clone(&inner)
            .transform_down(|expr| {
                if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                    let projected_index = *column_index_map.get(&column.index()).unwrap();
                    if projected_index != column.index() {
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            column.name(),
                            projected_index,
                        ))));
                    }
                }
                Ok(Transformed::no(expr))
            })
            .data()?;

        let projection = column_index_map
            .iter()
            .sorted_by_key(|(_, v)| **v)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();

        Ok(Self {
            original: Arc::clone(&inner),
            projected,
            projection,
        })
    }
}

/// Create a projected expression
pub fn projected(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(ProjectedExpr::maybe_wrap(expr, input_schema)?)
}

impl PhysicalExpr for ProjectedExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.original.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let projected_batch = batch.project(&self.projection)?;
        self.projected.evaluate(&projected_batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.original]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::try_new(
            children.into_iter().next().unwrap(),
        )?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.original.fmt_sql(f)
    }

    fn is_volatile_node(&self) -> bool {
        self.projected.is_volatile_node()
    }
}

// Manually derive `PartialEq`/`Hash` as `Arc<dyn PhysicalExpr>` does not
// implement these traits by default for the trait object.
impl PartialEq for ProjectedExpr {
    fn eq(&self, other: &Self) -> bool {
        self.original.eq(&other.original) && self.projection.eq(&other.projection)
    }
}

impl Hash for ProjectedExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.original.hash(state);
        self.projection.hash(state);
    }
}

impl Display for ProjectedExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.original.fmt(f)
    }
}

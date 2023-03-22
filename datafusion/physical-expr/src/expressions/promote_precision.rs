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

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// PromotePrecision expression wraps an expression which was promoted to a specific data type
#[derive(Debug)]
pub struct PromotePrecisionExpr {
    /// The expression to be promoted
    expr: Arc<dyn PhysicalExpr>,
}

impl PromotePrecisionExpr {
    /// Create a new PromotePrecisionExpr
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl fmt::Display for PromotePrecisionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PROMOTE_PRECISION({})", self.expr)
    }
}

impl PhysicalExpr for PromotePrecisionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        self.expr.data_type(_input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.expr.evaluate(batch)
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(PromotePrecisionExpr::new(children[0].clone())))
    }
}

impl PartialEq<dyn Any> for PromotePrecisionExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.expr.eq(&x.expr))
            .unwrap_or(false)
    }
}

/// Creates a unary expression PromotePrecisionExpr
pub fn promote_precision(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(PromotePrecisionExpr::new(arg)))
}

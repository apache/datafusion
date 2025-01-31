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

#![allow(deprecated)]

use crate::kernels::strings::substring;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct SubstringExpr {
    pub child: Arc<dyn PhysicalExpr>,
    pub start: i64,
    pub len: u64,
}

impl Hash for SubstringExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.start.hash(state);
        self.len.hash(state);
    }
}

impl PartialEq for SubstringExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.start.eq(&other.start) && self.len.eq(&other.len)
    }
}

impl SubstringExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, start: i64, len: u64) -> Self {
        Self { child, start, len }
    }
}

impl Display for SubstringExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StringSpace [start: {}, len: {}, child: {}]",
            self.start, self.len, self.child
        )
    }
}

impl PhysicalExpr for SubstringExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result = substring(&array, self.start, self.len)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Substring(scalar) should be fold in Spark JVM side.".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(SubstringExpr::new(
            Arc::clone(&children[0]),
            self.start,
            self.len,
        )))
    }
}

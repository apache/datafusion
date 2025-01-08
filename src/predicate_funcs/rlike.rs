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

use crate::SparkError;
use arrow::compute::take;
use arrow_array::builder::BooleanBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{Array, BooleanArray, DictionaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use datafusion::physical_expr_common::physical_expr::DynEq;
use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use regex::Regex;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Implementation of RLIKE operator.
///
/// Note that this implementation is not yet Spark-compatible and simply delegates to
/// the Rust regexp crate. It will match Spark behavior for some simple cases but has
/// differences in whitespace handling and does not support all the features of Java's
/// regular expression engine, which are documented at:
///
/// https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
#[derive(Debug)]
pub struct RLike {
    child: Arc<dyn PhysicalExpr>,
    // Only scalar patterns are supported
    pattern_str: String,
    pattern: Regex,
}

impl Hash for RLike {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.pattern_str.as_bytes());
    }
}

impl DynEq for RLike {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<Self>() {
            self.pattern_str == other.pattern_str
        } else {
            false
        }
    }
}

impl RLike {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, pattern: &str) -> Result<Self> {
        Ok(Self {
            child,
            pattern_str: pattern.to_string(),
            pattern: Regex::new(pattern).map_err(|e| {
                SparkError::Internal(format!("Failed to compile pattern {}: {}", pattern, e))
            })?,
        })
    }

    fn is_match(&self, inputs: &StringArray) -> BooleanArray {
        let mut builder = BooleanBuilder::with_capacity(inputs.len());
        if inputs.is_nullable() {
            for i in 0..inputs.len() {
                if inputs.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(self.pattern.is_match(inputs.value(i)));
                }
            }
        } else {
            for i in 0..inputs.len() {
                builder.append_value(self.pattern.is_match(inputs.value(i)));
            }
        }
        builder.finish()
    }
}

impl Display for RLike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RLike [child: {}, pattern: {}] ",
            self.child, self.pattern_str
        )
    }
}

impl PhysicalExpr for RLike {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.child.evaluate(batch)? {
            ColumnarValue::Array(array) if array.as_any().is::<DictionaryArray<Int32Type>>() => {
                let dict_array = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("dict array");
                let dict_values = dict_array
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("strings");
                // evaluate the regexp pattern against the dictionary values
                let new_values = self.is_match(dict_values);
                // convert to conventional (not dictionary-encoded) array
                let result = take(&new_values, dict_array.keys(), None)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Array(array) => {
                let inputs = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string array");
                let array = self.is_match(inputs);
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(_) => {
                internal_err!("non scalar regexp patterns are not supported")
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        Ok(Arc::new(RLike::try_new(
            Arc::clone(&children[0]),
            &self.pattern_str,
        )?))
    }
}

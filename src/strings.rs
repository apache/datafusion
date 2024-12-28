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

use crate::kernels::strings::{string_space, substring};
use arrow::{
    compute::{
        contains_dyn, contains_utf8_scalar_dyn, ends_with_dyn, ends_with_utf8_scalar_dyn, like_dyn,
        like_utf8_scalar_dyn, starts_with_dyn, starts_with_utf8_scalar_dyn,
    },
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue::Utf8};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

macro_rules! make_predicate_function {
    ($name: ident, $kernel: ident, $str_scalar_kernel: ident) => {
        #[derive(Debug, Eq)]
        pub struct $name {
            left: Arc<dyn PhysicalExpr>,
            right: Arc<dyn PhysicalExpr>,
        }

        impl $name {
            pub fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Self {
                Self { left, right }
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "$name [left: {}, right: {}]", self.left, self.right)
            }
        }

        impl Hash for $name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.left.hash(state);
                self.right.hash(state);
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.left.eq(&other.left) && self.right.eq(&other.right)
            }
        }

        impl PhysicalExpr for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn data_type(&self, _: &Schema) -> datafusion_common::Result<DataType> {
                Ok(DataType::Boolean)
            }

            fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
                Ok(true)
            }

            fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
                let left_arg = self.left.evaluate(batch)?;
                let right_arg = self.right.evaluate(batch)?;

                let array = match (left_arg, right_arg) {
                    // array (op) scalar
                    (ColumnarValue::Array(array), ColumnarValue::Scalar(Utf8(Some(string)))) => {
                        $str_scalar_kernel(&array, string.as_str())
                    }
                    (ColumnarValue::Array(_), ColumnarValue::Scalar(other)) => {
                        return Err(DataFusionError::Execution(format!(
                            "Should be String but got: {:?}",
                            other
                        )))
                    }
                    // array (op) array
                    (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                        $kernel(&array1, &array2)
                    }
                    // scalar (op) scalar should be folded at Spark optimizer
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Predicate on two literals should be folded at Spark".to_string(),
                        ))
                    }
                }?;

                Ok(ColumnarValue::Array(Arc::new(array)))
            }

            fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
                vec![&self.left, &self.right]
            }

            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn PhysicalExpr>>,
            ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
                Ok(Arc::new($name::new(
                    children[0].clone(),
                    children[1].clone(),
                )))
            }
        }
    };
}

make_predicate_function!(Like, like_dyn, like_utf8_scalar_dyn);

make_predicate_function!(StartsWith, starts_with_dyn, starts_with_utf8_scalar_dyn);

make_predicate_function!(EndsWith, ends_with_dyn, ends_with_utf8_scalar_dyn);

make_predicate_function!(Contains, contains_dyn, contains_utf8_scalar_dyn);

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
#[derive(Debug, Eq)]
pub struct StringSpaceExpr {
    pub child: Arc<dyn PhysicalExpr>,
}

impl Hash for StringSpaceExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PartialEq for StringSpaceExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl SubstringExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, start: i64, len: u64) -> Self {
        Self { child, start, len }
    }
}

impl StringSpaceExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
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

impl Display for StringSpaceExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StringSpace [child: {}] ", self.child)
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

impl PhysicalExpr for StringSpaceExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema)? {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type, Box::new(DataType::Utf8)))
            }
            _ => Ok(DataType::Utf8),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result = string_space(&array)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "StringSpace(scalar) should be fold in Spark JVM side.".to_string(),
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
        Ok(Arc::new(StringSpaceExpr::new(Arc::clone(&children[0]))))
    }
}

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

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::Result;
use datafusion_physical_expr::{expressions::CaseExpr, PhysicalExpr};
use std::hash::Hash;
use std::{any::Any, sync::Arc};

/// IfExpr is a wrapper around CaseExpr, because `IF(a, b, c)` is semantically equivalent to
/// `CASE WHEN a THEN b ELSE c END`.
#[derive(Debug, Eq)]
pub struct IfExpr {
    if_expr: Arc<dyn PhysicalExpr>,
    true_expr: Arc<dyn PhysicalExpr>,
    false_expr: Arc<dyn PhysicalExpr>,
    // we delegate to case_expr for evaluation
    case_expr: Arc<CaseExpr>,
}

impl Hash for IfExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.if_expr.hash(state);
        self.true_expr.hash(state);
        self.false_expr.hash(state);
        self.case_expr.hash(state);
    }
}
impl PartialEq for IfExpr {
    fn eq(&self, other: &Self) -> bool {
        self.if_expr.eq(&other.if_expr)
            && self.true_expr.eq(&other.true_expr)
            && self.false_expr.eq(&other.false_expr)
            && self.case_expr.eq(&other.case_expr)
    }
}

impl std::fmt::Display for IfExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "If [if: {}, true_expr: {}, false_expr: {}]",
            self.if_expr, self.true_expr, self.false_expr
        )
    }
}

impl IfExpr {
    /// Create a new IF expression
    pub fn new(
        if_expr: Arc<dyn PhysicalExpr>,
        true_expr: Arc<dyn PhysicalExpr>,
        false_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            if_expr: Arc::clone(&if_expr),
            true_expr: Arc::clone(&true_expr),
            false_expr: Arc::clone(&false_expr),
            case_expr: Arc::new(
                CaseExpr::try_new(None, vec![(if_expr, true_expr)], Some(false_expr)).unwrap(),
            ),
        }
    }
}

impl PhysicalExpr for IfExpr {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.true_expr.data_type(input_schema)?;
        Ok(data_type)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        if self.true_expr.nullable(_input_schema)? || self.true_expr.nullable(_input_schema)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.case_expr.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.if_expr, &self.true_expr, &self.false_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IfExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            Arc::clone(&children[2]),
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::StringArray, datatypes::*};
    use arrow_array::Int32Array;
    use datafusion::logical_expr::Operator;
    use datafusion_common::cast::as_int32_array;
    use datafusion_physical_expr::expressions::{binary, col, lit};

    use super::*;

    /// Create an If expression
    fn if_fn(
        if_expr: Arc<dyn PhysicalExpr>,
        true_expr: Arc<dyn PhysicalExpr>,
        false_expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IfExpr::new(if_expr, true_expr, false_expr)))
    }

    #[test]
    fn test_if_1() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        let schema_ref = batch.schema();

        // if a = 'foo' 123 else 999
        let if_expr = binary(
            col("a", &schema_ref)?,
            Operator::Eq,
            lit("foo"),
            &schema_ref,
        )?;
        let true_expr = lit(123i32);
        let false_expr = lit(999i32);

        let expr = if_fn(if_expr, true_expr, false_expr);
        let result = expr?.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(999)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_if_2() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        let schema_ref = batch.schema();

        // if a >=1 123 else 999
        let if_expr = binary(col("a", &schema_ref)?, Operator::GtEq, lit(1), &schema_ref)?;
        let true_expr = lit(123i32);
        let false_expr = lit(999i32);

        let expr = if_fn(if_expr, true_expr, false_expr);
        let result = expr?.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(123)]);
        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_if_children() {
        let if_expr = lit(true);
        let true_expr = lit(123i32);
        let false_expr = lit(999i32);

        let expr = if_fn(if_expr, true_expr, false_expr).unwrap();
        let children = expr.children();
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].to_string(), "true");
        assert_eq!(children[1].to_string(), "123");
        assert_eq!(children[2].to_string(), "999");
    }
}

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

use std::{
    any::Any,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::*,
    compute::{and, is_null, kernels::zip::zip, not, or_kleene},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{cast::as_boolean_array, Result};
use datafusion_physical_expr::PhysicalExpr;

use crate::utils::down_cast_any_ref;

#[derive(Debug, Hash)]
pub struct IfExpr {
    if_expr: Arc<dyn PhysicalExpr>,
    true_expr: Arc<dyn PhysicalExpr>,
    false_expr: Arc<dyn PhysicalExpr>,
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
            if_expr,
            true_expr,
            false_expr,
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
        let mut remainder = BooleanArray::from(vec![true; batch.num_rows()]);

        // evaluate if condition on batch
        let if_value = self.if_expr.evaluate_selection(batch, &remainder)?;
        let if_value = if_value.into_array(batch.num_rows())?;
        let if_value =
            as_boolean_array(&if_value).expect("if expression did not return a BooleanArray");

        let true_value = self.true_expr.evaluate_selection(batch, if_value)?;
        let true_value = true_value.into_array(batch.num_rows())?;

        remainder = and(
            &remainder,
            &or_kleene(&not(if_value)?, &is_null(if_value)?)?,
        )?;

        let false_value = self
            .false_expr
            .evaluate_selection(batch, &remainder)?
            .into_array(batch.num_rows())?;
        let current_value = zip(&remainder, &false_value, &true_value)?;

        Ok(ColumnarValue::Array(current_value))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.if_expr, &self.true_expr, &self.false_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IfExpr::new(
            children[0].clone(),
            children[1].clone(),
            children[2].clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.if_expr.hash(&mut s);
        self.true_expr.hash(&mut s);
        self.false_expr.hash(&mut s);
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for IfExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.if_expr.eq(&x.if_expr)
                    && self.true_expr.eq(&x.true_expr)
                    && self.false_expr.eq(&x.false_expr)
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::StringArray, datatypes::*};
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

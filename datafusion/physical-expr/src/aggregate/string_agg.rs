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

//! Defines physical expressions that can evaluated at runtime during query execution

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::{format_state_name, Literal};
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// STRING_AGG aggregate expression
#[derive(Debug)]
pub struct StringAgg {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    delimiter: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl StringAgg {
    /// Create a new StringAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        delimiter: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            delimiter,
            expr,
            nullable: true,
        }
    }
}

impl AggregateExpr for StringAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        if let Some(delimiter) = self.delimiter.as_any().downcast_ref::<Literal>() {
            match delimiter.value() {
                ScalarValue::Utf8(Some(delimiter))
                | ScalarValue::LargeUtf8(Some(delimiter)) => {
                    return Ok(Box::new(StringAggAccumulator::new(delimiter)));
                }
                ScalarValue::Null => {
                    return Ok(Box::new(StringAggAccumulator::new("")));
                }
                _ => {
                    return not_impl_err!(
                        "StringAgg not supported for {}: {} with delimiter {}",
                        self.name,
                        self.data_type,
                        delimiter.value()
                    )
                }
            }
        }
        not_impl_err!(
            "StringAgg not support for {}: {} with no Literal delimiter",
            self.name,
            self.data_type
        )
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "string_agg"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone(), self.delimiter.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for StringAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
                    && self.delimiter.eq(&x.delimiter)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct StringAggAccumulator {
    values: Option<String>,
    delimiter: String,
}

impl StringAggAccumulator {
    pub fn new(delimiter: &str) -> Self {
        Self {
            values: None,
            delimiter: delimiter.to_string(),
        }
    }
}

impl Accumulator for StringAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let string_array: Vec<_> = as_generic_string_array::<i64>(&values[0])?
            .iter()
            .filter_map(|v| v.as_ref().map(ToString::to_string))
            .collect();
        if !string_array.is_empty() {
            let s = string_array.join(self.delimiter.as_str());
            let v = self.values.get_or_insert("".to_string());
            if !v.is_empty() {
                v.push_str(self.delimiter.as_str());
            }
            v.push_str(s.as_str());
        }
        Ok(())
    }

    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.update_batch(values)?;
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::LargeUtf8(self.values.clone()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.values.as_ref().map(|v| v.capacity()).unwrap_or(0)
            + self.delimiter.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::tests::aggregate;
    use crate::expressions::{col, create_aggregate_expr, try_cast};
    use arrow::array::ArrayRef;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use arrow_array::LargeStringArray;
    use arrow_array::StringArray;
    use datafusion_expr::type_coercion::aggregates::coerce_types;
    use datafusion_expr::AggregateFunction;

    fn assert_string_aggregate(
        array: ArrayRef,
        function: AggregateFunction,
        distinct: bool,
        expected: ScalarValue,
        delimiter: String,
    ) {
        let data_type = array.data_type();
        let sig = function.signature();
        let coerced =
            coerce_types(&function, &[data_type.clone(), DataType::Utf8], &sig).unwrap();

        let input_schema = Schema::new(vec![Field::new("a", data_type.clone(), true)]);
        let batch =
            RecordBatch::try_new(Arc::new(input_schema.clone()), vec![array]).unwrap();

        let input = try_cast(
            col("a", &input_schema).unwrap(),
            &input_schema,
            coerced[0].clone(),
        )
        .unwrap();

        let delimiter = Arc::new(Literal::new(ScalarValue::Utf8(Some(delimiter))));
        let schema = Schema::new(vec![Field::new("a", coerced[0].clone(), true)]);
        let agg = create_aggregate_expr(
            &function,
            distinct,
            &[input, delimiter],
            &[],
            &schema,
            "agg",
        )
        .unwrap();

        let result = aggregate(&batch, agg).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn string_agg_utf8() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["h", "e", "l", "l", "o"]));
        assert_string_aggregate(
            a,
            AggregateFunction::StringAgg,
            false,
            ScalarValue::LargeUtf8(Some("h,e,l,l,o".to_owned())),
            ",".to_owned(),
        );
    }

    #[test]
    fn string_agg_largeutf8() {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["h", "e", "l", "l", "o"]));
        assert_string_aggregate(
            a,
            AggregateFunction::StringAgg,
            false,
            ScalarValue::LargeUtf8(Some("h|e|l|l|o".to_owned())),
            "|".to_owned(),
        );
    }
}

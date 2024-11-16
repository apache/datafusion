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

//! [`StringAgg`] accumulator for the `string_agg` function

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::Result;
use datafusion_common::{not_impl_err, ScalarValue};
use datafusion_expr::aggregate_doc_sections::DOC_SECTION_GENERAL;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, TypeSignature, Volatility,
};
use datafusion_physical_expr::expressions::Literal;
use std::any::Any;
use std::mem::size_of_val;
use std::sync::OnceLock;

make_udaf_expr_and_func!(
    StringAgg,
    string_agg,
    expr delimiter,
    "Concatenates the values of string expressions and places separator values between them",
    string_agg_udaf
);

/// STRING_AGG aggregate expression
#[derive(Debug)]
pub struct StringAgg {
    signature: Signature,
}

impl StringAgg {
    /// Create a new StringAgg aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Null]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for StringAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for StringAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeUtf8)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if let Some(lit) = acc_args.exprs[1].as_any().downcast_ref::<Literal>() {
            return match lit.value() {
                ScalarValue::Utf8(Some(delimiter))
                | ScalarValue::LargeUtf8(Some(delimiter)) => {
                    Ok(Box::new(StringAggAccumulator::new(delimiter.as_str())))
                }
                ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::Null => Ok(Box::new(StringAggAccumulator::new(""))),
                e => not_impl_err!("StringAgg not supported for delimiter {}", e),
            };
        }

        not_impl_err!("expect literal")
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_string_agg_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_string_agg_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description(
                "Concatenates the values of string expressions and places separator values between them."
            )
            .with_syntax_example("string_agg(expression, delimiter)")
            .with_sql_example(r#"```sql
> SELECT string_agg(name, ', ') AS names_list
  FROM employee;
+--------------------------+
| names_list               |
+--------------------------+
| Alice, Bob, Charlie      |
+--------------------------+
```"#, 
            )
            .with_argument("expression", "The string expression to concatenate. Can be a column or any valid string expression.")
            .with_argument("delimiter", "A literal string used as a separator between the concatenated values.")
            .build()
            .unwrap()
    })
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

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::LargeUtf8(self.values.clone()))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.as_ref().map(|v| v.capacity()).unwrap_or(0)
            + self.delimiter.capacity()
    }
}

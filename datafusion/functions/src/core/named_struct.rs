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

use arrow::array::StructArray;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Struct Functions"),
    description = "Returns an Arrow struct using the specified name and input expressions pairs.",
    syntax_example = "named_struct(expression1_name, expression1_input[, ..., expression_n_name, expression_n_input])",
    sql_example = r#"
For example, this query converts two columns `a` and `b` to a single column with
a struct type of fields `field_a` and `field_b`:
```sql
> select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+
> select named_struct('field_a', a, 'field_b', b) from t;
+-------------------------------------------------------+
| named_struct(Utf8("field_a"),t.a,Utf8("field_b"),t.b) |
+-------------------------------------------------------+
| {field_a: 1, field_b: 2}                              |
| {field_a: 3, field_b: 4}                              |
+-------------------------------------------------------+
```"#,
    argument(
        name = "expression_n_name",
        description = "Name of the column field. Must be a constant string."
    ),
    argument(
        name = "expression_n_input",
        description = "Expression to include in the output struct. Can be a constant, column, or function, and any combination of arithmetic or string operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NamedStructFunc {
    signature: Signature,
}

impl Default for NamedStructFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NamedStructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NamedStructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "named_struct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "named_struct: return_type called instead of return_field_from_args"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // do not accept 0 arguments.
        if args.scalar_arguments.is_empty() {
            return exec_err!(
                "named_struct requires at least one pair of arguments, got 0 instead"
            );
        }

        if !args.scalar_arguments.len().is_multiple_of(2) {
            return exec_err!(
                "named_struct requires an even number of arguments, got {} instead",
                args.scalar_arguments.len()
            );
        }

        let names = args
            .scalar_arguments
            .iter()
            .enumerate()
            .step_by(2)
            .map(|(i, sv)|
                sv.and_then(|sv| sv.try_as_str().flatten().filter(|s| !s.is_empty()))
                .map_or_else(
                    ||
                        exec_err!(
                    "{} requires {i}-th (0-indexed) field name as non-empty constant string",
                    self.name()
                ),
                Ok
                )
            )
            .collect::<Result<Vec<_>>>()?;
        let types = args
            .arg_fields
            .iter()
            .skip(1)
            .step_by(2)
            .map(|f| f.data_type())
            .collect::<Vec<_>>();

        let return_fields = names
            .into_iter()
            .zip(types.into_iter())
            .map(|(name, data_type)| Ok(Field::new(name, data_type.to_owned(), true)))
            .collect::<Result<Vec<Field>>>()?;

        Ok(Field::new(
            self.name(),
            DataType::Struct(Fields::from(return_fields)),
            true,
        )
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::Struct(fields) = args.return_type() else {
            return internal_err!("incorrect named_struct return type");
        };

        assert_eq!(
            fields.len(),
            args.args.len() / 2,
            "return type field count != argument count / 2"
        );

        let values: Vec<ColumnarValue> = args
            .args
            .chunks_exact(2)
            .map(|chunk| chunk[1].clone())
            .collect();
        let arrays = ColumnarValue::values_to_arrays(&values)?;
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            fields.clone(),
            arrays,
            None,
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

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
use arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::{ColumnarValue, Documentation, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Struct Functions"),
    description = "Returns an Arrow struct using the specified input expressions optionally named.
Fields in the returned struct use the optional name or the `cN` naming convention.
For example: `c0`, `c1`, `c2`, etc.",
    syntax_example = "struct(expression1[, ..., expression_n])",
    sql_example = r#"For example, this query converts two columns `a` and `b` to a single column with
a struct type of fields `field_a` and `c1`:
```sql
> select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+

-- use default names `c0`, `c1`
> select struct(a, b) from t;
+-----------------+
| struct(t.a,t.b) |
+-----------------+
| {c0: 1, c1: 2}  |
| {c0: 3, c1: 4}  |
+-----------------+

-- name the first field `field_a`
select struct(a as field_a, b) from t;
+--------------------------------------------------+
| named_struct(Utf8("field_a"),t.a,Utf8("c1"),t.b) |
+--------------------------------------------------+
| {field_a: 1, c1: 2}                              |
| {field_a: 3, c1: 4}                              |
+--------------------------------------------------+
```"#,
    argument(
        name = "expression1, expression_n",
        description = "Expression to include in the output struct. Can be a constant, column, or function, any combination of arithmetic or string operators."
    )
)]
#[derive(Debug)]
pub struct StructFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for StructFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StructFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("row")],
        }
    }
}

impl ScalarUDFImpl for StructFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "struct"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("struct requires at least one argument, got 0 instead");
        }

        let fields = arg_types
            .iter()
            .enumerate()
            .map(|(pos, dt)| Field::new(format!("c{pos}"), dt.clone(), true))
            .collect::<Vec<Field>>()
            .into();

        Ok(DataType::Struct(fields))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::Struct(fields) = args.return_type else {
            return internal_err!("incorrect struct return type");
        };

        assert_eq!(
            fields.len(),
            args.args.len(),
            "return type field count != argument count"
        );

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
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

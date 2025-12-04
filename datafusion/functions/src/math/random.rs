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

use std::any::Any;
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Float64;
use rand::{rng, Rng};

use datafusion_common::{assert_or_internal_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = r#"Returns a random float value in the range [0, 1).
The random seed is unique to each row."#,
    syntax_example = "random()",
    sql_example = r#"```sql
> SELECT random();
+------------------+
| random()         |
+------------------+
| 0.7389238902938  |
+------------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RandomFunc {
    signature: Signature,
}

impl Default for RandomFunc {
    fn default() -> Self {
        RandomFunc::new()
    }
}

impl RandomFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RandomFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert_or_internal_err!(
            args.args.is_empty(),
            "{} function does not accept arguments",
            self.name()
        );
        let mut rng = rng();
        let mut values = vec![0.0; args.number_rows];
        // Equivalent to set each element with rng.random_range(0.0..1.0), but more efficient
        rng.fill(&mut values[..]);
        let array = Float64Array::from(values);

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

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

use arrow::array::GenericStringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Utf8;
use uuid::Uuid;

use datafusion_common::{internal_err, Result};
use datafusion_expr::{ColumnarValue, Documentation, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns [`UUID v4`](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)) string value which is unique per row.",
    syntax_example = "uuid()",
    sql_example = r#"```sql
> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 6ec17ef8-1934-41cc-8d59-d0c8f9eea1f0 |
+--------------------------------------+
```"#
)]
#[derive(Debug)]
pub struct UuidFunc {
    signature: Signature,
}

impl Default for UuidFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for UuidFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uuid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    /// Prints random (v4) uuid values per row
    /// uuid() = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        if !args.is_empty() {
            return internal_err!("{} function does not accept arguments", self.name());
        }
        let values = std::iter::repeat_with(|| Uuid::new_v4().to_string()).take(num_rows);
        let array = GenericStringArray::<i32>::from_iter_values(values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

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

use arrow::array::GenericStringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Utf8;
use rand::Rng;
use uuid::Uuid;

use datafusion_common::{assert_or_internal_err, Result};
use datafusion_expr::{ColumnarValue, Documentation, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert_or_internal_err!(
            args.args.is_empty(),
            "{} function does not accept arguments",
            self.name()
        );

        // Generate random u128 values
        let mut rng = rand::rng();
        let mut randoms = vec![0u128; args.number_rows];
        rng.fill(&mut randoms[..]);

        let mut builder = GenericStringBuilder::<i32>::with_capacity(
            args.number_rows,
            args.number_rows * 36,
        );

        let mut buffer = [0u8; 36];
        for x in &mut randoms {
            // From Uuid::new_v4(): Mask out the version and variant bits
            *x = *x & 0xFFFFFFFFFFFF4FFFBFFFFFFFFFFFFFFF | 0x40008000000000000000;
            let uuid = Uuid::from_u128(*x);
            let fmt = uuid::fmt::Hyphenated::from_uuid(uuid);
            builder.append_value(fmt.encode_lower(&mut buffer));
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

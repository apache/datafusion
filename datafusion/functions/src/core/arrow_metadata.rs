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

use arrow::array::{MapBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrowMetadataFunc {
    signature: Signature,
}

impl ArrowMetadataFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for ArrowMetadataFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrowMetadataFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrow_metadata"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() == 2 {
            Ok(DataType::Utf8)
        } else if arg_types.len() == 1 {
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ))
        } else {
            exec_err!("arrow_metadata requires 1 or 2 arguments")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let metadata = args.arg_fields[0].metadata();

        if args.args.len() == 2 {
            let key = match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(k))) => k,
                _ => {
                    return exec_err!(
                        "Second argument to arrow_metadata must be a string literal key"
                    );
                }
            };
            let value = metadata.get(key).cloned();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value)))
        } else if args.args.len() == 1 {
            let mut map_builder =
                MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

            let mut entries: Vec<_> = metadata.iter().collect();
            entries.sort_by_key(|(k, _)| *k);

            for (k, v) in entries {
                map_builder.keys().append_value(k);
                map_builder.values().append_value(v);
            }
            map_builder.append(true)?;

            let map_array = map_builder.finish();

            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &map_array, 0,
            )?))
        } else {
            exec_err!("arrow_metadata requires 1 or 2 arguments")
        }
    }
}

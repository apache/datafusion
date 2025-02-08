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

//! [`VersionFunc`]: Implementation of the `version` function.

use crate::utils::take_function_args;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Returns the version of DataFusion.",
    syntax_example = "version()",
    sql_example = r#"```sql
> select version();
+--------------------------------------------+
| version()                                  |
+--------------------------------------------+
| Apache DataFusion 42.0.0, aarch64 on macos |
+--------------------------------------------+
```"#
)]
#[derive(Debug)]
pub struct VersionFunc {
    signature: Signature,
}

impl Default for VersionFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VersionFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [] = take_function_args(self.name(), args)?;
        Ok(DataType::Utf8)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        let [] = take_function_args(self.name(), args)?;
        // TODO it would be great to add rust version and arrow version,
        // but that requires a `build.rs` script and/or adding a version const to arrow-rs
        let version = format!(
            "Apache DataFusion {}, {} on {}",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::ARCH,
            std::env::consts::OS,
        );
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(version))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_version_udf() {
        let version_udf = ScalarUDF::from(VersionFunc::new());
        #[allow(deprecated)] // TODO: migrate to invoke_with_args
        let version = version_udf.invoke_batch(&[], 1).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(version))) = version {
            assert!(version.starts_with("Apache DataFusion"));
        } else {
            panic!("Expected version string");
        }
    }
}

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

use arrow::datatypes::DataType;
use datafusion_common::{not_impl_err, plan_err, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::OnceLock;

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
        if args.is_empty() {
            Ok(DataType::Utf8)
        } else {
            plan_err!("version expects no arguments")
        }
    }

    fn invoke(&self, _: &[ColumnarValue]) -> Result<ColumnarValue> {
        not_impl_err!("version does not take any arguments")
    }

    fn invoke_no_args(&self, _: usize) -> Result<ColumnarValue> {
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
        Some(get_version_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_version_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_OTHER)
            .with_description("Returns the version of DataFusion.")
            .with_syntax_example("version()")
            .with_sql_example(
                r#"```sql
> select version();
+--------------------------------------------+
| version()                                  |
+--------------------------------------------+
| Apache DataFusion 42.0.0, aarch64 on macos |
+--------------------------------------------+
```"#,
            )
            .build()
            .unwrap()
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_version_udf() {
        let version_udf = ScalarUDF::from(VersionFunc::new());
        let version = version_udf.invoke_no_args(0).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(version))) = version {
            assert!(version.starts_with("Apache DataFusion"));
        } else {
            panic!("Expected version string");
        }
    }
}

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

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Time64;
use arrow::datatypes::TimeUnit::Nanosecond;
use std::any::Any;
use chrono::Timelike;

use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Returns the current UTC time.

The `current_time()` return value is determined at query time and will return the same time, no matter when in the query plan the function executes.
"#,
    syntax_example = "current_time()"
)]
#[derive(Debug)]
pub struct CurrentTimeFunc {
    signature: Signature,
}

impl Default for CurrentTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentTimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for CurrentTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(Nanosecond))
    }

    fn invoke_with_args(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if !args.is_empty() {
        return Err(DataFusionError::Execution(
            "current_time() takes 0 arguments".to_string(),
        ));
    }

    let current_time = chrono::Utc::now().time();
    let nanos_since_midnight = current_time.num_seconds_from_midnight() as i64 * 1_000_000_000
        + current_time.nanosecond() as i64;
    let array: ArrayRef = Arc::new(Time64NanosecondArray::from_value(nanos_since_midnight, 1));
    Ok(ColumnarValue::Array(array))
}

    fn simplify(
        &self,
        _args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let now_ts = info.execution_props().query_execution_start_time;
        let nano = now_ts.timestamp_nanos_opt().map(|ts| ts % 86400000000000);
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Time64Nanosecond(nano),
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn setup_test_catalog() -> Result<ListingCatalog> {
        let store = Arc::new(InMemory::new());
        
        // Create CSV test data
        let csv_data = b"col1,col2\n1,a\n2,b\n";
        store
            .put(&object_store::path::Path::from("test.csv"), csv_data.to_vec())
            .await
            .unwrap();

        // Create JSON test data
        let json_data = b"[{\"name\":\"test\",\"value\":1}]";
        store
            .put(&object_store::path::Path::from("test.json"), json_data.to_vec())
            .await
            .unwrap();

        Ok(ListingCatalog::new(store, String::new()))
    }

    #[tokio::test]
    async fn test_schema_names() -> Result<()> {
        let catalog = setup_test_catalog().await?;
        let names = catalog.schema_names()?;
        assert_eq!(names, vec!["default"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_table_names() -> Result<()> {
        let catalog = setup_test_catalog().await?;
        let schema = catalog.schema("default").await?;
        let mut names = schema.table_names()?;
        names.sort();
        assert_eq!(names, vec!["test.csv", "test.json"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_table_exist() -> Result<()> {
        let catalog = setup_test_catalog().await?;
        let schema = catalog.schema("default").await?;
        assert!(schema.table_exist("test.csv"));
        assert!(schema.table_exist("test.json"));
        assert!(!schema.table_exist("nonexistent.csv"));
        Ok(())
    }

    #[tokio::test]
    async fn test_csv_table_provider() -> Result<()> {
        let catalog = setup_test_catalog().await?;
        let schema = catalog.schema("default").await?;
        let table = schema.table("test.csv").await?;
        
        let table_schema = table.schema();
        assert_eq!(table_schema.fields().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_json_table_provider() -> Result<()> {
        let catalog = setup_test_catalog().await?;
        let schema = catalog.schema("default").await?;
        let table = schema.table("test.json").await?;
        
        let table_schema = table.schema();
        assert!(table_schema.fields().len() > 0);
        Ok(())
    }
}

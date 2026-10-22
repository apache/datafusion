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

#![cfg(not(feature = "nested_expressions"))]

use datafusion::prelude::SessionContext;

#[tokio::test]
async fn set_operations_require_nested_expressions() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let err = ctx
        .sql("SELECT 1 INTERSECT ALL SELECT 1")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("built-in count and range functions to be available"),
        "{err}"
    );

    let dataframe = ctx.sql("SELECT 1").await?;
    let err = dataframe.clone().intersect(dataframe).unwrap_err();
    assert!(
        err.to_string()
            .contains("built-in count and range functions to be available"),
        "{err}"
    );
    Ok(())
}

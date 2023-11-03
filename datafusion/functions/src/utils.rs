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
use datafusion_common::{exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, FunctionImplementation, ScalarUDF, Signature, Volatility,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// Insert a function into the registry
pub(crate) fn insert(
    registry: &mut HashMap<String, Arc<ScalarUDF>>,
    udf: impl FunctionImplementation + Send + Sync + 'static,
) {
    let udf = ScalarUDF::new_from_impl(Arc::new(udf));
    registry.insert(udf.name().to_string(), Arc::new(udf));
}

/// A scalar function that always errors with a hint. This is used to stub out
/// functions that are not enabled with the current set of crate features.
pub struct StubFunc {
    name: &'static str,
    hint: &'static str,
}

impl StubFunc {
    /// Create a new stub function
    pub fn new(name: &'static str, hint: &'static str) -> Self {
        Self { name, hint }
    }
}

static STUB_SIGNATURE: OnceLock<Signature> = OnceLock::new();

impl FunctionImplementation for StubFunc {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        STUB_SIGNATURE.get_or_init(|| Signature::variadic_any(Volatility::Volatile))
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        plan_err!("function {} not available. {}", self.name, self.hint)
    }
    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        exec_err!("function {} not available. {}", self.name, self.hint)
    }
}

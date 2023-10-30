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

//! Udf module contains foundational types that are used to represent UDFs in DataFusion.

use crate::{
    ColumnarValue, Expr, FuncMonotonicity, ReturnTypeFunction,
    ScalarFunctionImplementation, Signature, TypeSignature, Volatility,
};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::{internal_err, DataFusionError, Result};
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

// TODO(PR): add doc comments
pub trait ScalarFunctionDef: Any + Sync + Send + std::fmt::Debug {
    /// Return as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    // May return 1 or more name as aliasing
    fn name(&self) -> &[&str];

    fn input_type(&self) -> TypeSignature;

    fn return_type(&self) -> FunctionReturnType;

    fn execute(&self, _args: &[ArrayRef]) -> Result<ArrayRef> {
        internal_err!("This method should be implemented if `supports_execute_raw()` returns `false`")
    }

    fn volatility(&self) -> Volatility;

    fn monotonicity(&self) -> Option<FuncMonotonicity>;

    // ===============================
    // OPTIONAL METHODS START BELOW
    // ===============================

    /// `execute()` and `execute_raw()` are two possible alternative for function definition:
    /// If returns `false`, `execute()` will be used for execution;
    /// If returns `true`, `execute_raw()` will be called.
    fn use_execute_raw_instead(&self) -> bool {
        false
    }

    /// An alternative function defination than `execute()`
    fn execute_raw(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        internal_err!("This method should be implemented if `supports_execute_raw()` returns `true`")
    }
}

/// Defines the return type behavior of a function.
pub enum FunctionReturnType {
    /// Matches the first argument's type.
    SameAsFirstArg,
    /// A predetermined type.
    FixedType(Arc<DataType>),
    /// Decided by a custom lambda function.
    LambdaReturnType(ReturnTypeFunction),
}

/// Logical representation of a UDF.
#[derive(Clone)]
pub struct ScalarUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    pub fun: ScalarFunctionImplementation,
}

impl Debug for ScalarUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for ScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for ScalarUDF {}

impl std::hash::Hash for ScalarUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDF {
    /// Create a new ScalarUDF
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        fun: &ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            fun: fun.clone(),
        }
    }

    /// creates a logical expression with a call of the UDF
    /// This utility allows using the UDF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF(crate::expr::ScalarUDF::new(Arc::new(self.clone()), args))
    }
}

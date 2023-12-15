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

//! [`ScalarUDF`]: Scalar User Defined Functions

use crate::{
    ColumnarValue, Expr, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

/// Logical representation of a Scalar User Defined Function.
///
/// A scalar function produces a single row output for each row of input.
///
/// This struct contains the information DataFusion needs to plan and invoke
/// functions such name, type signature, return type, and actual implementation.
///
#[derive(Clone)]
pub struct ScalarUDF {
    /// The name of the function
    name: String,
    /// The signature (the types of arguments that are supported)
    signature: Signature,
    /// Function that returns the return type given the argument types
    return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    fun: ScalarFunctionImplementation,
    /// Optional aliases for the function. This list should NOT include the value of `name` as well
    aliases: Vec<String>,
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
            aliases: vec![],
        }
    }

    /// Create a new `ScalarUDF` from a `FuncImpl`
    pub fn new_from_impl(
        fun: impl FunctionImplementation + Send + Sync + 'static,
    ) -> ScalarUDF {
        let arc_fun = Arc::new(fun);
        let captured_self = arc_fun.clone();
        let return_type: ReturnTypeFunction = Arc::new(move |arg_types| {
            let return_type = captured_self.return_type(arg_types)?;
            Ok(Arc::new(return_type))
        });

        let captured_self = arc_fun.clone();
        let func: ScalarFunctionImplementation =
            Arc::new(move |args| captured_self.invoke(args));

        ScalarUDF::new(arc_fun.name(), arc_fun.signature(), &return_type, &func)
    }

    /// Adds additional names that can be used to invoke this function, in addition to `name`
    pub fn with_aliases(
        mut self,
        aliases: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        self.aliases
            .extend(aliases.into_iter().map(|s| s.to_string()));
        self
    }

    /// creates a logical expression with a call of the UDF
    /// This utility allows using the UDF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(crate::expr::ScalarFunction::new_udf(
            Arc::new(self.clone()),
            args,
        ))
    }

    /// Returns this function's name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the aliases for this function. See [`ScalarUDF::with_aliases`] for more details
    pub fn aliases(&self) -> &[String] {
        &self.aliases
    }

    /// Returns this function's signature (what input types are accepted)
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Return the type of the function given its input types
    pub fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        // Old API returns an Arc of the datatype for some reason
        let res = (self.return_type)(args)?;
        Ok(res.as_ref().clone())
    }

    /// Return the actual implementation
    pub fn fun(&self) -> ScalarFunctionImplementation {
        self.fun.clone()
    }
}

/// Convenience trait for implementing ScalarUDF. See [`ScalarUDF::new_from_impl()`]
pub trait FunctionImplementation {
    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns this function's signature
    fn signature(&self) -> &Signature;

    /// return the return type of this function given the types of the arguments
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Invoke the function on `args`, returning the appropriate result
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue>;
}

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

use crate::utils::{arc_ptr_eq, arc_ptr_hash};
use crate::{
    udf_equals_hash, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef};
use async_trait::async_trait;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::internal_err;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::Signature;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A scalar UDF that can invoke using async methods
///
/// Note this is less efficient than the ScalarUDFImpl, but it can be used
/// to register remote functions in the context.
///
/// The name is chosen to mirror ScalarUDFImpl
#[async_trait]
pub trait AsyncScalarUDFImpl: ScalarUDFImpl {
    /// The ideal batch size for this function.
    ///
    /// This is used to determine what size of data to be evaluated at once.
    /// If None, the whole batch will be evaluated at once.
    fn ideal_batch_size(&self) -> Option<usize> {
        None
    }

    /// Invoke the function asynchronously with the async arguments
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        option: &ConfigOptions,
    ) -> Result<ArrayRef>;
}

/// A scalar UDF that must be invoked using async methods
///
/// Note this is not meant to be used directly, but is meant to be an implementation detail
/// for AsyncUDFImpl.
#[derive(Debug)]
pub struct AsyncScalarUDF {
    inner: Arc<dyn AsyncScalarUDFImpl>,
}

impl PartialEq for AsyncScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        let Self { inner } = self;
        // TODO when MSRV >= 1.86.0, switch to `inner.equals(other.inner.as_ref())` leveraging trait upcasting.
        arc_ptr_eq(inner, &other.inner)
    }
}

impl Hash for AsyncScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self { inner } = self;
        arc_ptr_hash(inner, state);
    }
}

impl AsyncScalarUDF {
    pub fn new(inner: Arc<dyn AsyncScalarUDFImpl>) -> Self {
        Self { inner }
    }

    /// The ideal batch size for this function
    pub fn ideal_batch_size(&self) -> Option<usize> {
        self.inner.ideal_batch_size()
    }

    /// Turn this AsyncUDF into a ScalarUDF, suitable for
    /// registering in the context
    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }

    /// Invoke the function asynchronously with the async arguments
    pub async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        option: &ConfigOptions,
    ) -> Result<ArrayRef> {
        self.inner.invoke_async_with_args(args, option).await
    }
}

impl ScalarUDFImpl for AsyncScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("async functions should not be called directly")
    }

    udf_equals_hash!(ScalarUDFImpl);
}

impl Display for AsyncScalarUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AsyncScalarUDF: {}", self.inner.name())
    }
}

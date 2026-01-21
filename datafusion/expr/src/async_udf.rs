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

use crate::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl};
use arrow::datatypes::{DataType, FieldRef};
use async_trait::async_trait;
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
    ) -> Result<ColumnarValue>;
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
        // Deconstruct to catch any new fields added in future
        let Self { inner } = self;
        inner.dyn_eq(other.inner.as_any())
    }
}
impl Eq for AsyncScalarUDF {}

impl Hash for AsyncScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Deconstruct to catch any new fields added in future
        let Self { inner } = self;
        inner.dyn_hash(state);
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
    ) -> Result<ColumnarValue> {
        self.inner.invoke_async_with_args(args).await
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
}

impl Display for AsyncScalarUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AsyncScalarUDF: {}", self.inner.name())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        hash::{DefaultHasher, Hash, Hasher},
        sync::Arc,
    };

    use arrow::datatypes::DataType;
    use async_trait::async_trait;
    use datafusion_common::error::Result;
    use datafusion_expr_common::{columnar_value::ColumnarValue, signature::Signature};

    use crate::{
        ScalarFunctionArgs, ScalarUDFImpl,
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
    };

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct TestAsyncUDFImpl1 {
        a: i32,
    }

    impl ScalarUDFImpl for TestAsyncUDFImpl1 {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            todo!()
        }

        fn signature(&self) -> &Signature {
            todo!()
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            todo!()
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            todo!()
        }
    }

    #[async_trait]
    impl AsyncScalarUDFImpl for TestAsyncUDFImpl1 {
        async fn invoke_async_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            todo!()
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct TestAsyncUDFImpl2 {
        a: i32,
    }

    impl ScalarUDFImpl for TestAsyncUDFImpl2 {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            todo!()
        }

        fn signature(&self) -> &Signature {
            todo!()
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            todo!()
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            todo!()
        }
    }

    #[async_trait]
    impl AsyncScalarUDFImpl for TestAsyncUDFImpl2 {
        async fn invoke_async_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            todo!()
        }
    }

    fn hash<T: Hash>(value: &T) -> u64 {
        let hasher = &mut DefaultHasher::new();
        value.hash(hasher);
        hasher.finish()
    }

    #[test]
    fn test_async_udf_partial_eq_and_hash() {
        // Inner is same cloned arc -> equal
        let inner = Arc::new(TestAsyncUDFImpl1 { a: 1 });
        let a = AsyncScalarUDF::new(Arc::clone(&inner) as Arc<dyn AsyncScalarUDFImpl>);
        let b = AsyncScalarUDF::new(inner);
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));

        // Inner is distinct arc -> still equal
        let a = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl1 { a: 1 }));
        let b = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl1 { a: 1 }));
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));

        // Negative case: inner is different value -> not equal
        let a = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl1 { a: 1 }));
        let b = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl1 { a: 2 }));
        assert_ne!(a, b);
        assert_ne!(hash(&a), hash(&b));

        // Negative case: different functions -> not equal
        let a = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl1 { a: 1 }));
        let b = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl2 { a: 1 }));
        assert_ne!(a, b);
        assert_ne!(hash(&a), hash(&b));
    }
}

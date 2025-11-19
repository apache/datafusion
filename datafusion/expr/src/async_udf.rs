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

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field},
    };
    use arrow::{
        array::{Array, RecordBatch, StringArray},
        datatypes::Schema,
    };
    use async_trait::async_trait;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::error::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::signature::{Signature, Volatility};

    use crate::{
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
        ScalarFunctionArgs, ScalarUDFImpl,
    };

    /// Helper function to convert ColumnarValue to Vec<String>
    fn columnar_to_vec_string(cv: &ColumnarValue) -> Result<Vec<String>> {
        match cv {
            ColumnarValue::Array(arr) => {
                let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(string_arr
                    .iter()
                    .map(|s| s.unwrap_or("").to_string())
                    .collect())
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(vec![s.clone()]),
            _ => panic!("Unexpected type"),
        }
    }

    /// Simulates calling an async external service
    async fn call_external_service(arg1: &ColumnarValue) -> Result<Vec<String>> {
        let vec1 = columnar_to_vec_string(arg1)?;

        Ok(vec1)
    }

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct TestAsyncUDFImpl {
        batch_size: usize,
        signature: Signature,
    }

    impl TestAsyncUDFImpl {
        fn new(batch_size: usize) -> Self {
            Self {
                batch_size,
                signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for TestAsyncUDFImpl {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "test_async_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("Call invoke_async_with_args instead")
        }
    }

    #[async_trait]
    impl AsyncScalarUDFImpl for TestAsyncUDFImpl {
        fn ideal_batch_size(&self) -> Option<usize> {
            Some(self.batch_size)
        }
        async fn invoke_async_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            let arg1 = &args.args[0];
            let results = call_external_service(arg1).await?;
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
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
        let inner = Arc::new(TestAsyncUDFImpl::new(1));
        let a = AsyncScalarUDF::new(Arc::clone(&inner) as Arc<dyn AsyncScalarUDFImpl>);
        let b = AsyncScalarUDF::new(inner);
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));

        // Inner is distinct arc -> still equal
        let a = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(1)));
        let b = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(1)));
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));

        // Negative case: inner is different value -> not equal
        let a = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(1)));
        let b = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(2)));
        assert_ne!(a, b);
        assert_ne!(hash(&a), hash(&b));

        // Negative case: different functions -> not equal
        let a = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(1)));
        let b = AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(1)));
        assert_ne!(a, b);
        assert_ne!(hash(&a), hash(&b));
    }

    #[tokio::test]
    async fn test_async_udf_with_ideal_batch_size() {
        // Create async UDF with ideal batch size of 2
        let udf = TestAsyncUDFImpl::new(2);
        assert_eq!(udf.ideal_batch_size(), Some(2));

        // Create test data with 3 rows, because 3 % 2 != 0
        let test_data = vec!["a", "b", "c"];
        let input = ColumnarValue::Array(Arc::new(StringArray::from(test_data.clone())));

        let args = ScalarFunctionArgs {
            args: vec![input],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Utf8, false))],
            number_rows: test_data.len(),
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        // Invoke the async function - it should handle all rows
        let result = udf.invoke_async_with_args(args).await.unwrap();

        // Verify all rows are processed
        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_arr.len(), 3);
                for (i, expected) in test_data.iter().enumerate() {
                    assert_eq!(string_arr.value(i), *expected);
                }
            }
            _ => panic!("Expected array result"),
        }
    }
}

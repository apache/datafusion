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

use crate::{AggregateUDFImpl, ScalarUDFImpl, WindowUDFImpl};
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

/// A wrapper around a pointer to UDF that implements `Eq` and `Hash` delegating to
/// corresponding methods on the UDF trait.
///
/// If you want to just compare pointers for equality, use [`super::ptr_eq::PtrEq`].
#[derive(Clone)]
#[expect(private_bounds)] // This is so that UdfEq can only be used with allowed pointer types (e.g. Arc), without allowing misuse.
pub struct UdfEq<Ptr: UdfPointer>(Ptr);

impl<Ptr> PartialEq for UdfEq<Ptr>
where
    Ptr: UdfPointer,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.equals(&other.0)
    }
}
impl<Ptr> Eq for UdfEq<Ptr> where Ptr: UdfPointer {}
impl<Ptr> Hash for UdfEq<Ptr>
where
    Ptr: UdfPointer,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash_value().hash(state);
    }
}

impl<Ptr> From<Ptr> for UdfEq<Ptr>
where
    Ptr: UdfPointer,
{
    fn from(ptr: Ptr) -> Self {
        UdfEq(ptr)
    }
}

impl<Ptr> Debug for UdfEq<Ptr>
where
    Ptr: UdfPointer + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<Ptr> Deref for UdfEq<Ptr>
where
    Ptr: UdfPointer,
{
    type Target = Ptr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

trait UdfPointer: Deref {
    fn equals(&self, other: &Self::Target) -> bool;
    fn hash_value(&self) -> u64;
}

impl UdfPointer for Arc<dyn ScalarUDFImpl + '_> {
    fn equals(&self, other: &(dyn ScalarUDFImpl + '_)) -> bool {
        self.as_ref().dyn_eq(other.as_any())
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.as_ref().dyn_hash(hasher);
        hasher.finish()
    }
}

impl UdfPointer for Arc<dyn AggregateUDFImpl + '_> {
    fn equals(&self, other: &(dyn AggregateUDFImpl + '_)) -> bool {
        self.as_ref().dyn_eq(other.as_any())
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.as_ref().dyn_hash(hasher);
        hasher.finish()
    }
}

impl UdfPointer for Arc<dyn WindowUDFImpl + '_> {
    fn equals(&self, other: &(dyn WindowUDFImpl + '_)) -> bool {
        self.as_ref().dyn_eq(other.as_any())
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.as_ref().dyn_hash(hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScalarFunctionArgs;
    use arrow::datatypes::DataType;
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::signature::{Signature, Volatility};
    use std::any::Any;
    use std::hash::DefaultHasher;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestScalarUDF {
        signature: Signature,
        name: &'static str,
    }
    impl ScalarUDFImpl for TestScalarUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            self.name
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> datafusion_common::Result<DataType> {
            unimplemented!()
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion_common::Result<ColumnarValue> {
            unimplemented!()
        }
    }

    #[test]
    pub fn test_eq_eq_wrapper() {
        let signature = Signature::any(1, Volatility::Immutable);

        let a1: Arc<dyn ScalarUDFImpl> = Arc::new(TestScalarUDF {
            signature: signature.clone(),
            name: "a",
        });
        let a2: Arc<dyn ScalarUDFImpl> = Arc::new(TestScalarUDF {
            signature: signature.clone(),
            name: "a",
        });
        let b: Arc<dyn ScalarUDFImpl> = Arc::new(TestScalarUDF {
            signature: signature.clone(),
            name: "b",
        });

        // Reflexivity
        let wrapper = UdfEq(Arc::clone(&a1));
        assert_eq!(wrapper, wrapper);

        // Two wrappers around equal pointer
        assert_eq!(UdfEq(Arc::clone(&a1)), UdfEq(Arc::clone(&a1)));
        assert_eq!(hash(UdfEq(Arc::clone(&a1))), hash(UdfEq(Arc::clone(&a1))));

        // Two wrappers around different pointers but equal in ScalarUDFImpl::equals sense
        assert_eq!(UdfEq(Arc::clone(&a1)), UdfEq(Arc::clone(&a2)));
        assert_eq!(hash(UdfEq(Arc::clone(&a1))), hash(UdfEq(Arc::clone(&a2))));

        // different functions (not equal)
        assert_ne!(UdfEq(Arc::clone(&a1)), UdfEq(Arc::clone(&b)));
    }

    fn hash<T: Hash>(value: T) -> u64 {
        let hasher = &mut DefaultHasher::new();
        value.hash(hasher);
        hasher.finish()
    }
}

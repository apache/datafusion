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

use crate::__private::ExInstantiable;
use crate::arg_type_list::ExArgTypeList;
use crate::bridge::ExcaliburScalarUdf;
use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::invoke::{excalibur_invoke, ApplyList};
use crate::signature::{create_excalibur_signature, ExcaliburSignature};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

pub fn create_excalibur_scalar_udf<T>() -> Arc<dyn ScalarUDFImpl>
where
    T: ExcaliburScalarUdf + Send + Sync + 'static,
    T::ArgumentRustTypes: ExArgTypeList,
    T::ArgumentRustTypes: ApplyList,
    (T::OutArgRustType, T::ReturnRustType): ExFullResultType<
        BuilderType: ExArrayBuilder<
            OutArg: for<'a> ExInstantiable<
                StackType<'a> = <T::OutArgRustType as ExInstantiable>::StackType<'a>,
            >,
            Return = T::ReturnRustType,
        >,
    >,
{
    Arc::new(ExcaliburScalarUdfImpl::<T> {
        signature: create_excalibur_signature::<T>(),
        _phantom: Default::default(),
    })
}

struct ExcaliburScalarUdfImpl<T> {
    signature: ExcaliburSignature,
    _phantom: PhantomData<T>,
}

impl<T> Debug for ExcaliburScalarUdfImpl<T>
where
    T: ExcaliburScalarUdf,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ExcaliburScalarUdfImpl({})", T::SQL_NAME))
    }
}

impl<T> ScalarUDFImpl for ExcaliburScalarUdfImpl<T>
where
    T: ExcaliburScalarUdf + Send + Sync + 'static,
    T::ArgumentRustTypes: ApplyList,
    (T::OutArgRustType, T::ReturnRustType): ExFullResultType<
        BuilderType: ExArrayBuilder<
            OutArg: for<'a> ExInstantiable<
                StackType<'a> = <T::OutArgRustType as ExInstantiable>::StackType<'a>,
            >,
            Return = T::ReturnRustType,
        >,
    >,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        T::SQL_NAME
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.signature.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        excalibur_invoke::<T>(args)
    }

    fn invoke_batch(
        &self,
        _args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        unimplemented!("invoke_batch is not implemented, it should be deprecated in https://github.com/apache/datafusion/issues/13515")
    }
}

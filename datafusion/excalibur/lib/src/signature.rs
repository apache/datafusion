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

use crate::arg_type_list::ExArgTypeList;
use crate::bridge::ExcaliburScalarUdf;
use crate::builder::ExFullResultType;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{Signature, Volatility};
use datafusion_expr_common::signature::Coercion;

pub fn create_excalibur_signature<T>() -> ExcaliburSignature
where
    T: ExcaliburScalarUdf,
    T::ArgumentRustTypes: ExArgTypeList,
    (T::OutArgRustType, T::ReturnRustType): ExFullResultType,
{
    ExcaliburSignature {
        signature: Signature::coercible(
            T::ArgumentRustTypes::type_signature()
                .into_iter()
                // TODO this *exact* is unintentional, see https://github.com/apache/datafusion/pull/14440#discussion_r1959483130
                .map(Coercion::new_exact)
                .collect(),
            Volatility::Immutable,
        ),
        return_type:
            <(T::OutArgRustType, T::ReturnRustType) as ExFullResultType>::data_type(),
    }
}

pub struct ExcaliburSignature {
    signature: Signature,
    return_type: DataType,
}

impl ExcaliburSignature {
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    pub fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
}

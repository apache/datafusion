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

use abi_stable::{
    std_types::{ROption, RVec},
    StableAbi,
};
use arrow::datatypes::DataType;
use datafusion::{
    common::exec_datafusion_err, error::DataFusionError, logical_expr::ReturnTypeArgs,
    scalar::ScalarValue,
};

use crate::{
    arrow_wrappers::WrappedSchema,
    util::{rvec_wrapped_to_vec_datatype, vec_datatype_to_rvec_wrapped},
};
use prost::Message;

/// A stable struct for sharing a [`ReturnTypeArgs`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ReturnTypeArgs {
    arg_types: RVec<WrappedSchema>,
    scalar_arguments: RVec<ROption<RVec<u8>>>,
    nullables: RVec<bool>,
}

impl TryFrom<ReturnTypeArgs<'_>> for FFI_ReturnTypeArgs {
    type Error = DataFusionError;

    fn try_from(value: ReturnTypeArgs) -> Result<Self, Self::Error> {
        let arg_types = vec_datatype_to_rvec_wrapped(value.arg_types)?;
        let scalar_arguments: Result<Vec<_>, Self::Error> = value
            .scalar_arguments
            .iter()
            .map(|maybe_arg| {
                maybe_arg
                    .map(|arg| {
                        let proto_value: datafusion_proto::protobuf::ScalarValue =
                            arg.try_into()?;
                        let proto_bytes: RVec<u8> = proto_value.encode_to_vec().into();
                        Ok(proto_bytes)
                    })
                    .transpose()
            })
            .collect();
        let scalar_arguments = scalar_arguments?.into_iter().map(ROption::from).collect();

        let nullables = value.nullables.into();
        Ok(Self {
            arg_types,
            scalar_arguments,
            nullables,
        })
    }
}

// TODO(tsaucer) It would be good to find a better way around this, but it
// appears a restriction based on the need to have a borrowed ScalarValue
// in the arguments when converted to ReturnTypeArgs
pub struct ForeignReturnTypeArgsOwned {
    arg_types: Vec<DataType>,
    scalar_arguments: Vec<Option<ScalarValue>>,
    nullables: Vec<bool>,
}

pub struct ForeignReturnTypeArgs<'a> {
    arg_types: &'a [DataType],
    scalar_arguments: Vec<Option<&'a ScalarValue>>,
    nullables: &'a [bool],
}

impl TryFrom<&FFI_ReturnTypeArgs> for ForeignReturnTypeArgsOwned {
    type Error = DataFusionError;

    fn try_from(value: &FFI_ReturnTypeArgs) -> Result<Self, Self::Error> {
        let arg_types = rvec_wrapped_to_vec_datatype(&value.arg_types)?;
        let scalar_arguments: Result<Vec<_>, Self::Error> = value
            .scalar_arguments
            .iter()
            .map(|maybe_arg| {
                let maybe_arg = maybe_arg.as_ref().map(|arg| {
                    let proto_value =
                        datafusion_proto::protobuf::ScalarValue::decode(arg.as_ref())
                            .map_err(|err| exec_datafusion_err!("{}", err))?;
                    let scalar_value: ScalarValue = (&proto_value).try_into()?;
                    Ok(scalar_value)
                });
                Option::from(maybe_arg).transpose()
            })
            .collect();
        let scalar_arguments = scalar_arguments?.into_iter().collect();

        let nullables = value.nullables.iter().cloned().collect();

        Ok(Self {
            arg_types,
            scalar_arguments,
            nullables,
        })
    }
}

impl<'a> From<&'a ForeignReturnTypeArgsOwned> for ForeignReturnTypeArgs<'a> {
    fn from(value: &'a ForeignReturnTypeArgsOwned) -> Self {
        Self {
            arg_types: &value.arg_types,
            scalar_arguments: value
                .scalar_arguments
                .iter()
                .map(|opt| opt.as_ref())
                .collect(),
            nullables: &value.nullables,
        }
    }
}

impl<'a> From<&'a ForeignReturnTypeArgs<'a>> for ReturnTypeArgs<'a> {
    fn from(value: &'a ForeignReturnTypeArgs) -> Self {
        ReturnTypeArgs {
            arg_types: value.arg_types,
            scalar_arguments: &value.scalar_arguments,
            nullables: value.nullables,
        }
    }
}

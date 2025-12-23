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

use abi_stable::StableAbi;
use abi_stable::std_types::{ROption, RVec};
use arrow_schema::FieldRef;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{DataFusionError, ffi_datafusion_err};
use datafusion_expr::ReturnFieldArgs;
use prost::Message;

use crate::arrow_wrappers::WrappedSchema;
use crate::util::{rvec_wrapped_to_vec_fieldref, vec_fieldref_to_rvec_wrapped};

/// A stable struct for sharing a [`ReturnFieldArgs`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ReturnFieldArgs {
    arg_fields: RVec<WrappedSchema>,
    scalar_arguments: RVec<ROption<RVec<u8>>>,
}

impl TryFrom<ReturnFieldArgs<'_>> for FFI_ReturnFieldArgs {
    type Error = DataFusionError;

    fn try_from(value: ReturnFieldArgs) -> Result<Self, Self::Error> {
        let arg_fields = vec_fieldref_to_rvec_wrapped(value.arg_fields)?;
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

        Ok(Self {
            arg_fields,
            scalar_arguments,
        })
    }
}

// TODO(tsaucer) It would be good to find a better way around this, but it
// appears a restriction based on the need to have a borrowed ScalarValue
// in the arguments when converted to ReturnFieldArgs
pub struct ForeignReturnFieldArgsOwned {
    arg_fields: Vec<FieldRef>,
    scalar_arguments: Vec<Option<ScalarValue>>,
}

pub struct ForeignReturnFieldArgs<'a> {
    arg_fields: &'a [FieldRef],
    scalar_arguments: Vec<Option<&'a ScalarValue>>,
}

impl TryFrom<&FFI_ReturnFieldArgs> for ForeignReturnFieldArgsOwned {
    type Error = DataFusionError;

    fn try_from(value: &FFI_ReturnFieldArgs) -> Result<Self, Self::Error> {
        let arg_fields = rvec_wrapped_to_vec_fieldref(&value.arg_fields)?;
        let scalar_arguments: Result<Vec<_>, Self::Error> = value
            .scalar_arguments
            .iter()
            .map(|maybe_arg| {
                let maybe_arg = maybe_arg.as_ref().map(|arg| {
                    let proto_value =
                        datafusion_proto::protobuf::ScalarValue::decode(arg.as_ref())
                            .map_err(|err| ffi_datafusion_err!("{}", err))?;
                    let scalar_value: ScalarValue = (&proto_value).try_into()?;
                    Ok(scalar_value)
                });
                Option::from(maybe_arg).transpose()
            })
            .collect();
        let scalar_arguments = scalar_arguments?.into_iter().collect();

        Ok(Self {
            arg_fields,
            scalar_arguments,
        })
    }
}

impl<'a> From<&'a ForeignReturnFieldArgsOwned> for ForeignReturnFieldArgs<'a> {
    fn from(value: &'a ForeignReturnFieldArgsOwned) -> Self {
        Self {
            arg_fields: &value.arg_fields,
            scalar_arguments: value
                .scalar_arguments
                .iter()
                .map(|opt| opt.as_ref())
                .collect(),
        }
    }
}

impl<'a> From<&'a ForeignReturnFieldArgs<'a>> for ReturnFieldArgs<'a> {
    fn from(value: &'a ForeignReturnFieldArgs) -> Self {
        ReturnFieldArgs {
            arg_fields: value.arg_fields,
            scalar_arguments: &value.scalar_arguments,
        }
    }
}

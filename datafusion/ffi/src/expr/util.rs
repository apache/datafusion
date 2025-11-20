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

use abi_stable::std_types::RVec;
use datafusion_common::{exec_datafusion_err, Result, ScalarValue};
use prost::Message;

pub fn scalar_value_to_rvec_u8(value: &ScalarValue) -> Result<RVec<u8>> {
    let value: datafusion_proto_common::ScalarValue = value.try_into()?;
    Ok(value.encode_to_vec().into())
}

pub fn rvec_u8_to_scalar_value(value: &RVec<u8>) -> Result<ScalarValue> {
    let value = datafusion_proto_common::ScalarValue::decode(value.as_ref())
        .map_err(|err| exec_datafusion_err!("{err}"))?;

    (&value)
        .try_into()
        .map_err(|err| exec_datafusion_err!("{err}"))
}

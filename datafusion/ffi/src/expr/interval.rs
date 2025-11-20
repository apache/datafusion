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
use abi_stable::StableAbi;
use datafusion_common::DataFusionError;
use datafusion_expr::interval_arithmetic::Interval;

use crate::expr::util::{rvec_u8_to_scalar_value, scalar_value_to_rvec_u8};

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Interval {
    lower: RVec<u8>,
    upper: RVec<u8>,
}

impl TryFrom<&Interval> for FFI_Interval {
    type Error = DataFusionError;
    fn try_from(value: &Interval) -> Result<Self, Self::Error> {
        let upper = scalar_value_to_rvec_u8(value.upper())?;
        let lower = scalar_value_to_rvec_u8(value.lower())?;

        Ok(FFI_Interval { upper, lower })
    }
}
impl TryFrom<Interval> for FFI_Interval {
    type Error = DataFusionError;
    fn try_from(value: Interval) -> Result<Self, Self::Error> {
        FFI_Interval::try_from(&value)
    }
}

impl TryFrom<&FFI_Interval> for Interval {
    type Error = DataFusionError;
    fn try_from(value: &FFI_Interval) -> Result<Self, Self::Error> {
        let upper = rvec_u8_to_scalar_value(&value.upper)?;
        let lower = rvec_u8_to_scalar_value(&value.lower)?;

        Interval::try_new(lower, upper)
    }
}

impl TryFrom<FFI_Interval> for Interval {
    type Error = DataFusionError;
    fn try_from(value: FFI_Interval) -> Result<Self, Self::Error> {
        Interval::try_from(&value)
    }
}

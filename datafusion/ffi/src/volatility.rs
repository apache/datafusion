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
use datafusion_expr::Volatility;

#[repr(C)]
#[derive(Debug, StableAbi, Clone)]
pub enum FFI_Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl From<Volatility> for FFI_Volatility {
    fn from(value: Volatility) -> Self {
        match value {
            Volatility::Immutable => Self::Immutable,
            Volatility::Stable => Self::Stable,
            Volatility::Volatile => Self::Volatile,
        }
    }
}

impl From<&FFI_Volatility> for Volatility {
    fn from(value: &FFI_Volatility) -> Self {
        match value {
            FFI_Volatility::Immutable => Self::Immutable,
            FFI_Volatility::Stable => Self::Stable,
            FFI_Volatility::Volatile => Self::Volatile,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::Volatility;

    use super::FFI_Volatility;

    fn test_round_trip_volatility(volatility: Volatility) {
        let ffi_volatility: FFI_Volatility = volatility.into();
        let round_trip: Volatility = (&ffi_volatility).into();

        assert_eq!(volatility, round_trip);
    }

    #[test]
    fn test_all_round_trip_volatility() {
        test_round_trip_volatility(Volatility::Immutable);
        test_round_trip_volatility(Volatility::Stable);
        test_round_trip_volatility(Volatility::Volatile);
    }
}

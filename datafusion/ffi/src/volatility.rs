// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::Volatility;

#[expect(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone)]
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

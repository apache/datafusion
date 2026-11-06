// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::DataFusionError;
use datafusion_expr::interval_arithmetic::Interval;

use crate::arrow_wrappers::WrappedArray;

/// A stable struct for sharing [`Interval`] across FFI boundaries.
/// See [`Interval`] for the meaning of each field. Scalar values
/// are passed as Arrow arrays of length 1.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_Interval {
    lower: WrappedArray,
    upper: WrappedArray,
}

impl TryFrom<&Interval> for FFI_Interval {
    type Error = DataFusionError;
    fn try_from(value: &Interval) -> Result<Self, Self::Error> {
        let upper = value.upper().try_into()?;
        let lower = value.lower().try_into()?;

        Ok(FFI_Interval { lower, upper })
    }
}
impl TryFrom<Interval> for FFI_Interval {
    type Error = DataFusionError;
    fn try_from(value: Interval) -> Result<Self, Self::Error> {
        FFI_Interval::try_from(&value)
    }
}

impl TryFrom<FFI_Interval> for Interval {
    type Error = DataFusionError;
    fn try_from(value: FFI_Interval) -> Result<Self, Self::Error> {
        let upper = value.upper.try_into()?;
        let lower = value.lower.try_into()?;

        Interval::try_new(lower, upper)
    }
}

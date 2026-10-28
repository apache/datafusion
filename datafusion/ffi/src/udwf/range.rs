// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::ops::Range;

/// A stable struct for sharing [`Range`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`Range`].
#[repr(C)]
#[derive(Debug)]
pub struct FFI_Range {
    pub start: usize,
    pub end: usize,
}

impl From<Range<usize>> for FFI_Range {
    fn from(value: Range<usize>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<FFI_Range> for Range<usize> {
    fn from(value: FFI_Range) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_ffi_range() {
        let original = Range { start: 10, end: 30 };

        let ffi_range: FFI_Range = original.clone().into();
        let round_trip: Range<usize> = ffi_range.into();

        assert_eq!(original, round_trip);
    }
}

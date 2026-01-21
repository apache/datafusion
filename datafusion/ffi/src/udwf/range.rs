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

use std::ops::Range;

use abi_stable::StableAbi;

/// A stable struct for sharing [`Range`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`Range`].
#[repr(C)]
#[derive(Debug, StableAbi)]
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

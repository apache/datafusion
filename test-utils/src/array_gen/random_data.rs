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

use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{
    Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, IntervalDayTime, IntervalDayTimeType, IntervalMonthDayNano,
    IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type, i256,
};
use rand::Rng;
use rand::distr::StandardUniform;
use rand::prelude::Distribution;
use rand::rngs::StdRng;

/// Generate corresponding NativeType value randomly according to
/// ArrowPrimitiveType.
pub trait RandomNativeData: ArrowPrimitiveType {
    fn generate_random_native_data(rng: &mut StdRng) -> Self::Native;
}

macro_rules! basic_random_data {
    ($ARROW_TYPE: ty) => {
        impl RandomNativeData for $ARROW_TYPE
        where
            StandardUniform: Distribution<Self::Native>,
        {
            #[inline]
            fn generate_random_native_data(rng: &mut StdRng) -> Self::Native {
                rng.random::<Self::Native>()
            }
        }
    };
}

basic_random_data!(Int8Type);
basic_random_data!(Int16Type);
basic_random_data!(Int32Type);
basic_random_data!(Int64Type);
basic_random_data!(UInt8Type);
basic_random_data!(UInt16Type);
basic_random_data!(UInt32Type);
basic_random_data!(UInt64Type);
basic_random_data!(Float32Type);
basic_random_data!(Float64Type);
basic_random_data!(Date32Type);
basic_random_data!(Time32SecondType);
basic_random_data!(Time32MillisecondType);
basic_random_data!(Time64MicrosecondType);
basic_random_data!(Time64NanosecondType);
basic_random_data!(IntervalYearMonthType);
basic_random_data!(Decimal32Type);
basic_random_data!(Decimal64Type);
basic_random_data!(Decimal128Type);
basic_random_data!(TimestampSecondType);
basic_random_data!(TimestampMillisecondType);
basic_random_data!(TimestampMicrosecondType);
basic_random_data!(TimestampNanosecondType);
// Note DurationSecondType is restricted to i64::MIN / 1000 to i64::MAX / 1000
// due to https://github.com/apache/arrow-rs/issues/7533 so handle it specially below
basic_random_data!(DurationMillisecondType);
basic_random_data!(DurationMicrosecondType);
basic_random_data!(DurationNanosecondType);

impl RandomNativeData for Date64Type {
    fn generate_random_native_data(rng: &mut StdRng) -> Self::Native {
        // TODO: constrain this range to valid dates if necessary
        let date_value = rng.random_range(i64::MIN..=i64::MAX);
        let millis_per_day = 86_400_000;
        date_value - (date_value % millis_per_day)
    }
}

impl RandomNativeData for IntervalDayTimeType {
    fn generate_random_native_data(rng: &mut StdRng) -> Self::Native {
        IntervalDayTime {
            days: rng.random::<i32>(),
            milliseconds: rng.random::<i32>(),
        }
    }
}

impl RandomNativeData for IntervalMonthDayNanoType {
    fn generate_random_native_data(rng: &mut StdRng) -> Self::Native {
        IntervalMonthDayNano {
            months: rng.random::<i32>(),
            days: rng.random::<i32>(),
            nanoseconds: rng.random::<i64>(),
        }
    }
}

// Restrict Duration(Seconds) to i64::MIN / 1000 to i64::MAX / 1000 to
// avoid panics on pretty printing. See
// https://github.com/apache/arrow-rs/issues/7533
impl RandomNativeData for DurationSecondType {
    fn generate_random_native_data(rng: &mut StdRng) -> Self::Native {
        rng.random::<i64>() / 1000
    }
}

impl RandomNativeData for Decimal256Type {
    fn generate_random_native_data(rng: &mut StdRng) -> Self::Native {
        i256::from_parts(rng.random::<u128>(), rng.random::<i128>())
    }
}

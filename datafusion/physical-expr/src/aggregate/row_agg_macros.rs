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

#[macro_export]
macro_rules! matches_all_supported_data_types {
    ($expression:expr) => {
        matches!(
            $expression,
            DataType::Boolean
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
        )
    };
}
pub use matches_all_supported_data_types;

#[macro_export]
macro_rules! dispatch_all_supported_data_types {
    ($macro:ident $(, $x:ident)*) => {
        $macro! {
                [$($x),*],
                { Boolean, BooleanArray },
                { Int8, Int8Array },
                { Int16, Int16Array },
                { Int32, Int32Array },
                { Int64, Int64Array },
                { UInt8, UInt8Array },
                { UInt16, UInt16Array },
                { UInt32, UInt32Array },
                { UInt64, UInt64Array },
                { Float32, Float32Array },
                { Float64, Float64Array },
                { Decimal128, Decimal128Array }
        }
    };
}
pub use dispatch_all_supported_data_types;

// TODO generate the matching type pairs
#[macro_export]
macro_rules! dispatch_all_supported_data_types_pairs {
    ($macro:ident $(, $x:ident)*) => {
       $macro! {
                [$($x),*],
                { Boolean, BooleanArray, Boolean, BooleanArray },
                { Int8, Int8Array, Boolean, BooleanArray },
                { Int16, Int16Array, Boolean, BooleanArray },
                { Int32, Int32Array, Boolean, BooleanArray },
                { Int64, Int64Array, Boolean, BooleanArray },
                { UInt8, UInt8Array, Boolean, BooleanArray },
                { UInt16, UInt16Array, Boolean, BooleanArray },
                { UInt32, UInt32Array, Boolean, BooleanArray },
                { UInt64, UInt64Array, Boolean, BooleanArray },
                { Float32, Float32Array, Boolean, BooleanArray },
                { Float64, Float64Array, Boolean, BooleanArray },
                { Decimal128, Decimal128Array, Boolean, BooleanArray },
                { Boolean, BooleanArray, Int8, Int8Array },
                { Int8, Int8Array, Int8, Int8Array },
                { Int16, Int16Array, Int8, Int8Array },
                { Int32, Int32Array, Int8, Int8Array },
                { Int64, Int64Array, Int8, Int8Array },
                { UInt8, UInt8Array, Int8, Int8Array },
                { UInt16, UInt16Array, Int8, Int8Array },
                { UInt32, UInt32Array, Int8, Int8Array },
                { UInt64, UInt64Array, Int8, Int8Array },
                { Float32, Float32Array, Int8, Int8Array },
                { Float64, Float64Array, Int8, Int8Array },
                { Decimal128, Decimal128Array, Int8, Int8Array },
                { Boolean, BooleanArray, Int16, Int16Array},
                { Int8, Int8Array, Int16, Int16Array },
                { Int16, Int16Array, Int16, Int16Array },
                { Int32, Int32Array, Int16, Int16Array },
                { Int64, Int64Array, Int16, Int16Array },
                { UInt8, UInt8Array, Int16, Int16Array },
                { UInt16, UInt16Array, Int16, Int16Array },
                { UInt32, UInt32Array, Int16, Int16Array },
                { UInt64, UInt64Array, Int16, Int16Array },
                { Float32, Float32Array, Int16, Int16Array },
                { Float64, Float64Array, Int16, Int16Array },
                { Decimal128, Decimal128Array, Int16, Int16Array },
                { Boolean, BooleanArray, Int32, Int32Array },
                { Int8, Int8Array, Int32, Int32Array },
                { Int16, Int16Array, Int32, Int32Array },
                { Int32, Int32Array, Int32, Int32Array },
                { Int64, Int64Array, Int32, Int32Array },
                { UInt8, UInt8Array, Int32, Int32Array },
                { UInt16, UInt16Array, Int32, Int32Array },
                { UInt32, UInt32Array, Int32, Int32Array },
                { UInt64, UInt64Array, Int32, Int32Array },
                { Float32, Float32Array, Int32, Int32Array },
                { Float64, Float64Array, Int32, Int32Array },
                { Decimal128, Decimal128Array, Int32, Int32Array },
                { Boolean, BooleanArray, Int64, Int64Array },
                { Int8, Int8Array, Int64, Int64Array },
                { Int16, Int16Array, Int64, Int64Array },
                { Int32, Int32Array, Int64, Int64Array },
                { Int64, Int64Array, Int64, Int64Array },
                { UInt8, UInt8Array, Int64, Int64Array },
                { UInt16, UInt16Array, Int64, Int64Array },
                { UInt32, UInt32Array, Int64, Int64Array },
                { UInt64, UInt64Array, Int64, Int64Array },
                { Float32, Float32Array, Int64, Int64Array },
                { Float64, Float64Array, Int64, Int64Array },
                { Decimal128, Decimal128Array, Int64, Int64Array },
                { Boolean, BooleanArray, UInt8, UInt8Array },
                { Int8, Int8Array, UInt8, UInt8Array },
                { Int16, Int16Array, UInt8, UInt8Array },
                { Int32, Int32Array, UInt8, UInt8Array },
                { Int64, Int64Array, UInt8, UInt8Array },
                { UInt8, UInt8Array, UInt8, UInt8Array },
                { UInt16, UInt16Array, UInt8, UInt8Array },
                { UInt32, UInt32Array, UInt8, UInt8Array },
                { UInt64, UInt64Array, UInt8, UInt8Array },
                { Float32, Float32Array, UInt8, UInt8Array },
                { Float64, Float64Array, UInt8, UInt8Array },
                { Decimal128, Decimal128Array, UInt8, UInt8Array },
                { Boolean, BooleanArray, UInt16, UInt16Array },
                { Int8, Int8Array, UInt16, UInt16Array },
                { Int16, Int16Array, UInt16, UInt16Array },
                { Int32, Int32Array, UInt16, UInt16Array },
                { Int64, Int64Array, UInt16, UInt16Array },
                { UInt8, UInt8Array, UInt16, UInt16Array },
                { UInt16, UInt16Array, UInt16, UInt16Array },
                { UInt32, UInt32Array, UInt16, UInt16Array },
                { UInt64, UInt64Array, UInt16, UInt16Array },
                { Float32, Float32Array, UInt16, UInt16Array },
                { Float64, Float64Array, UInt16, UInt16Array },
                { Decimal128, Decimal128Array, UInt16, UInt16Array },
                { Boolean, BooleanArray, UInt32, UInt32Array },
                { Int8, Int8Array, UInt32, UInt32Array },
                { Int16, Int16Array, UInt32, UInt32Array },
                { Int32, Int32Array, UInt32, UInt32Array },
                { Int64, Int64Array, UInt32, UInt32Array },
                { UInt8, UInt8Array, UInt32, UInt32Array },
                { UInt16, UInt16Array, UInt32, UInt32Array },
                { UInt32, UInt32Array, UInt32, UInt32Array },
                { UInt64, UInt64Array, UInt32, UInt32Array },
                { Float32, Float32Array, UInt32, UInt32Array },
                { Float64, Float64Array, UInt32, UInt32Array },
                { Decimal128, Decimal128Array, UInt32, UInt32Array },
                { Boolean, BooleanArray, UInt64, UInt64Array },
                { Int8, Int8Array, UInt64, UInt64Array },
                { Int16, Int16Array, UInt64, UInt64Array },
                { Int32, Int32Array, UInt64, UInt64Array },
                { Int64, Int64Array, UInt64, UInt64Array },
                { UInt8, UInt8Array, UInt64, UInt64Array },
                { UInt16, UInt16Array, UInt64, UInt64Array },
                { UInt32, UInt32Array, UInt64, UInt64Array },
                { UInt64, UInt64Array, UInt64, UInt64Array },
                { Float32, Float32Array, UInt64, UInt64Array },
                { Float64, Float64Array, UInt64, UInt64Array },
                { Decimal128, Decimal128Array, UInt64, UInt64Array },
                { Boolean, BooleanArray, Float32, Float32Array },
                { Int8, Int8Array, Float32, Float32Array },
                { Int16, Int16Array, Float32, Float32Array },
                { Int32, Int32Array, Float32, Float32Array },
                { Int64, Int64Array, Float32, Float32Array },
                { UInt8, UInt8Array, Float32, Float32Array },
                { UInt16, UInt16Array, Float32, Float32Array },
                { UInt32, UInt32Array, Float32, Float32Array },
                { UInt64, UInt64Array, Float32, Float32Array },
                { Float32, Float32Array, Float32, Float32Array },
                { Float64, Float64Array, Float32, Float32Array },
                { Decimal128, Decimal128Array, Float32, Float32Array },
                { Boolean, BooleanArray, Float64, Float64Array },
                { Int8, Int8Array, Float64, Float64Array },
                { Int16, Int16Array, Float64, Float64Array },
                { Int32, Int32Array, Float64, Float64Array },
                { Int64, Int64Array, Float64, Float64Array },
                { UInt8, UInt8Array, Float64, Float64Array },
                { UInt16, UInt16Array, Float64, Float64Array },
                { UInt32, UInt32Array, Float64, Float64Array },
                { UInt64, UInt64Array, Float64, Float64Array },
                { Float32, Float32Array, Float64, Float64Array },
                { Float64, Float64Array, Float64, Float64Array },
                { Decimal128, Decimal128Array, Float64, Float64Array },
                { Boolean, BooleanArray, Decimal128, Decimal128Array },
                { Int8, Int8Array, Decimal128, Decimal128Array },
                { Int16, Int16Array, Decimal128, Decimal128Array },
                { Int32, Int32Array, Decimal128, Decimal128Array },
                { Int64, Int64Array, Decimal128, Decimal128Array },
                { UInt8, UInt8Array, Decimal128, Decimal128Array },
                { UInt16, UInt16Array, Decimal128, Decimal128Array },
                { UInt32, UInt32Array, Decimal128, Decimal128Array },
                { UInt64, UInt64Array, Decimal128, Decimal128Array },
                { Float32, Float32Array, Decimal128, Decimal128Array },
                { Float64, Float64Array, Decimal128, Decimal128Array },
                { Decimal128, Decimal128Array, Decimal128, Decimal128Array }
        }
    };
}

pub use dispatch_all_supported_data_types_pairs;

/// Generate one row accumulator dispatch logic
#[macro_export]
macro_rules! impl_one_row_accumulator_dispatch {
    (
        [$array1_dt:ident, $array1:ident, $acc_idx1:ident, $self:ident, $update_func:ident, $groups_with_rows:ident, $filter_bool_array:ident], $({ $i1t:ident, $i1:ident}),*
    ) => {
        match ($array1_dt) {
            $(
                $i1t! { datatype_match_pattern } => {
                        let typed_array = downcast_value!($array1, $i1);
                        $self.$update_func(
                            $groups_with_rows,
                            &typed_array,
                            $acc_idx1,
                            &$filter_bool_array,
                        )?;
                }
            )*
            _ => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {} in RowAccumulator",
                        $array1_dt
                    )))
        }
    };
}

pub use impl_one_row_accumulator_dispatch;

/// Generate two row accumulators dispatch logic
#[macro_export]
macro_rules! impl_two_row_accumulators_dispatch {
    (
        [$array1_dt:ident, $array2_dt:ident, $array1:ident, $array2:ident, $acc_idx1:ident, $acc_idx2:ident, $self:ident, $update_func:ident, $groups_with_rows:ident, $filter_bool_array:ident], $({ $i1t:ident, $i1:ident, $i2t:ident, $i2:ident}),*
    ) => {
        match ($array1_dt, $array2_dt) {
            $(
                ($i1t! { datatype_match_pattern }, $i2t! { datatype_match_pattern }) => {
                        let typed_array1 = downcast_value!($array1, $i1);
                        let typed_array2 = downcast_value!($array2, $i2);
                        $self.$update_func(
                            $groups_with_rows,
                            &typed_array1,
                            &typed_array2,
                            $acc_idx1,
                            $acc_idx2,
                            &$filter_bool_array,
                        )?;
                }
            )*
            (_, _) => return Err(DataFusionError::Internal(format!(
                        "Unsupported data types {}, {} in RowAccumulator",
                        $array1_dt, $array2_dt
                    )))
        }
    };
}

pub use impl_two_row_accumulators_dispatch;

/// Generate row accumulator update single row dispatch logic
#[macro_export]
macro_rules! impl_row_accumulator_update_single_row_dispatch {
    (
        [$array_dt:ident, $array:ident, $row_index:ident, $update_func:ident, $accessor:ident, $self:ident], $({ $i1t:ident, $i1:ident}),*
    ) => {
        match ($array_dt) {
            $(
                $i1t! { datatype_match_pattern } => {
                        let typed_array = downcast_value!($array, $i1);
                        let value = typed_array.value_at($row_index);
                        value.$update_func($self.index, $accessor);
                }
            )*
            _ => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {} in RowAccumulator",
                        $array_dt
                    )))
        }
    };
}

pub use impl_row_accumulator_update_single_row_dispatch;

/// Generate AvgRowAccumulator update single row dispatch logic
#[macro_export]
macro_rules! impl_avg_row_accumulator_update_single_row_dispatch {
    (
        [$array_dt:ident, $array:ident, $row_index:ident, $accessor:ident, $self:ident], $({ $i1t:ident, $i1:ident}),*
    ) => {
        match ($array_dt) {
            $(
                $i1t! { datatype_match_pattern } => {
                        let typed_array = downcast_value!($array, $i1);
                        let value = typed_array.value_at($row_index);
                        $accessor.add_u64($self.state_index, 1);
                        value.add_to_row($self.state_index + 1, $accessor);
                }
            )*
            _ => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {} in AvgRowAccumulator",
                        $array_dt
                    )))
        }
    };
}

pub use impl_avg_row_accumulator_update_single_row_dispatch;

/// The type match pattern of the type macro. e.g., 'DataType::Decimal { .. }'.
#[macro_export]
macro_rules! datatype_match_pattern {
    ($match_pattern:pat) => {
        $match_pattern
    };
}

pub use datatype_match_pattern;

/// The type match pattern of the type 'DataType::Boolean'.
#[macro_export]
macro_rules! Boolean {
    ($macro:ident) => {
        $macro! {
            DataType::Boolean
        }
    };
}

pub use Boolean;

/// The type match pattern of the type 'DataType::Int8'.
#[macro_export]
macro_rules! Int8 {
    ($macro:ident) => {
        $macro! {
            DataType::Int8
        }
    };
}

pub use Int8;

/// The type match pattern of the type 'DataType::Int16'.
#[macro_export]
macro_rules! Int16 {
    ($macro:ident) => {
        $macro! {
            DataType::Int16
        }
    };
}

pub use Int16;

/// The type match pattern of the type 'DataType::Int32'.
#[macro_export]
macro_rules! Int32 {
    ($macro:ident) => {
        $macro! {
            DataType::Int32
        }
    };
}

pub use Int32;

/// The type match pattern of the type 'DataType::Int64'.
#[macro_export]
macro_rules! Int64 {
    ($macro:ident) => {
        $macro! {
            DataType::Int64
        }
    };
}

pub use Int64;

/// The type match pattern of the type 'DataType::UInt8'.
#[macro_export]
macro_rules! UInt8 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt8
        }
    };
}

pub use UInt8;

/// The type match pattern of the type 'DataType::UInt16'.
#[macro_export]
macro_rules! UInt16 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt16
        }
    };
}

pub use UInt16;

/// The type match pattern of the type 'DataType::UInt32'.
#[macro_export]
macro_rules! UInt32 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt32
        }
    };
}

pub use UInt32;

/// The type match pattern of the type 'DataType::UInt64'.
#[macro_export]
macro_rules! UInt64 {
    ($macro:ident) => {
        $macro! {
            DataType::UInt64
        }
    };
}

pub use UInt64;

/// The type match pattern of the type 'DataType::Float32'.
#[macro_export]
macro_rules! Float32 {
    ($macro:ident) => {
        $macro! {
            DataType::Float32
        }
    };
}

pub use Float32;

/// The type match pattern of the type 'DataType::Float64'.
#[macro_export]
macro_rules! Float64 {
    ($macro:ident) => {
        $macro! {
            DataType::Float64
        }
    };
}

pub use Float64;

/// The type match pattern of the type 'DataType::Decimal128'.
#[macro_export]
macro_rules! Decimal128 {
    ($macro:ident) => {
        $macro! {
            DataType::Decimal128 { .. }
        }
    };
}

pub use Decimal128;

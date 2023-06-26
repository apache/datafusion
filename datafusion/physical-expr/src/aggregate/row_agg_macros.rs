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

#[macro_export]
macro_rules! dispatch_all_supported_data_types_pairs {
    ([$macro:ident $(, $x:ident)*], $({ $i1t:ident, $i1:ident}),*) => {
            $macro! {
                [$($x),*],
                $(
                { Boolean, BooleanArray, $i1t, $i1 },
                { Int8, Int8Array, $i1t, $i1 },
                { Int16, Int16Array, $i1t, $i1 },
                { Int32, Int32Array, $i1t, $i1 },
                { Int64, Int64Array, $i1t, $i1 },
                { UInt8, UInt8Array, $i1t, $i1 },
                { UInt16, UInt16Array,$i1t, $i1 },
                { UInt32, UInt32Array, $i1t, $i1 },
                { UInt64, UInt64Array, $i1t, $i1 },
                { Float32, Float32Array, $i1t, $i1 },
                { Float64, Float64Array, $i1t, $i1 },
                { Decimal128, Decimal128Array, $i1t, $i1 },
                )*
            }
    };
}

pub use dispatch_all_supported_data_types_pairs;

#[macro_export]
macro_rules! dispatch_all_sum_supported_data_types_with_default_value {
    ($macro:ident $(, $x:ident)*) => {
        $macro! {
                [$($x),*],
                { Int8, Int8Array, 0 },
                { Int16, Int16Array, 0 },
                { Int32, Int32Array, 0 },
                { Int64, Int64Array, 0 },
                { UInt8, UInt8Array, 0 },
                { UInt16, UInt16Array, 0 },
                { UInt32, UInt32Array, 0 },
                { UInt64, UInt64Array, 0 },
                { Float32, Float32Array, 0.0 },
                { Float64, Float64Array, 0.0 },
                { Decimal128, Decimal128Array, 0 }
        }
    };
}

pub use dispatch_all_sum_supported_data_types_with_default_value;

#[macro_export]
macro_rules! dispatch_all_bit_and_or_xor_supported_data_types {
    ($macro:ident $(, $x:ident)*) => {
        $macro! {
                [$($x),*],
                { Int8, Int8Array },
                { Int16, Int16Array },
                { Int32, Int32Array },
                { Int64, Int64Array },
                { UInt8, UInt8Array },
                { UInt16, UInt16Array },
                { UInt32, UInt32Array },
                { UInt64, UInt64Array }
        }
    };
}

pub use dispatch_all_bit_and_or_xor_supported_data_types;

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
        [$array1_dt:ident, $array2_dt:ident, $array1:ident, $array2:ident, $acc_idx1:ident, $acc_idx2:ident, $self:ident, $update_func:ident, $groups_with_rows:ident, $filter_bool_array:ident], $({ $i1t:ident, $i1:ident, $i2t:ident, $i2:ident}),* $(,)?
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

/// Generate row accumulator update row indices dispatch logic
#[macro_export]
macro_rules! impl_row_accumulator_update_row_idx_dispatch {
    (
        [$array_dt:ident, $array:ident, $selected_row_idx:ident, $update_func:ident, $accessor:ident, $self:ident], $({ $i1t:ident, $i1:ident}),*
    ) => {
        match ($array_dt) {
            $(
                $i1t! { datatype_match_pattern } => {
                        let typed_array = downcast_value!($array, $i1);
                        for row_index in $selected_row_idx {
                            let value = typed_array.value_at(row_index);
                            value.$update_func($self.index, $accessor);
                        }
                }
            )*
            _ => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {} in RowAccumulator",
                        $array_dt
                    )))
        }
    };
}

pub use impl_row_accumulator_update_row_idx_dispatch;

/// Generate AvgRowAccumulator/SumRowAccumulator update row indices dispatch logic
#[macro_export]
macro_rules! impl_sum_row_accumulator_update_row_idx_dispatch {
    (
        [$array_dt:ident, $array:ident, $selected_row_idx:ident, $accessor:ident, $state_index:ident], $({ $i1t:ident, $i1:ident, $i1d:expr}),*
    ) => {
        match ($array_dt) {
            $(
                $i1t! { datatype_match_pattern } => {
                        let typed_array = downcast_value!($array, $i1);
                        let mut delta = $i1d;
                        for row_index in $selected_row_idx {
                            let value = typed_array.value_at(row_index);
                            delta += value;
                        }
                        delta.add_to_row($state_index, $accessor);
                }
            )*
            _ => return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {} in AvgRowAccumulator/SumRowAccumulator",
                        $array_dt
                    )))
        }
    };
}

pub use impl_sum_row_accumulator_update_row_idx_dispatch;

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

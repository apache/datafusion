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

use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::primitive_type;
use crate::reader::ExArrayReader;
use arrow::array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow::array::{ArrayRef, PrimitiveBuilder};
use arrow::datatypes::DataType;
use arrow::datatypes::{
    Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion_common::cast::{
    as_int16_array, as_int32_array, as_int64_array, as_int8_array, as_uint16_array,
    as_uint32_array, as_uint64_array, as_uint8_array,
};
use datafusion_common::Result;
use std::sync::Arc;

primitive_type!(i8, Int8, Int8Array, as_int8_array);
primitive_type!(i16, Int16, Int16Array, as_int16_array);
primitive_type!(i32, Int32, Int32Array, as_int32_array);
primitive_type!(i64, Int64, Int64Array, as_int64_array);

primitive_type!(u8, UInt8, UInt8Array, as_uint8_array);
primitive_type!(u16, UInt16, UInt16Array, as_uint16_array);
primitive_type!(u32, UInt32, UInt32Array, as_uint32_array);
primitive_type!(u64, UInt64, UInt64Array, as_uint64_array);

impl<'a, T> ExArrayReader<'a> for &'a PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    type ValueType = T::Native;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}

macro_rules! primitive_result_type {
    ($native_type:ty, $arrow_primitive_type:ty, $dt_option_name:ident) => {
        impl ExFullResultType for ((), $native_type) {
            type BuilderType = PrimitiveBuilder<$arrow_primitive_type>;

            fn data_type() -> DataType {
                DataType::$dt_option_name
            }

            fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
                Self::BuilderType::with_capacity(number_rows)
            }
        }
    };
}

primitive_result_type!(i8, Int8Type, Int8);
primitive_result_type!(i16, Int16Type, Int16);
primitive_result_type!(i32, Int32Type, Int32);
primitive_result_type!(i64, Int64Type, Int64);

primitive_result_type!(u8, UInt8Type, UInt8);
primitive_result_type!(u16, UInt16Type, UInt16);
primitive_result_type!(u32, UInt32Type, UInt32);
primitive_result_type!(u64, UInt64Type, UInt64);

impl<T> ExArrayBuilder for PrimitiveBuilder<T>
where
    T: ArrowPrimitiveType,
{
    type OutArg = ();
    type Return = T::Native;

    fn get_out_arg(&mut self, _position: usize) -> Self::OutArg {}

    fn append(&mut self, fn_ret: T::Native) -> Result<()> {
        self.append_value(fn_ret);
        Ok(())
    }

    fn append_null(&mut self) -> Result<()> {
        self.append_null();
        Ok(())
    }

    fn build(mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.finish()))
    }
}

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

pub mod bit_count;
pub mod bit_get;
pub mod bit_shift;
pub mod bitwise_not;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(
    bit_shift::SparkBitShift,
    shiftleft,
    bit_shift::SparkBitShift::left
);
make_udf_function!(
    bit_shift::SparkBitShift,
    shiftright,
    bit_shift::SparkBitShift::right
);
make_udf_function!(
    bit_shift::SparkBitShift,
    shiftrightunsigned,
    bit_shift::SparkBitShift::right_unsigned
);
make_udf_function!(bit_get::SparkBitGet, bit_get);
make_udf_function!(bit_count::SparkBitCount, bit_count);
make_udf_function!(bitwise_not::SparkBitwiseNot, bitwise_not);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((bit_get, "Returns the value of the bit (0 or 1) at the specified position.", col pos));
    export_functions!((
        bit_count,
        "Returns the number of bits set in the binary representation of the argument.",
        col
    ));
    export_functions!((
        bitwise_not,
        "Returns the result of a bitwise negation operation on the argument, where each bit in the binary representation is flipped, following two's complement arithmetic for signed integers.",
        col
    ));
    export_functions!((
        shiftleft,
        "Shifts the bits of the first argument left by the number of positions specified by the second argument. If the shift amount is negative or greater than or equal to the bit width, it is normalized to the bit width (i.e., pmod(shift, bit_width)).",
        value shift
    ));
    export_functions!((
        shiftright,
        "Shifts the bits of the first argument right by the number of positions specified by the second argument (arithmetic/signed shift). If the shift amount is negative or greater than or equal to the bit width, it is normalized to the bit width (i.e., pmod(shift, bit_width)).",
        value shift
    ));
    export_functions!((
        shiftrightunsigned,
        "Shifts the bits of the first argument right by the number of positions specified by the second argument (logical/unsigned shift). If the shift amount is negative or greater than or equal to the bit width, it is normalized to the bit width (i.e., pmod(shift, bit_width)).",
        value shift
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        bit_get(),
        bit_count(),
        bitwise_not(),
        shiftleft(),
        shiftright(),
        shiftrightunsigned(),
    ]
}

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

pub mod bit_shift;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(bit_shift::SparkShiftLeft, shiftleft);
make_udf_function!(bit_shift::SparkShiftRight, shiftright);
make_udf_function!(bit_shift::SparkShiftRightUnsigned, shiftrightunsigned);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        shiftleft,
        "Shifts the bits of the first argument left by the number of positions specified by the second argument.",
        value shift
    ));
    export_functions!((
        shiftright,
        "Shifts the bits of the first argument right by the number of positions specified by the second argument (arithmetic shift).",
        value shift
    ));
    export_functions!((
        shiftrightunsigned,
        "Shifts the bits of the first argument right by the number of positions specified by the second argument (logical shift).",
        value shift
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![shiftleft(), shiftright(), shiftrightunsigned()]
}

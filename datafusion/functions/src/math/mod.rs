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

//! "math" DataFusion functions

mod abs;
mod acos;
mod asin;
mod nans;

// create  UDFs
make_udf_function!(nans::IsNanFunc, ISNAN, isnan);
make_udf_function!(abs::AbsFunc, ABS, abs);
make_udf_function!(acos::AcosFunc, ACOS, acos);
make_udf_function!(asin::AsinFunc, ASIN, asin);

// Export the functions out of this package, both as expr_fn as well as a list of functions
export_functions!(
    (
        isnan,
        num,
        "returns true if a given number is +NaN or -NaN otherwise returns false"
    ),
    (abs, num, "returns the absolute value of a given number"),
    (
        acos,
        num,
        "returns the arc cosine or inverse cosine of a number"
    ),
    (
        asin,
        num,
        "returns the arc sine or inverse sine of a number"
    )
);

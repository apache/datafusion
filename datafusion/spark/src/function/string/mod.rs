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

pub mod ascii;
pub mod char;
pub mod concat;
pub mod elt;
pub mod format_string;
pub mod ilike;
pub mod length;
pub mod like;
pub mod luhn_check;
pub mod space;
pub mod substring;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(ascii::SparkAscii, ascii);
make_udf_function!(char::CharFunc, char);
make_udf_function!(concat::SparkConcat, concat);
make_udf_function!(ilike::SparkILike, ilike);
make_udf_function!(length::SparkLengthFunc, length);
make_udf_function!(elt::SparkElt, elt);
make_udf_function!(like::SparkLike, like);
make_udf_function!(luhn_check::SparkLuhnCheck, luhn_check);
make_udf_function!(format_string::FormatStringFunc, format_string);
make_udf_function!(space::SparkSpace, space);
make_udf_function!(substring::SparkSubstring, substring);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        ascii,
        "Returns the ASCII code point of the first character of string.",
        arg1
    ));
    export_functions!((
        char,
        "Returns the ASCII character having the binary equivalent to col. If col is larger than 256 the result is equivalent to char(col % 256).",
        arg1
    ));
    export_functions!((
        concat,
        "Concatenates multiple input strings into a single string. Returns NULL if any input is NULL.",
        args
    ));
    export_functions!((
        elt,
        "Returns the n-th input (1-indexed), e.g. returns 2nd input when n is 2. The function returns NULL if the index is 0 or exceeds the length of the array.",
        select_col arg1 arg2 argn
    ));
    export_functions!((
        ilike,
        "Returns true if str matches pattern (case insensitive).",
        str pattern
    ));
    export_functions!((
        length,
        "Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.",
        arg1
    ));
    export_functions!((
        like,
        "Returns true if str matches pattern (case sensitive).",
        str pattern
    ));
    export_functions!((
        luhn_check,
        "Returns whether the input string of digits is valid according to the Luhn algorithm.",
        arg1
    ));
    export_functions!((
        format_string,
        "Returns a formatted string from printf-style format strings.",
        strfmt args
    ));
    export_functions!((space, "Returns a string consisting of n spaces.", arg1));
    export_functions!((
        substring,
        "Returns the substring from string `str` starting at position `pos` with length `length.",
        str pos length
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        ascii(),
        char(),
        concat(),
        elt(),
        ilike(),
        length(),
        like(),
        luhn_check(),
        format_string(),
        space(),
        substring(),
    ]
}

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

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

pub mod parse_url;
pub mod try_parse_url;
pub mod try_url_decode;
pub mod url_decode;
pub mod url_encode;

make_udf_function!(parse_url::ParseUrl, parse_url);
make_udf_function!(try_parse_url::TryParseUrl, try_parse_url);
make_udf_function!(try_url_decode::TryUrlDecode, try_url_decode);
make_udf_function!(url_decode::UrlDecode, url_decode);
make_udf_function!(url_encode::UrlEncode, url_encode);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        parse_url,
        "Extracts a part from a URL, throwing an error if an invalid URL is provided.",
        args
    ));
    export_functions!((
        try_parse_url,
        "Same as parse_url but returns NULL if an invalid URL is provided.",
        args
    ));
    export_functions!((
        url_decode,
        "Decodes a URL-encoded string in ‘application/x-www-form-urlencoded’ format to its original format.",
        args
    ));
    export_functions!((
        try_url_decode,
        "Same as url_decode but returns NULL if an invalid URL-encoded string is provided",
        args
    ));
    export_functions!((
        url_encode,
        "Encodes a string into a URL-encoded string in ‘application/x-www-form-urlencoded’ format.",
        args
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        parse_url(),
        try_parse_url(),
        try_url_decode(),
        url_decode(),
        url_encode(),
    ]
}

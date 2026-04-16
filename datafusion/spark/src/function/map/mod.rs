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

pub mod map_from_arrays;
pub mod map_from_entries;
pub mod str_to_map;
mod utils;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(map_from_arrays::MapFromArrays, map_from_arrays);
make_udf_function!(map_from_entries::MapFromEntries, map_from_entries);
make_udf_function!(str_to_map::SparkStrToMap, str_to_map);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        map_from_arrays,
        "Creates a map from arrays of keys and values.",
        keys values
    ));

    export_functions!((
        map_from_entries,
        "Creates a map from array<struct<key, value>>.",
        arg1
    ));

    export_functions!((
        str_to_map,
        "Creates a map after splitting the text into key/value pairs using delimiters.",
        text pair_delim key_value_delim
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![map_from_arrays(), map_from_entries(), str_to_map()]
}

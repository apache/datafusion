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

//! "regx" DataFusion functions

pub mod regexplike;
pub mod regexpmatch;

// create UDFs
make_udf_function!(regexpmatch::RegexpMatchFunc, REGEXP_MATCH, regexp_match);
make_udf_function!(regexplike::RegexpLikeFunc, REGEXP_LIKE, regexp_like);
export_functions!((
    regexp_match,
    input_arg1 input_arg2,
    "returns a list of regular expression matches in a string. "
),(
    regexp_like,
    input_arg1 input_arg2,
    "Returns true if a has at least one match in a string,false otherwise."
));

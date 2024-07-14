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

use arrow_udf::function;

#[function("eq(bool, bool) -> bool", output="eval_eq_boolean")]
#[function("eq(string, string) -> bool", output="eval_eq_string")]
#[function("eq(binary, binary) -> bool", output="eval_eq_binary")]
#[function("eq(largestring, largestring) -> bool", output="eval_eq_largestring")]
#[function("eq(largebinary, largebinary) -> bool", output="eval_eq_largebinary")]
fn eq<T: std::cmp::Eq>(_lhs: T, _rhs: T) -> bool {
    _lhs == _rhs
}

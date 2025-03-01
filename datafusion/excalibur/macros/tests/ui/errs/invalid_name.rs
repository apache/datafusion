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

use datafusion_excalibur_macros::excalibur_function;

#[excalibur_function(name = unquoted_name)]
fn add_one(a: u64) -> u64 {
    a + 1
}

// not a string
#[excalibur_function(name = 123)]
fn add_two(a: u64) -> u64 {
    a + 2
}

// expected by trybuild
fn main() {}
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

use crate::case_conversion_inputs;
use criterion::{Criterion, criterion_group};
use datafusion_functions::string;

fn criterion_benchmark(c: &mut Criterion) {
    let lower = string::lower();
    for (name, array) in case_conversion_inputs::inputs() {
        case_conversion_inputs::bench_array(c, &lower, &format!("lower_{name}"), &array);
    }
}

criterion_group!(benches, criterion_benchmark);

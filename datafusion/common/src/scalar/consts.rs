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

// Constants defined for scalar construction.

// PI ~ 3.1415927 in f32
#[allow(clippy::approx_constant)]
pub(super) const PI_UPPER_F32: f32 = 3.141593_f32;

// PI ~ 3.141592653589793 in f64
pub(super) const PI_UPPER_F64: f64 = 3.141592653589794_f64;

// -PI ~ -3.1415927 in f32
#[allow(clippy::approx_constant)]
pub(super) const NEGATIVE_PI_LOWER_F32: f32 = -3.141593_f32;

// -PI ~ -3.141592653589793 in f64
pub(super) const NEGATIVE_PI_LOWER_F64: f64 = -3.141592653589794_f64;

// PI / 2 ~ 1.5707964 in f32
pub(super) const FRAC_PI_2_UPPER_F32: f32 = 1.5707965_f32;

// PI / 2 ~ 1.5707963267948966 in f64
pub(super) const FRAC_PI_2_UPPER_F64: f64 = 1.5707963267948967_f64;

// -PI / 2 ~ -1.5707964 in f32
pub(super) const NEGATIVE_FRAC_PI_2_LOWER_F32: f32 = -1.5707965_f32;

// -PI / 2 ~ -1.5707963267948966 in f64
pub(super) const NEGATIVE_FRAC_PI_2_LOWER_F64: f64 = -1.5707963267948967_f64;

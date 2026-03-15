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

// Next F16 value above π (upper bound)
pub(super) const PI_UPPER_F16: half::f16 = half::f16::from_bits(0x4249);

// Next f32 value above π (upper bound)
pub(super) const PI_UPPER_F32: f32 = std::f32::consts::PI.next_up();

// Next f64 value above π (upper bound)
pub(super) const PI_UPPER_F64: f64 = std::f64::consts::PI.next_up();

// Next f16 value below -π (lower bound)
pub(super) const NEGATIVE_PI_LOWER_F16: half::f16 = half::f16::from_bits(0xC249);

// Next f32 value below -π (lower bound)
pub(super) const NEGATIVE_PI_LOWER_F32: f32 = (-std::f32::consts::PI).next_down();

// Next f64 value below -π (lower bound)
pub(super) const NEGATIVE_PI_LOWER_F64: f64 = (-std::f64::consts::PI).next_down();

// Next f16 value above π/2 (upper bound)
pub(super) const FRAC_PI_2_UPPER_F16: half::f16 = half::f16::from_bits(0x3E49);

// Next f32 value above π/2 (upper bound)
pub(super) const FRAC_PI_2_UPPER_F32: f32 = std::f32::consts::FRAC_PI_2.next_up();

// Next f64 value above π/2 (upper bound)
pub(super) const FRAC_PI_2_UPPER_F64: f64 = std::f64::consts::FRAC_PI_2.next_up();

// Next f32 value below -π/2 (lower bound)
pub(super) const NEGATIVE_FRAC_PI_2_LOWER_F16: half::f16 = half::f16::from_bits(0xBE49);

// Next f32 value below -π/2 (lower bound)
pub(super) const NEGATIVE_FRAC_PI_2_LOWER_F32: f32 =
    (-std::f32::consts::FRAC_PI_2).next_down();

// Next f64 value below -π/2 (lower bound)
pub(super) const NEGATIVE_FRAC_PI_2_LOWER_F64: f64 =
    (-std::f64::consts::FRAC_PI_2).next_down();

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

/// Represents the behavior for null values when evaluating equality.
///
/// # Order
///
/// The order on this type represents the "restrictiveness" of the behavior. The more restrictive
/// a behavior is, the fewer elements are considered to be equal to null.
/// [NullEquality::NullEqualsNothing] represents the most restrictive behavior.
///
/// This mirrors the old order with `null_equals_null` booleans, as `false` indicated that
/// `null != null`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum NullEquality {
    /// Null is *not* equal to anything (`null != null`)
    NullEqualsNothing,
    /// Null is equal to null (`null == null`)
    NullEqualsNull,
}

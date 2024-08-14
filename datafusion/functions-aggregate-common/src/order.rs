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

/// Represents the sensitivity of an aggregate expression to ordering.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AggregateOrderSensitivity {
    /// Indicates that the aggregate expression is insensitive to ordering.
    /// Ordering at the input is not important for the result of the aggregator.
    Insensitive,
    /// Indicates that the aggregate expression has a hard requirement on ordering.
    /// The aggregator can not produce a correct result unless its ordering
    /// requirement is satisfied.
    HardRequirement,
    /// Indicates that ordering is beneficial for the aggregate expression in terms
    /// of evaluation efficiency. The aggregator can produce its result efficiently
    /// when its required ordering is satisfied; however, it can still produce the
    /// correct result (albeit less efficiently) when its required ordering is not met.
    Beneficial,
}

impl AggregateOrderSensitivity {
    pub fn is_insensitive(&self) -> bool {
        self.eq(&AggregateOrderSensitivity::Insensitive)
    }

    pub fn is_beneficial(&self) -> bool {
        self.eq(&AggregateOrderSensitivity::Beneficial)
    }

    pub fn hard_requires(&self) -> bool {
        self.eq(&AggregateOrderSensitivity::HardRequirement)
    }
}

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Represents the sensitivity of an aggregate expression to ordering.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AggregateOrderSensitivity {
    /// Indicates that the aggregate expression is insensitive to ordering.
    /// Ordering at the input is not important for the result of the aggregator.
    Insensitive,
    /// Indicates that the aggregate expression has a hard requirement on ordering.
    /// The aggregator cannot produce a correct result unless its ordering
    /// requirement is satisfied.
    HardRequirement,
    /// Indicates that the aggregator is more efficient when the input is ordered
    /// but can still produce its result correctly regardless of the input ordering.
    /// This is similar to, but stronger than, [`Self::Beneficial`].
    ///
    /// Similarly to [`Self::HardRequirement`], when possible DataFusion will insert
    /// a `SortExec`, to reorder the input to match the SoftRequirement. However,
    /// when such a `SortExec` cannot be inserted, (for example, due to conflicting
    /// [`Self::HardRequirement`] with other ordered aggregates in the query),
    /// the aggregate function will still execute, without the preferred order, unlike
    /// with [`Self::HardRequirement`]
    SoftRequirement,
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
        matches!(self, Self::SoftRequirement | Self::Beneficial)
    }

    pub fn hard_requires(&self) -> bool {
        self.eq(&AggregateOrderSensitivity::HardRequirement)
    }
}

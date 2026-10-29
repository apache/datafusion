// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Represents the behavior for null values when evaluating equality. Currently, its primary use
/// case is to define the behavior of joins for null values.
///
/// # Examples
///
/// The following table shows the expected equality behavior for `NullEquality`.
///
/// | A    | B    | NullEqualsNothing | NullEqualsNull |
/// |------|------|-------------------|----------------|
/// | NULL | NULL | false             | true           |
/// | NULL | 'b'  | false             | false          |
/// | 'a'  | NULL | false             | false          |
/// | 'a'  | 'b'  | false             | false          |
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

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

use std::fmt;

/// Operators applied to expressions
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
    /// Matches a wildcard pattern
    Like,
    /// Does not match a wildcard pattern
    NotLike,
    /// IS DISTINCT FROM
    IsDistinctFrom,
    /// IS NOT DISTINCT FROM
    IsNotDistinctFrom,
    /// Case sensitive regex match
    RegexMatch,
    /// Case insensitive regex match
    RegexIMatch,
    /// Case sensitive regex not match
    RegexNotMatch,
    /// Case insensitive regex not match
    RegexNotIMatch,
    /// Bitwise and, like `&`
    BitwiseAnd,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Like => "LIKE",
            Operator::NotLike => "NOT LIKE",
            Operator::RegexMatch => "~",
            Operator::RegexIMatch => "~*",
            Operator::RegexNotMatch => "!~",
            Operator::RegexNotIMatch => "!~*",
            Operator::IsDistinctFrom => "IS DISTINCT FROM",
            Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
            Operator::BitwiseAnd => "&",
        };
        write!(f, "{}", display)
    }
}

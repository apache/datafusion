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

use std::fmt::Debug;

use datafusion_common::Result;
use sqlparser::ast::BinaryOperator;

use crate::Operator;

pub trait ParseCustomOperator: Debug + Send + Sync {
    /// Return a human-readable name for this parser
    fn name(&self) -> &str;

    /// potentially parse a custom operator.
    ///
    /// Return `None` if the operator is not recognized
    fn op_from_ast(&self, op: &BinaryOperator) -> Result<Option<Operator>>;

    fn op_from_name(&self, raw_op: &str) -> Result<Option<Operator>>;
}

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

use crate::planner::{ContextProvider, SqlToRel};
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_expr::Operator;
use sqlparser::ast::JsonOperator;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn parse_sql_json_access(&self, op: JsonOperator) -> Result<Operator> {
        match op {
            JsonOperator::AtArrow => Ok(Operator::AtArrow),
            JsonOperator::ArrowAt => Ok(Operator::ArrowAt),
            _ => not_impl_err!("Unsupported SQL json operator {op:?}"),
        }
    }
}

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

use std::sync::Arc;

use datafusion_expr::{expr::AggregateFunction, Expr};
use sqlparser::ast::NullTreatment;

pub struct ExprBuilder {
    udf: Arc<crate::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    distinct: bool,
    /// Optional filter
    filter: Option<Box<Expr>>,
    /// Optional ordering
    order_by: Option<Vec<Expr>>,
    null_treatment: Option<NullTreatment>,
}

impl ExprBuilder {
    pub fn new(udf: Arc<crate::AggregateUDF>, args: Vec<Expr>) -> Self {
        Self {
            udf,
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        }
    }

    pub fn new_distinct(udf: Arc<crate::AggregateUDF>, args: Vec<Expr>) -> Self {
        Self {
            udf,
            args,
            distinct: true,
            filter: None,
            order_by: None,
            null_treatment: None,
        }
    }
}

impl ExprBuilder {
    pub fn build(self) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            self.udf,
            self.args,
            self.distinct,
            self.filter,
            self.order_by,
            self.null_treatment,
        ))
    }

    pub fn order_by(mut self, order_by: Vec<Expr>) -> Self {
        self.order_by = Some(order_by);
        self
    }

    pub fn filter(mut self, filter: Box<Expr>) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn null_treatment(mut self, null_treatment: NullTreatment) -> Self {
        self.null_treatment = Some(null_treatment);
        self
    }
}

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

use datafusion_common::arrow::datatypes::DataType;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

pub struct PartitionEvaluatorArgs<'a> {
    input_exprs: &'a [Arc<dyn PhysicalExpr>],
    input_types: &'a [DataType],
    is_reversed: bool,
    ignore_nulls: bool,
}

impl<'a> PartitionEvaluatorArgs<'a> {
    pub fn new(
        input_exprs: &'a [Arc<dyn PhysicalExpr>],
        input_types: &'a [DataType],
        is_reversed: bool,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            input_exprs,
            input_types,
            is_reversed,
            ignore_nulls,
        }
    }

    pub fn input_expr_at(&self, index: usize) -> Option<&Arc<dyn PhysicalExpr>> {
        self.input_exprs.get(index)
    }
}

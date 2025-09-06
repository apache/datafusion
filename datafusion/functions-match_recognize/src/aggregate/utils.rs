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

use datafusion_common::Result;
use datafusion_expr::match_recognize::columns::classifier_bits_col_name;
use datafusion_expr::match_recognize::find_symbol_predicate;
use datafusion_expr::{col, Expr};

/// Returns an expression for the boolean classifier bits column for the
/// first referenced symbol in `expr`, if any. Otherwise returns None.
pub fn bits_col_from_expr(expr: &Expr) -> Result<Option<Expr>> {
    let symbol = find_symbol_predicate(expr)?;
    if let Some(symbol) = symbol {
        let mask_col = col(classifier_bits_col_name(&symbol));
        return Ok(Some(mask_col));
    }
    Ok(None)
}

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

use datafusion::common::{plan_err, substrait_err, ScalarValue};
use datafusion::logical_expr::WindowFrameBound;
use substrait::proto::expression::{
    window_function::bound as SubstraitBound, window_function::bound::Kind as BoundKind,
    window_function::Bound,
};

pub(super) fn from_substrait_bound(
    bound: &Option<Bound>,
    is_lower: bool,
) -> datafusion::common::Result<WindowFrameBound> {
    match bound {
        Some(b) => match &b.kind {
            Some(k) => match k {
                BoundKind::CurrentRow(SubstraitBound::CurrentRow {}) => {
                    Ok(WindowFrameBound::CurrentRow)
                }
                BoundKind::Preceding(SubstraitBound::Preceding { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Preceding bound must be positive");
                    }
                    Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Following(SubstraitBound::Following { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Following bound must be positive");
                    }
                    Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Unbounded(SubstraitBound::Unbounded {}) => {
                    if is_lower {
                        Ok(WindowFrameBound::Preceding(ScalarValue::Null))
                    } else {
                        Ok(WindowFrameBound::Following(ScalarValue::Null))
                    }
                }
            },
            None => substrait_err!("WindowFunction missing Substrait Bound kind"),
        },
        None => {
            if is_lower {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            } else {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
        }
    }
}

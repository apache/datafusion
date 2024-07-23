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

//! Defines physical expressions that can evaluated at runtime during query execution

#[macro_use]
mod binary;
mod case;
mod in_list;
mod is_not_null;
mod is_null;
mod like;
mod negative;
mod no_op;
mod not;
mod try_cast;
mod unknown_column;

/// Module with some convenient methods used in expression building
pub mod helpers {
    pub use crate::aggregate::min_max::{max, min};
}
pub use crate::aggregate::array_agg_ordered::OrderSensitiveArrayAgg;
pub use crate::aggregate::build_in::create_aggregate_expr;
pub use crate::aggregate::min_max::{Max, MaxAccumulator, Min, MinAccumulator};
pub use crate::aggregate::stats::StatsType;
pub use crate::window::cume_dist::{cume_dist, CumeDist};
pub use crate::window::lead_lag::{lag, lead, WindowShift};
pub use crate::window::nth_value::NthValue;
pub use crate::window::ntile::Ntile;
pub use crate::window::rank::{dense_rank, percent_rank, rank, Rank, RankType};
pub use crate::window::row_number::RowNumber;
pub use crate::PhysicalSortExpr;

pub use binary::{binary, BinaryExpr};
pub use case::{case, CaseExpr};
pub use datafusion_expr::utils::format_state_name;
pub use datafusion_physical_expr_common::expressions::column::{col, Column};
pub use datafusion_physical_expr_common::expressions::literal::{lit, Literal};
pub use datafusion_physical_expr_common::expressions::{cast, CastExpr};
pub use in_list::{in_list, InListExpr};
pub use is_not_null::{is_not_null, IsNotNullExpr};
pub use is_null::{is_null, IsNullExpr};
pub use like::{like, LikeExpr};
pub use negative::{negative, NegativeExpr};
pub use no_op::NoOp;
pub use not::{not, NotExpr};
pub use try_cast::{try_cast, TryCastExpr};
pub use unknown_column::UnKnownColumn;

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
mod cast;
mod cast_column;
mod column;
mod dynamic_filters;
mod in_list;
mod is_not_null;
mod is_null;
mod like;
mod literal;
mod negative;
mod no_op;
mod not;
mod try_cast;
mod unknown_column;

pub use crate::PhysicalSortExpr;
/// Module with some convenient methods used in expression building
pub use crate::aggregate::stats::StatsType;

pub use binary::{BinaryExpr, binary, similar_to};
pub use case::{CaseExpr, case};
pub use cast::{CastExpr, cast};
pub use cast_column::CastColumnExpr;
pub use column::{Column, col, with_new_schema};
pub use datafusion_expr::utils::format_state_name;
pub use dynamic_filters::{DynamicFilterPhysicalExpr, PlannedDynamicFilterPhysicalExpr};
pub use in_list::{InListExpr, in_list};
pub use is_not_null::{IsNotNullExpr, is_not_null};
pub use is_null::{IsNullExpr, is_null};
pub use like::{LikeExpr, like};
pub use literal::{Literal, lit};
pub use negative::{NegativeExpr, negative};
pub use no_op::NoOp;
pub use not::{NotExpr, not};
pub use try_cast::{TryCastExpr, try_cast};
pub use unknown_column::UnKnownColumn;

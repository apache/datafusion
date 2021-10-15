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

//! This module contains end to end tests of running SQL queries using
//! DataFusion

use std::convert::TryFrom;
use std::sync::Arc;

use chrono::prelude::*;
use chrono::Duration;

extern crate arrow;
extern crate datafusion;

use arrow::{
    array::*, datatypes::*, record_batch::RecordBatch,
    util::display::array_value_to_string,
};

use datafusion::assert_batches_eq;
use datafusion::assert_batches_sorted_eq;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::functions::Volatility;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanVisitor;
use datafusion::prelude::*;
use datafusion::{datasource::MemTable, physical_plan::collect};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ColumnarValue,
};
use datafusion::{execution::context::ExecutionContext, physical_plan::displayable};

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

use async_trait::async_trait;
use datafusion_common::config::{ConfigOptions, TableOptions};
use datafusion_common::{DFSchema, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

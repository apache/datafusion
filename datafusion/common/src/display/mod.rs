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

//! Types for plan display

mod graphviz;
mod tree;
pub use graphviz::*;
pub use tree::*;

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

/// Represents which type of plan, when storing multiple
/// for use in EXPLAIN plans
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum PlanType {
    /// The initial LogicalPlan provided to DataFusion
    InitialLogicalPlan,
    /// The LogicalPlan which results from applying an analyzer pass
    AnalyzedLogicalPlan {
        /// The name of the analyzer which produced this plan
        analyzer_name: String,
    },
    /// The LogicalPlan after all analyzer passes have been applied
    FinalAnalyzedLogicalPlan,
    /// The LogicalPlan which results from applying an optimizer pass
    OptimizedLogicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The final, fully optimized LogicalPlan that was converted to a physical plan
    FinalLogicalPlan,
    /// The initial physical plan, prepared for execution
    InitialPhysicalPlan,
    /// The initial physical plan with stats, prepared for execution
    InitialPhysicalPlanWithStats,
    /// The initial physical plan with schema, prepared for execution
    InitialPhysicalPlanWithSchema,
    /// The ExecutionPlan which results from applying an optimizer pass
    OptimizedPhysicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The final, fully optimized physical plan which would be executed
    FinalPhysicalPlan,
    /// The final with stats, fully optimized physical plan which would be executed
    FinalPhysicalPlanWithStats,
    /// The final with schema, fully optimized physical plan which would be executed
    FinalPhysicalPlanWithSchema,
    /// An error creating the physical plan
    PhysicalPlanError,
}

impl Display for PlanType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PlanType::InitialLogicalPlan => write!(f, "initial_logical_plan"),
            PlanType::AnalyzedLogicalPlan { analyzer_name } => {
                write!(f, "logical_plan after {analyzer_name}")
            }
            PlanType::FinalAnalyzedLogicalPlan => write!(f, "analyzed_logical_plan"),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                write!(f, "logical_plan after {optimizer_name}")
            }
            PlanType::FinalLogicalPlan => write!(f, "logical_plan"),
            PlanType::InitialPhysicalPlan => write!(f, "initial_physical_plan"),
            PlanType::InitialPhysicalPlanWithStats => {
                write!(f, "initial_physical_plan_with_stats")
            }
            PlanType::InitialPhysicalPlanWithSchema => {
                write!(f, "initial_physical_plan_with_schema")
            }
            PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                write!(f, "physical_plan after {optimizer_name}")
            }
            PlanType::FinalPhysicalPlan => write!(f, "physical_plan"),
            PlanType::FinalPhysicalPlanWithStats => write!(f, "physical_plan_with_stats"),
            PlanType::FinalPhysicalPlanWithSchema => {
                write!(f, "physical_plan_with_schema")
            }
            PlanType::PhysicalPlanError => write!(f, "physical_plan_error"),
        }
    }
}

/// Represents some sort of execution plan, in String form
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct StringifiedPlan {
    /// An identifier of what type of plan this string represents
    pub plan_type: PlanType,
    /// The string representation of the plan
    pub plan: Arc<String>,
}

impl StringifiedPlan {
    /// Create a new Stringified plan of `plan_type` with string
    /// representation `plan`
    pub fn new(plan_type: PlanType, plan: impl Into<String>) -> Self {
        StringifiedPlan {
            plan_type,
            plan: Arc::new(plan.into()),
        }
    }

    /// Returns true if this plan should be displayed. Generally
    /// `verbose_mode = true` will display all available plans
    pub fn should_display(&self, verbose_mode: bool) -> bool {
        match self.plan_type {
            PlanType::FinalLogicalPlan
            | PlanType::FinalPhysicalPlan
            | PlanType::PhysicalPlanError => true,
            _ => verbose_mode,
        }
    }
}

/// Trait for something that can be formatted as a stringified plan
pub trait ToStringifiedPlan {
    /// Create a stringified plan with the specified type
    fn to_stringified(&self, plan_type: PlanType) -> StringifiedPlan;
}

pub trait DisplayAs {
    /// Format according to `DisplayFormatType`, used when verbose representation looks
    /// different from the default one
    ///
    /// Should not include a newline
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;
}

pub enum DisplayFormatType {
    /// Default, compact format. Example: `FilterExec: c12 < 10.0`
    ///
    /// This format is designed to provide a detailed textual description
    /// of all parts of the plan.
    Default,
    /// Verbose, showing all available details.
    ///
    /// This form is even more detailed than [`Self::Default`]
    Verbose,
    /// TreeRender, displayed in the `tree` explain type.
    ///
    /// This format is inspired by DuckDB's explain plans. The information
    /// presented should be "user friendly", and contain only the most relevant
    /// information for understanding a plan. It should NOT contain the same level
    /// of detail information as the  [`Self::Default`] format.
    ///
    /// In this mode, each line has one of two formats:
    ///
    /// 1. A string without a `=`, which is printed in its own line
    ///
    /// 2. A string with a `=` that is treated as a `key=value pair`. Everything
    ///    before the first `=` is treated as the key, and everything after the
    ///    first `=` is treated as the value.
    ///
    /// For example, if the output of `TreeRender` is this:
    /// ```text
    /// Parquet
    /// partition_sizes=[1]
    /// ```
    ///
    /// It is rendered in the center of a box in the following way:
    ///
    /// ```text
    /// ┌───────────────────────────┐
    /// │       DataSourceExec      │
    /// │    --------------------   │
    /// │    partition_sizes: [1]   │
    /// │          Parquet          │
    /// └───────────────────────────┘
    ///  ```
    TreeRender,
}

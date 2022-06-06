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

//! This is a legacy module that only contains re-exports of other modules

pub use crate::datasource::{provider_as_source, source_as_provider, DefaultTableSource};
pub use datafusion_expr::{
    logical_plan::{
        display::{GraphvizVisitor, IndentVisitor},
        Aggregate, Analyze, CreateCatalog, CreateCatalogSchema, CreateExternalTable,
        CreateMemoryTable, CreateView, CrossJoin, DropTable, EmptyRelation, Explain,
        Extension, FileType, Filter, Join, JoinConstraint, JoinType, Limit, LogicalPlan,
        Offset, Partitioning, PlanType, PlanVisitor, Projection, Repartition, Sort,
        StringifiedPlan, Subquery, SubqueryAlias, TableScan, ToStringifiedPlan, Union,
        UserDefinedLogicalNode, Values, Window,
    },
    TableProviderFilterPushDown, TableSource,
};

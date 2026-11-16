// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

pub mod builder;
mod ddl;
pub mod display;
pub mod dml;
mod extension;
pub(crate) mod invariants;
pub mod materialized_cte;
pub use invariants::{InvariantLevel, assert_expected_schema, check_subquery_expr};
pub mod information_schema;
mod plan;
mod statement;
pub mod tree_node;

pub use builder::{
    LogicalPlanBuilder, LogicalPlanBuilderOptions, LogicalTableSource, UNNAMED_TABLE,
    build_asof_join_schema, build_join_schema, requalify_sides_if_needed, table_scan,
    union, wrap_projection_for_join_if_necessary,
};
pub use ddl::{
    CreateCatalog, CreateCatalogSchema, CreateExternalCatalog, CreateExternalTable,
    CreateFunction, CreateFunctionBody, CreateIndex, CreateMemoryTable, CreateView,
    DdlStatement, DropCatalog, DropCatalogSchema, DropFunction, DropTable, DropView,
    OperateFunctionArg,
};
pub use dml::{
    DmlStatement, MergeIntoAction, MergeIntoClause, MergeIntoClauseKind, MergeIntoOp,
    WriteOp,
};
pub use plan::{
    Aggregate, Analyze, AsOfJoin, AsOfMatch, ColumnUnnestList, Distinct, DistinctOn,
    EmptyRelation, Explain, ExplainOption, Extension, FetchType, Filter, Join,
    JoinConstraint, JoinType, Limit, LogicalPlan, Partitioning, PlanType, Projection,
    RangePartitioning, RecursiveQuery, Repartition, Show, ShowKind, SkipType, Sort,
    StringifiedPlan, Subquery, SubqueryAlias, TableScan, TableScanBuilder,
    ToStringifiedPlan, Union, Unnest, Values, Window, projection_schema,
};
pub use statement::{
    Deallocate, Execute, Prepare, ResetVariable, SetVariable, Statement,
    TransactionAccessMode, TransactionConclusion, TransactionEnd,
    TransactionIsolationLevel, TransactionStart,
};

pub use datafusion_common::format::ExplainFormat;

pub use display::display_schema;

pub use extension::{UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
pub use materialized_cte::{
    MaterializedCte, MaterializedCteId, MaterializedCtePlan, MaterializedCteScan,
};

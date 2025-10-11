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

//! This example demonstrates using a custom relation planner to expose system
//! information and metrics as queryable tables using the `sysinfo` crate.
//!
//! Usage examples:
//!   - `SELECT * FROM system_info()` returns all system metrics
//!   - `SELECT * FROM system_info('cpu')` returns CPU information
//!   - `SELECT * FROM system_info('memory')` returns memory information
//!   - `SELECT * FROM system_info('system')` returns system metadata

use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    logical_plan::{builder::LogicalPlanBuilder, LogicalPlan},
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
    Expr,
};
use datafusion_sql::sqlparser::ast::{
    self, FunctionArg, FunctionArgExpr, ObjectNamePart, TableFactor,
};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register our custom relation planner
    ctx.register_relation_planner(Arc::new(SystemInfoPlanner))?;

    println!("Custom Relation Planner: System Information Example");
    println!("====================================================\n");

    // Example 1: Get all system information (CPU, memory, and system metadata)
    // Shows: Complete system metrics including CPU usage per core, memory stats, and system info
    // Expected: ~30 rows with CPU cores, memory usage, system name, hostname, uptime, load averages
    // Actual (truncated):
    // +----------+----------------------+----------------------------+-----------------+
    // | category | metric               | description                | value           |
    // +----------+----------------------+----------------------------+-----------------+
    // | cpu      | cpu_0                | 1                          | 59.41           |
    // | cpu      | cpu_1                | 2                          | 58.75           |
    // | ...      | ...                  | ...                        | ...             |
    // | system   | load_avg_15          | Load Average (15 min)      | 8.24            |
    // +----------+----------------------+----------------------------+-----------------+
    run_example(
        &ctx,
        "Example 1: Get all system information",
        "SELECT * FROM system_info()",
    )
    .await?;

    // Example 2: Get only CPU-related information
    // Shows: How to filter system info by category using function parameter
    // Expected: ~13 rows with individual CPU core usage, global CPU usage, and core counts
    // Actual (truncated):
    // +----------+----------------+---------------------+-------+
    // | category | metric         | description         | value |
    // +----------+----------------+---------------------+-------+
    // | cpu      | cpu_0          | 1                   | 59.41 |
    // | ...      | ...            | ...                 | ...   |
    // | cpu      | physical_cores | Physical Core Count | 10    |
    // +----------+----------------+---------------------+-------+
    run_example(
        &ctx,
        "Example 2: Get CPU information only",
        "SELECT * FROM system_info('cpu')",
    )
    .await?;

    // Example 3: Get only memory-related information
    // Shows: Memory statistics including total, used, free, available memory and swap
    // Expected: 8 rows with memory metrics in bytes and percentage usage
    // Actual:
    // +----------+----------------------+--------------------------+-------------+
    // | category | metric               | description              | value       |
    // +----------+----------------------+--------------------------+-------------+
    // | memory   | total_memory         | Total Memory (bytes)     | 68719476736 |
    // | memory   | used_memory          | Used Memory (bytes)      | 37513265152 |
    // | memory   | free_memory          | Free Memory (bytes)      | 1064894464  |
    // | memory   | available_memory     | Available Memory (bytes) | 26507608064 |
    // | memory   | total_swap           | Total Swap (bytes)       | 0           |
    // | memory   | used_swap            | Used Swap (bytes)        | 0           |
    // | memory   | free_swap            | Free Swap (bytes)        | 0           |
    // | memory   | memory_usage_percent | Memory Usage (%)         | 54.59       |
    // +----------+----------------------+--------------------------+-------------+
    run_example(
        &ctx,
        "Example 3: Get memory information only",
        "SELECT * FROM system_info('memory')",
    )
    .await?;

    // Example 4: Get only system metadata (OS info, hostname, uptime, load averages)
    // Shows: System identification and performance metadata
    // Expected: ~9 rows with system name, kernel version, hostname, uptime, load averages
    // Actual:
    // +----------+----------------+----------------------------+-----------------+
    // | category | metric         | description                | value           |
    // +----------+----------------+----------------------------+-----------------+
    // | system   | name           | System Name                | Darwin          |
    // | system   | kernel_version | Kernel Version             | 24.6.0          |
    // | system   | os_version     | OS Version                 | 15.7            |
    // | system   | hostname       | Hostname                   | COMP-EOSL454PLQ |
    // | system   | uptime         | Uptime (seconds)           | 876118          |
    // | system   | boot_time      | Boot Time (unix timestamp) | 1758434719      |
    // | system   | load_avg_1     | Load Average (1 min)       | 6.93            |
    // | system   | load_avg_5     | Load Average (5 min)       | 7.69            |
    // | system   | load_avg_15    | Load Average (15 min)      | 8.24            |
    // +----------+----------------+----------------------------+-----------------+
    run_example(
        &ctx,
        "Example 4: Get system metadata only",
        "SELECT * FROM system_info('system')",
    )
    .await?;

    // Example 5: Convert memory values to GB and filter for memory-related metrics
    // Shows: SQL transformations to convert bytes to GB and filter specific metrics
    // Expected: 5 rows showing memory metrics converted to GB (total: ~64GB, used: ~35GB)
    // Actual:
    // +----------------------+--------------------------+----------+
    // | metric               | description              | value_gb |
    // +----------------------+--------------------------+----------+
    // | total_memory         | Total Memory (bytes)     | 64.0     |
    // | used_memory          | Used Memory (bytes)      | 34.94    |
    // | free_memory          | Free Memory (bytes)      | 0.99     |
    // | available_memory     | Available Memory (bytes) | 24.69    |
    // | memory_usage_percent | Memory Usage (%)         | 0.0      |
    // +----------------------+--------------------------+----------+
    run_example(
        &ctx,
        "Example 5: Convert memory to GB and filter",
        r#"SELECT metric, description, 
           ROUND(CAST(value AS DOUBLE) / 1024 / 1024 / 1024, 2) as value_gb 
           FROM system_info('memory') 
           WHERE metric LIKE '%memory%'"#,
    )
    .await?;

    // Example 6: Calculate average CPU usage across all cores
    // Shows: Aggregation functions on system metrics data
    // Expected: 1 row with average CPU usage percentage across all cores
    // Actual:
    // +--------------------+
    // | avg_cpu_usage      |
    // +--------------------+
    // | 26.366999999999997 |
    // +--------------------+
    run_example(
        &ctx,
        "Example 6: Calculate average CPU usage",
        r#"SELECT AVG(CAST(value AS DOUBLE)) as avg_cpu_usage 
           FROM system_info('cpu') 
           WHERE metric LIKE 'cpu_%'"#,
    )
    .await?;

    // Example 7: Complex query using CTEs and JOINs to create system health dashboard
    // Shows: How to combine different system metrics and create conditional status indicators
    // Expected: 1 row with CPU usage, memory usage, and status indicators (NORMAL/HIGH)
    // Actual:
    // +-----------+-------------+------------+------------+
    // | cpu_usage | mem_percent | cpu_status | mem_status |
    // +-----------+-------------+------------+------------+
    // | 26.37     | 54.59       | NORMAL     | NORMAL     |
    // +-----------+-------------+------------+------------+
    run_example(
        &ctx,
        "Example 7: Complex query - JOIN different system info categories",
        r#"WITH cpu_stats AS (
             SELECT CAST(value AS DOUBLE) as cpu_usage 
             FROM system_info('cpu') 
             WHERE metric = 'global'
           ), 
           memory_stats AS (
             SELECT CAST(value AS DOUBLE) as mem_percent 
             FROM system_info('memory') 
             WHERE metric = 'memory_usage_percent'
           ) 
           SELECT c.cpu_usage, 
                  m.mem_percent,
                  CASE WHEN c.cpu_usage > 80.0 
                       THEN 'HIGH' ELSE 'NORMAL' END as cpu_status, 
                  CASE WHEN m.mem_percent > 80.0 
                       THEN 'HIGH' ELSE 'NORMAL' END as mem_status 
           FROM cpu_stats c 
           CROSS JOIN memory_stats m"#,
    )
    .await?;

    // Example 8: System health summary using conditional aggregation
    // Shows: Pivot-style query to create a single-row system summary
    // Expected: 1 row with hostname, CPU usage %, memory usage %, and uptime in hours
    // Actual:
    // +-----------------+---------------+---------------+--------------+
    // | hostname        | cpu_usage_pct | mem_usage_pct | uptime_hours |
    // +-----------------+---------------+---------------+--------------+
    // | COMP-EOSL454PLQ | 26.37         | 54.59         | 243.37       |
    // +-----------------+---------------+---------------+--------------+
    run_example(
        &ctx,
        "Example 8: System health summary",
        r#"WITH system_metrics AS (SELECT * FROM system_info()) 
           SELECT MAX(CASE WHEN metric = 'hostname' THEN value END) as hostname, 
                  MAX(CASE WHEN metric = 'global' THEN CAST(value AS DOUBLE) END) as cpu_usage_pct, 
                  MAX(CASE WHEN metric = 'memory_usage_percent' THEN CAST(value AS DOUBLE) END) as mem_usage_pct, 
                  MAX(CASE WHEN metric = 'uptime' THEN ROUND(CAST(value AS DOUBLE) / 3600, 2) END) as uptime_hours 
           FROM system_metrics"#,
    )
    .await?;

    Ok(())
}

async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<()> {
    println!("{title}:\n{sql}\n");
    let df = ctx.sql(sql).await?;
    df.show().await?;
    Ok(())
}

/// Custom relation planner that exposes system information as a table-valued function.
///
/// This planner handles SQL syntax forms like:
/// - `SELECT * FROM system_info()` - all system information
/// - `SELECT * FROM system_info('cpu')` - CPU metrics
/// - `SELECT * FROM system_info('memory')` - memory metrics
/// - `SELECT * FROM system_info('system')` - system metadata
#[derive(Debug)]
struct SystemInfoPlanner;

impl RelationPlanner for SystemInfoPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            // Handle: SELECT * FROM system_info(...) with arguments
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                if let Some(planned) = try_plan_system_info(&name, &args, alias.clone())?
                {
                    Ok(RelationPlanning::Planned(planned))
                } else {
                    Ok(RelationPlanning::Original(TableFactor::Function {
                        lateral,
                        name,
                        args,
                        alias,
                    }))
                }
            }
            // Handle: SELECT * FROM system_info() - parsed as Table with args
            TableFactor::Table {
                name,
                alias,
                args: Some(mut table_args),
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if is_system_info(&name) {
                    // Extract the args from table_args
                    let func_args = std::mem::take(&mut table_args.args);
                    if let Some(planned) = try_plan_system_info(&name, &func_args, alias)?
                    {
                        Ok(RelationPlanning::Planned(planned))
                    } else {
                        // Restore the args
                        table_args.args = func_args;
                        Ok(RelationPlanning::Original(TableFactor::Table {
                            name,
                            alias: None,
                            args: Some(table_args),
                            with_hints,
                            version,
                            with_ordinality,
                            partitions,
                            json_path,
                            sample,
                            index_hints,
                        }))
                    }
                } else {
                    Ok(RelationPlanning::Original(TableFactor::Table {
                        name,
                        alias,
                        args: Some(table_args),
                        with_hints,
                        version,
                        with_ordinality,
                        partitions,
                        json_path,
                        sample,
                        index_hints,
                    }))
                }
            }
            // Handle: SELECT * FROM TABLE(system_info(...))
            TableFactor::TableFunction { expr, alias } => {
                if let ast::Expr::Function(func) = &expr {
                    if let ast::FunctionArguments::List(list) = &func.args {
                        if let Some(planned) =
                            try_plan_system_info(&func.name, &list.args, alias.clone())?
                        {
                            return Ok(RelationPlanning::Planned(planned));
                        }
                    }
                }

                Ok(RelationPlanning::Original(TableFactor::TableFunction {
                    expr,
                    alias,
                }))
            }
            // Pass through any other relation types
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

/// Attempts to plan a `system_info` function call.
///
/// Returns `Ok(Some(...))` if the function is `system_info` and planning succeeds,
/// `Ok(None)` if the function is not `system_info`, or an error if planning fails.
fn try_plan_system_info(
    name: &ast::ObjectName,
    args: &[FunctionArg],
    alias: Option<ast::TableAlias>,
) -> Result<Option<PlannedRelation>> {
    if !is_system_info(name) {
        return Ok(None);
    }

    let category = parse_category_arg(args)?;
    let plan = build_system_info_plan(category)?;
    Ok(Some(PlannedRelation::new(plan, alias)))
}

/// Checks if the given object name refers to the `system_info` function.
fn is_system_info(name: &ast::ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("system_info"))
}

/// Parses the optional category argument from the function call.
///
/// Accepts either 0 arguments (return all info) or 1 string argument (return specific category).
fn parse_category_arg(args: &[FunctionArg]) -> Result<Option<String>> {
    match args.len() {
        0 => Ok(None),
        1 => {
            let arg = &args[0];
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => parse_string_literal(expr),
                _ => Err(DataFusionError::Plan(
                    "system_info argument must be a string literal".into(),
                )),
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "system_info expects 0 or 1 arguments, got {other}"
        ))),
    }
}

/// Parses a SQL string literal expression.
fn parse_string_literal(expr: &ast::Expr) -> Result<Option<String>> {
    match expr {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Some(s.clone()))
            }
            _ => Err(DataFusionError::Plan(format!(
                "system_info argument must be a string literal, got {expr}"
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "system_info argument must be a string literal, got {expr}"
        ))),
    }
}

/// Builds a logical plan that returns system information.
fn build_system_info_plan(category: Option<String>) -> Result<LogicalPlan> {
    let mut sys = System::new_with_specifics(
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );
    sys.refresh_specifics(
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );

    let rows = match category.as_deref() {
        None | Some("all") => collect_all_info(&sys),
        Some("cpu") => collect_cpu_info(&sys),
        Some("memory") => collect_memory_info(&sys),
        Some("system") => collect_system_info(&sys),
        Some(other) => {
            return Err(DataFusionError::Plan(format!(
                "Unknown system_info category: '{other}'. Valid categories: 'cpu', 'memory', 'system', 'all'"
            )))
        }
    };

    // Build the plan with proper column names
    LogicalPlanBuilder::values(rows)?
        .project(vec![
            col("column1").alias("category"),
            col("column2").alias("metric"),
            col("column3").alias("description"),
            col("column4").alias("value"),
        ])?
        .build()
}

/// Collects all system information.
fn collect_all_info(sys: &System) -> Vec<Vec<Expr>> {
    let mut rows = Vec::new();
    rows.extend(collect_cpu_info(sys));
    rows.extend(collect_memory_info(sys));
    rows.extend(collect_system_info(sys));
    rows
}

/// Collects CPU information.
fn collect_cpu_info(sys: &System) -> Vec<Vec<Expr>> {
    let mut rows = Vec::new();

    for (idx, cpu) in sys.cpus().iter().enumerate() {
        rows.push(vec![
            str_lit("cpu"),
            str_lit(format!("cpu_{idx}")),
            str_lit(cpu.name()),
            str_lit(format!("{:.2}", cpu.cpu_usage())),
        ]);
    }

    // Add global CPU usage
    rows.push(vec![
        str_lit("cpu"),
        str_lit("global"),
        str_lit("Global CPU Usage"),
        str_lit(format!("{:.2}", sys.global_cpu_usage())),
    ]);

    // Add CPU count
    rows.push(vec![
        str_lit("cpu"),
        str_lit("count"),
        str_lit("Total CPU Count"),
        str_lit(sys.cpus().len().to_string()),
    ]);

    // Add physical core count if available
    if let Some(physical_cores) = System::physical_core_count() {
        rows.push(vec![
            str_lit("cpu"),
            str_lit("physical_cores"),
            str_lit("Physical Core Count"),
            str_lit(physical_cores.to_string()),
        ]);
    }

    rows
}

/// Collects memory information.
fn collect_memory_info(sys: &System) -> Vec<Vec<Expr>> {
    vec![
        vec![
            str_lit("memory"),
            str_lit("total_memory"),
            str_lit("Total Memory (bytes)"),
            str_lit(sys.total_memory().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("used_memory"),
            str_lit("Used Memory (bytes)"),
            str_lit(sys.used_memory().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("free_memory"),
            str_lit("Free Memory (bytes)"),
            str_lit(sys.free_memory().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("available_memory"),
            str_lit("Available Memory (bytes)"),
            str_lit(sys.available_memory().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("total_swap"),
            str_lit("Total Swap (bytes)"),
            str_lit(sys.total_swap().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("used_swap"),
            str_lit("Used Swap (bytes)"),
            str_lit(sys.used_swap().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("free_swap"),
            str_lit("Free Swap (bytes)"),
            str_lit(sys.free_swap().to_string()),
        ],
        vec![
            str_lit("memory"),
            str_lit("memory_usage_percent"),
            str_lit("Memory Usage (%)"),
            str_lit(format!(
                "{:.2}",
                (sys.used_memory() as f64 / sys.total_memory() as f64) * 100.0
            )),
        ],
    ]
}

/// Collects system metadata information.
fn collect_system_info(_sys: &System) -> Vec<Vec<Expr>> {
    let mut rows = Vec::new();

    if let Some(name) = System::name() {
        rows.push(vec![
            str_lit("system"),
            str_lit("name"),
            str_lit("System Name"),
            str_lit(name),
        ]);
    }

    if let Some(kernel_version) = System::kernel_version() {
        rows.push(vec![
            str_lit("system"),
            str_lit("kernel_version"),
            str_lit("Kernel Version"),
            str_lit(kernel_version),
        ]);
    }

    if let Some(os_version) = System::os_version() {
        rows.push(vec![
            str_lit("system"),
            str_lit("os_version"),
            str_lit("OS Version"),
            str_lit(os_version),
        ]);
    }

    if let Some(host_name) = System::host_name() {
        rows.push(vec![
            str_lit("system"),
            str_lit("hostname"),
            str_lit("Hostname"),
            str_lit(host_name),
        ]);
    }

    rows.push(vec![
        str_lit("system"),
        str_lit("uptime"),
        str_lit("Uptime (seconds)"),
        str_lit(System::uptime().to_string()),
    ]);

    rows.push(vec![
        str_lit("system"),
        str_lit("boot_time"),
        str_lit("Boot Time (unix timestamp)"),
        str_lit(System::boot_time().to_string()),
    ]);

    let load_avg = System::load_average();
    rows.push(vec![
        str_lit("system"),
        str_lit("load_avg_1"),
        str_lit("Load Average (1 min)"),
        str_lit(format!("{:.2}", load_avg.one)),
    ]);

    rows.push(vec![
        str_lit("system"),
        str_lit("load_avg_5"),
        str_lit("Load Average (5 min)"),
        str_lit(format!("{:.2}", load_avg.five)),
    ]);

    rows.push(vec![
        str_lit("system"),
        str_lit("load_avg_15"),
        str_lit("Load Average (15 min)"),
        str_lit(format!("{:.2}", load_avg.fifteen)),
    ]);

    rows
}

/// Helper function to create a string literal expression.
fn str_lit(s: impl Into<String>) -> Expr {
    Expr::Literal(ScalarValue::Utf8(Some(s.into())), None)
}

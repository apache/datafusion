<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Building Logical Plans

A logical plan is a structured representation of a database query that describes the high-level operations and
transformations needed to retrieve data from a database or data source. It abstracts away specific implementation
details and focuses on the logical flow of the query, including operations like filtering, sorting, and joining tables.

This logical plan serves as an intermediate step before generating an optimized physical execution plan. This is
explained in more detail in the [Query Planning and Execution Overview] section of the [Architecture Guide].

## Building Logical Plans Manually

DataFusion's [LogicalPlan] is an enum containing variants representing all the supported operators, and also
contains an `Extension` variant that allows projects building on DataFusion to add custom logical operators.

It is possible to create logical plans by directly creating instances of the [LogicalPlan] enum as shown, but it is
much easier to use the [LogicalPlanBuilder], which is described in the next section.

Here is an example of building a logical plan directly:

```rust
use datafusion::common::DataFusionError;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{Filter, LogicalPlan, TableScan, LogicalTableSource};
use datafusion::prelude::*;
use std::sync::Arc;

fn main() -> Result<(), DataFusionError> {
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    // create a TableScan plan
    let projection = None; // optional projection
    let filters = vec![]; // optional filters to push down
    let fetch = None; // optional LIMIT
    let table_scan = LogicalPlan::TableScan(TableScan::try_new(
        "person",
        Arc::new(table_source),
        projection,
        filters,
        fetch,
        )?
    );

    // create a Filter plan that evaluates `id > 500` that wraps the TableScan
    let filter_expr = col("id").gt(lit(500));
    let plan = LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(table_scan)) ? );

    // print the plan
    println!("{}", plan.display_indent_schema());
    Ok(())
}
```

This example produces the following plan:

```text
Filter: person.id > Int32(500) [id:Int32;N, name:Utf8;N]
  TableScan: person [id:Int32;N, name:Utf8;N]
```

## Building Logical Plans with LogicalPlanBuilder

DataFusion logical plans can be created using the [LogicalPlanBuilder] struct. There is also a [DataFrame] API which is
a higher-level API that delegates to [LogicalPlanBuilder].

There are several functions that can can be used to create a new builder, such as

- `empty` - create an empty plan with no fields
- `values` - create a plan from a set of literal values
- `scan` - create a plan representing a table scan
- `scan_with_filters` - create a plan representing a table scan with filters

Once the builder is created, transformation methods can be called to declare that further operations should be
performed on the plan. Note that all we are doing at this stage is building up the logical plan structure. No query
execution will be performed.

Here are some examples of transformation methods, but for a full list, refer to the [LogicalPlanBuilder] API documentation.

- `filter`
- `limit`
- `sort`
- `distinct`
- `join`

The following example demonstrates building the same simple query plan as the previous example, with a table scan followed by a filter.

<!-- source for this example is in datafusion_docs::library_logical_plan::plan_builder_1 -->

```rust
use datafusion::common::DataFusionError;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
use datafusion::prelude::*;
use std::sync::Arc;

fn main() -> Result<(), DataFusionError> {
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    // optional projection
    let projection = None;

    // create a LogicalPlanBuilder for a table scan
    let builder = LogicalPlanBuilder::scan("person", Arc::new(table_source), projection)?;

    // perform a filter operation and build the plan
    let plan = builder
        .filter(col("id").gt(lit(500)))? // WHERE id > 500
        .build()?;

    // print the plan
    println!("{}", plan.display_indent_schema());
    Ok(())
}
```

This example produces the following plan:

```text
Filter: person.id > Int32(500) [id:Int32;N, name:Utf8;N]
  TableScan: person [id:Int32;N, name:Utf8;N]
```

## Translating Logical Plan to Physical Plan

Logical plans can not be directly executed. They must be "compiled" into an
[`ExecutionPlan`], which is often referred to as a "physical plan".

Compared to `LogicalPlan`s `ExecutionPlans` have many more details such as
specific algorithms and detailed optimizations compared to. Given a
`LogicalPlan` the easiest way to create an `ExecutionPlan` is using
[`SessionState::create_physical_plan`] as shown below

```rust
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
use datafusion::prelude::*;
use std::sync::Arc;

// Creating physical plans may access remote catalogs and data sources
// thus it must be run with an async runtime.
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {

    // create a default table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    // To create an ExecutionPlan we must provide an actual
    // TableProvider. For this example, we don't provide any data
    // but in production code, this would have `RecordBatch`es with
    // in memory data
    let table_provider = Arc::new(MemTable::try_new(Arc::new(schema), vec![])?);
    // Use the provider_as_source function to convert the TableProvider to a table source
    let table_source = provider_as_source(table_provider);

    // create a LogicalPlanBuilder for a table scan without projection or filters
    let logical_plan = LogicalPlanBuilder::scan("person", table_source, None)?.build()?;

    // Now create the physical plan by calling `create_physical_plan`
    let ctx = SessionContext::new();
    let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

    // print the plan
    println!("{}", DisplayableExecutionPlan::new(physical_plan.as_ref()).indent(true));
    Ok(())
}
```

This example produces the following physical plan:

```text
MemoryExec: partitions=0, partition_sizes=[]
```

## Table Sources

The previous examples use a [LogicalTableSource], which is used for tests and documentation in DataFusion, and is also
suitable if you are using DataFusion to build logical plans but do not use DataFusion's physical planner.

However, it is more common to use a [TableProvider]. To get a [TableSource] from a
[TableProvider], use [provider_as_source] or [DefaultTableSource].

[query planning and execution overview]: https://docs.rs/datafusion/latest/datafusion/index.html#query-planning-and-execution-overview
[architecture guide]: https://docs.rs/datafusion/latest/datafusion/index.html#architecture
[logicalplan]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[logicalplanbuilder]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html
[dataframe]: using-the-dataframe-api.md
[logicaltablesource]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/struct.LogicalTableSource.html
[defaulttablesource]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/struct.DefaultTableSource.html
[provider_as_source]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/fn.provider_as_source.html
[tableprovider]: https://docs.rs/datafusion/latest/datafusion/datasource/provider/trait.TableProvider.html
[tablesource]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.TableSource.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`sessionstate::create_physical_plan`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.create_physical_plan

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

//! Implementation of DataFrame API.

use std::sync::{Arc, Mutex};

use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::execution::context::{ExecutionContext, ExecutionContextState};
use crate::logical_plan::{
    col, DFSchema, Expr, FunctionRegistry, JoinType, LogicalPlan, LogicalPlanBuilder,
    Partitioning,
};
use crate::{
    dataframe::*,
    physical_plan::{collect, collect_partitioned},
};

use crate::arrow::util::pretty;
use crate::physical_plan::{
    execute_stream, execute_stream_partitioned, ExecutionPlan, SendableRecordBatchStream,
};
use async_trait::async_trait;

/// Implementation of DataFrame API
pub struct DataFrameImpl {
    ctx_state: Arc<Mutex<ExecutionContextState>>,
    plan: LogicalPlan,
}

impl DataFrameImpl {
    /// Create a new Table based on an existing logical plan
    pub fn new(ctx_state: Arc<Mutex<ExecutionContextState>>, plan: &LogicalPlan) -> Self {
        Self {
            ctx_state,
            plan: plan.clone(),
        }
    }

    /// Create a physical plan
    async fn create_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let state = self.ctx_state.lock().unwrap().clone();
        let ctx = ExecutionContext::from(Arc::new(Mutex::new(state)));
        let plan = ctx.optimize(&self.plan)?;
        ctx.create_physical_plan(&plan)
    }
}

#[async_trait]
impl DataFrame for DataFrameImpl {
    /// Apply a projection based on a list of column names
    fn select_columns(&self, columns: &[&str]) -> Result<Arc<dyn DataFrame>> {
        let fields = columns
            .iter()
            .map(|name| self.plan.schema().field_with_unqualified_name(name))
            .collect::<Result<Vec<_>>>()?;
        let expr: Vec<Expr> = fields.iter().map(|f| col(f.name())).collect();
        self.select(expr)
    }

    /// Create a projection based on arbitrary expressions
    fn select(&self, expr_list: Vec<Expr>) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .project(expr_list)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Create a filter based on a predicate expression
    fn filter(&self, predicate: Expr) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .filter(predicate)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Perform an aggregate query
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .limit(n)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Sort by specified sorting expressions
    fn sort(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .sort(expr)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Join with another DataFrame
    fn join(
        &self,
        right: Arc<dyn DataFrame>,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
    ) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .join(
                &right.to_logical_plan(),
                join_type,
                (left_cols.to_vec(), right_cols.to_vec()),
            )?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    fn repartition(
        &self,
        partitioning_scheme: Partitioning,
    ) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .repartition(partitioning_scheme)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Convert to logical plan
    fn to_logical_plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    /// Convert the logical plan represented by this DataFrame into a physical plan and
    /// execute it, collecting all resulting batches into memory
    async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let plan = self.create_physical_plan().await?;
        Ok(collect(plan).await?)
    }

    /// Print results.
    async fn show(&self) -> Result<()> {
        let results = self.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Print results and limit rows.
    async fn show_limit(&self, num: usize) -> Result<()> {
        let results = self.limit(num)?.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Convert the logical plan represented by this DataFrame into a physical plan and
    /// execute it, returning a stream over a single partition
    async fn execute_stream(&self) -> Result<SendableRecordBatchStream> {
        let plan = self.create_physical_plan().await?;
        execute_stream(plan).await
    }

    /// Convert the logical plan represented by this DataFrame into a physical plan and
    /// execute it, collecting all resulting batches into memory while maintaining
    /// partitioning
    async fn collect_partitioned(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let plan = self.create_physical_plan().await?;
        Ok(collect_partitioned(plan).await?)
    }

    /// Convert the logical plan represented by this DataFrame into a physical plan and
    /// execute it, returning a stream for each partition
    async fn execute_stream_partitioned(&self) -> Result<Vec<SendableRecordBatchStream>> {
        let plan = self.create_physical_plan().await?;
        Ok(execute_stream_partitioned(plan).await?)
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    fn explain(&self, verbose: bool, analyze: bool) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .explain(verbose, analyze)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    fn registry(&self) -> Arc<dyn FunctionRegistry> {
        let registry = self.ctx_state.lock().unwrap().clone();
        Arc::new(registry)
    }

    fn union(&self, dataframe: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.to_logical_plan())
            .union(dataframe.to_logical_plan())?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::logical_plan::*;
    use crate::{assert_batches_sorted_eq, execution::context::ExecutionContext};
    use crate::{datasource::csv::CsvReadOptions, physical_plan::ColumnarValue};
    use crate::{physical_plan::functions::ScalarFunctionImplementation, test};
    use arrow::datatypes::DataType;

    #[test]
    fn select_columns() -> Result<()> {
        // build plan using Table API
        let t = test_table()?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn select_expr() -> Result<()> {
        // build plan using Table API
        let t = test_table()?;
        let t2 = t.select(vec![col("c1"), col("c2"), col("c11")])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate() -> Result<()> {
        // build plan using DataFrame API
        let df = test_table()?;
        let group_expr = vec![col("c1")];
        let aggr_expr = vec![
            min(col("c12")),
            max(col("c12")),
            avg(col("c12")),
            sum(col("c12")),
            count(col("c12")),
            count_distinct(col("c12")),
        ];

        let df: Vec<RecordBatch> = df.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) | AVG(aggregate_test_100.c12) | SUM(aggregate_test_100.c12) | COUNT(aggregate_test_100.c12) | COUNT(DISTINCT aggregate_test_100.c12) |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| a  | 0.02182578039211991         | 0.9800193410444061          | 0.48754517466109415         | 10.238448667882977          | 21                            | 21                                     |",
                "| b  | 0.04893135681998029         | 0.9185813970744787          | 0.41040709263815384         | 7.797734760124923           | 19                            | 19                                     |",
                "| c  | 0.0494924465469434          | 0.991517828651004           | 0.6600456536439784          | 13.860958726523545          | 21                            | 21                                     |",
                "| d  | 0.061029375346466685        | 0.9748360509016578          | 0.48855379387549824         | 8.793968289758968           | 18                            | 18                                     |",
                "| e  | 0.01479305307777301         | 0.9965400387585364          | 0.48600669271341534         | 10.206140546981722          | 21                            | 21                                     |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
            ],
            &df
        );

        Ok(())
    }

    #[tokio::test]
    async fn join() -> Result<()> {
        let left = test_table()?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")?.select_columns(&["c1", "c3"])?;
        let left_rows = left.collect().await?;
        let right_rows = right.collect().await?;
        let join = left.join(right, JoinType::Inner, &["c1"], &["c1"])?;
        let join_rows = join.collect().await?;
        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(2008, join_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        Ok(())
    }

    #[test]
    fn limit() -> Result<()> {
        // build query using Table API
        let t = test_table()?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?.limit(10)?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn explain() -> Result<()> {
        // build query using Table API
        let df = test_table()?;
        let df = df
            .select_columns(&["c1", "c2", "c11"])?
            .limit(10)?
            .explain(false, false)?;
        let plan = df.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("EXPLAIN SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn registry() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100")?;

        // declare the udf
        let my_fn: ScalarFunctionImplementation =
            Arc::new(|_: &[ColumnarValue]| unimplemented!("my_fn is not implemented"));

        // create and register the udf
        ctx.register_udf(create_udf(
            "my_fn",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            my_fn,
        ));

        // build query with a UDF using DataFrame API
        let df = ctx.table("aggregate_test_100")?;

        let f = df.registry();

        let df = df.select(vec![f.udf("my_fn")?.call(vec![col("c12")])])?;
        let plan = df.to_logical_plan();

        // build query using SQL
        let sql_plan =
            ctx.create_logical_plan("SELECT my_fn(c12) FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn sendable() {
        let df = test_table().unwrap();
        // dataframes should be sendable between threads/tasks
        let task = tokio::task::spawn(async move {
            df.select_columns(&["c1"])
                .expect("should be usable in a task")
        });
        task.await.expect("task completed successfully");
    }

    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{:?}", plan1), format!("{:?}", plan2));
    }

    /// Create a logical plan from a SQL query
    fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100")?;
        ctx.create_logical_plan(sql)
    }

    fn test_table_with_name(name: &str) -> Result<Arc<dyn DataFrame + 'static>> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx, name)?;
        ctx.table(name)
    }

    fn test_table() -> Result<Arc<dyn DataFrame + 'static>> {
        test_table_with_name("aggregate_test_100")
    }

    fn register_aggregate_csv(
        ctx: &mut ExecutionContext,
        table_name: &str,
    ) -> Result<()> {
        let schema = test::aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        ctx.register_csv(
            table_name,
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            CsvReadOptions::new().schema(schema.as_ref()),
        )?;
        Ok(())
    }
}

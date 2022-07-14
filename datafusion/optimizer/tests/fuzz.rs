use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
use datafusion_optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion_optimizer::eliminate_filter::EliminateFilter;
use datafusion_optimizer::eliminate_limit::EliminateLimit;
use datafusion_optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion_optimizer::filter_push_down::FilterPushDown;
use datafusion_optimizer::limit_push_down::LimitPushDown;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::projection_push_down::ProjectionPushDown;
use datafusion_optimizer::reduce_outer_join::ReduceOuterJoin;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion_optimizer::subquery_filter_to_join::SubqueryFilterToJoin;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// TODO read SQL from file

#[test]
fn test_semi_join() {
    let sql = "SELECT _c48, _c49, _c50, _c51, _c52, _c53
FROM (SELECT test1.c0 AS _c48, test1.c1 AS _c49, test1.c2 AS _c50, test1.c3 AS _c51, test1.c4 AS _c52, test1.c5 AS _c53
  FROM (test1))
WHERE NOT EXISTS (SELECT _c59
FROM (SELECT _c54, _c55, _c56, _c57, _c58, _c59
  FROM (SELECT test0.c0 AS _c54, test0.c1 AS _c55, test0.c2 AS _c56, test0.c3 AS _c57, test0.c4 AS _c58, test0.c5 AS _c59
    FROM (test0))
  WHERE NOT EXISTS (SELECT _c65
  FROM (SELECT _c60, _c61, _c62, _c63, _c64, _c65
    FROM (SELECT test0.c0 AS _c60, test0.c1 AS _c61, test0.c2 AS _c62, test0.c3 AS _c63, test0.c4 AS _c64, test0.c5 AS _c65
      FROM (test0))
    WHERE _c60 != _c65)
  WHERE _c64 = _c58))
WHERE _c58 = _c52);
";

    test_sql(sql);
}

#[test]
fn test_mixed_joins() {
    test_sql("SELECT _c144, _c145, _c146, _c147, _c148, _c149
FROM (
  (
    (SELECT test0.c0 AS _c144, test0.c1 AS _c145, test0.c2 AS _c146, test0.c3 AS _c147, test0.c4 AS _c148, test0.c5 AS _c149
      FROM (test0))
    LEFT JOIN
    (
      (
        (
          (SELECT test1.c0 AS _c150, test1.c1 AS _c151, test1.c2 AS _c152, test1.c3 AS _c153, test1.c4 AS _c154, test1.c5 AS _c155
            FROM (test1))
          INNER JOIN
          (SELECT test1.c0 AS _c156, test1.c1 AS _c157, test1.c2 AS _c158, test1.c3 AS _c159, test1.c4 AS _c160, test1.c5 AS _c161
            FROM (test1))
          ON _c153 = _c160)
        FULL JOIN
        (SELECT test0.c0 AS _c162, test0.c1 AS _c163, test0.c2 AS _c164, test0.c3 AS _c165, test0.c4 AS _c166, test0.c5 AS _c167
          FROM (test0))
        ON _c153 = _c164)
      LEFT JOIN
      (SELECT test1.c0 AS _c168, test1.c1 AS _c169, test1.c2 AS _c170, test1.c3 AS _c171, test1.c4 AS _c172, test1.c5 AS _c173
        FROM (test1))
      ON _c154 = _c168)
    ON _c146 = _c154)
  INNER JOIN
  (SELECT test0.c0 AS _c174, test0.c1 AS _c175, test0.c2 AS _c176, test0.c3 AS _c177, test0.c4 AS _c178, test0.c5 AS _c179
    FROM (test0))
  ON _c148 = _c177);
");
}

fn test_sql(sql: &str) {
    // TODO we could make this more interesting by randomizing
    // the set of rules and the order of the rules

    let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
        // Simplify expressions first to maximize the chance
        // of applying other optimizations
        Arc::new(SimplifyExpressions::new()),
        Arc::new(SubqueryFilterToJoin::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(ProjectionPushDown::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(ReduceOuterJoin::new()),
        Arc::new(FilterPushDown::new()),
        Arc::new(LimitPushDown::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
    ];

    let optimizer = Optimizer::new(rules);

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let schema_provider = MySchemaProvider {};
    let sql_to_rel = SqlToRel::new(&schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    let mut config = OptimizerConfig::new();
    //.with_skip_failing_rules(false); // not available yet
    optimizer.optimize(&plan, &mut config, &observe).unwrap();
}

struct MySchemaProvider {}

impl ContextProvider for MySchemaProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if table_name.starts_with("test") {
            // TODO we could randomize the schema types and null flags
            let schema = Schema::new_with_metadata(
                vec![
                    Field::new("c0", DataType::Int32, true),
                    Field::new("c1", DataType::Float32, true),
                    Field::new("c2", DataType::Utf8, true),
                    Field::new("c3", DataType::Boolean, true),
                    Field::new("c4", DataType::Decimal(10, 2), true),
                    Field::new("c5", DataType::Date32, true),
                ],
                HashMap::new(),
            );

            Ok(Arc::new(MyTableSource {
                schema: Arc::new(schema),
            }))
        } else {
            Err(DataFusionError::Plan("table does not exist".to_string()))
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }
}

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

struct MyTableSource {
    schema: SchemaRef,
}

impl TableSource for MyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

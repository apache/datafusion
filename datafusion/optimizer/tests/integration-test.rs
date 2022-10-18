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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[test]
fn case_when() -> Result<()> {
    let sql = "SELECT CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END AS CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END\
         \n  TableScan: test projection=[col_int32]";
    assert_eq!(expected, format!("{:?}", plan));

    let sql = "SELECT CASE WHEN col_uint32 > 0 THEN 1 ELSE 0 END FROM test";
    let plan = test_sql(sql)?;
    let expected = "Projection: CASE WHEN CAST(test.col_uint32 AS Int64) > Int64(0) THEN Int64(1) ELSE Int64(0) END\
    \n  TableScan: test projection=[col_uint32]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn subquery_filter_with_cast() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/3760
    let sql = "SELECT col_int32 FROM test \
    WHERE col_int32 > (\
      SELECT AVG(col_int32) FROM test \
      WHERE col_utf8 BETWEEN '2002-05-08' \
        AND (cast('2002-05-08' as date) + interval '5 days')\
    )";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: test.col_int32\n  Filter: CAST(test.col_int32 AS Float64) > __sq_1.__value\
        \n    CrossJoin:\
        \n      TableScan: test projection=[col_int32]\
        \n      Projection: AVG(test.col_int32) AS __value, alias=__sq_1\
        \n        Aggregate: groupBy=[[]], aggr=[[AVG(test.col_int32)]]\
        \n          Filter: test.col_utf8 >= Utf8(\"2002-05-08\") AND test.col_utf8 <= Utf8(\"2002-05-13\")\
        \n            TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn case_when_aggregate() -> Result<()> {
    let sql = "SELECT col_utf8, SUM(CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END) AS n FROM test GROUP BY col_utf8";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_utf8, SUM(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END) AS n\
                    \n  Aggregate: groupBy=[[test.col_utf8]], aggr=[[SUM(CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END) AS SUM(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END)]]\
                    \n    TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn unsigned_target_type() -> Result<()> {
    let sql = "SELECT * FROM test WHERE col_uint32 > 0";
    let plan = test_sql(sql)?;
    let expected = "Projection: test.col_int32, test.col_uint32, test.col_utf8, test.col_date32, test.col_date64\
    \n  Filter: CAST(test.col_uint32 AS Int64) > Int64(0)\
    \n    TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn distribute_by() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/3234
    let sql = "SELECT col_int32, col_utf8 FROM test DISTRIBUTE BY (col_utf8)";
    let plan = test_sql(sql)?;
    let expected = "Repartition: DistributeBy(col_utf8)\
    \n  Projection: test.col_int32, test.col_utf8\
    \n    TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn semi_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/2888
    let sql = "SELECT * FROM test WHERE EXISTS (\
    SELECT * FROM test t2 WHERE test.col_int32 = t2.col_int32 \
    AND test.col_uint32 != t2.col_uint32)";
    let plan = test_sql(sql)?;
    let expected = r#"Projection: test.col_int32, test.col_uint32, test.col_utf8, test.col_date32, test.col_date64
  Semi Join: test.col_int32 = t2.col_int32 Filter: test.col_uint32 != t2.col_uint32
    TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]
    SubqueryAlias: t2
      TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]"#;
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn anti_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/2888
    let sql = "SELECT * FROM test WHERE NOT EXISTS (\
    SELECT * FROM test t2 WHERE test.col_int32 = t2.col_int32 \
    AND test.col_uint32 != t2.col_uint32)";
    let plan = test_sql(sql)?;
    let expected = r#"Projection: test.col_int32, test.col_uint32, test.col_utf8, test.col_date32, test.col_date64
  Anti Join: test.col_int32 = t2.col_int32 Filter: test.col_uint32 != t2.col_uint32
    TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]
    SubqueryAlias: t2
      TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]"#;
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn where_exists_distinct() -> Result<()> {
    // regression test for https://github.com/apache/arrow-datafusion/issues/3724
    let sql = "SELECT * FROM test WHERE EXISTS (\
    SELECT DISTINCT col_int32 FROM test t2 WHERE test.col_int32 = t2.col_int32)";
    let plan = test_sql(sql)?;
    let expected = r#"Projection: test.col_int32, test.col_uint32, test.col_utf8, test.col_date32, test.col_date64
  Semi Join: test.col_int32 = t2.col_int32
    TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]
    SubqueryAlias: t2
      TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64]"#;
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn intersect() -> Result<()> {
    let sql = "SELECT col_int32, col_utf8 FROM test \
    INTERSECT SELECT col_int32, col_utf8 FROM test \
    INTERSECT SELECT col_int32, col_utf8 FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Semi Join: test.col_int32 = test.col_int32, test.col_utf8 = test.col_utf8\
    \n  Distinct:\
    \n    Semi Join: test.col_int32 = test.col_int32, test.col_utf8 = test.col_utf8\
    \n      Distinct:\
    \n        TableScan: test projection=[col_int32, col_utf8]\
    \n      TableScan: test projection=[col_int32, col_utf8]\
    \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn between_date32_plus_interval() -> Result<()> {
    let sql = "SELECT count(1) FROM test \
    WHERE col_date32 between '1998-03-18' AND cast('1998-03-18' as date) + INTERVAL '90 days'";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: COUNT(UInt8(1))\n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
        \n    Filter: test.col_date32 >= Date32(\"10303\") AND test.col_date32 <= Date32(\"10393\")\
        \n      TableScan: test projection=[col_date32]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn between_date64_plus_interval() -> Result<()> {
    let sql = "SELECT count(1) FROM test \
    WHERE col_date64 between '1998-03-18T00:00:00' AND cast('1998-03-18' as date) + INTERVAL '90 days'";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: COUNT(UInt8(1))\n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
        \n    Filter: test.col_date64 >= Date64(\"890179200000\") AND test.col_date64 <= Date64(\"897955200000\")\
        \n      TableScan: test projection=[col_date64]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn concat_literals() -> Result<()> {
    let sql = "SELECT concat(true, col_int32, false, null, 'hello', col_utf8, 12, 3.4) \
        AS col
        FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: concat(Utf8(\"1\"), CAST(test.col_int32 AS Utf8), Utf8(\"0hello\"), test.col_utf8, Utf8(\"123.4\")) AS col\
        \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

#[test]
fn concat_ws_literals() -> Result<()> {
    let sql = "SELECT concat_ws('-', true, col_int32, false, null, 'hello', col_utf8, 12, '', 3.4) \
        AS col
        FROM test";
    let plan = test_sql(sql)?;
    let expected =
        "Projection: concatwithseparator(Utf8(\"-\"), Utf8(\"1\"), CAST(test.col_int32 AS Utf8), Utf8(\"0-hello\"), test.col_utf8, Utf8(\"12--3.4\")) AS col\
        \n  TableScan: test projection=[col_int32, col_utf8]";
    assert_eq!(expected, format!("{:?}", plan));
    Ok(())
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let schema_provider = MySchemaProvider {};
    let sql_to_rel = SqlToRel::new(&schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // optimize the logical plan
    let mut config = OptimizerConfig::new().with_skip_failing_rules(false);
    let optimizer = Optimizer::new(&config);
    optimizer.optimize(&plan, &mut config, &observe)
}

struct MySchemaProvider {}

impl ContextProvider for MySchemaProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if table_name.starts_with("test") {
            let schema = Schema::new_with_metadata(
                vec![
                    Field::new("col_int32", DataType::Int32, true),
                    Field::new("col_uint32", DataType::UInt32, true),
                    Field::new("col_utf8", DataType::Utf8, true),
                    Field::new("col_date32", DataType::Date32, true),
                    Field::new("col_date64", DataType::Date64, true),
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

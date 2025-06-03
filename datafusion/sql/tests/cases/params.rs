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

use crate::logical_plan;
use arrow::datatypes::DataType;
use datafusion_common::{assert_contains, ParamValues, ScalarValue};
use datafusion_expr::{LogicalPlan, Prepare, Statement};
use insta::assert_snapshot;
use std::collections::HashMap;

pub struct ParameterTest<'a> {
    pub sql: &'a str,
    pub expected_types: Vec<(&'a str, Option<DataType>)>,
    pub param_values: Vec<ScalarValue>,
}

impl ParameterTest<'_> {
    pub fn run(&self) -> String {
        let plan = logical_plan(self.sql).unwrap();

        let actual_types = plan.get_parameter_types().unwrap();
        let expected_types: HashMap<String, Option<DataType>> = self
            .expected_types
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        assert_eq!(actual_types, expected_types);

        let plan_with_params = plan
            .clone()
            .with_param_values(self.param_values.clone())
            .unwrap();

        format!("** Initial Plan:\n{plan}\n** Final Plan:\n{plan_with_params}")
    }
}

fn generate_prepare_stmt_and_data_types(sql: &str) -> (LogicalPlan, String) {
    let plan = logical_plan(sql).unwrap();
    let data_types = match &plan {
        LogicalPlan::Statement(Statement::Prepare(Prepare { data_types, .. })) => {
            format!("{data_types:?}")
        }
        _ => panic!("Expected a Prepare statement"),
    };
    (plan, data_types)
}

#[test]
fn test_prepare_statement_to_plan_panic_param_format() {
    // param is not number following the $ sign
    // panic due to error returned from the parser
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $foo";

    assert_snapshot!(
        logical_plan(sql).unwrap_err().strip_backtrace(),
        @r###"
        Error during planning: Invalid placeholder, not a number: $foo
        "###
    );
}

#[test]
fn test_prepare_statement_to_plan_panic_param_zero() {
    // param is zero following the $ sign
    // panic due to error returned from the parser
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $0";

    assert_snapshot!(
        logical_plan(sql).unwrap_err().strip_backtrace(),
        @r###"
        Error during planning: Invalid placeholder, zero is not a valid index: $0
        "###
    );
}

#[test]
fn test_prepare_statement_to_plan_panic_prepare_wrong_syntax() {
    // param is not number following the $ sign
    // panic due to error returned from the parser
    let sql = "PREPARE AS SELECT id, age  FROM person WHERE age = $foo";
    assert!(logical_plan(sql)
        .unwrap_err()
        .strip_backtrace()
        .contains("Expected: AS, found: SELECT"))
}

#[test]
fn test_prepare_statement_to_plan_panic_no_relation_and_constant_param() {
    let sql = "PREPARE my_plan(INT) AS SELECT id + $1";

    let plan = logical_plan(sql).unwrap_err().strip_backtrace();
    assert_snapshot!(
        plan,
        @r"Schema error: No field named id."
    );
}

#[test]
fn test_prepare_statement_should_infer_types() {
    // only provide 1 data type while using 2 params
    let sql = "PREPARE my_plan(INT) AS SELECT 1 + $1 + $2";
    let plan = logical_plan(sql).unwrap();
    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        ("$1".to_string(), Some(DataType::Int32)),
        ("$2".to_string(), Some(DataType::Int64)),
    ]);
    assert_eq!(actual_types, expected_types);
}

#[test]
fn test_non_prepare_statement_should_infer_types() {
    // Non prepared statements (like SELECT) should also have their parameter types inferred
    let sql = "SELECT 1 + $1";
    let plan = logical_plan(sql).unwrap();
    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        // constant 1 is inferred to be int64
        ("$1".to_string(), Some(DataType::Int64)),
    ]);
    assert_eq!(actual_types, expected_types);
}

#[test]
#[should_panic(
    expected = "Expected: [NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS, found: $1"
)]
fn test_prepare_statement_to_plan_panic_is_param() {
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age is $1";
    logical_plan(sql).unwrap();
}

#[test]
fn test_prepare_statement_to_plan_no_param() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32]
      Projection: person.id, person.age
        Filter: person.age = Int64(10)
          TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[Int32]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: person.id, person.age
      Filter: person.age = Int64(10)
        TableScan: person
    "
    );

    //////////////////////////////////////////
    // no embedded parameter and no declare it
    let sql = "PREPARE my_plan AS SELECT id, age  FROM person WHERE age = 10";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" []
      Projection: person.id, person.age
        Filter: person.age = Int64(10)
          TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[]"#);

    ///////////////////
    // replace params with values
    let param_values: Vec<ScalarValue> = vec![];
    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: person.id, person.age
      Filter: person.age = Int64(10)
        TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_to_plan_one_param_no_value_panic() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";
    let plan = logical_plan(sql).unwrap();
    // declare 1 param but provide 0
    let param_values: Vec<ScalarValue> = vec![];

    assert_snapshot!(
        plan.with_param_values(param_values)
        .unwrap_err()
        .strip_backtrace(),
        @r###"
        Error during planning: Expected 1 parameters, got 0
        "###);
}

#[test]
fn test_prepare_statement_to_plan_one_param_one_value_different_type_panic() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";
    let plan = logical_plan(sql).unwrap();
    // declare 1 param but provide 0
    let param_values = vec![ScalarValue::Float64(Some(20.0))];

    assert_snapshot!(
        plan.with_param_values(param_values)
            .unwrap_err()
            .strip_backtrace(),
        @r###"
        Error during planning: Expected parameter of type Int32, got Float64 at index 0
        "###
    );
}

#[test]
fn test_prepare_statement_to_plan_no_param_on_value_panic() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan AS SELECT id, age  FROM person WHERE age = 10";
    let plan = logical_plan(sql).unwrap();
    // declare 1 param but provide 0
    let param_values = vec![ScalarValue::Int32(Some(10))];

    assert_snapshot!(
        plan.with_param_values(param_values)
            .unwrap_err()
            .strip_backtrace(),
        @r###"
        Error during planning: Expected 0 parameters, got 1
        "###
    );
}

#[test]
fn test_prepare_statement_to_plan_params_as_constants() {
    let sql = "PREPARE my_plan(INT) AS SELECT $1";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32]
      Projection: $1
        EmptyRelation
    "#
    );
    assert_snapshot!(dt, @r#"[Int32]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: Int32(10) AS $1
      EmptyRelation
    "
    );

    ///////////////////////////////////////
    let sql = "PREPARE my_plan(INT) AS SELECT 1 + $1";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32]
      Projection: Int64(1) + $1
        EmptyRelation
    "#
    );
    assert_snapshot!(dt, @r#"[Int32]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: Int64(1) + Int32(10) AS Int64(1) + $1
      EmptyRelation
    "
    );

    ///////////////////////////////////////
    let sql = "PREPARE my_plan(INT, DOUBLE) AS SELECT 1 + $1 + $2";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32, Float64]
      Projection: Int64(1) + $1 + $2
        EmptyRelation
    "#
    );
    assert_snapshot!(dt, @r#"[Int32, Float64]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Int32(Some(10)),
        ScalarValue::Float64(Some(10.0)),
    ];
    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: Int64(1) + Int32(10) + Float64(10) AS Int64(1) + $1 + $2
      EmptyRelation
    "
    );
}

#[test]
fn test_infer_types_from_join() {
    let test = ParameterTest {
        sql:
            "SELECT id, order_id FROM person JOIN orders ON id = customer_id and age = $1",
        expected_types: vec![("$1", Some(DataType::Int32))],
        param_values: vec![ScalarValue::Int32(Some(10))],
    };

    assert_snapshot!(
        test.run(),
        @r"
    ** Initial Plan:
    Projection: person.id, orders.order_id
      Inner Join:  Filter: person.id = orders.customer_id AND person.age = $1
        TableScan: person
        TableScan: orders
    ** Final Plan:
    Projection: person.id, orders.order_id
      Inner Join:  Filter: person.id = orders.customer_id AND person.age = Int32(10)
        TableScan: person
        TableScan: orders
    "
    );
}

#[test]
fn test_prepare_statement_infer_types_from_join() {
    let test = ParameterTest {
        sql: "PREPARE my_plan AS SELECT id, order_id FROM person JOIN orders ON id = customer_id and age = $1",
        expected_types: vec![("$1", Some(DataType::Int32))],
        param_values: vec![ScalarValue::Int32(Some(10))]
    };

    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Prepare: "my_plan" [Int32]
      Projection: person.id, orders.order_id
        Inner Join:  Filter: person.id = orders.customer_id AND person.age = $1
          TableScan: person
          TableScan: orders
    ** Final Plan:
    Projection: person.id, orders.order_id
      Inner Join:  Filter: person.id = orders.customer_id AND person.age = Int32(10)
        TableScan: person
        TableScan: orders
    "#
    );
}

#[test]
fn test_infer_types_from_predicate() {
    let test = ParameterTest {
        sql: "SELECT id, age FROM person WHERE age = $1",
        expected_types: vec![("$1", Some(DataType::Int32))],
        param_values: vec![ScalarValue::Int32(Some(10))],
    };

    assert_snapshot!(
        test.run(),
        @r"
    ** Initial Plan:
    Projection: person.id, person.age
      Filter: person.age = $1
        TableScan: person
    ** Final Plan:
    Projection: person.id, person.age
      Filter: person.age = Int32(10)
        TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_infer_types_from_predicate() {
    let test = ParameterTest {
        sql: "PREPARE my_plan AS SELECT id, age FROM person WHERE age = $1",
        expected_types: vec![("$1", Some(DataType::Int32))],
        param_values: vec![ScalarValue::Int32(Some(10))],
    };
    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Prepare: "my_plan" [Int32]
      Projection: person.id, person.age
        Filter: person.age = $1
          TableScan: person
    ** Final Plan:
    Projection: person.id, person.age
      Filter: person.age = Int32(10)
        TableScan: person
    "#
    );
}

#[test]
fn test_infer_types_from_between_predicate() {
    let test = ParameterTest {
        sql: "SELECT id, age FROM person WHERE age BETWEEN $1 AND $2",
        expected_types: vec![
            ("$1", Some(DataType::Int32)),
            ("$2", Some(DataType::Int32)),
        ],
        param_values: vec![ScalarValue::Int32(Some(10)), ScalarValue::Int32(Some(30))],
    };

    assert_snapshot!(
        test.run(),
        @r"
    ** Initial Plan:
    Projection: person.id, person.age
      Filter: person.age BETWEEN $1 AND $2
        TableScan: person
    ** Final Plan:
    Projection: person.id, person.age
      Filter: person.age BETWEEN Int32(10) AND Int32(30)
        TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_infer_types_from_between_predicate() {
    let test = ParameterTest {
        sql: "PREPARE my_plan AS SELECT id, age FROM person WHERE age BETWEEN $1 AND $2",
        expected_types: vec![
            ("$1", Some(DataType::Int32)),
            ("$2", Some(DataType::Int32)),
        ],
        param_values: vec![ScalarValue::Int32(Some(10)), ScalarValue::Int32(Some(30))],
    };
    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Prepare: "my_plan" [Int32, Int32]
      Projection: person.id, person.age
        Filter: person.age BETWEEN $1 AND $2
          TableScan: person
    ** Final Plan:
    Projection: person.id, person.age
      Filter: person.age BETWEEN Int32(10) AND Int32(30)
        TableScan: person
    "#
    );
}

#[test]
fn test_infer_types_subquery() {
    let test = ParameterTest {
        sql: "SELECT id, age FROM person WHERE age = (select max(age) from person where id = $1)",
        expected_types: vec![("$1", Some(DataType::UInt32))],
        param_values: vec![ScalarValue::UInt32(Some(10))]
    };

    assert_snapshot!(
        test.run(),
        @r"
    ** Initial Plan:
    Projection: person.id, person.age
      Filter: person.age = (<subquery>)
        Subquery:
          Projection: max(person.age)
            Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
              Filter: person.id = $1
                TableScan: person
        TableScan: person
    ** Final Plan:
    Projection: person.id, person.age
      Filter: person.age = (<subquery>)
        Subquery:
          Projection: max(person.age)
            Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
              Filter: person.id = UInt32(10)
                TableScan: person
        TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_infer_types_subquery() {
    let test = ParameterTest {
        sql: "PREPARE my_plan AS SELECT id, age FROM person WHERE age = (select max(age) from person where id = $1)",
        expected_types: vec![("$1", Some(DataType::UInt32))],
        param_values: vec![ScalarValue::UInt32(Some(10))]
    };

    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Prepare: "my_plan" [UInt32]
      Projection: person.id, person.age
        Filter: person.age = (<subquery>)
          Subquery:
            Projection: max(person.age)
              Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
                Filter: person.id = $1
                  TableScan: person
          TableScan: person
    ** Final Plan:
    Projection: person.id, person.age
      Filter: person.age = (<subquery>)
        Subquery:
          Projection: max(person.age)
            Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
              Filter: person.id = UInt32(10)
                TableScan: person
        TableScan: person
    "#
    );
}

#[test]
fn test_update_infer() {
    let test = ParameterTest {
        sql: "update person set age=$1 where id=$2",
        expected_types: vec![
            ("$1", Some(DataType::Int32)),
            ("$2", Some(DataType::UInt32)),
        ],
        param_values: vec![ScalarValue::Int32(Some(42)), ScalarValue::UInt32(Some(1))],
    };

    assert_snapshot!(
        test.run(),
        @r"
    ** Initial Plan:
    Dml: op=[Update] table=[person]
      Projection: person.id AS id, person.first_name AS first_name, person.last_name AS last_name, $1 AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
        Filter: person.id = $2
          TableScan: person
    ** Final Plan:
    Dml: op=[Update] table=[person]
      Projection: person.id AS id, person.first_name AS first_name, person.last_name AS last_name, Int32(42) AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
        Filter: person.id = UInt32(1)
          TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_update_infer() {
    let test = ParameterTest {
        sql: "PREPARE my_plan AS update person set age=$1 where id=$2",
        expected_types: vec![
            ("$1", Some(DataType::Int32)),
            ("$2", Some(DataType::UInt32)),
        ],
        param_values: vec![ScalarValue::Int32(Some(42)), ScalarValue::UInt32(Some(1))],
    };

    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Prepare: "my_plan" [Int32, UInt32]
      Dml: op=[Update] table=[person]
        Projection: person.id AS id, person.first_name AS first_name, person.last_name AS last_name, $1 AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
          Filter: person.id = $2
            TableScan: person
    ** Final Plan:
    Dml: op=[Update] table=[person]
      Projection: person.id AS id, person.first_name AS first_name, person.last_name AS last_name, Int32(42) AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
        Filter: person.id = UInt32(1)
          TableScan: person
    "#
    );
}

#[test]
fn test_insert_infer() {
    let test = ParameterTest {
        sql: "insert into person (id, first_name, last_name) values ($1, $2, $3)",
        expected_types: vec![
            ("$1", Some(DataType::UInt32)),
            ("$2", Some(DataType::Utf8)),
            ("$3", Some(DataType::Utf8)),
        ],
        param_values: vec![
            ScalarValue::UInt32(Some(1)),
            ScalarValue::from("Alan"),
            ScalarValue::from("Turing"),
        ],
    };

    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Dml: op=[Insert Into] table=[person]
      Projection: column1 AS id, column2 AS first_name, column3 AS last_name, CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀
        Values: ($1, $2, $3)
    ** Final Plan:
    Dml: op=[Insert Into] table=[person]
      Projection: column1 AS id, column2 AS first_name, column3 AS last_name, CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀
        Values: (UInt32(1) AS $1, Utf8("Alan") AS $2, Utf8("Turing") AS $3)
    "#
    );
}

#[test]
fn test_prepare_statement_insert_infer() {
    let test = ParameterTest {
        sql: "PREPARE my_plan AS insert into person (id, first_name, last_name) values ($1, $2, $3)",
        expected_types: vec![
            ("$1", Some(DataType::UInt32)),
            ("$2", Some(DataType::Utf8)),
            ("$3", Some(DataType::Utf8)),
        ],
        param_values: vec![
            ScalarValue::UInt32(Some(1)),
            ScalarValue::from("Alan"),
            ScalarValue::from("Turing"),
        ]
    };
    assert_snapshot!(
        test.run(),
        @r#"
    ** Initial Plan:
    Prepare: "my_plan" [UInt32, Utf8, Utf8]
      Dml: op=[Insert Into] table=[person]
        Projection: column1 AS id, column2 AS first_name, column3 AS last_name, CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀
          Values: ($1, $2, $3)
    ** Final Plan:
    Dml: op=[Insert Into] table=[person]
      Projection: column1 AS id, column2 AS first_name, column3 AS last_name, CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀
        Values: (UInt32(1) AS $1, Utf8("Alan") AS $2, Utf8("Turing") AS $3)
    "#
    );
}

#[test]
fn test_prepare_statement_to_plan_one_param() {
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $1";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32]
      Projection: person.id, person.age
        Filter: person.age = $1
          TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[Int32]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];

    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: person.id, person.age
      Filter: person.age = Int32(10)
        TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_to_plan_data_type() {
    let sql = "PREPARE my_plan(DOUBLE) AS SELECT id, age  FROM person WHERE age = $1";

    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        // age is defined as Int32 but prepare statement declares it as DOUBLE/Float64
        // Prepare statement and its logical plan should be created successfully
        @r#"
    Prepare: "my_plan" [Float64]
      Projection: person.id, person.age
        Filter: person.age = $1
          TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[Float64]"#);

    ///////////////////
    // replace params with values still succeed and use Float64
    let param_values = vec![ScalarValue::Float64(Some(10.0))];

    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r"
    Projection: person.id, person.age
      Filter: person.age = Float64(10)
        TableScan: person
    "
    );
}

#[test]
fn test_prepare_statement_to_plan_multi_params() {
    let sql = "PREPARE my_plan(INT, STRING, DOUBLE, INT, DOUBLE, STRING) AS
        SELECT id, age, $6
        FROM person
        WHERE age IN ($1, $4) AND salary > $3 and salary < $5 OR first_name < $2";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32, Utf8, Float64, Int32, Float64, Utf8]
      Projection: person.id, person.age, $6
        Filter: person.age IN ([$1, $4]) AND person.salary > $3 AND person.salary < $5 OR person.first_name < $2
          TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[Int32, Utf8, Float64, Int32, Float64, Utf8]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Int32(Some(10)),
        ScalarValue::from("abc"),
        ScalarValue::Float64(Some(100.0)),
        ScalarValue::Int32(Some(20)),
        ScalarValue::Float64(Some(200.0)),
        ScalarValue::from("xyz"),
    ];

    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r#"
    Projection: person.id, person.age, Utf8("xyz") AS $6
      Filter: person.age IN ([Int32(10), Int32(20)]) AND person.salary > Float64(100) AND person.salary < Float64(200) OR person.first_name < Utf8("abc")
        TableScan: person
    "#
    );
}

#[test]
fn test_prepare_statement_to_plan_having() {
    let sql = "PREPARE my_plan(INT, DOUBLE, DOUBLE, DOUBLE) AS
        SELECT id, sum(age)
        FROM person \
        WHERE salary > $2
        GROUP BY id
        HAVING sum(age) < $1 AND sum(age) > 10 OR sum(age) in ($3, $4)\
        ";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int32, Float64, Float64, Float64]
      Projection: person.id, sum(person.age)
        Filter: sum(person.age) < $1 AND sum(person.age) > Int64(10) OR sum(person.age) IN ([$3, $4])
          Aggregate: groupBy=[[person.id]], aggr=[[sum(person.age)]]
            Filter: person.salary > $2
              TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[Int32, Float64, Float64, Float64]"#);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Int32(Some(10)),
        ScalarValue::Float64(Some(100.0)),
        ScalarValue::Float64(Some(200.0)),
        ScalarValue::Float64(Some(300.0)),
    ];

    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r#"
    Projection: person.id, sum(person.age)
      Filter: sum(person.age) < Int32(10) AND sum(person.age) > Int64(10) OR sum(person.age) IN ([Float64(200), Float64(300)])
        Aggregate: groupBy=[[person.id]], aggr=[[sum(person.age)]]
          Filter: person.salary > Float64(100)
            TableScan: person
    "#
    );
}

#[test]
fn test_prepare_statement_to_plan_limit() {
    let sql = "PREPARE my_plan(BIGINT, BIGINT) AS
        SELECT id FROM person \
        OFFSET $1 LIMIT $2";
    let (plan, dt) = generate_prepare_stmt_and_data_types(sql);
    assert_snapshot!(
        plan,
        @r#"
    Prepare: "my_plan" [Int64, Int64]
      Limit: skip=$1, fetch=$2
        Projection: person.id
          TableScan: person
    "#
    );
    assert_snapshot!(dt, @r#"[Int64, Int64]"#);

    // replace params with values
    let param_values = vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(200))];
    let plan_with_params = plan.with_param_values(param_values).unwrap();
    assert_snapshot!(
        plan_with_params,
        @r#"
    Limit: skip=10, fetch=200
      Projection: person.id
        TableScan: person
    "#
    );
}

#[test]
fn test_prepare_statement_unknown_list_param() {
    let sql = "SELECT id from person where id = $2";
    let plan = logical_plan(sql).unwrap();
    let param_values = ParamValues::List(vec![]);
    let err = plan.replace_params_with_values(&param_values).unwrap_err();
    assert_contains!(
        err.to_string(),
        "Error during planning: No value found for placeholder with id $2"
    );
}

#[test]
fn test_prepare_statement_unknown_hash_param() {
    let sql = "SELECT id from person where id = $bar";
    let plan = logical_plan(sql).unwrap();
    let param_values = ParamValues::Map(HashMap::new());
    let err = plan.replace_params_with_values(&param_values).unwrap_err();
    assert_contains!(
        err.to_string(),
        "Error during planning: No value found for placeholder with name $bar"
    );
}

#[test]
fn test_prepare_statement_bad_list_idx() {
    let sql = "SELECT id from person where id = $foo";
    let plan = logical_plan(sql).unwrap();
    let param_values = ParamValues::List(vec![]);

    let err = plan.replace_params_with_values(&param_values).unwrap_err();
    assert_contains!(err.to_string(), "Error during planning: Failed to parse placeholder id: invalid digit found in string");
}

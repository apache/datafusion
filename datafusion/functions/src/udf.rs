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

use arrow_udf::function;

#[function("eq(boolean, boolean) -> boolean")]
fn eq(lhs: bool, rhs: bool) -> bool {
    lhs == rhs
}

#[function("gcd(int, int) -> int", output = "eval_gcd")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};

    use arrow::{
        array::{BooleanArray, RecordBatch},
        datatypes::{Field, Schema},
    };
    use arrow_udf::sig::REGISTRY;

    #[test]
    fn test_eq() {
        let bool_field = Field::new("", arrow::datatypes::DataType::Boolean, false);
        let schema = Schema::new(vec![bool_field.clone()]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![true, false, true]))],
        )
        .unwrap();

        println!("Function signatures:");
        REGISTRY.iter().for_each(|sig| {
            println!("{:?}", sig.name);
            println!("{:?}", sig.arg_types);
            println!("{:?}", sig.return_type);
        });

        let eval_eq_boolean = REGISTRY
            .get("eq", &[bool_field.clone(), bool_field.clone()], &bool_field)
            .unwrap()
            .function
            .as_scalar()
            .unwrap();

        let result = eval_eq_boolean(&record_batch).unwrap();

        assert!(result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn test_gcd() {
        let int_field = Field::new("", arrow::datatypes::DataType::Int32, false);
        let schema = Schema::new(vec![int_field.clone(), int_field.clone()]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int32Array::from(vec![20, 30, 40])),
            ],
        )
        .unwrap();

        println!("Function signatures:");
        REGISTRY.iter().for_each(|sig| {
            println!("{:?}", sig.name);
            println!("{:?}", sig.arg_types);
            println!("{:?}", sig.return_type);
        });

        let eval_gcd_int = REGISTRY
            .get("gcd", &[int_field.clone(), int_field.clone()], &int_field)
            .unwrap()
            .function
            .as_scalar()
            .unwrap();

        let result = eval_gcd_int(&record_batch).unwrap();

        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            10
        );
    }
}

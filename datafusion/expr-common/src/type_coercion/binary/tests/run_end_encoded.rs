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

use super::*;
use DataType::*;

fn ree(value_type: DataType) -> DataType {
    RunEndEncoded(
        Arc::new(Field::new("run_ends", Int32, false)),
        Arc::new(Field::new("values", value_type, false)),
    )
}

#[test]
fn test_ree_type_coercion() {
    let lhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Int32, false)),
    );
    let rhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Int16, false)),
    );
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(Int32)
    );
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, false, comparison_coercion),
        Some(Int32)
    );

    // In comparison context, numeric is preferred over string
    let lhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Utf8, false)),
    );
    let rhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Int16, false)),
    );
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(Int16)
    );

    // Since we can coerce values of Utf8 to Binary can support this
    let lhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Utf8, false)),
    );
    let rhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Binary, false)),
    );
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(Binary)
    );
    let lhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Utf8, false)),
    );
    let rhs_type = Utf8;
    // Don't preserve REE
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, false, comparison_coercion),
        Some(Utf8)
    );
    // Preserve REE
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(lhs_type.clone())
    );

    let lhs_type = Utf8;
    let rhs_type = RunEndEncoded(
        Arc::new(Field::new("run_ends", Int8, false)),
        Arc::new(Field::new("values", Utf8, false)),
    );
    // Don't preserve REE
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, false, comparison_coercion),
        Some(Utf8)
    );
    // Preserve REE
    assert_eq!(
        ree_coercion(&lhs_type, &rhs_type, true, comparison_coercion),
        Some(rhs_type.clone())
    );
}

#[test]
fn test_ree_arithmetic_coercion() -> Result<()> {
    test_coercion_binary_rule!(ree(Int64), Int64, Operator::Plus, Int64);
    test_coercion_binary_rule!(Int64, ree(Int64), Operator::Multiply, Int64);
    test_coercion_binary_rule!(ree(Int32), ree(Int64), Operator::Plus, Int64);

    // Decimal unwrapping through math_decimal_coercion
    let (lhs, rhs) =
        BinaryTypeCoercer::new(&ree(Decimal128(10, 2)), &Operator::Plus, &Int32)
            .get_input_types()?;
    assert_eq!(lhs, Decimal128(10, 2));
    assert_eq!(rhs, Decimal128(10, 0));

    let (lhs, rhs) =
        BinaryTypeCoercer::new(&Int32, &Operator::Plus, &ree(Decimal128(10, 2)))
            .get_input_types()?;
    assert_eq!(lhs, Decimal128(10, 0));
    assert_eq!(rhs, Decimal128(10, 2));

    let result =
        BinaryTypeCoercer::new(&ree(Utf8), &Operator::Plus, &Int32).get_input_types();
    assert!(result.is_err());

    Ok(())
}

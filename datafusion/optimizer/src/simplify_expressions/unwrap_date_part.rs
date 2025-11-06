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

#[cfg(test)]
mod tests {
    use crate::simplify_expressions::ExprSimplifier;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::expr_fn::col;
    use datafusion_expr::{
        and, execution_props::ExecutionProps, lit, simplify::SimplifyContext, Expr,
    };
    use datafusion_functions::datetime::expr_fn;
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn test_unwrap_date_part_comparison() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("c1")).eq(lit(2024i32));
        let expected = and(
            col("c1").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
            col("c1").lt(lit(ScalarValue::Date32(Some(20088)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&props).with_schema(Arc::clone(schema)),
        );

        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![
                    Field::new("c1", DataType::Date32, false),
                    Field::new("c2", DataType::Date64, false),
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                    Field::new("ts_nano_utf", timestamp_nano_utc_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    // fn lit_timestamp_nano_none(ts: i64) -> Expr {
    //     lit(ScalarValue::TimestampNanosecond(Some(ts), None))
    // }

    // fn lit_timestamp_nano_utc(ts: i64) -> Expr {
    //     let utc = Some("+0:00".into());
    //     lit(ScalarValue::TimestampNanosecond(Some(ts), utc))
    // }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }
}

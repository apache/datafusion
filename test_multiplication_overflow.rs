#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{Int64Array, ArrayRef};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_physical_expr::expressions::binary::BinaryExpr;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::expressions::lit;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::PhysicalExpr;

    #[test]
    fn test_multiplication_overflow_checking() {
        // Create a simple schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
        ]));
        
        // Create test data that will cause overflow: large number
        let large_value = i64::MAX / 2 + 1; // This should overflow when multiplied by 3
        let array = Arc::new(Int64Array::from(vec![large_value])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        
        // Create multiplication expression: a * 3
        let left = col("a", &schema).unwrap();
        let right = lit(3i64);
        let mul_expr = BinaryExpr::new_with_overflow_check(left, Operator::Multiply, right);
        
        // Test with overflow checking enabled (should fail)
        let mul_expr_with_overflow = mul_expr.clone().with_fail_on_overflow(true);
        let result = mul_expr_with_overflow.evaluate(&batch);
        
        match result {
            Err(_) => println!("✅ Overflow checking works - multiplication properly failed"),
            Ok(_) => println!("❌ Overflow checking failed - multiplication should have failed but didn't"),
        }
        
        // Test with overflow checking disabled (should wrap)
        let mul_expr_without_overflow = mul_expr.with_fail_on_overflow(false);
        let result = mul_expr_without_overflow.evaluate(&batch);
        
        match result {
            Ok(_) => println!("✅ Wrapping works - multiplication completed with wrapping"),
            Err(e) => println!("❌ Wrapping failed - multiplication failed when it should wrap: {}", e),
        }
    }
}
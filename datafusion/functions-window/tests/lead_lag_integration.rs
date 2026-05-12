use std::sync::Arc;
use arrow::array::{Int32Array, RecordBatch, ArrayRef};
use arrow::datatypes::{Field, Schema};
use datafusion::prelude::*;
use datafusion::error::Result;

/// Helper to create a RecordBatch from columns
fn create_batch(
    columns: Vec<(&str, ArrayRef)>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        columns.iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect::<Vec<_>>()
    ));
    RecordBatch::try_new(schema, columns.into_iter().map(|(_, arr)| arr).collect()).unwrap()
}

#[tokio::test]
async fn test_lag_with_column_expression_mul_sql() -> Result<()> {
    // Create test data
    let batch = create_batch(vec![
        ("employee_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(3000), Some(2000), Some(3000), Some(4000),
            Some(5000), Some(6000), Some(7000), Some(8000),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    let sql = r#"
        SELECT 
            employee_id, 
            salary,
            lag(salary, 1, salary * employee_id) OVER (ORDER BY employee_id) AS prev_salary    
        FROM employees
        ORDER BY employee_id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    
    let prev_salary = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(3000), 
        Some(3000),
        Some(2000),
        Some(3000),
        Some(4000),
        Some(5000),
        Some(6000),
        Some(7000),
    ]);

    assert_eq!(prev_salary, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lag_with_partition_and_column_expression_sql() -> Result<()> {
    // Create test data
    let batch = create_batch(vec![
        ("employee_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(3000), Some(2000), Some(4000), Some(4000),
            Some(5000), Some(6000), Some(7000), Some(8000),
        ]))),
        ("department", Arc::new(Int32Array::from(vec![
            Some(1), Some(2), Some(1), Some(2),
            Some(3), Some(3), Some(1), Some(1),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    let sql = r#"
        SELECT 
            employee_id, 
            salary,
            lag(salary, 1, salary * employee_id) OVER (PARTITION BY department ORDER BY salary) AS prev_salary    
        FROM employees
        ORDER BY department
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    
    let prev_salary = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(3000), 
        Some(3000),
        Some(4000),
        Some(7000),
        Some(4000),
        Some(2000),
        Some(25000),
        Some(5000),
    ]);

    assert_eq!(prev_salary, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lead_with_scalar_default_sql() -> Result<()> {
    let batch = create_batch(vec![
        ("employee_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(1000), Some(2000), Some(3000), Some(4000),
            Some(5000), Some(6000), Some(7000), Some(8000),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    // Test LEAD with scalar default
    let sql = r#"
        SELECT 
            employee_id, 
            salary,
            lead(salary, 1, 0) OVER (ORDER BY employee_id) AS next_salary
        FROM employees
        ORDER BY employee_id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let next_salary = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(2000),
        Some(3000),
        Some(4000),
        Some(5000),
        Some(6000),
        Some(7000),
        Some(8000),
        Some(0),
    ]);

    assert_eq!(next_salary, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lag_with_column_default_sql() -> Result<()> {
    let batch = create_batch(vec![
        ("employee_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(1000), Some(2000), Some(3000), Some(4000),
            Some(5000), Some(6000), Some(7000), Some(8000),
        ]))),
        ("bonus", Arc::new(Int32Array::from(vec![
            Some(100), Some(200), Some(300), Some(400),
            Some(500), Some(600), Some(700), Some(800),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    let sql = r#"
        SELECT 
            employee_id, 
            salary,
            bonus,
            lag(salary, 1, bonus) OVER (ORDER BY employee_id) AS prev_salary_or_bonus
        FROM employees
        ORDER BY employee_id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let prev_salary_or_bonus = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(100),   // bonus for row 1 (no previous salary)
        Some(1000),  // previous salary
        Some(2000),
        Some(3000),
        Some(4000),
        Some(5000),
        Some(6000),
        Some(7000),
    ]);

    assert_eq!(prev_salary_or_bonus, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lead_with_column_default_sql() -> Result<()> {
    let batch = create_batch(vec![
        ("employee_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(1000), Some(2000), Some(3000), Some(4000),
            Some(5000), Some(6000), Some(7000), Some(8000),
        ]))),
        ("bonus", Arc::new(Int32Array::from(vec![
            Some(100), Some(200), Some(300), Some(400),
            Some(500), Some(600), Some(700), Some(800),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    // Test LEAD with column default
    let sql = r#"
        SELECT 
            employee_id, 
            salary,
            bonus,
            lead(salary, 1, bonus) OVER (ORDER BY employee_id) AS next_salary_or_bonus
        FROM employees
        ORDER BY employee_id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let next_salary_or_bonus = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(2000),
        Some(3000),
        Some(4000),
        Some(5000),
        Some(6000),
        Some(7000),
        Some(8000),
        Some(800),   // bonus for last row (no next salary)
    ]);

    assert_eq!(next_salary_or_bonus, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lag_with_partition_by_and_column_default_sql() -> Result<()> {
    let batch = create_batch(vec![
        ("department", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3]))),
        ("employee_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(1000), Some(2000), Some(3000), Some(4000),
            Some(5000), Some(6000), Some(7000), Some(8000),
        ]))),
        ("fallback_salary", Arc::new(Int32Array::from(vec![
            Some(999), Some(888), Some(777), Some(666),
            Some(555), Some(444), Some(333), Some(222),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    // Test LAG with PARTITION BY and column default
    let sql = r#"
        SELECT 
            department,
            employee_id, 
            salary,
            fallback_salary,
            lag(salary, 1, fallback_salary) OVER (
                PARTITION BY department 
                ORDER BY employee_id
            ) AS prev_salary
        FROM employees
        ORDER BY department, employee_id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let prev_salary = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Department 1: fallback for first row is 999
    // Department 2: fallback for first row is 666
    // Department 3: fallback for first row is 333
    let expected = Int32Array::from(vec![
        Some(999),   // dept 1, first row - gets fallback_salary
        Some(1000),  // dept 1, previous salary
        Some(2000),  // dept 1, previous salary
        Some(666),   // dept 2, first row - gets fallback_salary
        Some(4000),  // dept 2, previous salary
        Some(5000),  // dept 2, previous salary
        Some(333),   // dept 3, first row - gets fallback_salary
        Some(7000),  // dept 3, previous salary
    ]);

    assert_eq!(prev_salary, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lag_with_offset_and_column_default_sql() -> Result<()> {
    let batch = create_batch(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))),
        ("value", Arc::new(Int32Array::from(vec![
            Some(10), Some(20), Some(30), Some(40),
            Some(50), Some(60), Some(70), Some(80),
        ]))),
        ("backup", Arc::new(Int32Array::from(vec![
            Some(1), Some(2), Some(3), Some(4),
            Some(5), Some(6), Some(7), Some(8),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("data", batch)?;

    // Test LAG with offset 2 and column default
    let sql = r#"
        SELECT 
            id,
            value,
            backup,
            lag(value, 2, backup) OVER (ORDER BY id) AS lag2_value
        FROM data
        ORDER BY id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let lag2_value = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(1),    // backup for row 1 (offset 2, no row -2)
        Some(2),    // backup for row 2 (offset 2, no row -1)
        Some(10),   // value from row 1
        Some(20),   // value from row 2
        Some(30),   // value from row 3
        Some(40),   // value from row 4
        Some(50),   // value from row 5
        Some(60),   // value from row 6
    ]);

    assert_eq!(lag2_value, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lag_with_expression_default_sql() -> Result<()> {
    // Test with computed default: bonus * 2
    let batch = create_batch(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(100), Some(200), Some(300), Some(400), Some(500),
        ]))),
        ("bonus", Arc::new(Int32Array::from(vec![
            Some(10), Some(20), Some(30), Some(40), Some(50),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    // Test LAG with expression as default: bonus * 2
    let sql = r#"
        SELECT 
            id,
            salary,
            bonus,
            lag(salary, 1, bonus * 2) OVER (ORDER BY id) AS prev_salary_or_double_bonus
        FROM employees
        ORDER BY id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let result_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        Some(20),   // bonus*2 for first row (10*2)
        Some(100),  // previous salary
        Some(200),
        Some(300),
        Some(400),
    ]);

    assert_eq!(result_col, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lag_with_null_column_default_sql() -> Result<()> {
    // Test when default column has NULLs
    let batch = create_batch(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))),
        ("salary", Arc::new(Int32Array::from(vec![
            Some(100), Some(200), Some(300), Some(400), Some(500),
        ]))),
        ("default_val", Arc::new(Int32Array::from(vec![
            None, Some(999), None, Some(888), Some(777),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("employees", batch)?;

    let sql = r#"
        SELECT 
            id,
            salary,
            default_val,
            lag(salary, 1, default_val) OVER (ORDER BY id) AS prev_salary
        FROM employees
        ORDER BY id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    let prev_salary = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected = Int32Array::from(vec![
        None,       // default_val is NULL for first row
        Some(100),
        Some(200),
        Some(300),
        Some(400),
    ]);

    assert_eq!(prev_salary, &expected);
    Ok(())
}

#[tokio::test]
async fn test_lead_lag_mixed_with_column_defaults_sql() -> Result<()> {
    // Complex query using both LEAD and LAG with column defaults
    let batch = create_batch(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))),
        ("value", Arc::new(Int32Array::from(vec![
            Some(10), Some(20), Some(30), Some(40), Some(50),
        ]))),
        ("first_val", Arc::new(Int32Array::from(vec![
            Some(1), Some(2), Some(3), Some(4), Some(5),
        ]))),
        ("last_val", Arc::new(Int32Array::from(vec![
            Some(50), Some(40), Some(30), Some(20), Some(10),
        ]))),
    ]);

    let ctx = SessionContext::new();
    ctx.register_batch("data", batch)?;

    let sql = r#"
        SELECT 
            id,
            value,
            lag(value, 1, first_val) OVER (ORDER BY id) AS prev_value,
            lead(value, 1, last_val) OVER (ORDER BY id) AS next_value
        FROM data
        ORDER BY id
    "#;

    let result = ctx.sql(sql).await?.collect().await?;
    
    let batch = &result[0];
    
    let prev_value = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let next_value = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let expected_prev = Int32Array::from(vec![
        Some(1),    // first_val
        Some(10),
        Some(20),
        Some(30),
        Some(40),
    ]);

    let expected_next = Int32Array::from(vec![
        Some(20),
        Some(30),
        Some(40),
        Some(50),
        Some(10),   // last_val
    ]);

    assert_eq!(prev_value, &expected_prev);
    assert_eq!(next_value, &expected_next);
    Ok(())
}
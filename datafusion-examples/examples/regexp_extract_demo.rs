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

//! Example demonstrating the regexp_extract function in DataFusion
//! This function is equivalent to Spark's regexp_extract function

use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates how to use the regexp_extract function
/// which extracts substring(s) matching a regular expression from a string
#[tokio::main]
async fn main() -> Result<()> {
    // Create session context (regex functions are registered by default)
    let ctx = SessionContext::new();

    println!("DataFusion regexp_extract function demo");
    println!("========================================\n");

    // Create a test dataset with various string patterns
    let sql = r#"
        SELECT * FROM VALUES 
            ('100-200', 'email@domain.com', 'phone:(555) 123-4567'),
            ('300-400', 'user@example.org', 'phone:(888) 987-6543'),
            ('no-digits', 'invalid-email', 'no-phone'),
            ('50-75', 'admin@company.co.uk', 'phone:(111) 222-3333')
        AS t(numbers, email, phone)
    "#;

    println!("Test dataset:");
    let df = ctx.sql(sql).await?;
    df.show().await?;

    println!("\n1. Extract first and second capture groups from number range:");
    let sql = r#"
        SELECT 
            numbers,
            regexp_extract(numbers, '(\d+)-(\d+)', 1) as first_number,
            regexp_extract(numbers, '(\d+)-(\d+)', 2) as second_number,
            regexp_extract(numbers, '(\d+)-(\d+)', 0) as full_match
        FROM (
            SELECT * FROM VALUES 
                ('100-200'), ('300-400'), ('no-digits'), ('50-75')
            AS t(numbers)
        )
    "#;
    let df = ctx.sql(sql).await?;
    df.show().await?;

    println!("\n2. Extract parts from email addresses:");
    let sql = r#"
        SELECT 
            email,
            regexp_extract(email, '([^@]+)@([^.]+)\.(.+)', 1) as username,
            regexp_extract(email, '([^@]+)@([^.]+)\.(.+)', 2) as domain,
            regexp_extract(email, '([^@]+)@([^.]+)\.(.+)', 3) as tld
        FROM (
            SELECT * FROM VALUES 
                ('email@domain.com'), 
                ('user@example.org'), 
                ('admin@company.co.uk'),
                ('invalid-email')
            AS t(email)
        )
    "#;
    let df = ctx.sql(sql).await?;
    df.show().await?;

    println!("\n3. Extract area code from phone numbers:");
    let sql = r#"
        SELECT 
            phone,
            regexp_extract(phone, 'phone:\((\d{3})\)', 1) as area_code,
            regexp_extract(phone, 'phone:\(\d{3}\) (\d{3})-(\d{4})', 1) as exchange,
            regexp_extract(phone, 'phone:\(\d{3}\) (\d{3})-(\d{4})', 2) as number
        FROM (
            SELECT * FROM VALUES 
                ('phone:(555) 123-4567'), 
                ('phone:(888) 987-6543'), 
                ('phone:(111) 222-3333'),
                ('no-phone')
            AS t(phone)
        )
    "#;
    let df = ctx.sql(sql).await?;
    df.show().await?;

    println!("\n4. Test edge cases:");
    let sql = r#"
        SELECT 
            test_string,
            regexp_extract(test_string, '(\d+)', 1) as extract_digits,
            regexp_extract(test_string, '(\d+)', 0) as full_match,
            regexp_extract(test_string, '(\d+)', 99) as invalid_group,
            regexp_extract(test_string, '(\d+)', -1) as negative_group
        FROM (
            SELECT * FROM VALUES 
                ('abc123def'), 
                ('no-digits'), 
                (''), 
                ('456')
            AS t(test_string)
        )
    "#;
    let df = ctx.sql(sql).await?;
    df.show().await?;

    println!("\n5. Complex example with multiple extractions:");
    let sql = r#"
        SELECT 
            'user@example.com' as email,
            'Price: $99.50' as price,
            regexp_extract('user@example.com', '([^@]+)@.*', 1) as username,
            regexp_extract('user@example.com', '[^@]+@([^.]+)\..*', 1) as domain,
            regexp_extract('Price: $99.50', 'Price: \$(\d+)\.(\d+)', 1) as price_dollars,
            regexp_extract('Price: $99.50', 'Price: \$(\d+)\.(\d+)', 2) as price_cents
    "#;
    let df = ctx.sql(sql).await?;
    df.show().await?;

    println!("\nregexp_extract function demo completed successfully!");
    Ok(())
}

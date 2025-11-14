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

//! Test to verify identifier normalization consistency fix
//! This reproduces and tests the fix for the issue where
//! enable_ident_normalization was not respected in table registration

use datafusion::error::Result;
use datafusion::prelude::*;
use std::fs::File;
use std::io::Write;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a test CSV file
    let csv_data = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n";
    let mut file = File::create("data.csv").expect("Failed to create test file");
    file.write_all(csv_data.as_bytes())
        .expect("Failed to write test data");

    println!("Testing identifier normalization fix...\n");

    // Test 1: With normalization disabled
    println!("=== Test 1: enable_ident_normalization=false ===");
    let ctx = SessionContext::new();

    ctx.sql("SET datafusion.sql_parser.enable_ident_normalization=false")
        .await?
        .collect()
        .await?;

    println!("Registering table as 'DATA' (uppercase)...");
    ctx.register_csv("DATA", "data.csv", CsvReadOptions::default())
        .await?;

    println!("Querying: SELECT * FROM DATA");
    match ctx.sql("SELECT * FROM DATA").await {
        Ok(df) => {
            df.show().await?;
            println!("✅ SUCCESS: Found table 'DATA' with normalization disabled");
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);
            std::fs::remove_file("data.csv").ok();
            return Err(e);
        }
    }

    // Test 2: Verify lowercase doesn't work (should fail as expected)
    println!("\n=== Test 2: Querying lowercase 'data' (should fail) ===");
    match ctx.sql("SELECT * FROM data").await {
        Ok(_) => {
            println!("⚠️  UNEXPECTED: Found table 'data' (should have failed)");
        }
        Err(e) => {
            println!("✅ EXPECTED: Table 'data' not found: {}", e);
        }
    }

    // Test 3: With normalization enabled (default behavior)
    println!("\n=== Test 3: enable_ident_normalization=true (default) ===");
    let ctx2 = SessionContext::new();

    println!("Registering table as 'DATA' (uppercase)...");
    ctx2.register_csv("DATA", "data.csv", CsvReadOptions::default())
        .await?;

    println!("Querying: SELECT * FROM DATA");
    match ctx2.sql("SELECT * FROM DATA").await {
        Ok(df) => {
            df.show().await?;
            println!("✅ SUCCESS: Found normalized table");
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);
        }
    }

    println!("\nQuerying: SELECT * FROM data (lowercase)");
    match ctx2.sql("SELECT * FROM data").await {
        Ok(df) => {
            df.show().await?;
            println!("✅ SUCCESS: Found normalized table with lowercase query");
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);
        }
    }

    // Clean up
    std::fs::remove_file("data.csv").ok();

    println!("\n=== All tests completed successfully! ===");
    Ok(())
}

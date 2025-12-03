# DataFusion Contributor Learning Guide

Welcome! This guide is designed to help you transition from understanding buffer managers to becoming a DataFusion contributor and eventually a committer. DataFusion is an extensible query engine written in Rust that uses Apache Arrow as its in-memory format.

## Prerequisites

Before diving into DataFusion, ensure you have:
- Rust installed (use rustup: https://rustup.rs/)
- Basic understanding of Git and GitHub
- Familiarity with buffer management concepts (which you already have!)

## Phase 1: Database Fundamentals (2-3 weeks)

Since you understand buffer management, you're already familiar with one layer of the database stack. Let's build on that foundation.

### 1.1 Query Processing Pipeline

**Read these specific chapters/sections:**
- Book: "Database System Concepts" by Silberschatz - Chapter 15 (Query Processing) and Chapter 16 (Query Optimization)
- Alternative free resource: CMU 15-445 Lecture Notes on Query Processing: https://15445.courses.cs.cmu.edu/fall2023/notes/12-queryexecution1.pdf

**Key concepts to understand:**
- Query parsing and Abstract Syntax Trees (AST)
- Logical query plans vs Physical query plans
- Iterator model vs Vectorized execution (DataFusion uses vectorized)
- Cost-based optimization

### 1.2 Columnar Storage and Arrow Format

**Essential reading:**
- Apache Arrow Columnar Format Guide: https://arrow.apache.org/docs/format/Columnar.html
- "The Design and Implementation of Modern Column-Oriented Database Systems" - Sections 1-3: http://db.csail.mit.edu/pubs/abadi-column-stores.pdf

**Hands-on exercise:**
```rust
// Create a simple Rust project to work with Arrow arrays
// cargo new arrow_basics && cd arrow_basics
// Add to Cargo.toml: arrow = "53.0"
```

### 1.3 SQL Fundamentals

**Focus areas:**
- SQL query structure (SELECT, JOIN, GROUP BY, WINDOW functions)
- Common Table Expressions (CTEs)
- Aggregate functions

**Practice resource:**
- SQLZoo for interactive SQL practice: https://sqlzoo.net/
- Focus on JOIN and aggregation sections

## Phase 2: Understanding DataFusion Architecture (2-3 weeks)

### 2.1 Core Components

Study these DataFusion modules in order:

1. **datafusion/expr** - Expression system
   - Read: `datafusion/expr/src/expr.rs`
   - Understand how SQL expressions are represented

2. **datafusion/sql** - SQL parsing
   - Read: `datafusion/sql/src/parser.rs`
   - See how SQL text becomes logical plans

3. **datafusion/core** - Query execution
   - Read: `datafusion/core/src/physical_planner.rs`
   - Understand logical to physical plan conversion

4. **datafusion/physical-plan** - Execution operators
   - Start with: `datafusion/physical-plan/src/filter.rs`
   - Then: `datafusion/physical-plan/src/projection.rs`

### 2.2 Execution Model

**Key files to study:**
- `datafusion/physical-plan/src/stream.rs` - Streaming execution
- `datafusion/physical-plan/src/memory.rs` - In-memory operations

**Create a learning example:**
```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Register a CSV file as a table
    ctx.register_csv("example", "test.csv", CsvReadOptions::new()).await?;
    
    // Execute a query
    let df = ctx.sql("SELECT * FROM example WHERE value > 10").await?;
    df.show().await?;
    
    Ok(())
}
```

### 2.3 Understanding the Plan Hierarchy

**Study the transformation pipeline:**
1. SQL String → AST (via sqlparser-rs)
2. AST → Logical Plan
3. Logical Plan → Optimized Logical Plan
4. Optimized Logical Plan → Physical Plan
5. Physical Plan → Execution

**Debugging tip:** Use `EXPLAIN` to see these plans:
```sql
EXPLAIN SELECT * FROM table;
EXPLAIN VERBOSE SELECT * FROM table;
```

## Phase 3: Hands-On Learning (3-4 weeks)

### 3.1 Run DataFusion Examples

```bash
cd datafusion/datafusion-examples
cargo run --example dataframe
cargo run --example sql_query
cargo run --example custom_datasource
```

### 3.2 Your First Contributions

**Start with documentation and tests:**

1. Find typos or unclear documentation in `/docs`
2. Add test cases for existing functionality
3. Improve error messages

**Good first issues to tackle:**
- Look for issues labeled "good first issue": https://github.com/apache/datafusion/labels/good%20first%20issue
- Start with issues in these areas:
  - Adding SQL function implementations
  - Improving error messages
  - Adding test coverage

### 3.3 Implement a Simple Scalar Function

Create your own scalar UDF (User Defined Function):

```rust
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::common::Result;
use datafusion::logical_expr::{Volatility, create_udf};
use std::sync::Arc;

// Implement a function that doubles an integer
fn double_int(args: &[ArrayRef]) -> Result<ArrayRef> {
    let input = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let result: Int64Array = input.iter()
        .map(|v| v.map(|x| x * 2))
        .collect();
    Ok(Arc::new(result))
}

// Register it with DataFusion
let double_udf = create_udf(
    "double",
    vec![DataType::Int64],
    Arc::new(DataType::Int64),
    Volatility::Immutable,
    Arc::new(double_int),
);
```

## Phase 4: Deep Dive Topics (4-6 weeks)

### 4.1 Query Optimization

**Study these optimizer rules in order:**
1. `datafusion/optimizer/src/push_down_filter.rs` - Filter pushdown
2. `datafusion/optimizer/src/push_down_projection.rs` - Projection pushdown
3. `datafusion/optimizer/src/common_subexpr_eliminate.rs` - CSE

**Exercise:** Implement a simple optimizer rule that removes redundant sorts.

### 4.2 Aggregation and Grouping

**Essential files:**
- `datafusion/physical-plan/src/aggregates/mod.rs`
- `datafusion/functions-aggregate/src/sum.rs`
- `datafusion/functions-aggregate/src/average.rs`

**Concepts to master:**
- Hash aggregation vs Sort aggregation
- Partial aggregation
- Window functions

### 4.3 Join Algorithms

**Study implementations:**
- `datafusion/physical-plan/src/joins/hash_join.rs`
- `datafusion/physical-plan/src/joins/sort_merge_join.rs`
- `datafusion/physical-plan/src/joins/nested_loop_join.rs`

**Understand trade-offs:**
- Memory usage vs performance
- When to use each join type
- Join reordering optimization

## Phase 5: Advanced Contributions (Ongoing)

### 5.1 Performance Optimization

**Learn to benchmark:**
```bash
cd datafusion/benchmarks
cargo bench --bench aggregate_query_sql
```

**Profile DataFusion:**
- Use `cargo-flamegraph` for profiling
- Learn to read and interpret flame graphs
- Identify bottlenecks in query execution

### 5.2 Contributing New Features

**Process for larger contributions:**

1. **Discuss first:** Open an issue describing your proposed feature
2. **Design document:** For complex features, write a brief design doc
3. **Incremental PRs:** Break large features into smaller PRs
4. **Tests and docs:** Always include comprehensive tests and documentation

### 5.3 Areas for Specialization

Choose an area to become an expert in:

- **SQL Compatibility:** Implement missing SQL functions or syntax
- **Performance:** Work on vectorization, SIMD optimizations
- **File Formats:** Add support for new formats or improve existing ones
- **Distributed Execution:** Work on Ballista (distributed DataFusion)
- **Optimizer Rules:** Develop new optimization strategies

## Best Practices for Contributors

### Code Review

- Review others' PRs to learn the codebase
- Provide constructive feedback
- Test PRs locally before approving

### Communication

- Join Discord: https://discord.com/invite/Qw5gKqHxUM
- Participate in discussions on GitHub issues
- Attend community meetings (check the Apache DataFusion calendar)

### Testing

Always include tests for your changes:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_your_feature() {
        // Your test here
    }
}
```

### Documentation

- Add doc comments to public APIs
- Update relevant documentation in `/docs`
- Include examples in your doc comments

## Path to Becoming a Committer

1. **Consistent Contributions:** Make regular, high-quality contributions over 6-12 months
2. **Code Reviews:** Actively review others' PRs
3. **Community Involvement:** Help others in Discord/GitHub issues
4. **Domain Expertise:** Become an expert in specific areas of the codebase
5. **Trust Building:** Demonstrate good judgment and commitment to the project

## Recommended Learning Projects

### Week 1-2: Setup and First PR
- Set up development environment
- Run all tests locally
- Submit a documentation fix or typo correction

### Week 3-4: Simple Function Implementation
- Implement a missing SQL scalar function
- Add comprehensive tests
- Document the function

### Week 5-8: Optimizer Rule
- Study existing optimizer rules
- Implement a simple optimization (e.g., constant folding improvement)
- Benchmark the improvement

### Week 9-12: Performance Improvement
- Profile DataFusion on TPC-H queries
- Identify a bottleneck
- Implement and benchmark an optimization

### Month 4+: Major Feature
- Propose a significant feature or improvement
- Work with maintainers on design
- Implement with multiple PRs

## Resources and References

### Essential Documentation
- DataFusion User Guide: https://datafusion.apache.org/user-guide/
- DataFusion API Docs: https://docs.rs/datafusion/latest/datafusion/
- Arrow Rust Documentation: https://docs.rs/arrow/latest/arrow/

### Learning Materials
- CMU Database Course: https://15445.courses.cs.cmu.edu/
- Designing Data-Intensive Applications by Martin Kleppmann (Chapters 2, 3, 7)
- Modern B-Tree Techniques by Goetz Graefe: https://w6113.github.io/files/papers/btreesurvey-graefe.pdf

### Community Resources
- GitHub Issues: https://github.com/apache/datafusion/issues
- Discord: https://discord.com/invite/Qw5gKqHxUM
- Mailing List: dev@datafusion.apache.org

### Development Tools
- Rust Analyzer (VS Code extension)
- cargo-watch for auto-recompilation
- cargo-expand to see macro expansions
- flamegraph for performance profiling

## Quick Reference Commands

```bash
# Build DataFusion
cargo build

# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run benchmarks
cargo bench

# Check formatting
cargo fmt -- --check

# Run clippy
cargo clippy --all-targets --workspace -- -D warnings

# Build documentation
cargo doc --open

# Run a specific example
cargo run --example dataframe
```

## Final Tips

1. **Start Small:** Don't try to understand everything at once. Focus on one component at a time.
2. **Ask Questions:** The community is friendly and helpful. Don't hesitate to ask in Discord or GitHub.
3. **Read Code Daily:** Spend 30 minutes daily reading DataFusion code, even if you don't understand everything.
4. **Write Code:** The best way to learn is by doing. Write small programs using DataFusion.
5. **Be Patient:** Becoming proficient takes time. Most contributors take 3-6 months to feel comfortable with the codebase.

Good luck on your journey to becoming a DataFusion contributor! Remember, every expert was once a beginner, and the DataFusion community values all contributions, no matter how small.
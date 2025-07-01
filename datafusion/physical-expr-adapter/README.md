# DataFusion Physical Expression Adapter

This crate provides physical expression schema adaptation utilities for DataFusion that allow adapting a `PhysicalExpr` to different schema types.
This handles cases such as `lit(SclarValue::Int32(123)) = int64_column` by rewriting it to `lit(SclarValue::Int32(123)) = cast(int64_column, 'Int32')`
(note: this does not attempt to then simplify such expressions, that is done by shared simplifiers).

## Overview

The `PhysicalExprSchemaRewriter` allows rewriting physical expressions to match different schemas, including:

- Type casting when logical and physical schemas have different types
- Handling missing columns by inserting null literals
- Struct support: expressions such as `struct_column.field_that_is_missing_in_schema` get rewritten to `null`
- Partition column replacement with literal values

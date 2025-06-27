# DataFusion Physical Expression Adapter

This crate provides physical expression schema adaptation utilities for DataFusion, with support for complex nested struct type handling.

## Overview

The `PhysicalExprSchemaRewriter` allows rewriting physical expressions to match different schemas, including:

- Type casting when logical and physical schemas have different types
- Handling missing columns by inserting null literals
- Struct support: Recursive rebuilding of struct expressions with proper field mapping
- Partition column replacement with literal values

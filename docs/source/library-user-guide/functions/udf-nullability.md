<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Aggregate UDF Nullability

## Overview

DataFusion distinguishes between the nullability of aggregate function outputs and their input fields. This document explains how nullability is computed for aggregate User Defined Functions (UDAFs) and how to correctly specify it in your custom functions.

## The Change: From `is_nullable()` to `return_field()`

### What Changed?

In earlier versions of DataFusion, aggregate function nullability was controlled by the `is_nullable()` method on `AggregateUDFImpl`. This method returned a simple boolean indicating whether the function could ever return `NULL`, regardless of input characteristics.

**This approach has been deprecated** in favor of computing nullability more accurately via the `return_field()` method, which has access to the actual field metadata of the inputs. This allows for more precise nullability inference based on whether input fields are nullable.

### Why the Change?

1. **Input-aware nullability**: The new approach allows the function's nullability to depend on the nullability of its inputs. For example, `MIN(column)` should only be nullable if `column` is nullable.

2. **Consistency with scalar UDFs**: Scalar UDFs already follow this pattern using `return_field_from_args()`, and aggregate UDFs now align with this design.

3. **More accurate schema inference**: Query optimizers and executors can now make better decisions about whether intermediate or final results can be null.

## How Nullability Works Now

By default, the `return_field()` method in `AggregateUDFImpl` computes the output field using this logic:

```text
output_is_nullable = ANY input field is nullable
```

In other words:
- **If ALL input fields are non-nullable**, the output is **non-nullable**
- **If ANY input field is nullable**, the output is **nullable**

This default behavior works well for most aggregate functions like `MIN`, `MAX`, `SUM`, and `AVG`.

## Implementing Custom Aggregate UDFs

### Default Behavior (Recommended)

For most aggregate functions, you don't need to override `return_field()`. The default implementation will correctly infer nullability from inputs:

```rust
use std::sync::Arc;
use std::any::Any;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;

#[derive(Debug)]
struct MyAggregateFunction {
    signature: Signature,
}

impl MyAggregateFunction {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for MyAggregateFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "my_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    // Don't override return_field() - let the default handle nullability
    // The default will make the output nullable if any input is nullable

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Implementation...
        # unimplemented!()
    }

    // ... other required methods
}
```

### Custom Nullability (Advanced)

If your function has special nullability semantics, you can override `return_field()`:

```rust
use std::sync::Arc;
use arrow::datatypes::Field;

impl AggregateUDFImpl for MyAggregateFunction {
    // ... other methods ...

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types: Vec<_> = arg_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect();
        let data_type = self.return_type(&arg_types)?;

        // Example: COUNT always returns non-nullable i64, regardless of input
        let is_nullable = false; // COUNT never returns NULL

        Ok(Arc::new(Field::new(self.name(), data_type, is_nullable)))
    }
}
```

## Migration Guide

If you have existing code that uses `is_nullable()`, here's how to migrate:

### Before (Deprecated)

```rust
impl AggregateUDFImpl for MyFunction {
    fn is_nullable(&self) -> bool {
        // Only returns true or false, independent of inputs
        true
    }
}
```

### After (Recommended)

**Option 1: Use the default (simplest)**

```rust
impl AggregateUDFImpl for MyFunction {
    // Remove is_nullable() entirely
    // The default return_field() will compute nullability from inputs
}
```

**Option 2: Override return_field() for custom logic**

```rust
impl AggregateUDFImpl for MyFunction {
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types: Vec<_> = arg_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect();
        let data_type = self.return_type(&arg_types)?;

        // Your custom nullability logic here
        let is_nullable = arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(self.name(), data_type, is_nullable)))
    }
}
```

## Examples

### Example 1: MIN Function

`MIN` returns the smallest value from a group. It should be nullable if and only if its input is nullable:

```rust
impl AggregateUDFImpl for MinFunction {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    // No need to override return_field() - the default handles it correctly:
    // - If input is nullable, output is nullable
    // - If input is non-nullable, output is non-nullable

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Implementation...
    }
}
```

### Example 2: COUNT Function

`COUNT` always returns a non-nullable integer, regardless of input nullability:

```rust
impl AggregateUDFImpl for CountFunction {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    // Override return_field to always return non-nullable
    fn return_field(&self, _arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Implementation...
    }
}
```

## Deprecation Timeline

- **Deprecated in v42.0.0**: The `is_nullable()` method in `AggregateUDFImpl` is now deprecated
- **Will be removed in a future version**: `is_nullable()` will be removed entirely

During the deprecation period, both `is_nullable()` and the new `return_field()` approach work, but new code should use `return_field()`.

## Troubleshooting

### Issue: "Function `X` uses deprecated `is_nullable()`"

**Solution**: Remove the `is_nullable()` implementation and let the default `return_field()` handle nullability, or override `return_field()` directly.

### Issue: "Output field nullability is incorrect"

**Check**:
1. Are your input fields correctly marked as nullable/non-nullable?
2. Does your function need custom nullability logic? If so, override `return_field()`.

### Issue: "Tests fail with null value where non-null expected"

**Check**:
1. Verify that your function's accumulator actually returns a non-null default value when the input is empty and your function declares non-nullable output
2. Override `return_field()` to adjust the nullability if needed

## See Also

- [Adding User Defined Functions](adding-udfs.md) - General guide to implementing UDFs
- [Scalar UDF Nullability](#) - Similar concepts for scalar UDFs (which already use `return_field_from_args()`)

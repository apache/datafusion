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

# Schema Adapter and Column Casting

DataFusion's `SchemaAdapter` maps `RecordBatch`es produced by a data source to the
schema expected by a query. When a field exists in both schemas but their types
differ, the adapter invokes [`cast_column`](../../../datafusion/common/src/nested_struct.rs)
to coerce the column to the target [`Field`] type. `cast_column` recursively
handles nested `Struct` values, inserting `NULL` arrays for fields that are
missing in the source and ignoring extra fields.

## Casting structs with nullable fields

```rust
use std::sync::Arc;
use arrow::array::{Int32Array, StructArray};
use arrow::datatypes::{DataType, Field};
use datafusion_common::nested_struct::cast_column;

// source schema: { info: { id: Int32 } }
let source_field = Field::new(
    "info",
    DataType::Struct(vec![Field::new("id", DataType::Int32, true)].into()),
    false,
);

let target_field = Field::new(
    "info",
    DataType::Struct(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Int32, true),
    ].into()),
    true,
);

let id = Arc::new(Int32Array::from(vec![Some(1), None])) as _;
let source_struct = StructArray::from(vec![(source_field.children()[0].clone(), id)]);
let casted = cast_column(&Arc::new(source_struct) as _, &target_field).unwrap();
assert_eq!(casted.data_type(), target_field.data_type());
```

The new `score` field is filled with `NULL` and `id` is promoted to a nullable
`Int64`, demonstrating how nested casting can reconcile schema differences.

## Pitfalls and performance

- **Field name mismatches**: only matching field names are cast. Extra source
  fields are dropped and missing target fields are filled with `NULL`.
- **Non-struct sources**: attempting to cast a non-struct array to a struct
  results in an error.
- **Nested cost**: each level of nesting requires building new arrays. Deep or
  wide structs can increase memory use and CPU time, so avoid unnecessary
  casting in hot paths.

[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html

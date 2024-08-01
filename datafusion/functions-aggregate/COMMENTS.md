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

# Why Is List Item Always Nullable?

## Motivation

There were independent proposals to make the `nullable` setting of list
items in accumulator state be configurable. This meant adding additional
fields which captured the `nullable` setting from schema in planning for
the first argument to the aggregation function, and the returned value.

These fields were to be added to `StateFieldArgs`. But then we found out
that aggregate computation does not depend on it, and it can be avoided.

This document exists to make that reasoning explicit.

## Background

The list data type is used in the accumulator state for a few aggregate
functions like:

- `sum`
- `count`
- `array_agg`
- `bit_and`, `bit_or` and `bit_xor`
- `nth_value`

In all of the above cases the data type of the list item is equivalent
to either the first argument of the aggregate function or the returned
value.

For example, in `array_agg` the data type of item is equivalent to the
first argument and the definition looks like this:

```rust
// `args`       : `StateFieldArgs`
// `input_type` :  data type of the first argument
let mut fields = vec![Field::new_list(
    format_state_name(self.name(), "nth_value"),
    Field::new("item", args.input_types[0].clone(), true /* nullable of list item */ ),
    false, // nullable of list itself
)];
```

For all the aggregates listed above, the list item is always defined as
nullable.

## Computing Intermediate State

By setting `nullable` (of list item) to be always `true` like this we
ensure that the aggregate computation works even when nulls are
present. The advantage of doing it this way is that it eliminates the
need for additional code and special treatment of nulls in the
accumulator state.

## Nullable Of List Itself

The `nullable` of list itself depends on the aggregate. In the case of
`array_agg` the list is nullable(`true`), meanwhile for `sum` the list
is not nullable(`false`).

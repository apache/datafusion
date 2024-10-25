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

# Special Functions

- [unnest](#unnest)
- [unnest(struct)](#unnest-struct)

### `unnest`

Transforms an array into rows.

#### Arguments

- **array**: Array expression to unnest.
  Can be a constant, column, or function, and any combination of array operators.

#### Examples

```
> select unnest(make_array(1, 2, 3, 4, 5));
+------------------------------------------------------------------+
| unnest(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5))) |
+------------------------------------------------------------------+
| 1                                                                |
| 2                                                                |
| 3                                                                |
| 4                                                                |
| 5                                                                |
+------------------------------------------------------------------+
```

```
> select unnest(range(0, 10));
+-----------------------------------+
| unnest(range(Int64(0),Int64(10))) |
+-----------------------------------+
| 0                                 |
| 1                                 |
| 2                                 |
| 3                                 |
| 4                                 |
| 5                                 |
| 6                                 |
| 7                                 |
| 8                                 |
| 9                                 |
+-----------------------------------+
```

### `unnest (struct)`

Unwraps struct fields into columns.

#### Arguments

- **struct**: Object expression to unnest.
  Can be a constant, column, or function, and any combination of object operators.

#### Examples

```
> select * from foo;
+---------------------+
| column1             |
+---------------------+
| {a: 5, b: a string} |
+---------------------+

> select unnest(column1) from foo;
+-----------------------+-----------------------+
| unnest(foo.column1).a | unnest(foo.column1).b |
+-----------------------+-----------------------+
| 5                     | a string              |
+-----------------------+-----------------------+
```

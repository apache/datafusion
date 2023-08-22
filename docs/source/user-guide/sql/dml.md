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

# DML

## COPY

Copy a table to file(s). Supported file formats are `parquet`, `csv`, and `json`.

<pre>
COPY <i><b>table_name</i></b> TO '<i><b>file_name</i></b>' [ ( <i><b>option</i></b> [, ... ] ) ]

where <i><b>option</i></b> can be one of:
    FORMAT <i><b>format_name</i></b>
    PER_THREAD_OUTPUT <i><b>boolean</i></b>
    ROW_GROUP_SIZE <i><b>integer</i></b>
    ROW_GROUP_LIMIT_BYTES <i><b>integer</i></b>
</pre>

```sql
> COPY source_table TO 'table' (FORMAT parquet, PER_THREAD_OUTPUT true);
+-------+
| count |
+-------+
| 2     |
+-------+
```

## INSERT

Insert values into a table.

<pre>
INSERT INTO <i><b>table_name</i></b> { VALUES ( <i><b>expression</i></b> [, ...] ) [, ...] | <i><b>query</i></b> }
</pre>

```sql
> INSERT INTO source_table VALUES (1, 'Foo'), (2, 'Bar');
+-------+
| count |
+-------+
| 2     |
+-------+
```

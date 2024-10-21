#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SOURCE_DIR}/../" && pwd


TARGET_FILE="docs/source/user-guide/sql/aggregate_functions_new.md"
PRINT_AGGREGATE_FUNCTION_DOCS_COMMAND="cargo run --manifest-path datafusion/core/Cargo.toml --bin print_functions_docs -- aggregate"

echo "Inserting header"
cat <<'EOF' > "$TARGET_FILE"
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

<!---
This file was generated by the dev/update_function_docs.sh script.
Do not edit it manually as changes will be overwritten.
Instead, edit the AggregateUDFImpl's documentation() function to
update documentation for an individual UDF or the
dev/update_function_docs.sh file for updating surrounding text.
-->

# Aggregate Functions (NEW)

Note: this documentation is in the process of being migrated to be  [automatically created from the codebase].
Please see the [Aggregate Functions (old)](aggregate_functions.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

Aggregate functions operate on a set of values to compute a single result.
EOF

echo "Running CLI and inserting aggregate function docs table"
$PRINT_AGGREGATE_FUNCTION_DOCS_COMMAND >> "$TARGET_FILE"

echo "Running prettier"
npx prettier@2.3.2 --write "$TARGET_FILE"

echo "'$TARGET_FILE' successfully updated!"

TARGET_FILE="docs/source/user-guide/sql/scalar_functions_new.md"
PRINT_SCALAR_FUNCTION_DOCS_COMMAND="cargo run --manifest-path datafusion/core/Cargo.toml --bin print_functions_docs -- scalar"

echo "Inserting header"
cat <<'EOF' > "$TARGET_FILE"
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

<!---
This file was generated by the dev/update_function_docs.sh script.
Do not edit it manually as changes will be overwritten.
Instead, edit the ScalarUDFImpl's documentation() function to
update documentation for an individual UDF or the
dev/update_function_docs.sh file for updating surrounding text.
-->

# Scalar Functions (NEW)

Note: this documentation is in the process of being migrated to be [automatically created from the codebase].
Please see the [Scalar Functions (old)](aggregate_functions.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

EOF

echo "Running CLI and inserting scalar function docs table"
$PRINT_SCALAR_FUNCTION_DOCS_COMMAND >> "$TARGET_FILE"

echo "Running prettier"
npx prettier@2.3.2 --write "$TARGET_FILE"

echo "'$TARGET_FILE' successfully updated!"

TARGET_FILE="docs/source/user-guide/sql/window_functions_new.md"
PRINT_WINDOW_FUNCTION_DOCS_COMMAND="cargo run --manifest-path datafusion/core/Cargo.toml --bin print_functions_docs -- window"

echo "Inserting header"
cat <<'EOF' > "$TARGET_FILE"
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

<!---
This file was generated by the dev/update_function_docs.sh script.
Do not edit it manually as changes will be overwritten.
Instead, edit the WindowUDFImpl's documentation() function to
update documentation for an individual UDF or the
dev/update_function_docs.sh file for updating surrounding text.
-->


# Window Functions (NEW)

Note: this documentation is in the process of being migrated to be  [automatically created from the codebase].
Please see the [Window Functions (Old)](window_functions.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

A _window function_ performs a calculation across a set of table rows that are somehow related to the current row.
This is comparable to the type of calculation that can be done with an aggregate function.
However, window functions do not cause rows to become grouped into a single output row like non-window aggregate calls would.
Instead, the rows retain their separate identities. Behind the scenes, the window function is able to access more than just the current row of the query result

Here is an example that shows how to compare each employee's salary with the average salary in his or her department:

```sql
SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) FROM empsalary;

+-----------+-------+--------+-------------------+
| depname   | empno | salary | avg               |
+-----------+-------+--------+-------------------+
| personnel | 2     | 3900   | 3700.0            |
| personnel | 5     | 3500   | 3700.0            |
| develop   | 8     | 6000   | 5020.0            |
| develop   | 10    | 5200   | 5020.0            |
| develop   | 11    | 5200   | 5020.0            |
| develop   | 9     | 4500   | 5020.0            |
| develop   | 7     | 4200   | 5020.0            |
| sales     | 1     | 5000   | 4866.666666666667 |
| sales     | 4     | 4800   | 4866.666666666667 |
| sales     | 3     | 4800   | 4866.666666666667 |
+-----------+-------+--------+-------------------+
```

A window function call always contains an OVER clause directly following the window function's name and argument(s). This is what syntactically distinguishes it from a normal function or non-window aggregate. The OVER clause determines exactly how the rows of the query are split up for processing by the window function. The PARTITION BY clause within OVER divides the rows into groups, or partitions, that share the same values of the PARTITION BY expression(s). For each row, the window function is computed across the rows that fall into the same partition as the current row. The previous example showed how to count the average of a column per partition.

You can also control the order in which rows are processed by window functions using ORDER BY within OVER. (The window ORDER BY does not even have to match the order in which the rows are output.) Here is an example:

```sql
SELECT depname, empno, salary,
       rank() OVER (PARTITION BY depname ORDER BY salary DESC)
FROM empsalary;

+-----------+-------+--------+--------+
| depname   | empno | salary | rank   |
+-----------+-------+--------+--------+
| personnel | 2     | 3900   | 1      |
| develop   | 8     | 6000   | 1      |
| develop   | 10    | 5200   | 2      |
| develop   | 11    | 5200   | 2      |
| develop   | 9     | 4500   | 4      |
| develop   | 7     | 4200   | 5      |
| sales     | 1     | 5000   | 1      |
| sales     | 4     | 4800   | 2      |
| personnel | 5     | 3500   | 2      |
| sales     | 3     | 4800   | 2      |
+-----------+-------+--------+--------+
```

There is another important concept associated with window functions: for each row, there is a set of rows within its partition called its window frame. Some window functions act only on the rows of the window frame, rather than of the whole partition. Here is an example of using window frames in queries:

```sql
SELECT depname, empno, salary,
    avg(salary) OVER(ORDER BY salary ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg,
    min(salary) OVER(ORDER BY empno ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_min
FROM empsalary
ORDER BY empno ASC;

+-----------+-------+--------+--------------------+---------+
| depname   | empno | salary | avg                | cum_min |
+-----------+-------+--------+--------------------+---------+
| sales     | 1     | 5000   | 5000.0             | 5000    |
| personnel | 2     | 3900   | 3866.6666666666665 | 3900    |
| sales     | 3     | 4800   | 4700.0             | 3900    |
| sales     | 4     | 4800   | 4866.666666666667  | 3900    |
| personnel | 5     | 3500   | 3700.0             | 3500    |
| develop   | 7     | 4200   | 4200.0             | 3500    |
| develop   | 8     | 6000   | 5600.0             | 3500    |
| develop   | 9     | 4500   | 4500.0             | 3500    |
| develop   | 10    | 5200   | 5133.333333333333  | 3500    |
| develop   | 11    | 5200   | 5466.666666666667  | 3500    |
+-----------+-------+--------+--------------------+---------+
```

When a query involves multiple window functions, it is possible to write out each one with a separate OVER clause, but this is duplicative and error-prone if the same windowing behavior is wanted for several functions. Instead, each windowing behavior can be named in a WINDOW clause and then referenced in OVER. For example:

```sql
SELECT sum(salary) OVER w, avg(salary) OVER w
FROM empsalary
WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
```

## Syntax

The syntax for the OVER-clause is

```
function([expr])
  OVER(
    [PARTITION BY expr[, …]]
    [ORDER BY expr [ ASC | DESC ][, …]]
    [ frame_clause ]
    )
```

where **frame_clause** is one of:

```
  { RANGE | ROWS | GROUPS } frame_start
  { RANGE | ROWS | GROUPS } BETWEEN frame_start AND frame_end
```

and **frame_start** and **frame_end** can be one of

```sql
UNBOUNDED PRECEDING
offset PRECEDING
CURRENT ROW
offset FOLLOWING
UNBOUNDED FOLLOWING
```

where **offset** is an non-negative integer.

RANGE and GROUPS modes require an ORDER BY clause (with RANGE the ORDER BY must specify exactly one column).

## Aggregate functions

All [aggregate functions](aggregate_functions.md) can be used as window functions.

EOF

echo "Running CLI and inserting window function docs table"
$PRINT_WINDOW_FUNCTION_DOCS_COMMAND >> "$TARGET_FILE"

echo "Running prettier"
npx prettier@2.3.2 --write "$TARGET_FILE"

echo "'$TARGET_FILE' successfully updated!"

TARGET_FILE="docs/source/user-guide/sql/special_functions_new.md"
PRINT_SPECIAL_FUNCTION_DOCS_COMMAND="cargo run --manifest-path datafusion/core/Cargo.toml --bin print_functions_docs -- special"

echo "Inserting header"
cat <<'EOF' > "$TARGET_FILE"
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

<!---
This file was generated by the dev/update_function_docs.sh script.
Do not edit it manually as changes will be overwritten.
Instead, edit the SpecialUDFImpl's documentation() function to
update documentation for an individual UDF or the
dev/update_function_docs.sh file for updating surrounding text.
-->

# Special Functions (NEW)

Note: this documentation is in the process of being migrated to be [automatically created from the codebase].

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

EOF

echo "Running CLI and inserting special function docs table"
$PRINT_SPECIAL_FUNCTION_DOCS_COMMAND >> "$TARGET_FILE"

echo "Running prettier"
npx prettier@2.3.2 --write "$TARGET_FILE"

echo "'$TARGET_FILE' successfully updated!"
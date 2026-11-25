<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Table Constraint Enforcement

Table providers can describe table constraints using the
[`TableConstraint`] and [`Constraints`] APIs. These constraints include
primary keys, unique keys, foreign keys and check constraints.

DataFusion does **not** currently enforce these constraints at runtime.
They are provided for informational purposes and can be used by custom
`TableProvider` implementations or other parts of the system.

- **Nullability**: The only property enforced by DataFusion is the
  nullability of each [`Field`] in a schema. Returning data with null values
  for Columns marked as not nullable will result in runtime errors during execution. DataFusion
  does not check or enforce nullability when data is ingested.
- **Primary and unique keys**: DataFusion does not verify that the data
  satisfies primary or unique key constraints. Table providers that
  require this behaviour must implement their own checks.
- **Foreign keys and check constraints**: These constraints are parsed
  but are not validated or used during query planning.

[`tableconstraint`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/ast/enum.TableConstraint.html
[`constraints`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Constraints.html
[`field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html

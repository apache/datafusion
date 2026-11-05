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

[`tableconstraint`]: https://docs.rs/datafusion/latest/datafusion/sql/planner/enum.TableConstraint.html
[`constraints`]: https://docs.rs/datafusion/latest/datafusion/common/functional_dependencies/struct.Constraints.html
[`field`]: https://docs.rs/arrow/latest/arrow/datatype/struct.Field.html

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

# Securing DataFusion

As described in the [DataFusion security policy](../../../SECURITY.md), the end
application is responsible for security decisions. The settings below can help
control what a query can do.

## Restrict SQL statements

[`SQLOptions`] allows an application to reject classes of SQL statements when
creating a `DataFrame`. DDL (such as [`CREATE TABLE`](../user-guide/sql/ddl.md))
and DML (such as [`INSERT`](../user-guide/sql/dml.md)) are allowed by default.
Disable the classes that the application does not need and pass the options to
[`SessionContext::sql_with_options`] for every user-provided query:

```rust
use datafusion::prelude::*;

let options = SQLOptions::new()
    .with_allow_ddl(false)
    .with_allow_dml(false)
    .with_allow_statements(false);

let dataframe = ctx.sql_with_options(sql, options).await?;
```

## Limit file access

[`SessionContext::enable_url_table()`] is an opt-in feature that lets SQL query
local files by path. Leave it disabled when users should only query tables
registered by the application. If it is needed, run DataFusion with filesystem
permissions limited to the files the application intends to expose.

## Set query memory limits

Set an appropriate `datafusion.runtime.memory_limit` for the workload using the
[runtime configuration settings](../user-guide/configs.md#runtime-configuration-settings).
This limits memory used by DataFusion's query execution memory pool; use
process- or container-level resource limits as well when a hard bound on total
application memory is required.

## Bound spill storage

When an execution operator supports spilling, DataFusion may write intermediate
query data to temporary files under memory pressure. Use
`datafusion.runtime.temp_directory` and
`datafusion.runtime.max_temp_directory_size` to configure the location and size
of that temporary storage; see the [runtime configuration settings](../user-guide/configs.md#runtime-configuration-settings).

[sqloptions]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SQLOptions.html
[sessioncontext::sql_with_options]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql_with_options
[sessioncontext::enable_url_table()]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.enable_url_table

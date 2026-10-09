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

# Extensions List

DataFusion tries to provide a good set of features "out of the box" to quickly
start with a working system, but it can't include every useful feature (e.g.
`TableProvider`s for all data formats).

Thankfully one of the core features of DataFusion is a flexible extension API
that allows users to extend its behavior at all points. This page lists some
community maintained extensions available for DataFusion. These extensions are
not part of the core DataFusion project, and not under Apache Software
Foundation governance but we list them here to be useful to others in the
community.

If you know of an available extension that is not listed below, please open a PR
to add it to this page. If there is some feature you would like to see in
DataFusion, please consider creating a new extension in the `datafusion-contrib`
project (see [below](#datafusion-contrib)). Please [contact] us via github issue, slack, or Discord and
we'll gladly set up a new repository for your extension.

| Name                         | Type              | Description                                                                       |
| ---------------------------- | ----------------- | --------------------------------------------------------------------------------- |
| [DataFusion Table Providers] | [`TableProvider`] | Support for `PostgreSQL`, `MySQL`, `SQLite`, `DuckDB`, and `Flight SQL`           |
| [DataFusion Federation]      | Framework         | Allows DataFusion to execute (part of) a query plan by a remote execution engine. |
| [DataFusion ORC]             | [`TableProvider`] | [Apache ORC] file format                                                          |
| [DataFusion JSON Functions]  | Functions         | Scalar functions for querying JSON strings                                        |

[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[datafusion table providers]: https://github.com/datafusion-contrib/datafusion-table-providers
[datafusion federation]: https://github.com/datafusion-contrib/datafusion-federation
[datafusion orc]: https://github.com/datafusion-contrib/datafusion-orc
[apache orc]: https://orc.apache.org/
[datafusion json functions]: https://github.com/datafusion-contrib/datafusion-functions-json

## `datafusion-contrib`

The [`datafusion-contrib`] project contains a collection of community maintained
extensions that are not part of the core DataFusion project, and not under
Apache Software Foundation governance but may be useful to others in the
community. If you are interested adding a feature to DataFusion, a new extension
in `datafusion-contrib` is likely a good place to start. Please [contact] us via
github issue, slack, or Discord and we'll gladly set up a new repository for
your extension.

[`datafusion-contrib`]: https://github.com/datafusion-contrib
[contact]: ../contributor-guide/communication.md

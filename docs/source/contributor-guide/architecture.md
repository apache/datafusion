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

# Architecture

DataFusion's code structure and organization is described in the
[crates.io documentation], to keep it as close to the source as
possible. You can find the most up to date version in the [source code].

[crates.io documentation]: https://docs.rs/datafusion/latest/datafusion/index.html#architecture
[source code]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/lib.rs

## Forks vs Extension APIs

DataFusion is a fast moving project, which results in frequent internal changes.
This benefits DataFusion by allowing it to evolve and respond quickly to
requests, but also means that maintaining a fork with major modifications
sometimes requires non trivial work.

The public API (what is accessible if you use the DataFusion releases from
crates.io) is typically much more stable (though it does change from release to
release as well).

Thus, rather than forks, we recommend using one of the many extension APIs (such
as `TableProvider`, `OptimizerRule`, or `ExecutionPlan`) to customize
DataFusion. If you can not do what you want with the existing APIs, we would
welcome you working with us to add new APIs to enable your use case, as
described in the next section.

Please see the [Extensions] section to find out more about existing DataFusion
extensions and how to contribute your extension to the community.

[extensions]: ../library-user-guide/extensions.md

## Creating new Extension APIs

DataFusion aims to be a general-purpose query engine, and thus the core crates
contain features that are useful for a wide range of use cases. Use case specific
functionality (such as very specific time series or stream processing features)
are typically implemented using the extension APIs.

If have a use case that is not covered by the existing APIs, we would love to
work with you to design a new general purpose API. There are often others who are
interested in similar extensions and the act of defining the API often improves
the code overall for everyone.

Extension APIs that provide "safe" default behaviors are more likely to be
suitable for inclusion in DataFusion, while APIs that require major changes to
built-in operators are less likely. For example, it might make less sense
to add an API to support a stream processing feature if that would result in
slower performance for built-in operators. It may still make sense to add
extension APIs for such features, but leave implementation of such operators in
downstream projects.

The process to create a new extension API is typically:

- Look for an existing issue describing what you want to do, and file one if it
  doesn't yet exist.
- Discuss what the API would look like. Feel free to ask contributors (via `@`
  mentions) for feedback (you can find such people by looking at the most
  recently changed PRs and issues)
- Prototype the new API, typically by adding an example (in
  `datafusion-examples` or refactoring existing code) to show how it would work
- Create a PR with the new API, and work with the community to get it merged

Some benefits of using an example based approach are

- Any future API changes will also keep your example going ensuring no
  regression in functionality
- There will be a blue print of any needed changes to your code if the APIs do change
  (just look at what changed in your example)

An example of this process was [creating a SQL Extension Planning API].

[creating a sql extension planning api]: https://github.com/apache/datafusion/issues/11207

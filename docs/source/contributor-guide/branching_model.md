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

# DataFusion Branching Model

Most PRs are raised directly against master, and we keep master in a releasable state at all times by
only having dependencies on released crates. This allows us to reliably release new versions of DataFusion
on a fixed schedule.

This is problematic when making breaking changes to dependencies such as `arrow-rs` or `sqlparser` and then making
corresponding changes in DataFusion.

One solution to this problem is to use feature branches. For example, suppose that the latest `sqlparser` crate is
version 0.20 and there are multiple breaking changes in `sqlparser` main branch since that release. We can create
a `sqlparser-0.21` branch in DataFusion and raise PRs against that branch, going through the usual review and
approval process.

We can keep bumping the rev of the dependency in this branch as more features are added. Once the 0.21 crate is
published, we can then change the dependency to the released version and then PR the feature branch to master.

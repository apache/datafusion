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

# API health policy

To maintain API health, developers must track and properly deprecate outdated methods.
When deprecating a method:

- clearly mark the API as deprecated and specify the exact DataFusion version in which it was deprecated.
- concisely describe the preferred API, if relevant

API deprecation example:

```rust
    #[deprecated(since = "41.0.0", note = "Use SessionStateBuilder")]
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self
```

Deprecated methods will remain in the codebase for a period of 6 major versions or 6 months, whichever is longer, to provide users ample time to transition away from them.

Please refer to [DataFusion releases](https://crates.io/crates/datafusion/versions) to plan ahead API migration

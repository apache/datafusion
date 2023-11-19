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

# DataFusion Function Library

[DataFusion][df] is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

This crate contains several sets of functions that can be used with DataFusion.

Each module should implement a "function package" that should have a function with the signature:

``` rust
XXXX...
```

Which returns an actual function implementation when the relevant feature is activated, or returns
a stub function when the feature is not activated that errros and explains what feature
flag is needed to actually activate them.

This means that the same functions are always registered so the same set of functions is always availabke
even if some only generate runtime errors when called. 

TODO: should we also offer a mode / configuration flag that will not register such functions?

[df]: https://crates.io/crates/datafusion

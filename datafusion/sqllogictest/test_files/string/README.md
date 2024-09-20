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

# String Test Files

This directory contains test files for the `string` test suite.
To ensure consistent behavior across different string types, we should run the same tests with the same inputs on all string types.
There is a framework in place to execute the same tests across different string types.

See [#12415](https://github.com/apache/datafusion/issues/12415) for more background.

## Directory Structure

```
string/
    - init_data.slt.part        // generate the testing data
    - string_query.slt.part     // the sharing tests for all string type
    - string.slt                // the entrypoint for string type
    - large_string.slt          // the entrypoint for large_string type
    - string_view.slt           // the entrypoint for string_view type and the string_view specific tests
    - string_literal.slt        // the tests for string literal
```

## Pattern for Test Entry Point Files

Any entry point file should include `init_data.slt.part` and `string_query.slt.part`.

Planning-related tests (e.g., EXPLAIN ...) should be placed in their own entry point file (e.g., `string_view.slt`) as they are only used to assert planning behavior specific to that type.

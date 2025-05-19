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

# Regexp Test Files

This directory contains test files for regular expression (regexp) functions in DataFusion.

## Directory Structure

```
regexp/
    - init_data.slt.part      // Shared test data for regexp functions
    - regexp_like.slt         // Tests for regexp_like function
    - regexp_count.slt        // Tests for regexp_count function
    - regexp_match.slt        // Tests for regexp_match function
    - regexp_replace.slt      // Tests for regexp_replace function
```

## Tested Functions

1. `regexp_like`: Check if a string matches a regular expression
2. `regexp_count`: Count occurrences of a pattern in a string
3. `regexp_match`: Extract matching substrings
4. `regexp_replace`: Replace matched substrings

## Test Data

Test data is centralized in the `init_data.slt.part` file and imported into each test file using the `include` directive. This approach ensures:

Consistent test data across different regexp function tests
Easy maintenance of test data
Reduced duplication

## Test Coverage

Each test file covers:

Basic functionality
Case-insensitive matching
Null handling
Start position tests
Capture group handling
Different string types (UTF-8, Unicode)

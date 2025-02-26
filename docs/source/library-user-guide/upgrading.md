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

# Upgrade Guides

## DataFusion `46.0.0`


### Changes to `invoke()` and `invoke_batch()` deprecated

We are migrating away from `ScalarUDFImpl::invoke()` and
`ScalarUDFImpl::invoke_batch()` in favor of `ScalarUDFImpl::invoke_with_args()`. (TODO get code links) 

If you see errors such as 
```text
Example
```

You can resolve them by replacing all .invoke() and .invoke_batch()calls with .invoke_with_args(). 
```text
TODO example
```

Example of changes:
- [PR XXXX] TODO


### `ParquetExec`, `AvroExec`, `CsvExec`, `JsonExec` deprecated

See more information
- Change PR [PR #14224](https://github.com/apache/datafusion/pull/14224)
- Example of an Upgrade [PR in delta-rs](https://github.com/delta-io/delta-rs/pull/3261)

DataFusion 46 has a major change to how the built in DataSources are organized. The 

### Cookbook: Changes to `ParquetExecBuilder`

#### Old pattern:
```test
TODO
```

#### New Pattern


```test
TODO
```


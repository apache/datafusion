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

# DataFusion docs

## Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python virtualenv.

- Python
- `pip install -r requirements.txt`
- Datafusion python package. You can install the latest version by running `maturin develop` inside `../python` directory.

## Build

```bash
make html
```

## Release

The documentation is served through the
[arrow-site](https://github.com/apache/arrow-site/) repo. To release a new
version of the docs, follow these steps:

- Run `make html` inside `docs` folder to generate the docs website inside the `build/html` folder.
- Clone the arrow-site repo and checkout to the `asf-site` branch
- Copy build artifacts into `arrow-site` repo's `datafusion` folder: `'cp' -rT ./build/html/ ../arrow-site/datafusion/`
- Commit changes in `arrow-site` and send a PR.

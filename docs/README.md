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

# DataFusion Documentation

This folder contains the source content of the [User Guide](./source/user-guide)
and [Contributor Guide](./source/contributor-guide). These are both published to
https://arrow.apache.org/datafusion/ as part of the release process.

## Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python virtualenv.

- Python
- `pip install -r requirements.txt`

## Build & Preview

Run the provided script to build the HTML pages.

```bash
./build.sh
```

The HTML will be generated into a `build` directory.

Preview the site on Linux by running this command.

```bash
firefox build/html/index.html
```

## Release Process

The documentation is served through the
[arrow-site](https://github.com/apache/arrow-site/) repo. To release a new
version of the docs, follow these steps:

1. Run `./build.sh` inside `docs` folder to generate the docs website inside the `build/html` folder.
2. Clone the arrow-site repo
3. Checkout to the `asf-site` branch (NOT `master`)
4. Copy build artifacts into `arrow-site` repo's `datafusion` folder with a command such as

- `cp -rT ./build/html/ ../../arrow-site/datafusion/` (doesn't work on mac)
- `rsync -avzr ./build/html/ ../../arrow-site/datafusion/`

5. Commit changes in `arrow-site` and send a PR.

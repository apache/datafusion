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

# Download

While DataFusion is also distributed via the Rust [crates.io] package manager as a convenience, the
official Apache DataFusion releases are provided as source artifacts.

[crates.io]: https://crates.io/crates/datafusion

## Releases

The latest source release is [41.0.0][source-link] ([asc][asc-link],
[sha512][sha512-link]).

[source-link]: https://www.apache.org/dyn/closer.lua/datafusion/datafusion-41.0.0/apache-datafusion-41.0.0.tar.gz?action=download
[asc-link]: https://downloads.apache.org/datafusion/datafusion-41.0.0/apache-datafusion-41.0.0.tar.gz.asc
[sha512-link]: https://downloads.apache.org/datafusion/datafusion-41.0.0/apache-datafusion-41.0.0.tar.gz.sha512

For previous releases, please check the [archive](https://archive.apache.org/dist/datafusion/).

For releases earlier than 37.0.0, please check [Arrow's archive](https://archive.apache.org/dist/arrow/).

## Notes

- When downloading a release, please verify the OpenPGP compatible signature (or failing that, check the SHA-512); these should be fetched from the main Apache site.
- The KEYS file contains the public keys used for signing release. It is recommended that (when possible) a web of trust is used to confirm the identity of these keys.
- Please download the [KEYS](https://downloads.apache.org/datafusion/KEYS) as well as the .asc signature files.

### To verify the signature of the release artifact

You will need to download both the release artifact and the .asc signature file for that artifact. Then verify the signature by:

- Download the KEYS file and the .asc signature files for the relevant release artifacts.
- Import the KEYS file to your GPG keyring:

  ```shell
  gpg --import KEYS
  ```

- Verify the signature of the release artifact using the following command:

  ```shell
  gpg --verify <artifact>.asc <artifact>
  ```

### To verify the checksum of the release artifact

You will need to download both the release artifact and the .sha512 checksum file for that artifact. Then verify the checksum by:

```shell
shasum -a 512 -c <artifact>.sha512
```

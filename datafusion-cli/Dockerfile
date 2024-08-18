# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM rust:1.78-bookworm AS builder

COPY . /usr/src/datafusion
COPY ./datafusion /usr/src/datafusion/datafusion
COPY ./datafusion-cli /usr/src/datafusion/datafusion-cli

WORKDIR /usr/src/datafusion/datafusion-cli

RUN rustup component add rustfmt

RUN cargo build --release

FROM debian:bookworm-slim

COPY --from=builder /usr/src/datafusion/datafusion-cli/target/release/datafusion-cli /usr/local/bin

RUN mkdir /data

ENTRYPOINT ["datafusion-cli"]

CMD ["--data-path", "/data"]

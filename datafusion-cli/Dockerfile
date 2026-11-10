# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

FROM rust:bookworm AS builder

COPY . /usr/src/datafusion

WORKDIR /usr/src/datafusion

RUN cargo build -p datafusion-cli --release

FROM debian:bookworm-slim

COPY --from=builder /usr/src/datafusion/target/release/datafusion-cli /usr/local/bin

RUN mkdir /data

ENTRYPOINT ["datafusion-cli"]

CMD ["--data-path", "/data"]

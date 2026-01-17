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

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

# Install Rust 1.92.0
ENV RUST_VERSION=1.92.0
ENV RUSTUP_VERSION=1.27.1
RUN set -eux; \
    arch="$(uname -m)"; \
    case "${arch}" in \
        x86_64) rustup_arch="x86_64-unknown-linux-gnu" ;; \
        aarch64) rustup_arch="aarch64-unknown-linux-gnu" ;; \
        *) echo "Unsupported architecture: ${arch}" >&2; exit 1 ;; \
    esac; \
    curl --proto '=https' --tlsv1.2 -sSf \
        "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${rustup_arch}/rustup-init" \
        -o rustup-init; \
    chmod +x rustup-init; \
    ./rustup-init \
        --default-toolchain "${RUST_VERSION}" \
        --component rustfmt \
        --profile minimal \
        -y; \
    rm rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

# Install cargo-depgraph
RUN cargo install cargo-depgraph --version ^1.6 --locked

# Set working directory
WORKDIR /work

# Copy Python requirements and install them
COPY docs/requirements.txt /work/docs/requirements.txt
RUN pip install --no-cache-dir -r /work/docs/requirements.txt

# Copy the entire repository
COPY . /work

# Copy and set up the entrypoint script
COPY docs/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Fix line endings for shell scripts at build time (convert CRLF to LF)
RUN find /work -name "*.sh" -type f -exec sh -c 'tr -d "\r" < "$1" > "$1.tmp" && mv "$1.tmp" "$1"' _ {} \;

# Set working directory to docs for build
WORKDIR /work/docs

# Use entrypoint script for better readability and maintainability
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

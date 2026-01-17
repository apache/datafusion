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
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
    --default-toolchain ${RUST_VERSION} \
    --component rustfmt \
    --profile minimal \
    -y

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

# Fix line endings for shell scripts (convert CRLF to LF)
RUN find /work -name "*.sh" -type f -exec sh -c 'tr -d "\r" < "$1" > "$1.tmp" && mv "$1.tmp" "$1"' _ {} \;

# Set working directory to docs for build
WORKDIR /work/docs

# Default command: build documentation
# Fix line endings when running (for volume-mounted files from Windows) and then build
CMD ["bash", "-c", "find /work -name '*.sh' -type f -exec sh -c 'tr -d \"\\r\" < \"$1\" > \"$1.tmp\" && mv \"$1.tmp\" \"$1\"' _ {} \\; ; cd /work/docs && bash build.sh"]

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

# Turn .dockerignore to .dockerallow by excluding everything and explicitly
# allowing specific files and directories. This enables us to quickly add
# dependency files to the docker content without scanning the whole directory.
# This setup requires to all of our docker containers have arrow's source
# as a mounted directory.

# Base image extends debian:buster-slim
FROM rust:1.53.0-buster AS base

RUN apt update && apt -y install musl musl-dev musl-tools libssl-dev openssl

ARG SCCACHE_TAR_URL=https://github.com/mozilla/sccache/releases/download/v0.2.15/sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz

RUN curl -LsSf ${SCCACHE_TAR_URL} > /tmp/sccache.tar.gz && \
	tar axvf /tmp/sccache.tar.gz --strip-components=1 -C /usr/local/bin --wildcards --no-anchored 'sccache' && \
	chmod +x /usr/local/bin/sccache && \
	sccache --version && \
	rm -rf /tmp/sccache.tar.gz

ENV RUSTC_WRAPPER=/usr/local/bin/sccache
ENV CARGO_INCREMENTAL=0
ENV SCCACHE_CACHE_SIZE=1G

#NOTE: the following was copied from https://github.com/emk/rust-musl-builder/blob/master/Dockerfile under Apache 2.0 license

# The OpenSSL version to use. We parameterize this because many Rust
# projects will fail to build with 1.1.
#ARG OPENSSL_VERSION=1.0.2r
ARG OPENSSL_VERSION=1.1.1b

# Build a static library version of OpenSSL using musl-libc.  This is needed by
# the popular Rust `hyper` crate.
#
# We point /usr/local/musl/include/linux at some Linux kernel headers (not
# necessarily the right ones) in an effort to compile OpenSSL 1.1's "engine"
# component. It's possible that this will cause bizarre and terrible things to
# happen. There may be "sanitized" header
RUN --mount=type=cache,target=/root/.cache/sccache \
    echo "Building OpenSSL" && \
    mkdir -p /usr/local/musl/include && \
    ln -s /usr/include/linux /usr/local/musl/include/linux && \
    ln -s /usr/include/x86_64-linux-gnu/asm /usr/local/musl/include/asm && \
    ln -s /usr/include/asm-generic /usr/local/musl/include/asm-generic && \
    cd /tmp && \
    curl -LO "https://www.openssl.org/source/openssl-$OPENSSL_VERSION.tar.gz" && \
    tar xzf "openssl-$OPENSSL_VERSION.tar.gz" && cd "openssl-$OPENSSL_VERSION" && \
    env CC="sccache musl-gcc" ./Configure no-shared no-zlib -fPIC --prefix=/usr/local/musl -DOPENSSL_NO_SECURE_MEMORY linux-x86_64 && \
    env C_INCLUDE_PATH=/usr/local/musl/include/ make -s depend && \
    env C_INCLUDE_PATH=/usr/local/musl/include/ make -s && \
    make -s install 1>/dev/null && \
    rm /usr/local/musl/include/linux /usr/local/musl/include/asm /usr/local/musl/include/asm-generic && \
    rm -r /tmp/*

RUN --mount=type=cache,target=/root/.cache/sccache \
    echo "Building zlib" && \
    cd /tmp && \
    ZLIB_VERSION=1.2.11 && \
    curl -LO "http://zlib.net/zlib-$ZLIB_VERSION.tar.gz" && \
    tar xzf "zlib-$ZLIB_VERSION.tar.gz" && cd "zlib-$ZLIB_VERSION" && \
    CC="sccache musl-gcc" ./configure --static --prefix=/usr/local/musl && \
    make -s && make -s install 1>/dev/null && \
    rm -r /tmp/*

RUN --mount=type=cache,target=/root/.cache/sccache \
    echo "Building libpq" && \
    cd /tmp && \
    POSTGRESQL_VERSION=11.2 && \
    curl -LO "https://ftp.postgresql.org/pub/source/v$POSTGRESQL_VERSION/postgresql-$POSTGRESQL_VERSION.tar.gz" && \
    tar xzf "postgresql-$POSTGRESQL_VERSION.tar.gz" && cd "postgresql-$POSTGRESQL_VERSION" && \
    CC="sccache musl-gcc" CPPFLAGS=-I/usr/local/musl/include LDFLAGS=-L/usr/local/musl/lib ./configure --with-openssl --without-readline --prefix=/usr/local/musl && \
    cd src/interfaces/libpq && make -s all-static-lib && make -s install-lib-static && \
    cd ../../bin/pg_config && make -s && make -s install && \
    rm -r /tmp/*

ENV OPENSSL_DIR=/usr/local/musl/ \
    OPENSSL_INCLUDE_DIR=/usr/local/musl/include/ \
    DEP_OPENSSL_INCLUDE=/usr/local/musl/include/ \
    OPENSSL_LIB_DIR=/usr/local/musl/lib/ \
    OPENSSL_STATIC=1 \
    PQ_LIB_STATIC_X86_64_UNKNOWN_LINUX_MUSL=1 \
    PG_CONFIG_X86_64_UNKNOWN_LINUX_GNU=/usr/bin/pg_config \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    LIBZ_SYS_STATIC=1 \
    TARGET=musl

# The content copied mentioned in the NOTE above ends here.

## Download the target for static linking.
RUN rustup target add x86_64-unknown-linux-musl
RUN --mount=type=cache,target=/root/.cache/sccache \
    cargo install cargo-build-deps

# prepare toolchain
RUN --mount=type=cache,target=/root/.cache/sccache \
    rustup update && \
    rustup component add rustfmt

WORKDIR /tmp/ballista
RUN apt-get -y install cmake
RUN cargo install cargo-chef


ARG RELEASE_FLAG=--release

FROM base as planner
RUN mkdir /tmp/ballista/ballista
RUN mkdir /tmp/ballista/benchmarks
RUN mkdir /tmp/ballista/datafusion
RUN mkdir /tmp/ballista/datafusion-examples
ADD Cargo.toml .
COPY benchmarks ./benchmarks/
COPY datafusion ./datafusion/
COPY datafusion-cli ./datafusion-cli/
COPY datafusion-examples ./datafusion-examples/
COPY ballista ./ballista/
RUN cargo chef prepare --recipe-path recipe.json

FROM base as cacher
COPY --from=planner /tmp/ballista/recipe.json recipe.json
RUN cargo chef cook $RELEASE_FLAG --recipe-path recipe.json

FROM base as builder
RUN mkdir /tmp/ballista/ballista
RUN mkdir /tmp/ballista/benchmarks
RUN mkdir /tmp/ballista/datafusion
RUN mkdir /tmp/ballista/datafusion-cli
RUN mkdir /tmp/ballista/datafusion-examples
ADD Cargo.toml .
COPY benchmarks ./benchmarks/
COPY datafusion ./datafusion/
COPY ballista ./ballista/
COPY datafusion-cli ./datafusion-cli/
COPY datafusion-examples ./datafusion-examples/
COPY --from=cacher /tmp/ballista/target target
ARG RELEASE_FLAG=--release

# force build.rs to run to generate configure_me code.
ENV FORCE_REBUILD='true'
RUN cargo build $RELEASE_FLAG

# put the executor on /executor (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/ballista-executor /executor; else mv /tmp/ballista/target/release/ballista-executor /executor; fi

# put the scheduler on /scheduler (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/ballista-scheduler /scheduler; else mv /tmp/ballista/target/release/ballista-scheduler /scheduler; fi

# put the tpch on /tpch (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/tpch /tpch; else mv /tmp/ballista/target/release/tpch /tpch; fi

# Copy the binary into a new container for a smaller docker image
FROM base

COPY --from=builder /executor /

COPY --from=builder /scheduler /

COPY --from=builder /tpch /

ADD benchmarks/run.sh /
RUN mkdir /queries
COPY benchmarks/queries/ /queries/

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/executor"]

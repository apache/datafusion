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

FROM ubuntu
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETPLATFORM

RUN apt-get update && \
    apt-get install -y git build-essential

# Install R, curl, and python deps
RUN apt-get -y install --no-install-recommends --no-install-suggests \
    ca-certificates software-properties-common gnupg2 gnupg1 \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
    && add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' \
    && apt-get -y install r-base \
    && apt-get -y install curl \
    && apt-get -y install python3.8 \
    && apt-get -y install python3-pip

# Install R libraries
RUN R -e "install.packages('data.table',dependencies=TRUE, repos='http://cran.rstudio.com/')" \
    && R -e "install.packages('dplyr',dependencies=TRUE, repos='http://cran.rstudio.com/')"

# Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Clone db-benchmark and download data
RUN git clone https://github.com/h2oai/db-benchmark \
    && cd db-benchmark \
    && Rscript _data/groupby-datagen.R 1e7 1e2 0 0 \
    && Rscript _data/join-datagen.R 1e7 0 0 0 \
    && mkdir data \
    && mv G1_1e7_1e2_0_0.csv data \
    && mv J1_1e7_1e1_0_0.csv data \
    && mv J1_1e7_1e4_0_0.csv data \
    && mv J1_1e7_1e7_0_0.csv data \
    && mv J1_1e7_NA_0_0.csv data \
    && cd ..

# Clone datafusion-python and build python library
# Not sure if the wheel will be the same on all computers
RUN git clone https://github.com/datafusion-contrib/datafusion-python \
    && cd datafusion-python && git reset --hard 368b50ed9662d5e93c70b539f94cceace685265e \
    && python3 -m pip install pip \
    && python3 -m pip install pandas \
    && python3 -m pip install -r requirements.txt \
    && cd ..

# Copy local arrow-datafusion
COPY . arrow-datafusion

# 1. datafusion-python that builds from datafusion version referenced datafusion-python
RUN cd datafusion-python \
    && maturin build --release \
    && case "${TARGETPLATFORM}" in \
    */amd64) CPUARCH=x86_64 ;; \
    */arm64) CPUARCH=aarch64 ;; \
    *) exit 1 ;; \
    esac \
    # Version will need to be updated in conjunction with datafusion-python version
    && python3 -m pip install target/wheels/datafusion-0.4.0-cp36-abi3-linux_${CPUARCH}.whl \
    && cd ..

# 2. datafusion-python that builds from local datafusion.  use this when making local changes to datafusion.
# Currently, as of March 5th 2022, this done not build (i think) because datafusion is being split into multiple crates
# and datafusion-python has not yet been updated to reflect this.
# RUN cd datafusion-python \
# && sed -i '/datafusion =/c\datafusion = { path = "../arrow-datafusion/datafusion", features = ["pyarrow"] }' Cargo.toml \
# && sed -i '/fuzz-utils/d' ../arrow-datafusion/datafusion/Cargo.toml \
# && maturin build --release \
# && case "${TARGETPLATFORM}" in \
#     */amd64) CPUARCH=x86_64 ;; \
#     */amd64) CPUARCH=aarch64 ;; \
#     *) exit 1 ;; \
# esac \
# && python3 -m pip install target/wheels/datafusion-0.4.0-cp36-abi3-linux_${CPUARCH}.whl \
# && cd ..

# Make datafusion directory in db-benchmark
RUN mkdir db-benchmark/datafusion \
    && cp ../arrow-datafusion/benchmarks/db-benchmark/groupby-datafusion.py db-benchmark/datafusion \
    && cp ../arrow-datafusion/benchmarks/db-benchmark/join-datafusion.py db-benchmark/datafusion \
    && cp ../arrow-datafusion/benchmarks/db-benchmark/run-bench.sh db-benchmark/ \
    && chmod +x db-benchmark/run-bench.sh

WORKDIR /db-benchmark

RUN ls && ls -al data/

ENTRYPOINT ./run-bench.sh
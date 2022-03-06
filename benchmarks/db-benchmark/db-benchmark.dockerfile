FROM ubuntu
ARG DEBIAN_FRONTEND=noninteractive

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
    && maturin build --release \
    && python3 -m pip install target/wheels/datafusion-0.4.0-cp36-abi3-linux_aarch64.whl \
    && cd ..

# Copy local arrow-datafusion
COPY . arrow-datafusion

# Make datafusion directory in db-benchmark
RUN mkdir db-benchmark/datafusion \
    && cp ../arrow-datafusion/benchmarks/db-benchmark/groupby-datafusion.py db-benchmark/datafusion \
    && cp ../arrow-datafusion/benchmarks/db-benchmark/join-datafusion.py db-benchmark/datafusion \
    && cp ../arrow-datafusion/benchmarks/db-benchmark/run-bench.sh db-benchmark/ \
    && chmod +x db-benchmark/run-bench.sh

WORKDIR /db-benchmark

RUN ls && ls -al data/

ENTRYPOINT ./run-bench.sh





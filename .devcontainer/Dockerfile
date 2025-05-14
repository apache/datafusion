FROM rust:bookworm

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    # Remove imagemagick due to https://security-tracker.debian.org/tracker/CVE-2019-10131
    && apt-get purge -y imagemagick imagemagick-6-common

# Add protoc
# https://datafusion.apache.org/contributor-guide/getting_started.html#protoc-installation
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-x86_64.zip \
    && unzip protoc-25.1-linux-x86_64.zip -d $HOME/.local \
    && rm protoc-25.1-linux-x86_64.zip

ENV PATH="$PATH:$HOME/.local/bin"
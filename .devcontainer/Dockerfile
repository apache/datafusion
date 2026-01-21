FROM rust:bookworm

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    # Remove imagemagick due to https://security-tracker.debian.org/tracker/CVE-2019-10131
    && apt-get purge -y imagemagick imagemagick-6-common

# setup the containers WORKDIR so npm install works
# https://stackoverflow.com/questions/57534295/npm-err-tracker-idealtree-already-exists-while-creating-the-docker-image-for
WORKDIR /root

# Add protoc, npm, prettier
# https://datafusion.apache.org/contributor-guide/development_environment.html#protoc-installation
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev npm nodejs\
    && rm -rf /var/lib/apt/lists/*

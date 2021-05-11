#!/bin/bash
if [ -z "${DOCKER_REPO}" ]; then
  echo "DOCKER_REPO env var must be set"
  exit -1
fi
cargo install cross
cross build --release --target aarch64-unknown-linux-gnu
rm -rf temp-ballista-docker
mkdir temp-ballista-docker
cp target/aarch64-unknown-linux-gnu/release/ballista-executor temp-ballista-docker
cp target/aarch64-unknown-linux-gnu/release/ballista-scheduler temp-ballista-docker
docker buildx build --push -t $DOCKER_REPO/ballista-arm64 --platform=linux/arm64 -f dev/docker/ballista-arm64.Dockerfile temp-ballista-docker
rm -rf temp-ballista-docker
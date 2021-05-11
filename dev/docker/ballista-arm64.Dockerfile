FROM arm64v8/ubuntu

ADD ballista-scheduler /
ADD ballista-executor /

ENV RUST_LOG=info
FROM eclipse-temurin:21-jdk-noble
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential git curl ca-certificates pkg-config clang \
      libssl-dev zlib1g-dev liblmdb-dev libflatbuffers-dev libsecp256k1-dev libzstd-dev \
      libyaml-perl libtemplate-perl libregexp-grammars-perl \
      util-linux netcat-openbsd procps python3 \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.99.1 --profile minimal
ENV PATH=/root/.cargo/bin:$PATH
WORKDIR /bench

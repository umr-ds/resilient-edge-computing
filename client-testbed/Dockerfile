# Build dtn7-rs
FROM rust:1 AS dtn7-rs-builder
RUN git clone https://github.com/umr-ds/dtn7-rs.git && \
    cd dtn7-rs && \
    git switch rec && \
    cargo install --locked --bin dtnd --path ./core/dtn7/

# Build dtn7-go
FROM golang:1 AS dtn7-go-builder
RUN git clone https://github.com/umr-ds/dtn7-go.git && \
    cd dtn7-go && \
    git switch rec && \
    go build -race -o /dtn7god ./cmd/dtnd


FROM ubuntu:latest

# Install basic dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl ca-certificates tar fish iproute2 iputils-ping tini python3 && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/

# Zellij
ARG ZELLIJ_VERSION=0.43.1
RUN curl -fsSL -o /tmp/zellij.tar.gz \
    https://github.com/zellij-org/zellij/releases/download/v${ZELLIJ_VERSION}/zellij-x86_64-unknown-linux-musl.tar.gz && \
    tar -xzf /tmp/zellij.tar.gz -C /usr/local/bin && \
    rm /tmp/zellij.tar.gz

# Zellij config/layout
RUN mkdir -p /root/.config/zellij && \
    zellij setup --dump-config > /root/.config/zellij/config.kdl && \
    printf "\nshow_startup_tips false\nshow_release_notes false\ndefault_shell \"fish\"\n" >> /root/.config/zellij/config.kdl
COPY ./client-testbed/dtn.kdl /root/.config/zellij/layouts/dtn.kdl

# DTN configs
COPY ./client-testbed/dtn7rsd1.toml /root/dtn7rsd1.toml
COPY ./client-testbed/dtn7rsd2.toml /root/dtn7rsd2.toml
COPY ./client-testbed/dtn7god1.toml /root/dtn7god1.toml
COPY ./client-testbed/dtn7god2.toml /root/dtn7god2.toml

# Binaries
COPY --from=dtn7-rs-builder /usr/local/cargo/bin/dtnd /usr/local/bin/dtn7rsd
COPY --from=dtn7-go-builder /dtn7god /usr/local/bin/dtn7god

# Entrypoint
COPY ./client-testbed/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Python project
COPY ./pyproject.toml /root/resilient-edge-computing/pyproject.toml
COPY ./uv.lock /root/resilient-edge-computing/uv.lock
COPY ./rec /root/resilient-edge-computing/rec
WORKDIR /root/resilient-edge-computing
RUN uv sync

ENTRYPOINT ["/usr/bin/tini","-g","--","/usr/local/bin/entrypoint.sh"]

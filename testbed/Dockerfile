FROM golang:1 AS dtnd-go-builder
RUN git clone https://gitlab.uni-marburg.de/fb12/ag-freisleben/projects/dtn7-go.git && \
    cd dtn7-go && \
    git switch rec && \
    go build -race -o /dtnd-go ./cmd/dtnd


FROM ubuntu:22.04

# Install basic dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    iproute2 curl ca-certificates tar fish && \
    rm -rf /var/lib/apt/lists/*

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
COPY testbed/dtn.kdl /root/.config/zellij/layouts/dtn.kdl


COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY --from=dtnd-go-builder /dtnd-go /usr/local/bin/dtnd-go

COPY testbed/dtnd_client.toml /root/dtnd_client.toml
COPY testbed/dtnd_broker.toml /root/dtnd_broker.toml
COPY testbed/dtnd_datastore.toml /root/dtnd_datastore.toml
COPY testbed/dtnd_executor.toml /root/dtnd_executor.toml

COPY testbed/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh


WORKDIR /app

RUN uv python install 3.12

COPY pyproject.toml uv.lock README.md ./

RUN uv sync --no-install-project

COPY rec/ ./rec/
COPY tests/dtn/artifacts/ ./artifacts/


ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

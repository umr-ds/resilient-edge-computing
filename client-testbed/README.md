# Playground for Testing the Communication with the DTN Daemon

A tiny testbed to test and improve this projects communication with the DTN Daemons.

## Build and run

Build and run the image interactively:

```bash
docker build -t rec_dtn -f ./client-testbed/Dockerfile . && \
docker run --privileged --rm -it --name rec_dtn rec_dtn
```

Two shells are opened automatically.

## Running the REC DTN Client

Run the client against a certain DTN Daemon:

```bash
uv run rec_dtn_client --dtn-socket "/tmp/dtnd_go1.socket"
uv run rec_dtn_client --dtn-socket "/tmp/dtnd_rs1.socket"
uv run rec_dtn_client --dtn-socket "/tmp/dtnd_go2.socket"
uv run rec_dtn_client --dtn-socket "/tmp/dtnd_rs2.socket"
```

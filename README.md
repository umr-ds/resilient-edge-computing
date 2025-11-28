[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# Resilient Edge Computing

## Setup

### REC

Either create a `virtualenv` and install directly via `pip`, or use `uv`.
If you are planning to modify the `REC` code, it would be useful to do an editable install.

```shell
git clone git@gitlab.uni-marburg.de:fb12/ag-freisleben/projects/resilient-edge-computing.git

cd resilient-edge-computing

pip install -e .
# or, using uv
uv sync
```

### DTN

In order to run the DTN version of `REC`, you need to also run a DTN daemon. You can either use `dtn7-go` or `dtn7-rs`, just make sure to checkout the `rec` branch in either repository.

#### dtn7-go

Install the [Go programming language](https://go.dev/)

Clone and build `dtnd`

```shell
git clone git@gitlab.uni-marburg.de:fb12/ag-freisleben/projects/dtn7-go.git

cd dtn7-go

git checkout rec

go build ./cmd/dtnd
```

This should leave you with an executable called `dtnd` in the project root directory.
Next, you need a config file, like this:

```toml
node_id = "dtn://<node id>/"
log_level = "Debug"

[Store]
path = "<path to storage directory>"

[Routing]
algorithm = "epidemic"

[Agents]
[Agents.REC]
socket = "<path to socket>"

[[Listener]]
type = "QUICL"
address = ":35037"

[Cron]
dispatch = "10s"
```

Where you need to replace all instances of `<...>` with some appropriate value.

- `node_id`: Name of the node; can be the same as the name of the `REC` that will be running, but does not have to be.
  Something like `dtn://rec_1/` will work fine; just make sure no two instances of `dtnd` are running with the same `node_id`.
- `path`: Path to some folder on your computer's filesystem where `dtnd` will store bundles.
- `socket`: `REC` and `dtnd` communicate via a UNIX domain socket. `dtnd` will create the socket, so it needs to be started first.

Lastly, run `dtnd` and point it to your config file:

```shell
./dtnd config.toml
```

#### dtn7-rs

Install the [Rust programming language](https://www.rust-lang.org/tools/install)

Clone and build `dtnd`

```shell
git clone git@gitlab.uni-marburg.de:fb12/ag-freisleben/projects/dtn7-rs.git

cd dtn7-rs

git checkout rec

cargo install --locked --bin dtnd --path ./core/dtn7/
```

This will install the `dtnd` binary to your Cargo bin directory (usually `~/.cargo/bin/`).
Next, you need a config file, like this:

```toml
nodeid = "<node id>"
debug = true
beacon-period = true

workdir = "<path to storage directory>"
db = "mem"

recsocket = "<path to socket>"

[routing]
strategy = "epidemic"

[convergencylayers]
cla.0.id = "tcp"
cla.0.port = 16163

[core]
janitor = "10s"

[discovery]
interval = "2s"
peer-timeout = "20s"
port = 3003
```

Where you need to replace all instances of `<...>` with some appropriate value.

- `nodeid`: Name of the node; can be the same as the name of the `REC` that will be running, but does not have to be.
  Something like `rec_1` will work fine; just make sure no two instances of `dtnd` are running with the same `nodeid`.
- `workdir`: Path to some folder on your computer's filesystem where `dtnd` will store bundles.
- `recsocket`: `REC` and `dtnd` communicate via a UNIX domain socket. `dtnd` will create the socket, so it needs to be started first.

Lastly, run `dtnd` and point it to your config file:

```shell
dtnd -c config.toml
```

## Usage

Make sure `dtnd` is running (see above).
The entry-point is `rec/run_dtn.py`, which is pointed to by the `rec_dtn` command.
For arguments, etc., see `rec_dtn --help` and its subcommands.

### Start network

To start your own small test-network, use the following steps:

1. Start a `broker`. There must be at least one broker running before the other nodes can do anything.

   ```shell
   rec_dtn v -s <path to socket> -i dtn://broker_1/ broker
   ```

   This starts a broker with the node id `dtn://broker_1`.
   Replace `<path to socket>` with the same path as in the `dtnd` config.

2. Start a `datastore`

   ```shell
   rec_dtn -v -s <path to socket> -i dtn://datastore_1/ datastore <path to store directory>
   ```

   This starts a datastore with the node id `dtn://datastore_1`.
   Replace `<path to socket>` with the same path as in the `dtnd` config.
   Replace `<path to store directory>` with a path where you want the store to store its data.
   This MUST NOT be the same path as the one in the `dtnd` config!

### Interact with the network

You can interact with the network via the `rec_dtn client` command.
Here are some examples:

```shell
rec_dtn -v -s <path to socket> -i dtn://client_1/ client -r <path to results directory> data test/data put <path to file>
```

This starts a client with the node id `dtn://client_1/`.
Replace `<path to socket>` with the same path as in the `dtnd` config.
Replace `<path to results directory>` with a path where the client can store results of jobs.

We use the `client` command, and its `data` subcommand.
We are using the data-name `test/data`, we are using the `put` action to submit data to the store, and lastly, we are sending the contents of `<path to file>` (this needs to point to an existing file, of course).

```shell
rec_dtn -v -s <path to socket> -i dtn://client_1/ client -r <path to results directory> data test/data get
```

This starts a client with the node id `dtn://client_1/`.
Replace `<path to socket>` with the same path as in the `dtnd` config.
Replace `<path to results directory>` with a path where the client can store results of jobs.

We use the `client` command, and its `data` subcommand.
We are using the data-name `test/data`, we are using the `get` action to retrieve data from the store.
Effectively, we are querying the same data that we stored with the previous command.

### Execution Plans

Execution plans are TOML files that define batch job executions. They allow you to:

- Publish shared named data (can be referenced by any future jobs)
- Define multiple jobs to be executed
- Chain jobs together (use output from one job as input to another)

To execute an execution plan:

```shell
rec_dtn -v -s <path to socket> -i dtn://client_1/ client -r <path to results directory> exec <path to execution plan>
```

#### Execution Plan Format

An execution plan consists of two main sections:

1. **`[named_data]`** (optional): Shared named data that can be referenced by multiple jobs
2. **`[[jobs]]`**: (optional) List of jobs to execute

#### Simple Execution Plan Example

Here's a basic execution plan that runs a single WebAssembly module:

```toml
[[jobs]]

[jobs.metadata]
wasm_module = "wasm-module"                    # Name of the WASM module to execute
results_receiver = "dtn://client/"             # Where to send results
argv = ["a", "b", "c"]                         # Command-line arguments
stdin_file = "stdin"                           # Named data to use as stdin
dirs = ["/output", "/temp"]                    # Directories to create in execution environment
stdout_file = "/output/stdout.log"             # Where to write stdout
stderr_file = "/output/stderr.log"             # Where to write stderr
results = ["/out.txt", "/output"]              # Files/dirs to return directly to results_receiver

[jobs.metadata.capabilities]
cpu_cores = 1                                  # Required CPU cores
free_cpu_capacity = 0                          # Required free CPU capacity (0-100 per core)
free_memory = 0                                # Required free memory in bytes
free_disk_space = 0                            # Required free disk space in bytes

[jobs.metadata.env]
FOO = "bar"                                    # Environment variables

[jobs.metadata.data]
"/data.bin" = "databin"                        # Map execution environment paths to named data

[jobs.metadata.named_results]
"/out.txt" = "wasm_output_file"                # Store file as named data
"/output" = "output_archive"                   # Store directory as named data (zipped)

[jobs.data]
wasm-module = "../wasi-module.wasm"            # Local file paths for named data
stdin = "data/stdin.txt"
databin = "data/data.bin"
```

#### Execution Plan with Shared Data

Use the `[named_data]` section to avoid duplicating data across multiple jobs:

```toml
# Shared data that can be referenced by multiple jobs
[named_data]
wasm-module = "../wasi-module.wasm"
stdin = "data/stdin.txt"
databin = "data/data.bin"

# First job
[[jobs]]

[jobs.metadata]
wasm_module = "wasm-module"
results_receiver = "dtn://client/"
argv = ["a", "b", "c"]
stdin_file = "stdin"
dirs = ["/output"]
results = ["/output"]

[jobs.metadata.capabilities]
cpu_cores = 1

[jobs.metadata.data]
"/data.bin" = "databin"

[jobs.metadata.named_results]
"/output" = "output_archive_1"

# Second job - can reference the same shared data
[[jobs]]

[jobs.metadata]
wasm_module = "wasm-module"
results_receiver = "dtn://client/"
argv = ["x", "y", "z"]
stdin_file = "stdin"
dirs = ["/output"]
results = ["/output"]

[jobs.metadata.capabilities]
cpu_cores = 1

[jobs.metadata.data]
"/data.bin" = "databin"

[jobs.metadata.named_results]
"/output" = "output_archive_2"
```

#### Chaining Jobs

You can chain jobs by using named results from one job as input to another:

```toml
# First job: Process initial data
[[jobs]]

[jobs.metadata]
wasm_module = "processor"
results_receiver = "dtn://client/"
stdin_file = "input_data"
results = ["/out.txt"]

[jobs.metadata.capabilities]
cpu_cores = 1

[jobs.metadata.named_results]
"/out.txt" = "processed_data"                  # Store output as named data

[jobs.data]
processor = "../processor.wasm"
input_data = "data/initial.txt"

# Second job: Use output from first job as input
[[jobs]]

[jobs.metadata]
wasm_module = "analyzer"
results_receiver = "dtn://client/"
stdin_file = "processed_data"                  # Use first job's output
results = ["/analysis.txt"]

[jobs.metadata.capabilities]
cpu_cores = 1

[jobs.metadata.named_results]
"/analysis.txt" = "final_analysis"

[jobs.data]
analyzer = "../analyzer.wasm"
```

#### Field Reference

**Job Metadata Fields:**

- `wasm_module` (required): Named reference to the WebAssembly module
- `results_receiver`: EID where results should be sent
- `argv`: Command-line arguments for the WASM module
- `env`: Environment variables (key-value pairs)
- `stdin_file`: Named data to use as standard input
- `dirs`: Directories to create before execution
- `data`: Map execution environment paths to named data references
- `stdout_file`: Path where stdout should be written
- `stderr_file`: Path where stderr should be written
- `results`: Paths to collect and send directly to `results_receiver` (zipped)
- `named_results`: Map paths to named data identifiers for persistent storage

**Capabilities Fields:**

- `cpu_cores`: Number of CPU cores required
- `free_cpu_capacity`: Required CPU capacity (0-100 per core, max = cores \* 100)
- `free_memory`: Required memory in bytes
- `free_disk_space`: Required disk space in bytes

**Data Specifications:**

- Paths in `[jobs.data]` or `[named_data]` are relative to the execution plan file
- Named data in `named_results` is automatically published to datastores
- Directories in `named_results` are automatically zipped before storage
- Files and directories in `results` are packaged together into a zip file and sent directly to `results_receiver`

### Testbed

The project also comes with a small interactive testbed in a Docker container.
To build and run the testbed, use the following command:

```shell
docker build --pull -t rec_testbed -f testbed/Dockerfile . && \
docker run --privileged --rm -it --name rec_testbed -e LOGLEVEL=DEBUG -e DAEMON=go rec_testbed
```

The testbed supports both the Go and Rust DTN daemon implementations. You can select which daemon to use via the `DAEMON` environment variable:

```shell
# Use Go daemon (default)
docker run --privileged --rm -it --name rec_testbed -e LOGLEVEL=DEBUG -e DAEMON=go rec_testbed

# Use Rust daemon
docker run --privileged --rm -it --name rec_testbed -e LOGLEVEL=DEBUG -e DAEMON=rust rec_testbed
```

Valid values for `DAEMON` are `go` (default) or `rust` (case-insensitive).

The testbed comes with pre-configured DTN daemons and running broker, datastore, and executor nodes.
The only thing you need to do is to start a client node in the top-left Zellij pane.
For example:

```shell
uv run rec_dtn --id dtn://client/ --socket /tmp/client.socket client -r /results query dtn://client/
# or any of the other client commands, e.g. to execute an execution plan:
uv run rec_dtn --id dtn://client/ --socket /tmp/client.socket client -r /results exec artifacts/execution_plans/execution_plan_once.toml
uv run rec_dtn --id dtn://client/ --socket /tmp/client.socket client -r /results exec artifacts/execution_plans/execution_plan_twice.toml
uv run rec_dtn --id dtn://client/ --socket /tmp/client.socket client -r /results exec artifacts/execution_plans/execution_plan_twice_named.toml
# To retrieve result bundles, you can either check once:
uv run rec_dtn --id dtn://client/ --socket /tmp/client.socket client -r /results check
# or continuously listen for new bundles:
uv run rec_dtn --id dtn://client/ --socket /tmp/client.socket client -r /results listen
```

## Development

If you want to participate in development, there are some additional steps:

### Install development dependencies

[The Project file](pyproject.toml) defines some additional dependencies which are not necessary to run the software, but are required for development.
To install these, run:

```shell
pip install --group dev .
pip install --group lint .
# or, using uv
uv sync --all-groups
```

### Setup pre-commit

[pre-commit](https://pre-commit.com/) is a utility which runs a number of tasks (in our case linting and input sorting) before each commit.
If you have installed the development dependencies as mentioned above, then `pre-commit` should already be installed, but you need to set it up to run:

```shell
pre-commit install
# or, using uv
uv run pre-commit install
```

If you don't lint your files before pushing, then the CI pipeline will fail your commits!

### Linting

To make sure that `pre-commit` does not fail your commit, you can manually run the linters before committing:

```shell
black **/**/*.py
isort --profile black .
# or, using uv
uv run black **/**/*.py
uv run isort --profile black .
```

### Tests

Please make sure to write unit tests whenever feasible.
Tests are stored in the `tests` directory.
To run all tests, just run pytest in the project root:

```shell
pytest
# or, using uv
uv run pytest
```

If tests are failing, then the CI pipeline will fail your commits!

#### Privileged CI runner

If you clone this project and want to run the CI pipeline, you will need to setup a "privileged" runner for the integration tests.

A step-by-step howto can be found in [the runner readme](GITLAB_RUNNER_SETUP.md).

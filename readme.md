# Resilient Edge Computing

## Setup

### REC

Either create a `virtualenv` and install directly via `pip`, or use `uv`.
If you are planning to modify the `REC` code, it would be useful to do an editable install.

```shell
git clone git@gitlab.uni-marburg.de:fb12/ag-freisleben/projects/resilient-edge-computing.git

cd resilient-edge-computing

pip install -e .
``` 

### DTN

In order to run the dtn-version of `REC`, you need to als orun a dtn-daemon. You can either use `dtn7-go` or `dtn7-rs`, just make sure to checkout the `rec` brach in either repository.

For `dtn7-go` use the following steps:

Install the [go programming langauge](https://go.dev/)

Clone and build `dtnd`

```shell
git clone git@gitlab.uni-marburg.de:fb12/ag-freisleben/projects/dtn7-go.git

cd dtn7-go

git checkout rec

go build ./cmd/dtnd
```

This should leave you with an executable called `dtnd` in the project root directory.
Next you need a config file, like this

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
dispatch ="10s"
```

where you need to replace all instances of `<...>` with some appropriate value.

- `node_id`: name of the node, can be the same as the name of the `REC` that will be running, but does not have to be.
   Something like `dtn://rec_1/` will work fine, just make sure no two instances of `dtnd` are running with the same `node_id`.
- `path`: Path to some folder on you computer's filesystem where `dtnd` will store bundles.
- `socket`: `REC` and `dtnd` communicate via a UNIX domain socket. `dtnd` will create the socket, so it needs to be started first.

Lastly, run `dtnd` and point it to you config file:

```shell
./dtnd config.toml
```

## Usage

Make sure `dtnd` is running (see above).
The entry-point is `rec/run_dtn.py`, which is pointed to by the `rec_dtn` command.
For arguments, etc see `rec_dtn --help` and its subcommands.

### Start network

To start you own small test-network, use the following steps:

1. Start a `broker`. There must be at least one broker running before the other nodes can do anthing.
   
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
rec_dtn -v -s <path to socket> -i dtn://client_1/ client data dtn://datastore_1/ test/data put <path to file>
```

This starts a client with the node id `dtn://client_1/`
Replace `<path to socket>` with the same path as in the `dtnd` config.

We use the `client` command, and its `data` subcommand.
The datastore we are addressing is `dtn://datastore_1/`, we are using the data-name `test/data`, we are using the `put` action to submit data to the store, and lastly, we are sending the contents of `<path to file` (this needs to point to an existing file, of course).

```shell
rec_dtn -v -s <path to socket> -i dtn://client_1/ client data dtn://datastore_1/ test/data get
```

This starts a client with the node id `dtn://client_1/`
Replace `<path to socket>` with the same path as in the `dtnd` config.
We use the `client` command, and its `data` subcommand.
The datastore we are addressing is `dtn://datastore_1/`, we are using the data-name `test/data`, we are using the `get` action to retrieve data from the store.
Effectively, we are querying the same data that we stored with the previous command.

## Development

If you want to participate in development, there are some additional steps:

### Install development dependencies

[The Project file](pyproject.toml) defines some additional dependencies which are not necessary to run the software, but are required for development.
To install these, run:

```shell
pip install --group dev .
pip install --group lint .
```

### Setup pre-commit

[pre-commit](https://pre-commit.com/) is a utility which runs a number of tasks (in oru case linting and input sorting) before each commit.
If you have installed the development dependencies as mentioned above, then `pre-commit` should already be installed, but you need to set it up to run:

```shell
pre-commit install
```

If you don't lint your files before pushing, then the CI pipeline will fail your commits!

### Linting

To make sure that `pre-commit` does not fail your commit, you can manually run the linters before committing:

```shell
black **/**/*.py
isort --profile black .
```

### Tests

Please make sure to write unit tests whenever feasible.
Test are stores in the `tests` directory.
To run all tests, just run pytest in the project root:

```shell
pytest
```

If tests are failing, then the CI pipeline will fail your commits!
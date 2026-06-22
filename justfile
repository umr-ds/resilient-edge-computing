default:
    @just --list

fmt:
    uv run ruff format

check:
    uv run ty check

lint:
    uv run ruff check

vermin:
    uv run vermin .

test:
    uv run pytest

prek:
    uv run prek run --all-files

testbed:
    docker build --pull -t rec_testbed -f testbed/Dockerfile . && \
    docker run --privileged --rm -it --name rec_testbed -e LOGLEVEL=INFO -e DAEMON=rust rec_testbed

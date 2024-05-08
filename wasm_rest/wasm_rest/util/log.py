import logging
import sys

LOG = logging.Logger("wasm_rest_logger")
if __name__ == 'wasm_rest.util.log':
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG)

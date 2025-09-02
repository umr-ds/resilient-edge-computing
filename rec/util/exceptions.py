class WasmRestException(BaseException):
    msg: str

    def __init__(self, msg: str = "") -> None:
        super().__init__()
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


class ConnectionTimeoutException(WasmRestException):
    """connection to a node timed out"""

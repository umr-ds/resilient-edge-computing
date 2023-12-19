import json


class WasmRestException(BaseException):
    msg: str

    def __init__(self, msg: str = "") -> None:
        super().__init__()
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


class ServerException(WasmRestException):
    pass


class ClientException(WasmRestException):
    pass


def raise_server_error(result: bytes, endpoint: str):
    try:
        msg = json.loads(result)["detail"]
        raise ServerException(msg)
    except json.JSONDecodeError:
        raise ServerException(f"Unknow error in {endpoint}")

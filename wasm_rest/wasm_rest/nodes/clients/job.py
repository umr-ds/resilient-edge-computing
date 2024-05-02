from uuid import UUID

from wasm_rest.exceptions import WasmRestException
from wasm_rest.model import JobInfo
from wasm_rest.nodetypes.broker import Broker
from wasm_rest.util.util import try_store_named_data


class Job:
    id: UUID

    def __init__(self, job_id: UUID) -> None:
        self.id = job_id

    def job_data_name(self, name: str) -> str:
        return f"{self.id}/{name}"

    def upload_job_file(self, name: str, path: str, broker: Broker) -> bool:
        return try_store_named_data(self.job_data_name(name), path, broker)

    def transform_job_info_broker(self, job_info: JobInfo) -> None:
        try:
            if type(job_info.wasm_bin) is str:
                job_info.wasm_bin = self.job_data_name("exec.wasm")
            elif type(job_info.wasm_bin) is tuple:
                job_info.wasm_bin = (self.job_data_name(job_info.wasm_bin[1]),
                                     job_info.wasm_bin[1])
            else:
                raise WasmRestException("Invalid Formatting in wasm_bin")

            if type(job_info.stdin) is str:
                job_info.job_data[self.job_data_name(job_info.stdin[1])] = "stdin"
                job_info.stdin = "stdin"
            elif type(job_info.stdin) is tuple:
                if job_info.stdin_is_named:
                    pass
                else:
                    job_info.job_data[self.job_data_name(job_info.stdin[1])] = job_info.stdin[1]
                job_info.stdin = (job_info.job_data[self.job_data_name(job_info.stdin[1])], job_info.stdin[1])
            else:
                raise WasmRestException("Invalid Formatting in stdin")

            job_info.job_data = {self.job_data_name(path): path
                                 for _, path in job_info.job_data.items()}

        except ValueError as e:
            raise WasmRestException("Invalid Formatting") from e

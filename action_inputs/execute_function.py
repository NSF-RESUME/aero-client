import os
import sys

from globus_compute_sdk import Executor
from typing import Literal
from typing import TypeAlias


Action: TypeAlias = Literal["register", "download", "custom", "commit"]

endpoind_uuid = os.environ["ENDPOINT_UUID"]
download_function_uuid = os.environ["DOWNLOAD_FUNCTION_UUID"]
custom_function_uuid = os.environ["CUSTOM_FUNCTION_UUID"]
commit_function_uuid = os.environ["COMMIT_FUNCTION_UUID"]


def register(endpoint_uuid, custom_function_uuid):
    import json
    from uuid import uuid4
    from aero_client.api import register_flow

    output_data = {
        "out0": {  # key here needs to match the name of your parameter, will also be the name of the record in the db
            "tmpdir": "~/postdoc/DSaaS-client",
            "url": "https://g-c952d0.1305de.36fe.data.globus.org/output/synthetic_data.txt",
            "collection_uuid": "ff01d581-69c4-44f0-bb4c-ed4484706226",
            "collection_url": "https://g-8b681.fd635.8443.data.globus.org/valerie/",
        }
    }

    function_args = {
        "rand_arg": str(uuid4()),
        "metrics": True,
    }  # update params as needed, keys need to match function param names
    description = "noop ingestion"
    fl = register_flow(
        endpoint_uuid=endpoint_uuid,
        function_uuid=custom_function_uuid,
        kwargs=function_args,
        output_data=output_data,
        description=description,
    )

    return json.loads(fl["function_args"])["kwargs"]


def run_function(act: Action, run_inputs: str | None = None):
    with Executor(endpoint_id=endpoind_uuid) as gce:
        if act == "register":
            future = gce.submit(register, endpoind_uuid, custom_function_uuid)
        elif act == "download":
            future = gce.submit_to_registered_function(
                function_id=download_function_uuid, kwargs=eval(run_inputs)
            )
        elif act == "custom":
            future = gce.submit_to_registered_function(
                function_id=custom_function_uuid, kwargs=eval(run_inputs)[1]
            )
        else:
            future = gce.submit_to_registered_function(
                function_id=commit_function_uuid, kwargs=eval(run_inputs)
            )

        return future.result()


if __name__ == "__main__":
    act: Action = sys.argv[1]
    run_inputs = sys.argv[2] if len(sys.argv) > 2 else None

    results = run_function(act=act, run_inputs=run_inputs)

    print(f"result={results}")

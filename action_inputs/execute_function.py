import os
import sys

from globus_compute_sdk import Executor

endpoind_uuid = os.environ["ENDPOINT_UUID"]
download_function_uuid = os.environ["DOWNLOAD_FUNCTION_UUID"]
custom_function_uuid = os.environ["CUSTOM_FUNCTION_UUID"]

download_inputs = {
    "rand_arg": 63712,
    "aero": {
        "input_data": {},
        "output_data": {
            "out0": {
                "tmpdir": "~/postdoc/DSaaS-client",
                "url": "https://g-c952d0.1305de.36fe.data.globus.org/output/synthetic_data.txt",
                "collection_uuid": "ff01d581-69c4-44f0-bb4c-ed4484706226",
                "collection_url": "https://g-8b681.fd635.8443.data.globus.org/valerie/",
                "id": "f10cdca2-5600-4771-a3a0-9b7a4c1a67d6",
            }
        },
        "flow_id": "fd5691a7-5ac5-42a0-8d5a-7d30c8fc883e",
    },
}

function_uuid = sys.argv[1]

with Executor(endpoint_id=endpoind_uuid) as gce:
    if function_uuid == download_function_uuid:
        future = gce.submit_to_registered_function(
            function_id=function_uuid, kwargs=download_inputs
        )
    elif function_uuid == custom_function_uuid:
        future = gce.submit_to_registered_function(
            function_id=function_uuid, kwargs=eval(sys.argv[2])[1]
        )
    else:
        future = gce.submit_to_registered_function(
            function_id=function_uuid, kwargs=eval(sys.argv[2])
        )

    print(f"result={future.result()}")

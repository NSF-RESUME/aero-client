import os
import sys

from globus_compute_sdk import Executor

endpoind_uuid = os.environ["ENDPOINT_UUID"]
download_function_uuid = os.environ["DOWNLOAD_FUNCTION_UUID"]
custom_function_uuid = os.environ["CUSTOM_FUNCTION_UUID"]

download_inputs = {
    "rand_arg": 63712,
    "metrics": True,
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

args = [
    [],
    {
        "aero": {
            "flow_id": "d9bf4e2d-5b32-4c24-88a7-2fe1dd4d6628",
            "input_data": {},
            "output_data": {
                "out0": {
                    "checksum": "7818c9cba4fbfac4b6e3d8802397ae2e",
                    "collection_url": "https://g-8b681.fd635.8443.data.globus.org/valerie/",
                    "collection_uuid": "ff01d581-69c4-44f0-bb4c-ed4484706226",
                    "download": True,
                    "encoding": "UTF-8",
                    "file": "/Users/valeriehayot-sasson/aero/875ba579-7531-4537-aaa1-a630174935eb",
                    "file_bn": "875ba579-7531-4537-aaa1-a630174935eb",
                    "file_format": ".txt",
                    "id": "3e4e744f-1a6c-4f7d-912f-79e9f566ad98",
                    "size": 61865984,
                    "temp_dir": "/Users/valeriehayot-sasson/aero",
                    "tmpdir": "~/postdoc/DSaaS-client",
                    "url": "https://g-c952d0.1305de.36fe.data.globus.org/output/synthetic_data.txt",
                }
            },
        },
        "download_metrics": {
            "duration": 13028524000,
            "task_end": 1734321528873406000,
            "task_start": 1734321515844882000,
        },
        "metrics": True,
        "rand_arg": 284068800,
    },
]

function_uuid = sys.argv[1]

with Executor(endpoint_id=endpoind_uuid) as gce:
    if function_uuid == download_function_uuid:
        future = gce.submit_to_registered_function(
            function_id=function_uuid, kwargs=download_inputs
        )
    elif function_uuid == custom_function_uuid:
        future = gce.submit_to_registered_function(
            function_id=function_uuid, kwargs=args[1] #, kwargs=eval(sys.argv[2])[1]
        )
    else:
        future = gce.submit_to_registered_function(
            function_id=function_uuid, kwargs=eval(sys.argv[2])
        )

    print(f"result={future.result()}")

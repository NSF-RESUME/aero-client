from aero_client.api import register_flow
from aero_client.utils import PolicyEnum


def register_ingestion():
    endpoint_uuid = "87e6579f-3408-41ee-9429-6559eb8cb713"
    function_uuid = "97571dc9-c288-4b0e-b36f-e294a1d81db7"
    output_data = {
        "output": {
            "url": "https://iwss.uillinois.edu/wastewater-treatment-plant/download/169/",
            "collection_uuid": "52f7f6bc-444f-439a-ad48-a4569d10c3d1",
            "collection_url": "https://g-c952d0.1305de.36fe.data.globus.org/",
        }
    }
    description = "my test description"
    policy = PolicyEnum.INGESTION
    timer_delay = 86400
    fl = register_flow(
        endpoint_uuid=endpoint_uuid,
        function_uuid=function_uuid,
        output_data=output_data,
        description=description,
        policy=policy,
        timer_delay=timer_delay,
    )

    return fl


def register_analysis():
    endpoint_uuid = "87e6579f-3408-41ee-9429-6559eb8cb713"
    function_uuid = "a1632617-aa9e-47ca-8c7f-8edc8de84c1d"
    function_args = {"arg1": 4, "arg2": 2}

    input_data = {
        "in1": {"id": "de9b61b6-95b9-4a2e-8ecc-943876eb86da", "version": None}
    }

    output_data = {
        "out1": {
            "collection_uuid": "52f7f6bc-444f-439a-ad48-a4569d10c3d1",
            "collection_url": "https://g-c952d0.1305de.36fe.data.globus.org/",
        }
    }

    description = "my analysis test"
    policy = PolicyEnum.ALL
    fl = register_flow(
        endpoint_uuid=endpoint_uuid,
        function_uuid=function_uuid,
        input_data=input_data,
        output_data=output_data,
        kwargs=function_args,
        policy=policy,
        description=description,
    )

    return fl


print(register_ingestion())

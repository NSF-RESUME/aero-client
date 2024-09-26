from aero_client.api import register_flow
from aero_client.utils import PolicyEnum


def register_ingestion():
    endpoint_uuid = "87e6579f-3408-41ee-9429-6559eb8cb713"  # update to be your endpoint
    function_uuid = "f6200dd8-726d-4541-9116-825ea36bda04"  # update to be you function
    output_data = {
        "myoutput": {  # key here needs to match the name of your parameter, will also be the name of the record in the db
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
    endpoint_uuid = "87e6579f-3408-41ee-9429-6559eb8cb713"  # update to your endpoint
    function_uuid = "124be92f-49f6-4268-8833-6823667b92db"  # update to be you function
    function_args = {
        "arg1": 4,
        "arg2": 2,
    }  # update params as needed, keys need to match function param names

    input_data = {
        "in1": {
            "id": "a0580e9e-d50a-475e-8b6a-043ad061ec88",
            "version": None,
        }  # need to update id post ingestion and key name needs to match parameter name in function
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


print(register_analysis())

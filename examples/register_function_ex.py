from aero_client.utils import register_function


def my_analysis(in1, arg1, arg2):
    from datetime import datetime

    return {
        "name": "out1",
        "file_bn": "myfilename",
        "file_format": "json",
        "checksum": "2222",
        "size": 300,
        "created_at": datetime.now().ctime(),
    }


def my_ingestion(output):
    from datetime import datetime

    return {"name": "output", "created_at": datetime.now().ctime()}


print(register_function(my_analysis))

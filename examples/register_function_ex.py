from aero_client.utils import register_function


def my_analysis(in1, arg1, arg2):
    from pathlib import Path

    from aero_client.utils import AeroOutput

    assert Path(in1).exists()

    return AeroOutput(name="out1", path=in1)


def my_ingestion(myoutput):
    from aero_client.utils import AeroOutput

    with open(myoutput, "wb+") as f:
        f.write(bytes("mybinaryoutput", encoding="utf-8"))

    return AeroOutput(name="myoutput", path=myoutput)


print(register_function(my_analysis))

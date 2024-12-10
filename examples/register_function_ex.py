from aero_client.utils import AeroOutput

from aero_client.utils import register_function


def my_analysis(in1, arg1, arg2):
    from pathlib import Path

    from aero_client.utils import AeroOutput

    assert Path(in1).exists()

    return AeroOutput(name="out1", path=in1)


def wastewater_ingestion(wastewater: str) -> AeroOutput:
    """Example transformation function for wastewater data.

    Args:
        wastewater (str): Path of file on local filesystem.

    Returns:
        AeroOutput: name and path of updated data on fs.
    """
    import pandas as pd
    from aero_client.utils import AeroOutput

    # load wastewater data and perform analysis
    df = pd.read_csv(wastewater)
    df = df.drop(columns=["influenza_a", "influenza_b"])  # some transformation

    df.to_csv(wastewater)

    return AeroOutput(name="wastewater", path=wastewater)


print(register_function(my_analysis))

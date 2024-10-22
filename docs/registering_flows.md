# Registering Automated Flows

## Function Registration

```py title="Function Registration" linenums="1" hl_lines="18"
def wastewater_ingestion(wastewater: str) -> AeroOutput:
    """
    Example transformation function for wastewater data.

    Args:
        wastewater (str): Path of file on local filesystem.

    Returns:
        Name and path of the updated data on the filesystem.
    """
    import pandas as pd
    from aero_client.utils import AeroOutput

    df = pd.read_csv(wastewater) # (1)
    df = df.drop(columns=["influenza_a", "influenza_b"]) # (2)
    df.to_csv(wastewater)

    return AeroOutput(name="wastewater", path=wastewater)
```

1. Load the waste water data at the path given as an argument to perform the analyses.
2. Perform some transformation onto the data.


## Registering Ingestions

```py title="Flow Registration" linenums="1" hl_lines="5"
def register_ingestion():
    endpoint_uuid = "..."
    function_uuid = "..."
    output_data = {
        "wastewater": { # (1)
            "url": "...",
            "collection_uuid": "...",
            "collection_url": "...",
        },
    }

    return register_flow(
        endpoint_uuid=endpoint_uuid,
        function_uuid=function_uuid,
        output_data=output_data,
        description="Ingestion flow for waste water data.",
        policy=PolicyEnum.INGESTION, # (2)
        timer_delay=86400, # (3)
    )
```

1. The key (i.e., `"wastewater"`) must match the key from the earlier code sample for function registration.
2. Specify a timer-based data ingestion policy, which has an implementation provided by AERO.
3. Timer delay is specified in *seconds*. This value of `86400` makes the flow run on a daily basis.


## Flow Output

```json title="AERO Output" linenums="1"
{
    "contributed_to": [
        {
            "available_versions": 1,
            "collection_url": "...",
            "collection_uuid": "...",
            "description": "...",
            "id": "...",
            "name": "wastewater", # (1)
            "url": "..."
        }
    ],
    "derived_from" :[],
    "description": "...",
    "endpoint": "...",
    "function_args": "...",
    "function_id": "...",
    "id": "...",
    "last_executed": null,
    "policy": 0,
    "timer": 86400,
    "timer_job_id": "..."
}
```
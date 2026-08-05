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


## Registering Analysis Flows from the CLI

An **analysis flow** consumes one or more existing AERO sources (via `input_data`) and reruns
automatically when they get a new version. Register one from a YAML file with `aero register -f`:

```yaml title="analysis.yaml"
endpoint_uuid: <globus compute endpoint uuid>
function_uuid: <uuid printed when you register your analysis function>
policy: ANY            # (1)
description: LHS results CSV summary

input_data:
  lhs_input:           # (2)
    id: <source data id>   # printed by `aero create`
    version: null          # null = latest

output_data:
  summary:             # (3)
    collection_uuid: <gcs guest collection uuid>
    collection_url: <gcs guest collection domain>

kwargs: {}             # extra keyword args passed to the function
```

1. `ANY` reruns when **any** input gets a new version; `ALL` only when **all** inputs have. Use one
   of these (not `INGESTION`/`TIMER`) for an analysis meant to react to new data. Defaults to `ANY`.
2. The `input_data` key must match your analysis function's **parameter name**; `id` is the source
   Data id.
3. The `output_data` key must match the `AeroOutput` **name** your function returns.

```sh
aero register -f analysis.yaml
```

Scalar values can be overridden on the command line (`-e/--endpoint-uuid`, `-u/--function-uuid`,
`-p/--policy`, `-d/--description`, `-k/--kwargs KEY=VALUE`); explicit flags win over the file.

> **Note:** with `ANY`/`ALL`, the flow also runs **once at registration** (its `last_executed` starts
> empty), so the first analysis run happens immediately, before any new source version.


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
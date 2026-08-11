# Writing functions

Your functions run on a Globus Compute endpoint, inside a wrapper AERO supplies. The wrapper
materializes each declared input as a local file, passes it to you by name, and stores whatever you
return.

## The rules

**Be self-contained.** The function is serialized and shipped to the endpoint, so every import and
helper must live *inside* the function body. A module-level import that happens to work on your
machine will not be there on the worker.

**Parameter names are the contract.** Each key in the flow's `input_data` is passed as a keyword
argument of that name, holding the path of a local file. Each `AeroOutput` name you return must
match a key in `output_data`.

**Register from a matching Python version.** Serialization is version-sensitive: register from the
same Python minor version the endpoint runs, or it will fail to deserialize the function.

## An analysis function

```python title="csv_summary.py"
def csv_summary(lhs_input):
    """Summarize the input CSV.

    Args:
        lhs_input (str): Local path of the input, materialized by AERO. The
            parameter name must match the input_data key in the flow YAML.

    Returns:
        AeroOutput: name matching the output_data key, and the path to store.
    """
    import pandas as pd
    from aero_client.utils import AeroOutput

    df = pd.read_csv(lhs_input)
    summary = df.describe(include="all")

    out_path = f"{lhs_input}.summary.csv"
    summary.to_csv(out_path)

    return AeroOutput(name="summary", path=out_path)


if __name__ == "__main__":
    from aero_client.utils import register_function

    print(register_function(csv_summary))   # prints the uuid for the flow YAML
```

```sh
python csv_summary.py     # -> function_uuid for analysis.yaml
```

## Storing your own results

If the analysis writes its result itself — back into the object store it read from, say — return
`None` and declare no `output_data`. AERO records the run and the inputs it consumed, and stores
nothing:

```python
def analyse(lhs_input):
    import pandas as pd
    from minio import Minio

    df = pd.read_csv(lhs_input)
    ...
    Minio(...).put_object(...)     # your code, your bucket

    return None
```

Returning an output when the flow declares none — or declaring one and returning nothing — is an
error naming the mismatch, rather than a silent no-op.

## Knowing which object triggered the run

A type can own many objects, and the local path AERO hands you is a temporary name. To find out
which object this run is about, declare an extra parameter named after the input with a `_url`
suffix. It is passed only if you ask for it, so existing functions are unaffected:

```python
def analyse(lhs_input, lhs_input_url):
    # lhs_input      -> local path of the fetched object
    # lhs_input_url  -> stable url of the object that triggered this run
    print(f"summarizing {lhs_input_url}")
    ...
```

`<name>_signed_url` is also available — the presigned URL the fetch used, when there was one. It is
transient and only present on a notify-triggered run; prefer `<name>_url` for anything you record.

## Accepting a path or a URL

A function that also runs by hand is easier to test. `pandas` reads URLs directly, so accepting
either costs little — just remember a URL is not a writable path when naming outputs:

```python
def csv_summary(lhs_input=None, lhs_input_url=None):
    import os
    import tempfile
    import urllib.parse

    import pandas as pd
    from aero_client.utils import AeroOutput

    def is_url(v):
        return isinstance(v, str) and urllib.parse.urlparse(v).scheme in ("http", "https")

    # Prefer a local file: under AERO it is already fetched, and re-fetching
    # risks a signed url that has since expired.
    source = lhs_input if lhs_input else lhs_input_url

    df = pd.read_csv(source)

    if is_url(source):
        name = os.path.basename(urllib.parse.urlparse(source).path)
        out_path = os.path.join(tempfile.mkdtemp(), f"{name}.summary.csv")
    else:
        out_path = f"{source}.summary.csv"

    df.describe(include="all").to_csv(out_path)
    return AeroOutput(name="summary", path=out_path)
```

## Ingestion functions

A **copy** source can transform the pulled file before it is stored, via `--verifier`. The function
receives the downloaded file and returns an `AeroOutput` the same way:

```python
def wastewater_ingestion(wastewater):
    """Drop unused columns before the file is stored."""
    import pandas as pd
    from aero_client.utils import AeroOutput

    df = pd.read_csv(wastewater)
    df = df.drop(columns=["influenza_a", "influenza_b"])
    df.to_csv(wastewater)

    return AeroOutput(name="wastewater", path=wastewater)
```

Omit `--verifier` and AERO registers a passthrough that stores the file unchanged. **No-copy sources
have no ingestion function at all** — nothing is pulled, so there is nothing to transform.

## Registering

`register_function` from `aero_client.utils` wraps your function in the AERO wrapper and registers
it with Globus Compute, returning the uuid to put in the flow YAML:

```python
from aero_client.utils import register_function

print(register_function(csv_summary))
```

Re-register after changing the function body — the wrapper is serialized at registration, so an
old uuid still runs the old code.

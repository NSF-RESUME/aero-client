# Writing functions

Your functions run on a Globus Compute endpoint, inside a wrapper AERO supplies. The wrapper
materializes each declared input as a local file, passes it to you by name, and stores whatever you
return.

## The rules

**Be self-contained.** The function is serialized and shipped to the endpoint, so every import and
helper must live *inside* the function body. A module-level import that happens to work on your
machine will not be there on the worker.

**Parameter names are the contract.** Each key in the flow's `input_data` is passed as a keyword
argument of that name, holding the path of a local file — see
[How an input reaches your function](#how-an-input-reaches-your-function). Each `AeroOutput` name
you return must match a key in `output_data`.

**Register from a matching Python version.** Serialization is version-sensitive: register from the
same Python minor version the endpoint runs, or it will fail to deserialize the function.

## How an input reaches your function

Every `input_data` key becomes a keyword argument of that name, and its value is **always a local
filesystem path** — never a source id and never a URL. Given:

```yaml
input_data:
  lhs_input:  {id: 9526394f-..., version: null}
  wind_input: {id: a1b2c3d4-..., version: null}
kwargs:
  threshold: 0.5
```

AERO calls `csv_summary(lhs_input="/tmp/…", wind_input="/tmp/…", threshold=0.5)`. The flow's
`kwargs` are merged in as further keyword arguments, so your signature covers both.

A key with no matching parameter fails at run time with
`csv_summary() got an unexpected keyword argument '<key>'`.

### Copy and no-copy sources look the same to you

Where the bytes come from differs; what you receive does not.

| Source | AERO fetches from | your argument holds |
|---|---|---|
| copy | the Globus guest collection, with a transfer token | `/tmp/<uuid>` |
| **no-copy** | the object's own URL — signed when the relay supplied one | `/tmp/<uuid>/lhs_results.csv` |

**"No copy" means AERO keeps no durable copy, not that you get a URL.** The worker still downloads
the object for the duration of the run and deletes the temp file afterwards. So the same function
body works against either kind of source, and a source can switch between them without touching
your code.

One difference is visible: the no-copy path **preserves the object's filename and extension**,
because the temp file is named from the object's url. The copy path gives an extension-less uuid.
That matters if your code sniffs the suffix — `.xml.gz` handling, say.

If the fetch fails — most often a 403 from an unsigned or expired url — the run raises rather than
writing the error page to the temp file and handing it to you as data.

### Fetching an input yourself

Only the input whose notify triggered a run carries a signed url, so on a private bucket any other
no-copy input is fetched unsigned and 403s. AERO cannot sign — it holds no object-store
credentials. Mark that input `fetch: false` in the flow YAML and it hands over the url instead:

```python
def run(a_input_url, b_input, a_input_signed_url=None):
    # a_input_url  -> url of the object; fetch it with your own credentials
    # b_input      -> local path, fetched by AERO as usual
    import boto3
    ...
```

The opted-out input arrives as `<name>_url` and **`<name>` is not passed at all**, so a signature
still expecting a path fails immediately. `<name>_signed_url` comes too if you name it, and is
`None` whenever that input did not trigger the run — give it a default, or the function breaks
depending on which source happened to fire.

The input's version is still resolved and recorded in provenance; only the download is skipped.

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

A type can own many objects, and the path you get is a temporary one. To find out *which* object
this run is about, declare an extra parameter named after the input with a `_url` suffix. It is
passed only if you ask for it — the signature is inspected first — so existing functions are
unaffected:

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

!!! warning "The source's `name` is this parameter's name"

    An analysis names its inputs in `input_data`; an ingestion has none, so the **source name**
    plays that role. All three of these must be the same string:

    - the parameter the pulled file arrives as (`wastewater` above)
    - the `AeroOutput(name=...)` you return
    - the `name` given to `aero create`

    Mismatch it and the run fails with
    `wastewater_ingestion() got an unexpected keyword argument '<the source name>'`. Keep the
    human-readable label in the source's `description`.

Parameters beyond the file come from the flow's kwargs — `aero create -k bin_freq=5min`, or a
`kwargs:` block in the YAML. Give them defaults so the function still runs when they are not set:

```python
def traffic_to_csv(output, bin_freq="1min", db_dsn=None):
    ...
    return AeroOutput(name="output", path=csv_path)
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

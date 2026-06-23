"""A minimal FastAPI server for testing ``aero_client.jobs.download()``.

It stands in for the real AERO server locally:

* ``GET /flow/{flow_id}`` returns the flow JSON that ``download()`` expects, i.e.
  ``{"contributed_to": [<data dict>]}`` where the data dict carries ``id``,
  ``name`` and a ``url`` pointing at a file this same server serves.
* ``GET /files/{filename}`` serves a file from the configured data directory.

The module deliberately does NOT import ``aero_client`` so it stays trivially
importable and reusable (e.g. as a standalone process or a pytest fixture).

Standalone usage::

    python tests/download_server.py \\
        --file "Stickney Water Reclamation Plant - North.csv" --name post_preds

then point an AERO ``config.toml`` at ``server = "http://127.0.0.1:8000"``.
"""

import argparse
import mimetypes
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse


def create_app(
    data_dir: Path,
    flows: dict[str, dict],
    default: dict | None = None,
) -> FastAPI:
    """Build the test server app.

    Args:
        data_dir: directory the ``/files`` endpoint serves from.
        flows: mapping of ``flow_id -> {"filename", "name", "id"}``. Held by
            reference, so callers may mutate it after construction (e.g. a test
            registering a flow on a fixture).
        default: entry returned by ``/flow`` when the id is not in ``flows``.
    """
    data_dir = Path(data_dir)
    app = FastAPI()

    @app.get("/flow/{flow_id}")
    def get_flow(flow_id: str, request: Request) -> dict:
        entry = flows.get(flow_id, default)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"unknown flow {flow_id}")
        file_url = str(request.base_url) + "files/" + entry["filename"]
        return {
            "contributed_to": [
                {
                    "id": entry["id"],
                    "name": entry["name"],
                    "url": file_url,
                }
            ]
        }

    @app.get("/files/{filename}")
    def get_file(filename: str) -> FileResponse:
        # Serve by basename only -- reject path traversal / nested paths.
        if filename != Path(filename).name:
            raise HTTPException(status_code=400, detail="invalid filename")
        path = data_dir / filename
        if not path.is_file():
            raise HTTPException(status_code=404, detail=f"no such file {filename}")

        media_type, _ = mimetypes.guess_type(str(path))
        media_type = media_type or "application/octet-stream"
        # Pin a charset for text so the client decodes deterministically.
        if media_type.startswith("text/"):
            media_type = f"{media_type}; charset=utf-8"
        return FileResponse(path, media_type=media_type, filename=filename)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", default="tests/data")
    parser.add_argument("--file", required=True, help="filename in --data-dir to serve")
    parser.add_argument("--name", default="data", help="data record name")
    parser.add_argument("--id", default="test-data-id", help="data record id")
    args = parser.parse_args()

    default = {"filename": args.file, "name": args.name, "id": args.id}
    app = create_app(Path(args.data_dir), flows={}, default=default)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

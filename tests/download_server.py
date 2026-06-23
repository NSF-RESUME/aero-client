"""A minimal FastAPI server for testing ``aero_client.jobs.download()``.

It stands in for the real AERO server locally:

* ``GET /flow/{flow_id}`` returns the flow JSON that ``download()`` expects, i.e.
  ``{"contributed_to": [<data dict>]}`` where the data dict carries ``id``,
  ``name`` and a ``url`` pointing at a file this same server serves.
* ``GET /files/{filename}`` serves a file from the configured data directory.
* ``GET /secure-files/{filename}`` serves the same files but behind HTTP Basic
  Auth -- used to exercise download()'s basic-auth path.

The module deliberately does NOT import ``aero_client`` so it stays trivially
importable and reusable (e.g. as a standalone process or a pytest fixture).

Standalone usage::

    python tests/download_server.py \\
        --file "Stickney Water Reclamation Plant - North.csv" --name post_preds

then point an AERO ``config.toml`` at ``server = "http://127.0.0.1:8000"``.
"""

import argparse
import mimetypes
import secrets
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials


def create_app(
    data_dir: Path,
    flows: dict[str, dict],
    default: dict | None = None,
    auth: tuple[str, str] = ("user", "pass"),
) -> FastAPI:
    """Build the test server app.

    Args:
        data_dir: directory the file endpoints serve from.
        flows: mapping of ``flow_id -> {"filename", "name", "id", "secure"?}``.
            Held by reference, so callers may mutate it after construction (e.g. a
            test registering a flow on a fixture). When an entry has
            ``secure=True``, ``/flow`` returns a url pointing at ``/secure-files``
            with credentials embedded as ``...:user=<u>:pwd=<p>``.
        default: entry returned by ``/flow`` when the id is not in ``flows``.
        auth: the ``(username, password)`` accepted by ``/secure-files``.
    """
    data_dir = Path(data_dir)
    app = FastAPI()
    security = HTTPBasic()

    def _serve(filename: str) -> FileResponse:
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

    def _require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
        ok_user = secrets.compare_digest(credentials.username, auth[0])
        ok_pwd = secrets.compare_digest(credentials.password, auth[1])
        if not (ok_user and ok_pwd):
            raise HTTPException(
                status_code=401,
                detail="invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )

    @app.get("/flow/{flow_id}")
    def get_flow(flow_id: str, request: Request) -> dict:
        entry = flows.get(flow_id, default)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"unknown flow {flow_id}")
        if entry.get("secure"):
            file_url = (
                str(request.base_url)
                + "secure-files/"
                + entry["filename"]
                + f":user={auth[0]}:pwd={auth[1]}"
            )
        else:
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
        return _serve(filename)

    @app.get("/secure-files/{filename}")
    def get_secure_file(
        filename: str, _: None = Depends(_require_auth)
    ) -> FileResponse:
        return _serve(filename)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", default="tests/data")
    parser.add_argument("--file", required=True, help="filename in --data-dir to serve")
    parser.add_argument("--name", default="data", help="data record name")
    parser.add_argument("--id", default="test-data-id", help="data record id")
    parser.add_argument(
        "--secure",
        action="store_true",
        help="serve the default flow behind HTTP Basic Auth",
    )
    args = parser.parse_args()

    default = {
        "filename": args.file,
        "name": args.name,
        "id": args.id,
        "secure": args.secure,
    }
    app = create_app(Path(args.data_dir), flows={}, default=default)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

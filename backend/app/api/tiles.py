from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter()

TILES_DIR = Path("infra/tiles/output")


@router.get("/manifest")
def tile_manifest() -> dict[str, object]:
    if not TILES_DIR.exists():
        return {"layers": [], "message": "No tile output directory. Run tile build pipeline."}
    archives = sorted(TILES_DIR.glob("*.pmtiles"))
    return {
        "layers": [
            {
                "id": archive.stem,
                "path": f"/api/tiles/{archive.stem}.pmtiles",
                "size_bytes": archive.stat().st_size,
            }
            for archive in archives
        ],
    }


@router.get("/{filename}")
def serve_tile(filename: str) -> FileResponse:
    if not filename.endswith(".pmtiles"):
        raise HTTPException(status_code=400, detail="Only .pmtiles files are served")
    path = TILES_DIR / filename
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail=f"Tile archive not found: {filename}")
    return FileResponse(
        path=str(path),
        media_type="application/octet-stream",
        headers={"Access-Control-Allow-Origin": "*"},
    )

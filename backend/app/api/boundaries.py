"""Serve sub-national boundary GeoJSON for global regions with GEOID baked in.

First request fetches from external APIs, simplifies geometries, and caches
to disk.  Subsequent requests serve instantly from disk cache.
"""

from __future__ import annotations

import json
import logging
import math
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.app.services.artifact_store import load_latest_artifact_bundle

router = APIRouter()
logger = logging.getLogger(__name__)

CACHE_DIR = Path("backend/data/boundary_cache")

SKIP_SA4_WORDS = frozenset(["Migratory", "No usual address", "Outside Australia", "Other Territories"])

AU_STATE_ABBREV: dict[str, str] = {
    "1": "NSW", "2": "VIC", "3": "QLD", "4": "SA",
    "5": "WA", "6": "TAS", "7": "NT", "8": "ACT",
}

IN_STATE_NAME_TO_ID: dict[str, str] = {
    "Andhra Pradesh": "IN-AP", "Arunachal Pradesh": "IN-AR", "Assam": "IN-AS",
    "Bihar": "IN-BR", "Chhattisgarh": "IN-CT", "Goa": "IN-GA",
    "Gujarat": "IN-GJ", "Haryana": "IN-HR", "Himachal Pradesh": "IN-HP",
    "Jharkhand": "IN-JH", "Karnataka": "IN-KA", "Kerala": "IN-KL",
    "Madhya Pradesh": "IN-MP", "Maharashtra": "IN-MH", "Manipur": "IN-MN",
    "Meghalaya": "IN-ML", "Mizoram": "IN-MZ", "Nagaland": "IN-NL",
    "Orissa": "IN-OR", "Odisha": "IN-OR", "Punjab": "IN-PB",
    "Rajasthan": "IN-RJ", "Sikkim": "IN-SK", "Tamil Nadu": "IN-TN",
    "Telangana": "IN-TG", "Tripura": "IN-TR", "Uttar Pradesh": "IN-UP",
    "Uttaranchal": "IN-UK", "Uttarakhand": "IN-UK", "West Bengal": "IN-WB",
    "Delhi": "IN-DL", "Jammu and Kashmir": "IN-JK",
    "Chandigarh": "IN-CH", "Puducherry": "IN-PY",
    "Andaman and Nicobar": "IN-AN", "Dadra and Nagar Haveli": "IN-DN",
    "Daman and Diu": "IN-DD", "Lakshadweep": "IN-LD",
}


# ---------------------------------------------------------------------------
# Geometry simplification (Ramer-Douglas-Peucker)
# ---------------------------------------------------------------------------

def _perpendicular_distance(pt: list[float], line_start: list[float], line_end: list[float]) -> float:
    dx = line_end[0] - line_start[0]
    dy = line_end[1] - line_start[1]
    if dx == 0 and dy == 0:
        return math.hypot(pt[0] - line_start[0], pt[1] - line_start[1])
    t = ((pt[0] - line_start[0]) * dx + (pt[1] - line_start[1]) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    proj_x = line_start[0] + t * dx
    proj_y = line_start[1] + t * dy
    return math.hypot(pt[0] - proj_x, pt[1] - proj_y)


def _rdp(points: list[list[float]], epsilon: float) -> list[list[float]]:
    """Ramer-Douglas-Peucker line simplification."""
    if len(points) <= 2:
        return points
    max_dist = 0.0
    max_idx = 0
    for i in range(1, len(points) - 1):
        d = _perpendicular_distance(points[i], points[0], points[-1])
        if d > max_dist:
            max_dist = d
            max_idx = i
    if max_dist > epsilon:
        left = _rdp(points[:max_idx + 1], epsilon)
        right = _rdp(points[max_idx:], epsilon)
        return left[:-1] + right
    return [points[0], points[-1]]


def _simplify_ring(ring: list[list[float]], epsilon: float) -> list[list[float]]:
    simplified = _rdp(ring, epsilon)
    if len(simplified) < 4:
        return ring if len(ring) >= 4 else simplified
    if simplified[0] != simplified[-1]:
        simplified.append(simplified[0])
    return simplified


def _round_coords(coords: list, precision: int = 3) -> list:
    if not coords:
        return coords
    if isinstance(coords[0], (int, float)):
        return [round(c, precision) for c in coords]
    return [_round_coords(c, precision) for c in coords]


def _simplify_geometry(geometry: dict[str, Any], epsilon: float = 0.01) -> dict[str, Any]:
    """Simplify a GeoJSON geometry, reducing coordinate count significantly."""
    gtype = geometry.get("type", "")
    coords = geometry.get("coordinates")
    if not coords:
        return geometry

    if gtype == "Polygon":
        new_rings = [_simplify_ring(ring, epsilon) for ring in coords]
        return {"type": "Polygon", "coordinates": _round_coords(new_rings)}
    elif gtype == "MultiPolygon":
        new_polys = []
        for polygon in coords:
            new_rings = [_simplify_ring(ring, epsilon) for ring in polygon]
            if any(len(r) >= 4 for r in new_rings):
                new_polys.append(new_rings)
        if not new_polys:
            return geometry
        return {"type": "MultiPolygon", "coordinates": _round_coords(new_polys)}
    else:
        return {"type": gtype, "coordinates": _round_coords(coords)}


def _simplify_feature_collection(fc: dict[str, Any], epsilon: float = 0.01) -> dict[str, Any]:
    """Simplify every feature's geometry in a FeatureCollection."""
    features = []
    for f in fc.get("features", []):
        geom = f.get("geometry")
        if geom:
            f = {**f, "geometry": _simplify_geometry(geom, epsilon)}
        features.append(f)
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Data fetching helpers
# ---------------------------------------------------------------------------

def _fetch_geojson(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read())


def _make_district_geoid(state_id: str, district_name: str) -> str:
    import hashlib as _hl
    clean = district_name.lower().replace(" ", "").replace("-", "")
    slug = clean[:6]
    short_hash = _hl.md5(district_name.encode()).hexdigest()[:3]
    return f"{state_id}-{slug}{short_hash}"


def _load_or_build(cache_name: str, builder: Any) -> dict[str, Any]:
    """Load from disk cache or build and save."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{cache_name}.json"
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if data.get("features"):
                return data
        except (json.JSONDecodeError, OSError):
            pass

    data = builder()
    cache_file.write_text(json.dumps(data), encoding="utf-8")
    return data


def _us_place_ids_with_metrics() -> set[str]:
    """US place GEOIDs (SSPPPPP) present in the metric artifact bundle."""
    bundle = load_latest_artifact_bundle("all")
    rows = bundle.get("metrics", [])
    place_ids = {
        str(r.get("geography_id", ""))
        for r in rows
        if str(r.get("geography_id", "")).isdigit() and len(str(r.get("geography_id", ""))) == 7
    }
    logger.info(
        "[US Places] metric dataset samples: total_rows=%s, unique_place_ids=%s",
        len(rows),
        len(place_ids),
    )
    return place_ids


# ---------------------------------------------------------------------------
# Region builders
# ---------------------------------------------------------------------------

def _build_au_geojson() -> dict[str, Any]:
    url = (
        "https://geo.abs.gov.au/arcgis/rest/services/ASGS2021/SA4/MapServer/0/query"
        "?where=1%3D1&outFields=sa4_code_2021,sa4_name_2021,state_code_2021,state_name_2021"
        "&returnGeometry=true&f=geojson&outSR=4326&resultRecordCount=200"
    )
    data = _fetch_geojson(url)
    filtered = []
    for f in data.get("features", []):
        props = f.get("properties", {})
        name = props.get("sa4_name_2021", "")
        state_code = str(props.get("state_code_2021", ""))
        state_abbr = AU_STATE_ABBREV.get(state_code)
        if not state_abbr or any(w in name for w in SKIP_SA4_WORDS):
            continue
        sa4_code = str(props.get("sa4_code_2021", ""))
        f["properties"] = {
            "GEOID": f"AU-SA4{sa4_code}",
            "name": name,
            "state": state_abbr,
            "score_opportunity": 0,
        }
        filtered.append(f)
    fc = {"type": "FeatureCollection", "features": filtered}
    return _simplify_feature_collection(fc, epsilon=0.005)


def _build_in_geojson() -> dict[str, Any]:
    url = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
    data = _fetch_geojson(url)
    for f in data.get("features", []):
        props = f.get("properties", {})
        state_name = props.get("NAME_1", "")
        district_name = props.get("NAME_2", "")
        state_id = IN_STATE_NAME_TO_ID.get(state_name, "")
        geo_id = _make_district_geoid(state_id, district_name) if state_id else ""
        f["properties"] = {
            "GEOID": geo_id,
            "name": f"{district_name}, {state_name}" if district_name else state_name,
            "state": state_name,
            "score_opportunity": 0,
        }
    return _simplify_feature_collection(data, epsilon=0.005)


def _build_eu_geojson() -> dict[str, Any]:
    url = "https://raw.githubusercontent.com/eurostat/Nuts2json/master/pub/v2/2024/4326/20M/nutsrg_2.json"
    data = _fetch_geojson(url)
    for f in data.get("features", []):
        nuts_id = f["properties"].get("id", "")
        f["properties"] = {
            "GEOID": f"EU-{nuts_id}" if nuts_id else "",
            "name": f["properties"].get("na", nuts_id),
            "score_opportunity": 0,
        }
    return _simplify_feature_collection(data, epsilon=0.005)


def _build_us_places_geojson() -> dict[str, Any]:
    """Build US place boundaries from Census TIGERweb (incorporated + CDP)."""
    place_ids = _us_place_ids_with_metrics()

    # TIGERweb 2025: 4=Incorporated Places, 5=Census Designated Places
    base = (
        "https://tigerweb.geo.census.gov/arcgis/rest/services/"
        "TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer"
    )
    features_by_geoid: dict[str, dict[str, Any]] = {}
    kind_counts: dict[str, int] = {"city": 0, "town": 0, "district": 0}

    def _classify_place_kind(name: str, lsadc: str, layer_id: int) -> str:
        lname = name.lower()
        if "town" in lname:
            return "town"
        # CDP and similar non-incorporated place labels.
        if layer_id == 5 or lsadc == "57":
            return "district"
        return "city"

    def _fetch_layer_features(layer_id: int) -> list[dict[str, Any]]:
        target_ids = sorted(place_ids)
        if not target_ids:
            return []
        out: list[dict[str, Any]] = []
        chunk_size = 180
        total_chunks = (len(target_ids) + chunk_size - 1) // chunk_size
        logger.info(
            "[US Places] fetch start: layer_id=%s target_ids=%s chunks=%s",
            layer_id,
            len(target_ids),
            total_chunks,
        )
        for i in range(0, len(target_ids), chunk_size):
            id_chunk = target_ids[i:i + chunk_size]
            in_list = ",".join(f"'{gid}'" for gid in id_chunk)
            where = urllib.parse.quote(f"GEOID IN ({in_list})", safe="()',")
            url = (
                f"{base}/{layer_id}/query"
                f"?where={where}"
                "&outFields=GEOID,NAME,STATE,PLACE,LSADC"
                "&returnGeometry=true"
                "&f=geojson"
                "&outSR=4326"
            )
            data = _fetch_geojson(url)
            chunk = data.get("features", [])
            out.extend(chunk)
            logger.info(
                "[US Places] fetch chunk: layer_id=%s chunk=%s/%s requested=%s fetched=%s",
                layer_id,
                (i // chunk_size) + 1,
                total_chunks,
                len(id_chunk),
                len(chunk),
            )
        logger.info("[US Places] fetch done: layer_id=%s fetched_total=%s", layer_id, len(out))
        return out

    for layer_id in (4, 5):
        for f in _fetch_layer_features(layer_id):
            props = f.get("properties", {})
            geoid = str(props.get("GEOID", ""))
            if not geoid or geoid not in place_ids:
                continue
            place_name = str(props.get("NAME", geoid))
            place_kind = _classify_place_kind(place_name, str(props.get("LSADC", "")), layer_id)
            kind_counts[place_kind] = kind_counts.get(place_kind, 0) + 1
            f["properties"] = {
                "GEOID": geoid,
                "name": place_name,
                "state": str(props.get("STATE", "")),
                "place_kind": place_kind,
                "score_opportunity": 0,
            }
            features_by_geoid.setdefault(geoid, f)

    matched = len(features_by_geoid)
    target = len(place_ids)
    missing = max(0, target - matched)
    fill_pct = round((matched / target) * 100, 2) if target else 0.0
    logger.info(
        "[US Places] boundary fill coverage: matched=%s target=%s missing=%s fill_pct=%s",
        matched,
        target,
        missing,
        fill_pct,
    )
    logger.info(
        "[US Places] kind coverage: city=%s town=%s district=%s",
        kind_counts.get("city", 0),
        kind_counts.get("town", 0),
        kind_counts.get("district", 0),
    )
    return {"type": "FeatureCollection", "features": list(features_by_geoid.values())}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/au")
def get_au_boundaries() -> JSONResponse:
    data = _load_or_build("au_sa4_v2", _build_au_geojson)
    return JSONResponse(content=data, headers={"Cache-Control": "public, max-age=86400"})


@router.get("/in")
def get_in_boundaries() -> JSONResponse:
    data = _load_or_build("in_districts_v2", _build_in_geojson)
    return JSONResponse(content=data, headers={"Cache-Control": "public, max-age=86400"})


@router.get("/eu")
def get_eu_boundaries() -> JSONResponse:
    data = _load_or_build("eu_nuts2_v2", _build_eu_geojson)
    return JSONResponse(content=data, headers={"Cache-Control": "public, max-age=86400"})


@router.get("/us_places")
def get_us_place_boundaries() -> JSONResponse:
    data = _load_or_build("us_places_v1", _build_us_places_geojson)
    return JSONResponse(content=data, headers={"Cache-Control": "public, max-age=86400"})

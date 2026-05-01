"""AI-powered geographic industry research using SerpAPI + OpenAI."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

FIPS_TO_STATE: dict[str, str] = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa", "20": "Kansas",
    "21": "Kentucky", "22": "Louisiana", "23": "Maine", "24": "Maryland",
    "25": "Massachusetts", "26": "Michigan", "27": "Minnesota", "28": "Mississippi",
    "29": "Missouri", "30": "Montana", "31": "Nebraska", "32": "Nevada",
    "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico", "36": "New York",
    "37": "North Carolina", "38": "North Dakota", "39": "Ohio", "40": "Oklahoma",
    "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island", "45": "South Carolina",
    "46": "South Dakota", "47": "Tennessee", "48": "Texas", "49": "Utah",
    "50": "Vermont", "51": "Virginia", "53": "Washington", "54": "West Virginia",
    "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico",
}

CBSA_NAMES: dict[str, str] = {
    "12060": "Atlanta, GA", "12420": "Austin, TX", "13820": "Birmingham, AL",
    "14460": "Boston, MA", "16740": "Charlotte, NC", "16980": "Chicago, IL",
    "17460": "Cleveland, OH", "19100": "Dallas-Fort Worth, TX",
    "19740": "Denver, CO", "19820": "Detroit, MI", "26420": "Houston, TX",
    "29820": "Las Vegas, NV", "31080": "Los Angeles, CA",
    "33100": "Miami, FL", "33460": "Minneapolis, MN",
    "35380": "New Orleans, LA", "35620": "New York, NY",
    "36740": "Orlando, FL", "37980": "Philadelphia, PA",
    "38060": "Phoenix, AZ", "38300": "Pittsburgh, PA",
    "38900": "Portland, OR", "40060": "Richmond, VA",
    "40140": "Riverside, CA", "41180": "St. Louis, MO",
    "41620": "Salt Lake City, UT", "41700": "San Antonio, TX",
    "41740": "San Diego, CA", "41860": "San Francisco, CA",
    "42660": "Seattle, WA", "45300": "Tampa, FL", "47900": "Washington, DC",
}

EU_NUTS_COUNTRY: dict[str, str] = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia",
    "CY": "Cyprus", "CZ": "Czechia", "DK": "Denmark", "EE": "Estonia",
    "FI": "Finland", "FR": "France", "DE": "Germany", "EL": "Greece",
    "HU": "Hungary", "IE": "Ireland", "IT": "Italy", "LV": "Latvia",
    "LT": "Lithuania", "LU": "Luxembourg", "MT": "Malta", "NL": "Netherlands",
    "PL": "Poland", "PT": "Portugal", "RO": "Romania", "SK": "Slovakia",
    "SI": "Slovenia", "ES": "Spain", "SE": "Sweden", "NO": "Norway",
    "CH": "Switzerland", "UK": "United Kingdom", "IS": "Iceland",
    "AL": "Albania", "RS": "Serbia", "ME": "Montenegro", "MK": "North Macedonia",
    "TR": "Turkey", "BA": "Bosnia and Herzegovina",
}

AU_STATE_NAMES: dict[str, str] = {
    "NSW": "New South Wales", "VIC": "Victoria", "QLD": "Queensland",
    "SA": "South Australia", "WA": "Western Australia", "TAS": "Tasmania",
    "NT": "Northern Territory", "ACT": "Australian Capital Territory",
}

IN_STATE_NAMES: dict[str, str] = {
    "AP": "Andhra Pradesh", "AR": "Arunachal Pradesh", "AS": "Assam",
    "BR": "Bihar", "CT": "Chhattisgarh", "GA": "Goa", "GJ": "Gujarat",
    "HR": "Haryana", "HP": "Himachal Pradesh", "JH": "Jharkhand",
    "KA": "Karnataka", "KL": "Kerala", "MP": "Madhya Pradesh",
    "MH": "Maharashtra", "MN": "Manipur", "ML": "Meghalaya", "MZ": "Mizoram",
    "NL": "Nagaland", "OR": "Odisha", "PB": "Punjab", "RJ": "Rajasthan",
    "SK": "Sikkim", "TN": "Tamil Nadu", "TG": "Telangana", "TR": "Tripura",
    "UP": "Uttar Pradesh", "UK": "Uttarakhand", "WB": "West Bengal",
    "DL": "Delhi", "JK": "Jammu and Kashmir", "CH": "Chandigarh",
    "PY": "Puducherry", "AN": "Andaman and Nicobar",
    "DN": "Dadra and Nagar Haveli", "DD": "Daman and Diu", "LD": "Lakshadweep",
}

IN_STATE_NAMES_TO_ID: dict[str, str] = {}
for _code, _name in IN_STATE_NAMES.items():
    IN_STATE_NAMES_TO_ID[_name] = _code
IN_STATE_NAMES_TO_ID["Orissa"] = "OR"
IN_STATE_NAMES_TO_ID["Uttaranchal"] = "UK"

CACHE_DIR = Path("backend/data/ai_research_cache")
NAME_CACHE_FILE = Path("backend/data/geography_names.json")
CACHE_TTL_SECONDS = 86400 * 7
AI_PROMPT_VERSION = "v4-anti-generic"

_name_cache: dict[str, str] = {}
_boundary_names: dict[str, str] = {}


def _load_name_cache() -> dict[str, str]:
    global _name_cache
    if _name_cache:
        return _name_cache
    if NAME_CACHE_FILE.exists():
        try:
            _name_cache = json.loads(NAME_CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            _name_cache = {}
    return _name_cache


def _save_name_cache() -> None:
    NAME_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    NAME_CACHE_FILE.write_text(json.dumps(_name_cache, indent=2), encoding="utf-8")


def _load_boundary_names() -> dict[str, str]:
    """Load region names from lightweight API calls (no geometry)."""
    global _boundary_names
    if _boundary_names:
        return _boundary_names

    import urllib.request
    import urllib.error
    import hashlib as _hl

    try:
        url = (
            "https://geo.abs.gov.au/arcgis/rest/services/ASGS2021/SA4/MapServer/0/query"
            "?where=1%3D1&outFields=sa4_code_2021,sa4_name_2021"
            "&returnGeometry=false&f=json&resultRecordCount=200"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
        for feat in data.get("features", []):
            attr = feat.get("attributes", {})
            code = str(attr.get("sa4_code_2021", ""))
            name = attr.get("sa4_name_2021", "")
            if code and name:
                _boundary_names[f"AU-SA4{code}"] = f"{name}, Australia"
    except Exception as exc:
        print(f"[AI Research] AU SA4 name load failed: {exc}")

    try:
        url = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
        for f in data.get("features", []):
            p = f.get("properties", {})
            state_name = p.get("NAME_1", "")
            district_name = p.get("NAME_2", "")
            state_id = IN_STATE_NAMES_TO_ID.get(state_name)
            if state_id and district_name:
                clean = district_name.lower().replace(" ", "").replace("-", "")
                slug = clean[:6]
                short_hash = _hl.md5(district_name.encode()).hexdigest()[:3]
                geo_id = f"IN-{state_id}-{slug}{short_hash}"
                if district_name.lower() == state_name.lower():
                    _boundary_names[geo_id] = f"{district_name}, India"
                else:
                    _boundary_names[geo_id] = f"{district_name}, {state_name}, India"
    except Exception as exc:
        print(f"[AI Research] IN district name load failed: {exc}")

    try:
        url = "http://localhost:8000/api/boundaries/eu"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        for f in data.get("features", []):
            geo_id = f["properties"].get("GEOID", "")
            name = f["properties"].get("name", "")
            if geo_id and name:
                nuts_code = geo_id[3:] if geo_id.startswith("EU-") else ""
                country_code = nuts_code[:2] if nuts_code else ""
                country = EU_NUTS_COUNTRY.get(country_code, "")
                _boundary_names[geo_id] = f"{name}, {country}" if country else name
    except Exception as exc:
        print(f"[AI Research] EU name load failed: {exc}")

    return _boundary_names


def _census_name_lookup(geography_id: str) -> str | None:
    import urllib.request
    import urllib.error

    census_key = os.getenv("CENSUS_API_KEY", "").strip()
    if not census_key:
        return None

    try:
        if len(geography_id) == 5 and geography_id.isdigit():
            state_fips = geography_id[:2]
            county_fips = geography_id[2:]
            url = (
                f"https://api.census.gov/data/2022/acs/acs5"
                f"?get=NAME&for=county:{county_fips}&in=state:{state_fips}"
                f"&key={census_key}"
            )
        elif len(geography_id) == 7 and geography_id.isdigit():
            state_fips = geography_id[:2]
            place_fips = geography_id[2:]
            url = (
                f"https://api.census.gov/data/2022/acs/acs5"
                f"?get=NAME&for=place:{place_fips}&in=state:{state_fips}"
                f"&key={census_key}"
            )
        else:
            return None

        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            if "text/html" in resp.headers.get("Content-Type", ""):
                return None
            data = json.loads(resp.read())
            if len(data) >= 2:
                return data[1][0]
    except (urllib.error.URLError, json.JSONDecodeError, IndexError, OSError):
        pass
    return None


def _detect_country(geography_id: str) -> str:
    """Return the country context for a geography ID."""
    if geography_id.startswith("AU-"):
        return "Australia"
    if geography_id.startswith("IN-"):
        return "India"
    if geography_id.startswith("EU-"):
        nuts_code = geography_id[3:]
        country_code = nuts_code[:2]
        return EU_NUTS_COUNTRY.get(country_code, "Europe")
    return "United States"


def _detect_region_context(geography_id: str) -> str:
    """Return state/province context for sub-national IDs."""
    if geography_id.startswith("AU-SA4"):
        bd_names = _load_boundary_names()
        name = bd_names.get(geography_id, "")
        state = ""
        for f_geo_id, f_name in bd_names.items():
            if f_geo_id == geography_id:
                break
        return "Australia"

    if geography_id.startswith("IN-") and geography_id.count("-") >= 2:
        state_code = geography_id.split("-")[1]
        state_name = IN_STATE_NAMES.get(state_code, "")
        if state_name:
            return f"{state_name}, India"
        return "India"

    if geography_id.startswith("EU-"):
        nuts_code = geography_id[3:]
        country_code = nuts_code[:2]
        country = EU_NUTS_COUNTRY.get(country_code, "Europe")
        return country

    if geography_id.isdigit():
        state_fips = geography_id[:2]
        state = FIPS_TO_STATE.get(state_fips, "")
        if state:
            return f"{state}, United States"
    return "United States"


def _resolve_geography_name(geography_id: str) -> str:
    """Resolve any geography ID to a human-readable name.

    Boundary names (from live GeoJSON) take precedence over disk cache.
    """
    bd_names = _load_boundary_names()
    if geography_id in bd_names:
        name = bd_names[geography_id]
        _name_cache[geography_id] = name
        _save_name_cache()
        return name

    cache = _load_name_cache()
    if geography_id in cache and "Geography " not in cache[geography_id]:
        return cache[geography_id]

    if len(geography_id) == 2 and geography_id.isdigit():
        name = FIPS_TO_STATE.get(geography_id, f"State {geography_id}")
    elif geography_id in CBSA_NAMES:
        name = CBSA_NAMES[geography_id]
    elif (len(geography_id) == 5 or len(geography_id) == 7) and geography_id.isdigit():
        census_name = _census_name_lookup(geography_id)
        if census_name:
            name = census_name
        else:
            state_fips = geography_id[:2]
            state_name = FIPS_TO_STATE.get(state_fips, "")
            kind = "County" if len(geography_id) == 5 else "Place"
            name = f"{kind} {geography_id}, {state_name}" if state_name else f"{kind} {geography_id}"
    elif geography_id.startswith("AU-SA4"):
        name = f"SA4 Region {geography_id[6:]}, Australia"
    elif geography_id.startswith("AU-"):
        state_code = geography_id[3:]
        name = AU_STATE_NAMES.get(state_code, f"Australia {state_code}")
    elif geography_id.startswith("IN-") and geography_id.count("-") >= 2:
        state_code = geography_id.split("-")[1]
        state_name = IN_STATE_NAMES.get(state_code, state_code)
        name = f"District in {state_name}, India"
    elif geography_id.startswith("IN-"):
        state_code = geography_id[3:]
        name = IN_STATE_NAMES.get(state_code, f"India {state_code}")
    elif geography_id.startswith("EU-"):
        nuts_code = geography_id[3:]
        country_code = nuts_code[:2]
        country = EU_NUTS_COUNTRY.get(country_code, "Europe")
        name = f"NUTS-2 Region {nuts_code}, {country}"
    else:
        name = f"Geography {geography_id}"

    _name_cache[geography_id] = name
    _save_name_cache()
    return name


def _serpapi_locale(geography_id: str) -> dict[str, str]:
    """Return SerpAPI gl (country) and hl (language) based on geography."""
    if geography_id.startswith("AU-"):
        return {"gl": "au", "hl": "en"}
    if geography_id.startswith("IN-"):
        return {"gl": "in", "hl": "en"}
    if geography_id.startswith("EU-"):
        nuts_code = geography_id[3:]
        cc = nuts_code[:2].lower()
        gl_map = {
            "at": "at", "be": "be", "bg": "bg", "hr": "hr", "cy": "cy",
            "cz": "cz", "dk": "dk", "ee": "ee", "fi": "fi", "fr": "fr",
            "de": "de", "el": "gr", "hu": "hu", "ie": "ie", "it": "it",
            "lv": "lv", "lt": "lt", "lu": "lu", "mt": "mt", "nl": "nl",
            "pl": "pl", "pt": "pt", "ro": "ro", "sk": "sk", "si": "si",
            "es": "es", "se": "se", "no": "no", "ch": "ch", "uk": "uk",
            "is": "is", "al": "al", "rs": "rs", "me": "me", "mk": "mk",
            "tr": "tr", "ba": "ba",
        }
        return {"gl": gl_map.get(cc, "us"), "hl": "en"}
    return {"gl": "us", "hl": "en"}


def _cache_key(geography_id: str) -> str:
    return hashlib.sha256(geography_id.encode()).hexdigest()[:16]


def _metrics_signature(metric_context: dict[str, Any], scenario_id: str) -> str:
    watched = {
        "scenario_id": scenario_id,
        "opportunity_score": metric_context.get("opportunity_score"),
        "business_demand_score": metric_context.get("business_demand_score"),
        "talent_scarcity_score": metric_context.get("talent_scarcity_score"),
        "market_gap_score": metric_context.get("market_gap_score"),
        "unemployment_rate": metric_context.get("unemployment_rate"),
        "labor_force": metric_context.get("labor_force"),
        "population": metric_context.get("population"),
    }
    raw = json.dumps(watched, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _load_cached(geography_id: str) -> dict[str, Any] | None:
    cache_file = CACHE_DIR / f"{_cache_key(geography_id)}.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        if time.time() - data.get("timestamp", 0) > CACHE_TTL_SECONDS:
            return None
        return data
    except (json.JSONDecodeError, KeyError):
        return None


def _save_cached(geography_id: str, payload: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload["timestamp"] = time.time()
    cache_file = CACHE_DIR / f"{_cache_key(geography_id)}.json"
    cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _serpapi_search(query: str, locale: dict[str, str] | None = None) -> list[dict[str, str]]:
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("SERPAPI_KEY not set. Get one at https://serpapi.com/")

    from serpapi import GoogleSearch

    params = {
        "q": query,
        "api_key": api_key,
        "engine": "google",
        "num": 8,
        "hl": "en",
        "gl": "us",
    }
    if locale:
        params.update(locale)

    search = GoogleSearch(params)
    results = search.get_dict()

    organic: list[dict[str, str]] = []
    for item in results.get("organic_results", []):
        organic.append({
            "title": item.get("title", ""),
            "snippet": item.get("snippet", ""),
            "link": item.get("link", ""),
        })

    knowledge_graph = results.get("knowledge_graph", {})
    if knowledge_graph.get("description"):
        organic.insert(0, {
            "title": knowledge_graph.get("title", "Knowledge Graph"),
            "snippet": knowledge_graph["description"],
            "link": knowledge_graph.get("source", {}).get("link", ""),
        })

    return organic[:10]


def _build_research_context(
    geography_name: str,
    search_results: list[dict[str, str]],
    metric_context: dict[str, Any] | None = None,
) -> str:
    parts: list[str] = []
    for i, sr in enumerate(search_results, 1):
        parts.append(f"[{i}] {sr['title']}\n{sr['snippet']}\nSource: {sr['link']}")

    search_text = "\n\n".join(parts)

    metric_text = ""
    if metric_context:
        metric_lines = []
        for k, v in metric_context.items():
            if isinstance(v, float):
                metric_lines.append(f"  {k}: {v:,.2f}")
            else:
                metric_lines.append(f"  {k}: {v}")
        metric_text = "\n--- Internal Metrics ---\n" + "\n".join(metric_lines)

    return f"""Web Research Results for: {geography_name}

{search_text}
{metric_text}"""


def _build_system_prompt(geography_name: str, country: str, region_context: str) -> str:
    """Build a system prompt adapted for the geography's country context."""

    if country == "United States":
        area_term = "county"
        extra_instruction = "ONLY this exact county/city. Do not generalize about the state."
    elif country == "Australia":
        area_term = "statistical area"
        extra_instruction = f"Focus specifically on this SA4 statistical area in {region_context}. Do not generalize about all of Australia."
    elif country == "India":
        area_term = "district"
        extra_instruction = f"Focus specifically on this district in {region_context}. Use both English and local-language employer names where relevant."
    else:
        area_term = "region"
        extra_instruction = f"Focus specifically on this NUTS-2 region in {country}. Name employers and industries specific to this region, not the whole country."

    return f"""You are a geographic labor-market analyst covering {country}. Write a structured intel brief for the {area_term} **{geography_name}** using the numbered web sources [1], [2], etc. Cite web sources inline as [N]. Do NOT cite internal metrics — just state the numbers naturally.

Use ONLY bullet points. Every section must use bullet-point lists. No long paragraphs.

## Top Industries

List EXACTLY 4 industries for this specific {area_term}. For each, write a detailed bullet covering:
1. Industry name in bold
2. Estimated share of {area_term} workforce OR GDP contribution (use % — estimate from sources or general knowledge of this specific {area_term})
3. 2-3 major employers by name (local employers specific to this {area_term})
4. What they are hiring for (roles/positions) or recent hiring activity from sources [N]
5. Any news: expansions, layoffs, closures, investments — cite [N]
6. **Buyer receptivity**: which functional buyer role(s) in these companies are most likely to adopt an outcome-based, low-overhead automation/outsourcing service. Name specific roles and explain pain-point fit in 1-2 sentences.

Example format:
- **Manufacturing** (~22% of workforce): SEAT (Martorell plant), Nissan. Hiring production line operators, mechanical engineers. SEAT announced EV expansion creating 1,200 jobs [3].
- **Healthcare** (~15% of workforce): Hospital Clínic, Quirónsalud. Hiring registered nurses, medical technicians. Hospital expansion underway [6].

Under each of the 4 industries, include a sub-bullet:
- **Most receptive buyer roles**: <role 1>, <role 2> — why these roles are likely buyers of outcome-based services with minimal operational overhead.

Do not anchor analysis to the example roles above. Choose roles based on the actual industry structure and hiring dynamics in this geography.

You MUST list exactly 4. Use your knowledge of this {area_term}'s economy combined with the sources. If sources don't cover an industry, still list it with what you know but note the roles typically hired for.

## Hiring & Talent Situation

Bullet points — one per relevant source:
- **[N] "Article title"**: What it reports about hiring, layoffs, or workforce in this area.

Then a final bullet:
- **Overall**: Shortage / Surplus / Balanced — 1-2 sentences using internal metrics (unemployment rate, labor force, education level) stated as plain numbers without citation tags.

## Operating Conditions

Bullet points using internal metrics as plain numbers:
- **Cost of living**: relative to national average
- **Infrastructure**: broadband access, transport connectivity
- **Labor market**: key characteristics (education level, workforce size, specializations)
- **Competition**: staffing competitors or major recruiters if named in sources

## Assessment

- **Demand**: Weak / Moderate / Strong — 1 sentence, cite [N]
- **Talent**: Surplus / Balanced / Scarce — 1 sentence
- **Feasibility**: Low / Moderate / High — 1 sentence
- **Bottom line**: 1 blunt sentence.

Rules:
- USE BULLET POINTS EVERYWHERE. No walls of text.
- Cite web sources as [N]. NEVER cite internal metrics.
- {extra_instruction}
- Do NOT fabricate employer names you aren't confident about. But DO estimate workforce shares.
- NEVER write filler like "No recent coverage found" — just skip it.
- Ban generic phrasing. Avoid broad claims like "strong ecosystem", "well-positioned", "in today's market", or "across many sectors" unless immediately backed by a concrete local fact.
- Every industry bullet must include at least one concrete local detail (named employer, role title, expansion/layoff event, or numeric indicator).
- Neutral, direct tone. No promotional language.
- Calibrate your tone to internal metrics: if internal demand/supply/opportunity signals are weak, the bottom line must be cautious (Pilot/Monitor/Avoid), not optimistic.
- If web news appears positive but internal metrics are weak (or vice versa), explicitly call out the divergence in one bullet.
- Use this calibration guide when `opportunity_score` is present in internal metrics:
  - `<45`: risk-first tone; do not use "strong opportunity" wording.
  - `45-60`: mixed/conditional tone.
  - `>60`: stronger opportunity tone only if sources support it.
- 450-600 words total."""


def _gpt_synthesize(
    geography_name: str,
    context: str,
    country: str,
    region_context: str,
    metric_context: dict[str, Any] | None = None,
) -> tuple[str, float]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY not set.")

    from openai import OpenAI
    from backend.app.services.llm_config import grounding_preamble, primary_model

    client = OpenAI(api_key=api_key)
    model = primary_model()

    system_prompt = f"{grounding_preamble()}\n\n" + _build_system_prompt(geography_name, country, region_context)

    opp = None
    if metric_context:
        try:
            opp = float(metric_context.get("opportunity_score"))
        except (TypeError, ValueError):
            opp = None
    if opp is None:
        score_band = "unknown"
    elif opp < 45:
        score_band = "low"
    elif opp <= 60:
        score_band = "mixed"
    else:
        score_band = "high"

    user_prompt = f"""Produce a geographic intelligence briefing for: **{geography_name}** ({region_context})

Score alignment guardrails:
- Current internal opportunity_score: {opp if opp is not None else "N/A"}
- score_band: {score_band}
- Assessment and Bottom line MUST match this score band unless you explicitly justify a divergence.

{context}"""

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
        max_completion_tokens=2000,
    )

    briefing = response.choices[0].message.content or ""
    adjustment = _compute_news_score_adjustment(client, model, geography_name, briefing)
    return briefing, adjustment


def _compute_news_score_adjustment(
    client: Any,
    model: str,
    geography_name: str,
    briefing: str,
) -> float:
    score_prompt = f"""You are scoring the news impact on a staffing firm's opportunity in {geography_name}.

Based ONLY on the briefing below, rate how recent news should adjust the base opportunity score.

Consider from a STAFFING FIRM perspective:
- Layoffs / closures = MORE opportunity (displaced workers need placement, but also signals weakening demand — net effect depends on scale)
- New facility openings / expansions / hiring surges = MORE opportunity (employers need staff)
- Labor shortages reported = STRONG positive (staffing firms thrive on scarcity)
- Surplus of talent / rising unemployment = slightly negative (less urgency for staffing services)
- No relevant news = 0 adjustment
- Mixed signals = small adjustment in the dominant direction

Return ONLY a single number between -15 and +15 (integer). No text, no explanation, just the number.

Briefing:
{briefing}"""

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": score_prompt}],
            temperature=0.0,
            max_completion_tokens=10,
        )
        raw = (resp.choices[0].message.content or "0").strip()
        val = float(raw)
        return max(-15.0, min(15.0, val))
    except (ValueError, TypeError):
        return 0.0


def _gather_internal_metrics(geography_id: str, scenario_id: str) -> dict[str, Any]:
    try:
        from backend.app.services.metrics_engine import metric_bundle_from_artifacts
        from backend.app.services.analysis_engine import score_for_geography
        metrics = metric_bundle_from_artifacts(geography_id)
        raw: dict[str, Any] = {}
        for m in metrics:
            raw[m.metric_name] = m.raw_value
        key_metrics = {}
        for name in [
            "population", "labor_force", "unemployment_rate",
            "industry_employment", "business_establishments",
            "median_household_income", "educational_attainment_bachelors_plus",
            "target_occupation_employment", "gdp_current_dollars",
            "gdp_per_capita_ppp", "internet_access_rate",
            "regional_price_parity", "job_creation_rate",
            "work_from_home_rate", "employment_to_pop_ratio",
        ]:
            if name in raw:
                key_metrics[name] = raw[name]
        try:
            score, features = score_for_geography(geography_id=geography_id, scenario_id=scenario_id)
            key_metrics["opportunity_score"] = score.score_value
            key_metrics["business_demand_score"] = round(features.get("business_demand", 0.0) * 100, 2)
            key_metrics["talent_scarcity_score"] = round((1.0 - features.get("talent_supply", 0.0)) * 100, 2)
            key_metrics["market_gap_score"] = round(features.get("market_gap", 0.0) * 100, 2)
        except Exception:
            pass
        return key_metrics
    except Exception:
        return {}


def _build_search_queries(geography_name: str, geography_id: str, country: str, region_context: str) -> list[str]:
    """Build targeted search queries based on geography type and country.

    Year literals are bound at call-time to the current + prior year so
    searches always surface the freshest reporting regardless of when
    Figwork is running.
    """
    from backend.app.services.llm_config import current_year

    year = current_year()
    years = f"{year - 1} {year}"

    if country == "United States":
        return [
            f'"{geography_name}" hiring layoffs employers jobs {years}',
            f'"{geography_name}" labor shortage workforce unemployment employment trends',
            f'"{geography_name}" top industries economy major employers',
            f'"{geography_name}" new jobs expansion closing economic news',
        ]

    if country == "Australia":
        return [
            f'"{geography_name}" Australia jobs employers hiring {years}',
            f'"{geography_name}" {region_context} industries economy workforce',
            f'"{geography_name}" labour shortage employment unemployment Australia',
            f'"{geography_name}" new investment expansion major employers Australia',
        ]

    if country == "India":
        return [
            f'"{geography_name}" India jobs hiring employers industries {years}',
            f'"{geography_name}" {region_context} economy workforce employment',
            f'"{geography_name}" India industrial growth investment manufacturing IT',
            f'"{geography_name}" India unemployment labour market talent',
        ]

    return [
        f'"{geography_name}" {country} jobs employers hiring {years}',
        f'"{geography_name}" {country} industries economy workforce employment',
        f'"{geography_name}" {country} labor market unemployment talent trends',
        f'"{geography_name}" {country} investment expansion economic news',
    ]


def research_geography(geography_id: str, scenario_id: str = "default-opportunity") -> dict[str, Any]:
    """Main entry point: research a geography's local industry and talent landscape."""
    metric_context = _gather_internal_metrics(geography_id, scenario_id=scenario_id)
    current_sig = _metrics_signature(metric_context, scenario_id=scenario_id)
    cached = _load_cached(geography_id)
    if (
        cached
        and cached.get("summary")
        and cached.get("prompt_version") == AI_PROMPT_VERSION
        and cached.get("metrics_signature") == current_sig
    ):
        return {
            "geography_id": geography_id,
            "scenario_id": scenario_id,
            "geography_name": cached["geography_name"],
            "summary": cached["summary"],
            "sources": cached.get("sources", []),
            "news_score_adjustment": cached.get("news_score_adjustment", 0.0),
            "cached": True,
        }

    geography_name = _resolve_geography_name(geography_id)
    country = _detect_country(geography_id)
    region_context = _detect_region_context(geography_id)

    locale = _serpapi_locale(geography_id)
    search_queries = _build_search_queries(geography_name, geography_id, country, region_context)

    serpapi_available = bool(os.getenv("SERPAPI_KEY", "").strip())
    all_results: list[dict[str, str]] = []

    if serpapi_available:
        for query in search_queries:
            try:
                results = _serpapi_search(query, locale=locale)
                all_results.extend(results)
            except Exception as exc:
                print(f"[AI Research] SerpAPI search failed for '{query}': {exc}")

    seen_links: set[str] = set()
    deduped: list[dict[str, str]] = []
    for r in all_results:
        if r["link"] not in seen_links:
            seen_links.add(r["link"])
            deduped.append(r)

    openai_available = bool(os.getenv("OPENAI_API_KEY", "").strip())

    if deduped:
        research_context = _build_research_context(geography_name, deduped[:12], metric_context)
        summary, news_adj = _gpt_synthesize(
            geography_name,
            research_context,
            country,
            region_context,
            metric_context=metric_context,
        )
        sources = [{"title": r["title"], "url": r["link"]} for r in deduped[:8]]
    elif openai_available and metric_context:
        summary, news_adj = _metrics_only_synthesis(
            geography_name, country, region_context, metric_context
        )
        sources = []
    else:
        summary = _plain_metrics_summary(geography_name, metric_context)
        news_adj = 0.0
        sources = []

    result = {
        "geography_id": geography_id,
        "scenario_id": scenario_id,
        "geography_name": geography_name,
        "summary": summary,
        "sources": sources,
        "news_score_adjustment": round(news_adj, 1),
        "prompt_version": AI_PROMPT_VERSION,
        "metrics_signature": current_sig,
        "cached": False,
    }

    _save_cached(geography_id, result)
    return result


def _metrics_only_synthesis(
    geography_name: str,
    country: str,
    region_context: str,
    metric_context: dict[str, Any],
) -> tuple[str, float]:
    """Generate a briefing from internal metrics only (no web sources) via OpenAI."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _plain_metrics_summary(geography_name, metric_context), 0.0

    from openai import OpenAI
    from backend.app.services.llm_config import grounding_preamble, primary_model

    client = OpenAI(api_key=api_key)
    model = primary_model()

    metric_lines = []
    for k, v in metric_context.items():
        metric_lines.append(f"  {k}: {v:,.2f}" if isinstance(v, float) else f"  {k}: {v}")
    metric_block = "\n".join(metric_lines)

    system = (
        f"{grounding_preamble()}\n\n"
        f"You are a geographic labor-market analyst. The user will provide internal metrics "
        f"for **{geography_name}** ({region_context}, {country}). No web sources are available. "
        f"Write a concise intelligence brief using ONLY bullet points. Cover: key metrics summary, "
        f"likely top industries (based on your knowledge of this area), operating conditions, "
        f"and a short assessment (Demand/Talent/Feasibility/Bottom line). "
        f"300-400 words. Neutral, direct tone. Do NOT fabricate specific employers unless confident. "
        f"Never use broad boilerplate wording; each bullet must reference a concrete metric, role, "
        f"or local industry fact from the provided metrics."
    )
    user_msg = f"Internal metrics for {geography_name}:\n{metric_block}"

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_completion_tokens=1500,
        )
        briefing = resp.choices[0].message.content or ""
    except Exception as exc:
        print(f"[AI Research] Metrics-only synthesis failed: {exc}")
        return _plain_metrics_summary(geography_name, metric_context), 0.0

    return briefing, 0.0


def _plain_metrics_summary(geography_name: str, metric_context: dict[str, Any]) -> str:
    """Last-resort plain-text summary when neither SerpAPI nor OpenAI is available."""
    if not metric_context:
        return (
            f"**{geography_name}**\n\n"
            f"No scoring data or web research is available for this geography. "
            f"Configure `SERPAPI_KEY` and/or `OPENAI_API_KEY` in `.env` for full AI briefings, "
            f"or run the data pipeline to generate metric artifacts."
        )
    lines = [f"**{geography_name}** — Internal Metrics Summary\n"]
    for k, v in metric_context.items():
        label = k.replace("_", " ").title()
        lines.append(f"- **{label}**: {v:,.2f}" if isinstance(v, float) else f"- **{label}**: {v}")
    lines.append(
        "\n*This is an internal-metrics-only summary. "
        "Configure `SERPAPI_KEY` for web-sourced research and richer AI briefings.*"
    )
    return "\n".join(lines)

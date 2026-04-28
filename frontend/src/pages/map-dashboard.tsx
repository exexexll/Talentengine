import React from "react";
import { createPortal } from "react-dom";
import type MapLibreGL from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { marked } from "marked";

import "./map-dashboard.css";

const CACHE_DB = "figwork_map_cache";
const CACHE_STORE = "geodata";
const CACHE_VERSION = 2;
const CACHE_TTL_MS = 1000 * 60 * 60 * 24 * 3;

function openCacheDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(CACHE_DB, CACHE_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(CACHE_STORE)) db.createObjectStore(CACHE_STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function getCached<T>(key: string): Promise<T | null> {
  try {
    const db = await openCacheDB();
    return new Promise((resolve) => {
      const tx = db.transaction(CACHE_STORE, "readonly");
      const store = tx.objectStore(CACHE_STORE);
      const req = store.get(key);
      req.onsuccess = () => {
        const entry = req.result;
        if (entry && Date.now() - (entry.ts || 0) < CACHE_TTL_MS) resolve(entry.data as T);
        else resolve(null);
      };
      req.onerror = () => resolve(null);
    });
  } catch { return null; }
}

async function setCache(key: string, data: unknown): Promise<void> {
  try {
    const db = await openCacheDB();
    const tx = db.transaction(CACHE_STORE, "readwrite");
    tx.objectStore(CACHE_STORE).put({ data, ts: Date.now() }, key);
  } catch { /* silent */ }
}

async function cachedFetch<T>(url: string, cacheKey?: string): Promise<T> {
  const key = cacheKey || url;
  const cached = await getCached<T>(key);
  if (cached) return cached;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status}`);
  const data = await res.json();
  await setCache(key, data);
  return data as T;
}

// In-memory (session) dedupe + short-TTL cache for rapid nav.  This prevents
// the same endpoint from being hit repeatedly on every geography click when
// the response depends only on scenario_id.
const _memoryCache = new Map<string, { data: unknown; ts: number }>();
const _inFlight = new Map<string, Promise<unknown>>();
async function memoFetch<T>(url: string, ttlMs = 5 * 60 * 1000): Promise<T> {
  const hit = _memoryCache.get(url);
  if (hit && Date.now() - hit.ts < ttlMs) return hit.data as T;
  const pending = _inFlight.get(url);
  if (pending) return pending as Promise<T>;
  const p = (async () => {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`${res.status}`);
      const data = (await res.json()) as T;
      _memoryCache.set(url, { data, ts: Date.now() });
      return data;
    } finally {
      _inFlight.delete(url);
    }
  })();
  _inFlight.set(url, p);
  return p;
}

async function memoFetchPost<T>(url: string, body: unknown, cacheKey: string, ttlMs = 5 * 60 * 1000): Promise<T> {
  const hit = _memoryCache.get(cacheKey);
  if (hit && Date.now() - hit.ts < ttlMs) return hit.data as T;
  const pending = _inFlight.get(cacheKey);
  if (pending) return pending as Promise<T>;
  const p = (async () => {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(`${res.status}`);
      const data = (await res.json()) as T;
      _memoryCache.set(cacheKey, { data, ts: Date.now() });
      return data;
    } finally {
      _inFlight.delete(cacheKey);
    }
  })();
  _inFlight.set(cacheKey, p);
  return p;
}

type ScenarioOption = {
  id: string;
  name: string;
};

type RankedScore = {
  rank: number;
  geography_id: string;
  score_value: number;
  confidence: number;
};

type CompareResponseRow = {
  geography_id: string;
  score: number;
  recommendation: string;
  demand_score: number;
  supply_score: number;
  confidence: number;
};

type RecommendationResponse = {
  label: string;
};

type RecommendationExplainResponse = {
  key_drivers: string[];
};

type RecommendationDistributionResponse = {
  rows: Array<{ label: string; count: number }>;
  total_geographies: number;
};

type ScoreDeltaResponse = {
  rows: Array<{
    geography_id: string;
    rank_change: number;
    baseline_rank: number;
    scenario_rank: number;
    score_change: number;
  }>;
};

type GeographySearchRow = {
  geography_id: string;
  geography_type: string;
  name: string;
};

type MapLayerId =
  | "opportunity"
  | "talent_supply"
  | "local_core_industry"
  | "demand_gap"
  | "migration"
  | "affordability"
  | "broadband"
  | "population";

const MAP_LAYERS: Array<{ id: MapLayerId; label: string }> = [
  { id: "opportunity", label: "Opportunity Score" },
  { id: "talent_supply", label: "Talent Supply" },
  { id: "local_core_industry", label: "Local Core Industry" },
  { id: "demand_gap", label: "Demand Gap" },
  { id: "affordability", label: "Affordability" },
  { id: "broadband", label: "Broadband" },
];

const LAYER_TO_FEATURE: Record<MapLayerId, string> = {
  // Keep opportunity mapped to raw score so map color matches
  // the numeric opportunity score shown in sidebar/rankings.
  opportunity: "score_opportunity",
  talent_supply: "score_talent_supply_display",
  local_core_industry: "score_industry_fit_display",
  demand_gap: "score_market_gap_display",
  migration: "score_market_gap_display",
  affordability: "score_cost_efficiency_display",
  broadband: "score_execution_feasibility_display",
  population: "score_opportunity_display",
};

const STATE_BOUNDARIES_URL =
  "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json";
const COUNTY_BOUNDARIES_URL =
  "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json";

const STATE_NAME_TO_FIPS: Record<string, string> = {
  Alabama: "01", Alaska: "02", Arizona: "04", Arkansas: "05", California: "06",
  Colorado: "08", Connecticut: "09", Delaware: "10", Florida: "12", Georgia: "13",
  Hawaii: "15", Idaho: "16", Illinois: "17", Indiana: "18", Iowa: "19",
  Kansas: "20", Kentucky: "21", Louisiana: "22", Maine: "23", Maryland: "24",
  Massachusetts: "25", Michigan: "26", Minnesota: "27", Mississippi: "28",
  Missouri: "29", Montana: "30", Nebraska: "31", Nevada: "32", "New Hampshire": "33",
  "New Jersey": "34", "New Mexico": "35", "New York": "36", "North Carolina": "37",
  "North Dakota": "38", Ohio: "39", Oklahoma: "40", Oregon: "41", Pennsylvania: "42",
  "Rhode Island": "44", "South Carolina": "45", "South Dakota": "46", Tennessee: "47",
  Texas: "48", Utah: "49", Vermont: "50", Virginia: "51", Washington: "53",
  "West Virginia": "54", Wisconsin: "55", Wyoming: "56", "District of Columbia": "11",
  "Puerto Rico": "72",
};

const FIPS_TO_STATE_NAME: Record<string, string> = Object.fromEntries(
  Object.entries(STATE_NAME_TO_FIPS).map(([name, fips]) => [fips, name]),
);

const MAPLIBRE_STYLE: MapLibreGL.StyleSpecification = {
  version: 8,
  name: "Figwork Base",
  sources: {
    "carto-light": {
      type: "raster",
      tiles: [
        "https://basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}@2x.png",
      ],
      tileSize: 256,
      attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
    },
    "carto-labels": {
      type: "raster",
      tiles: [
        "https://basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}@2x.png",
      ],
      tileSize: 256,
    },
  },
  layers: [
    { id: "background", type: "background", paint: { "background-color": "#eef2f3" } },
    { id: "carto-light", type: "raster", source: "carto-light", minzoom: 0, maxzoom: 20 },
    { id: "carto-labels", type: "raster", source: "carto-labels", minzoom: 0, maxzoom: 20 },
  ],
  glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
};

const SCORE_COLOR_STOPS: [number, string][] = [
  [0, "#d94e4e"],
  [30, "#e89b4e"],
  [50, "#e8d44e"],
  [65, "#8cc063"],
  [80, "#3a9e6e"],
  [100, "#1a7a4e"],
];

function buildFillColorExpression(property: string): MapLibreGL.DataDrivenPropertyValueSpecification<string> {
  return [
    "interpolate",
    ["linear"],
    ["coalesce", ["get", property], 0],
    ...SCORE_COLOR_STOPS.flat(),
  ] as unknown as MapLibreGL.DataDrivenPropertyValueSpecification<string>;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let maplibregl: any;

type FeaturesBulk = Record<string, Record<string, number>>;

type DataQualityInfo = {
  score: number;
  share: number;
  present: number;
  required: number;
};

function qualityLabel(score: number): string {
  if (score >= 75) return "High direct-data";
  if (score >= 55) return "Moderate direct-data";
  return "Low direct-data";
}

function featureToScoreProperties(
  geoId: string,
  featuresMap: FeaturesBulk,
): Record<string, number> {
  const features = featuresMap[geoId];
  if (!features) return { score_opportunity: 0, score_opportunity_display: 0 };
  const opp = features.opportunity_score ?? 0;
  const talentSupply = (features.talent_supply ?? 0) * 100;
  const industryFit = (features.industry_fit ?? 0) * 100;
  const marketGap = (features.market_gap ?? 0) * 100;
  const costEfficiency = (features.cost_efficiency ?? 0) * 100;
  const executionFeasibility = (features.execution_feasibility ?? 0) * 100;
  const demandCapture = (features.demand_capture ?? 0) * 100;
  const talentConversion = (features.talent_conversion ?? 0) * 100;
  return {
    score_opportunity: opp,
    score_opportunity_display: opp,
    score_talent_supply: talentSupply,
    score_talent_supply_display: talentSupply,
    score_industry_fit: industryFit,
    score_industry_fit_display: industryFit,
    score_market_gap: marketGap,
    score_market_gap_display: marketGap,
    score_cost_efficiency: costEfficiency,
    score_cost_efficiency_display: costEfficiency,
    score_execution_feasibility: executionFeasibility,
    score_execution_feasibility_display: executionFeasibility,
    score_demand_capture: demandCapture,
    score_talent_conversion: talentConversion,
  };
}

function normalizeDisplayScores(
  features: Array<{ properties?: Record<string, unknown> }>,
): void {
  const metricKeys = [
    "score_opportunity",
    "score_talent_supply",
    "score_industry_fit",
    "score_market_gap",
    "score_cost_efficiency",
    "score_execution_feasibility",
  ] as const;

  const minMaxByMetric: Record<string, { min: number; max: number }> = {};
  for (const k of metricKeys) {
    const vals: number[] = [];
    for (const f of features) {
      const raw = Number(f.properties?.[k] ?? 0);
      if (Number.isFinite(raw)) vals.push(raw);
    }
    minMaxByMetric[k] = {
      min: vals.length > 0 ? Math.min(...vals) : 0,
      max: vals.length > 0 ? Math.max(...vals) : 0,
    };
  }

  for (const f of features) {
    const props = { ...(f.properties ?? {}) };
    for (const k of metricKeys) {
      const raw = Number(props[k] ?? 0);
      const mm = minMaxByMetric[k];
      if (!Number.isFinite(raw)) {
        props[`${k}_display`] = 0;
        continue;
      }
      if (mm.max <= mm.min) {
        props[`${k}_display`] = raw;
        continue;
      }
      props[`${k}_display`] = ((raw - mm.min) / (mm.max - mm.min)) * 100;
    }
    f.properties = props;
  }
}

function isCountyLikeGeography(geoId: string): boolean {
  return /^\d{5}$/.test(geoId); // US county FIPS only
}

function isCityLikeGeography(geoId: string): boolean {
  return /^\d{7}$/.test(geoId); // US place/town/city GEOID (state+place)
}

function isWorldDistrictLikeGeography(geoId: string): boolean {
  if (/^\d{5}$/.test(geoId)) return true; // US county FIPS
  if (geoId.startsWith("AU-SA4")) return true; // AU district-equivalent
  if (geoId.startsWith("IN-") && geoId.split("-").length >= 3) return true; // India district
  if (geoId.startsWith("EU-") && geoId.length >= 6) return true; // EU NUTS-2
  return false;
}

function collectMappableWorldGeographyIds(
  countyGeo: { features?: Array<{ properties?: { GEOID?: string } }> } | null,
  globalRefs: Record<string, { features?: Array<{ properties?: { GEOID?: string } }> }>,
): Set<string> {
  const ids = new Set<string>();
  for (const f of countyGeo?.features ?? []) {
    const gid = String(f.properties?.GEOID ?? "");
    if (gid) ids.add(gid);
  }
  for (const k of ["au", "in", "eu"]) {
    for (const f of globalRefs[k]?.features ?? []) {
      const gid = String(f.properties?.GEOID ?? "");
      if (gid) ids.add(gid);
    }
  }
  return ids;
}

function ringCentroid(ring: number[][]): [number, number] | null {
  if (!ring?.length) return null;
  let sx = 0;
  let sy = 0;
  let n = 0;
  for (const c of ring) {
    if (typeof c[0] !== "number" || typeof c[1] !== "number") continue;
    sx += c[0];
    sy += c[1];
    n += 1;
  }
  return n > 0 ? [sx / n, sy / n] : null;
}

function geometryCenter(geometry: { type?: string; coordinates?: unknown } | null): [number, number] | null {
  if (!geometry?.type || !geometry.coordinates) return null;
  if (geometry.type === "Polygon") {
    const coords = geometry.coordinates as number[][][];
    const outer = coords[0];
    return outer ? ringCentroid(outer) : null;
  }
  if (geometry.type === "MultiPolygon") {
    const coords = geometry.coordinates as number[][][][];
    const outer = coords[0]?.[0];
    return outer ? ringCentroid(outer) : null;
  }
  return null;
}

function findLngLatForGeography(
  geoId: string,
  countyGeo: { features?: Array<{ properties?: { GEOID?: string }; geometry?: { type?: string; coordinates?: unknown } }> } | null,
  placeGeo: { features?: Array<{ properties?: { GEOID?: string }; geometry?: { type?: string; coordinates?: unknown } }> } | null,
  globalRefs: Record<string, { features?: Array<{ properties?: { GEOID?: string }; geometry?: { type?: string; coordinates?: unknown } }> }>,
): [number, number] | null {
  const tryCollection = (fc: typeof countyGeo) => {
    if (!fc?.features) return null;
    for (const f of fc.features) {
      if (String(f.properties?.GEOID ?? "") !== geoId) continue;
      const c = geometryCenter(f.geometry ?? null);
      if (c) return c;
    }
    return null;
  };
  const fromCounty = tryCollection(countyGeo);
  if (fromCounty) return fromCounty;
  const fromPlace = tryCollection(placeGeo);
  if (fromPlace) return fromPlace;
  for (const key of ["in", "au", "eu"]) {
    const hit = tryCollection(globalRefs[key] ?? null);
    if (hit) return hit;
  }
  return null;
}

function resolveGeoName(geoId: string, names: Record<string, string>): string {
  if (names[geoId]) return names[geoId];
  if (geoId.length === 2 && /^\d+$/.test(geoId)) return FIPS_TO_STATE_NAME[geoId] ?? geoId;
  if (geoId.startsWith("AU-SA4")) return `SA4 ${geoId.slice(6)}, Australia`;
  if (geoId.startsWith("AU-")) return geoId.slice(3);
  if (geoId.startsWith("IN-") && geoId.split("-").length >= 3) {
    const parts = geoId.split("-");
    return `${parts[2].replace(/[a-f0-9]{3}$/, "")}, ${parts[1]}`;
  }
  if (geoId.startsWith("IN-")) return geoId.slice(3);
  if (geoId.startsWith("EU-")) return geoId.slice(3);
  return geoId;
}

const TOOLTIP_WIDTH = 300;
const TOOLTIP_GAP = 10;

type ContactBadgeInfo = {
  found: boolean;
  total: number;
  personal?: number;
  generic?: number;
  pattern?: string;
  departments?: Record<string, number>;
  seniority?: Record<string, number>;
} | null;

function ContactBadge({ count, loading, employeeFallback }: {
  count: ContactBadgeInfo;
  loading: boolean;
  employeeFallback: number;
}) {
  // 1) Hunter has the domain indexed with >0 public emails → show real count.
  if (count?.found && count.total > 0) {
    return (
      <span
        className="cdp-contact-pill cdp-contact-pill-strong"
        title={`${count.total} public email${count.total === 1 ? "" : "s"} indexed by Hunter. Names + addresses fetched on +SDR intake (uses credits).`}
      >
        ✉ {count.total >= 1000 ? `${Math.round(count.total / 100) / 10}k` : count.total}
      </span>
    );
  }
  // 2) Hunter returned 0 but Apollo told us the company has N employees →
  //    Apollo's contact DB will almost certainly cover them on intake.
  if (count && !count.found && employeeFallback > 0) {
    return (
      <span
        className="cdp-contact-pill cdp-contact-pill-soft"
        title={`Hunter has no public emails indexed, but Apollo sees ~${employeeFallback} employees. Intake runs the full Apollo + Hunter waterfall.`}
      >
        ✉ ~{employeeFallback}
      </span>
    );
  }
  // 3) Still loading — subtle placeholder to avoid layout shift.
  if (loading) {
    return <span className="cdp-contact-pill cdp-contact-pill-loading" aria-hidden>✉ …</span>;
  }
  // 4) No Hunter data, no employee count → neutral muted placeholder.
  if (count && !count.found) {
    return (
      <span
        className="cdp-contact-pill cdp-contact-pill-muted"
        title="No public contacts known. +SDR intake will run the full Apollo + Hunter + title-search waterfall."
      >
        ✉ —
      </span>
    );
  }
  return null;
}

function CompanyRow({ company: c, domain: d, name: n, status, intakeLoading, onIntake }: {
  company: Record<string, unknown>; domain: string; name: string; status: string | undefined; intakeLoading: boolean; onIntake: () => void;
}) {
  const [hover, setHover] = React.useState(false);
  const [enriched, setEnriched] = React.useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = React.useState(false);
  // Free Hunter email-count check — tells us whether any public contacts
  // exist without burning Hunter credits.  Prefetched on mount for all rows
  // (the endpoint is free) so every row can show a ✉ badge.
  const [contactCount, setContactCount] = React.useState<ContactBadgeInfo>(null);
  const [countLoading, setCountLoading] = React.useState(false);
  const [anchor, setAnchor] = React.useState<{ top: number; left: number; placement: "right" | "left" } | null>(null);
  const timerRef = React.useRef<number | null>(null);
  const rowRef = React.useRef<HTMLDivElement>(null);

  const computeAnchor = React.useCallback(() => {
    const el = rowRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    const vw = window.innerWidth;
    const spaceRight = vw - rect.right;
    const placement: "right" | "left" = spaceRight >= TOOLTIP_WIDTH + TOOLTIP_GAP + 12 ? "right" : "left";
    const left = placement === "right"
      ? Math.min(rect.right + TOOLTIP_GAP, vw - TOOLTIP_WIDTH - 8)
      : Math.max(8, rect.left - TOOLTIP_WIDTH - TOOLTIP_GAP);
    const top = Math.max(8, Math.min(rect.top, window.innerHeight - 40));
    setAnchor({ top, left, placement });
  }, []);

  const onEnter = () => {
    timerRef.current = window.setTimeout(() => {
      computeAnchor();
      setHover(true);
      // Apollo+Hunter enrichment (1 Apollo + 1 Hunter credit; cached per session)
      if (!enriched && d && !loading) {
        setLoading(true);
        fetch(`/api/worktrigger/vendors/companies/enrich?domain=${encodeURIComponent(d)}`, { method: "POST" })
          .then(r => r.ok ? r.json() : null)
          .then(data => { if (data?.found) setEnriched(data); })
          .catch(() => {})
          .finally(() => setLoading(false));
      }
    }, 400);
  };

  // Free Hunter email-count prefetch (no credits consumed) — fires on mount
  // so every row in the visible list shows a ✉ badge.  For companies Hunter
  // doesn't cover (mostly small local SMBs), the badge falls back to the
  // Apollo-known employee count so the user always has a signal.
  React.useEffect(() => {
    if (!d) return;
    let cancelled = false;
    setCountLoading(true);
    memoFetch<NonNullable<ContactBadgeInfo>>(
      `/api/worktrigger/vendors/companies/contacts-count?domain=${encodeURIComponent(d)}`,
      60 * 60 * 1000,
    )
      .then((data) => { if (!cancelled && data) setContactCount(data); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setCountLoading(false); });
    return () => { cancelled = true; };
  }, [d]);
  const onLeave = () => {
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
    setHover(false);
  };

  // Re-compute anchor when the row scrolls inside the panel so the tooltip
  // tracks the hovered row instead of detaching.
  React.useEffect(() => {
    if (!hover) return;
    const onScrollOrResize = () => computeAnchor();
    window.addEventListener("scroll", onScrollOrResize, true);
    window.addEventListener("resize", onScrollOrResize);
    return () => {
      window.removeEventListener("scroll", onScrollOrResize, true);
      window.removeEventListener("resize", onScrollOrResize);
    };
  }, [hover, computeAnchor]);

  return (
    <div
      ref={rowRef}
      className="cdp-row"
      style={{ position: "relative" }}
      onMouseEnter={onEnter}
      onMouseLeave={onLeave}
    >
      {c.logo_url ? (
        <img src={String(c.logo_url)} alt="" className="cdp-avatar" />
      ) : (
        <div className="cdp-avatar-placeholder">{n.charAt(0).toUpperCase()}</div>
      )}
      <div className="cdp-info">
        <div className="cdp-name">
          {n}
          {c.hiring_signal ? <span style={{ marginLeft: 6, fontSize: 10, color: "#059669", fontWeight: 600, background: "#ecfdf5", padding: "1px 5px", borderRadius: 8 }}>Hiring {String(c.hiring_count || "")}</span> : null}
          <ContactBadge count={contactCount} loading={countLoading} employeeFallback={Number(c.employee_count || 0)} />
        </div>
        <div className="cdp-meta">
          {[c.industry, c.employee_count ? `${c.employee_count} emp` : "", c.funding_stage, c.city].filter(Boolean).join(" \u00B7 ")}
        </div>
        {c.founded_year || c.total_funding ? (
          <div style={{ fontSize: 10, color: "#6b7280", marginTop: 1 }}>
            {[c.founded_year ? `Est. ${c.founded_year}` : "", c.total_funding ? `$${(Number(c.total_funding) / 1e6).toFixed(0)}M raised` : ""].filter(Boolean).join(" · ")}
          </div>
        ) : null}
      </div>
      {status ? (
        <span style={{ fontSize: 11, color: status.startsWith("Added") ? "#059669" : "#dc2626", fontWeight: 600, whiteSpace: "nowrap" }}>{status}</span>
      ) : (
        <button className="cdp-sdr-btn" disabled={intakeLoading} onClick={onIntake}>
          {intakeLoading ? "..." : "+ SDR"}
        </button>
      )}

      {/* Hover tooltip rendered via portal so it never gets clipped by the
          expanded panel's overflow: auto boundary. */}
      {hover && anchor ? createPortal(
        <div
          className="cdp-tooltip cdp-tooltip-portal"
          style={{ top: anchor.top, left: anchor.left, width: TOOLTIP_WIDTH }}
          data-placement={anchor.placement}
        >
          <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 6, color: "#111827" }}>{n}</div>
          {loading ? (
            <div style={{ fontSize: 12, color: "#6b7280" }}>Loading...</div>
          ) : enriched ? (
            <div style={{ display: "grid", gap: 3, fontSize: 12 }}>
              {enriched.industry ? <div><span style={{ color: "#6b7280" }}>Industry:</span> {String(enriched.industry)}</div> : null}
              {enriched.employee_count ? <div><span style={{ color: "#6b7280" }}>Employees:</span> {Number(enriched.employee_count).toLocaleString()}</div> : null}
              {enriched.annual_revenue ? <div><span style={{ color: "#6b7280" }}>Revenue:</span> ${(Number(enriched.annual_revenue) / 1e6).toFixed(0)}M</div> : null}
              {enriched.funding_stage ? <div><span style={{ color: "#6b7280" }}>Funding:</span> {String(enriched.funding_stage)}</div> : null}
              {enriched.total_funding ? <div><span style={{ color: "#6b7280" }}>Raised:</span> ${(Number(enriched.total_funding) / 1e6).toFixed(1)}M</div> : null}
              {enriched.founded_year ? <div><span style={{ color: "#6b7280" }}>Founded:</span> {String(enriched.founded_year)}</div> : null}
              {enriched.city || enriched.country ? <div><span style={{ color: "#6b7280" }}>HQ:</span> {[enriched.city, enriched.state, enriched.country].filter(Boolean).join(", ")}</div> : null}
              {Array.isArray(enriched.tech_stack) && (enriched.tech_stack as string[]).length > 0 ? <div><span style={{ color: "#6b7280" }}>Tech:</span> {(enriched.tech_stack as string[]).slice(0, 6).join(", ")}</div> : null}
              {Array.isArray(enriched.sources_used) && (enriched.sources_used as string[]).length > 0 ? <div style={{ fontSize: 10, color: "#9ca3af" }}>Sources: {(enriched.sources_used as string[]).join(", ")}</div> : null}
              {enriched.short_description ? <div style={{ marginTop: 4, color: "#374151", lineHeight: 1.4 }}>{String(enriched.short_description).slice(0, 200)}{String(enriched.short_description).length > 200 ? "..." : ""}</div> : null}
            </div>
          ) : c.short_description ? (
            <div style={{ fontSize: 12, color: "#374151", lineHeight: 1.4 }}>{String(c.short_description)}</div>
          ) : (
            <div style={{ fontSize: 12, color: "#6b7280" }}>
              {[c.industry, c.employee_count ? `${c.employee_count} employees` : "", c.city, c.country].filter(Boolean).join(" · ")}
            </div>
          )}
          {d ? <div style={{ marginTop: 4, fontSize: 11, color: "#2563eb" }}>{d}</div> : null}

          {/* Contact availability — free Hunter email-count (0 credits) with
              Apollo employee count as fallback. */}
          <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid #f1f5f9" }}>
            {countLoading && !contactCount ? (
              <div style={{ fontSize: 11, color: "#9ca3af" }}>Checking contacts…</div>
            ) : contactCount && contactCount.found && contactCount.total > 0 ? (
              <>
                <div style={{ fontSize: 10, fontWeight: 700, color: "#059669", textTransform: "uppercase", letterSpacing: ".4px", marginBottom: 3 }}>
                  {contactCount.total.toLocaleString()} public email{contactCount.total === 1 ? "" : "s"} · Hunter
                </div>
                {contactCount.departments && Object.keys(contactCount.departments).length > 0 ? (
                  <div style={{ fontSize: 11, color: "#374151", lineHeight: 1.4 }}>
                    {Object.entries(contactCount.departments)
                      .filter(([, v]) => (v as number) > 0)
                      .sort(([, a], [, b]) => (b as number) - (a as number))
                      .slice(0, 4)
                      .map(([k, v]) => `${k} ${v}`)
                      .join(" · ")}
                  </div>
                ) : null}
                {contactCount.pattern ? (
                  <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>Pattern: {contactCount.pattern}</div>
                ) : null}
                <div style={{ fontSize: 10, color: "#6b7280", marginTop: 4, fontStyle: "italic" }}>
                  Names + emails fetched on +SDR intake (uses credits).
                </div>
              </>
            ) : contactCount && !contactCount.found && Number(c.employee_count || 0) > 0 ? (
              <>
                <div style={{ fontSize: 10, fontWeight: 700, color: "#475569", textTransform: "uppercase", letterSpacing: ".4px", marginBottom: 3 }}>
                  ~{Number(c.employee_count).toLocaleString()} employees · Apollo
                </div>
                <div style={{ fontSize: 11, color: "#6b7280", lineHeight: 1.4 }}>
                  Hunter has no public emails indexed (typical for small local businesses), but Apollo will find contacts for employees on intake.
                </div>
              </>
            ) : contactCount ? (
              <div style={{ fontSize: 11, color: "#9ca3af" }}>
                No public contacts indexed. +SDR intake runs Apollo + Hunter + title-search waterfall.
              </div>
            ) : null}
          </div>
        </div>,
        document.body,
      ) : null}
    </div>
  );
}


type MapDashboardProps = {
  onOpenFullRankingsPage?: () => void;
  onOpenSdr?: () => void;
};

export function MapDashboard({ onOpenFullRankingsPage, onOpenSdr }: MapDashboardProps): JSX.Element {
  const mapContainerRef = React.useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapRef = React.useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stateGeoRef = React.useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const countyGeoRef = React.useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placeGeoRef = React.useRef<any>(null);
  const globalGeoRefs = React.useRef<Record<string, any>>({});
  const [mapReady, setMapReady] = React.useState(false);
  const [currentZoom, setCurrentZoom] = React.useState(4);
  const [activeLayer, setActiveLayer] = React.useState<MapLayerId>("opportunity");
  const [geoGranularity, setGeoGranularity] = React.useState<"county" | "city">("county");
  const geoGranularityRef = React.useRef<"county" | "city">("county");
  const [rankingScope, setRankingScope] = React.useState<"us" | "world">("us");

  const [scenarioId, setScenarioId] = React.useState("default-opportunity");
  const [scenarioOptions, setScenarioOptions] = React.useState<ScenarioOption[]>([
    { id: "default-opportunity", name: "Default Opportunity" },
  ]);
  const [fullCountyRanked, setFullCountyRanked] = React.useState<RankedScore[]>([]);
  const [rankingsListLoading, setRankingsListLoading] = React.useState(true);
  const [rankingSidebarFilter, setRankingSidebarFilter] = React.useState("");
  const [sidebarRankPage, setSidebarRankPage] = React.useState(0);
  const rankingsSectionRef = React.useRef<HTMLDivElement>(null);
  const sidebarRef = React.useRef<HTMLElement>(null);
  const [compareRows, setCompareRows] = React.useState<
    Array<{ geographyId: string; score: number; recommendation: string }>
  >([]);
  const [selectedGeography, setSelectedGeography] = React.useState("06");
  const [selectedScore, setSelectedScore] = React.useState(0);
  const [selectedRank, setSelectedRank] = React.useState<number | null>(null);
  const [rankTotal, setRankTotal] = React.useState(0);
  const [selectedRecommendation, setSelectedRecommendation] = React.useState("Monitor");
  const [selectedDataQuality, setSelectedDataQuality] = React.useState<DataQualityInfo | null>(null);
  const [keyDrivers, setKeyDrivers] = React.useState<string[]>([]);
  const [distribution, setDistribution] = React.useState<Array<{ label: string; count: number }>>([]);
  const [deltaHighlights, setDeltaHighlights] = React.useState<string[]>([]);
  const [searchQuery, setSearchQuery] = React.useState("");
  const [searchResults, setSearchResults] = React.useState<GeographySearchRow[]>([]);
  const [pinnedGeography, setPinnedGeography] = React.useState<string | null>(null);
  const [isLoading, setIsLoading] = React.useState(false);
  const [loadError, setLoadError] = React.useState<string | null>(null);

  const [aiSummary, setAiSummary] = React.useState<string | null>(null);
  const [aiSources, setAiSources] = React.useState<Array<{ title: string; url: string }>>([]);
  const [aiGeoName, setAiGeoName] = React.useState("");
  const [aiLoading, setAiLoading] = React.useState(false);
  const [aiError, setAiError] = React.useState<string | null>(null);
  const [aiPanelOpen, setAiPanelOpen] = React.useState(false);
  const [aiExpanded, setAiExpanded] = React.useState(false);
  const [geoNames, setGeoNames] = React.useState<Record<string, string>>({});
  const [newsScoreAdj, setNewsScoreAdj] = React.useState<number | null>(null);
  const [featuresBulkCache, setFeaturesBulkCache] = React.useState<FeaturesBulk>({});
  const [discoveredCompanies, setDiscoveredCompanies] = React.useState<Array<Record<string, unknown>>>([]);
  const [discoveryLoading, setDiscoveryLoading] = React.useState(false);
  const [discoveryOpen, setDiscoveryOpen] = React.useState(false);
  const [intakeLoading, setIntakeLoading] = React.useState<string | null>(null);
  const [intakeResults, setIntakeResults] = React.useState<Record<string, string>>({});
  const [discoveryIndustry, setDiscoveryIndustry] = React.useState("");
  const [discoveryMinEmp, setDiscoveryMinEmp] = React.useState(0);
  const [discoveryMaxEmp, setDiscoveryMaxEmp] = React.useState(0);
  const [discoveryPage, setDiscoveryPage] = React.useState(1);
  const [discoveryTotal, setDiscoveryTotal] = React.useState(0);
  const [discoveryExpanded, setDiscoveryExpanded] = React.useState(false);
  const [selectedCompanies, setSelectedCompanies] = React.useState<Set<string>>(new Set());
  const [batchLoading, setBatchLoading] = React.useState(false);
  const [batchResult, setBatchResult] = React.useState("");

  React.useEffect(() => {
    geoGranularityRef.current = geoGranularity;
  }, [geoGranularity]);

  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const mod = await import("maplibre-gl");
        maplibregl = mod.default ?? mod;
      } catch {
        setLoadError("MapLibre GL JS not available. Install maplibre-gl.");
        return;
      }
      if (cancelled || !mapContainerRef.current) return;

      const map = new maplibregl.Map({
        container: mapContainerRef.current,
        style: MAPLIBRE_STYLE,
        center: [15, 25],
        zoom: 2,
        minZoom: 1.5,
        maxZoom: 14,
      });
      map.addControl(new maplibregl.NavigationControl(), "top-right");
      mapRef.current = map;

      map.on("zoom", () => setCurrentZoom(Math.round(map.getZoom())));

      map.on("load", async () => {
        try {
          const stateGeo = await cachedFetch<any>(STATE_BOUNDARIES_URL, "geo:us_states");
          for (const feature of stateGeo.features) {
            feature.properties = feature.properties ?? {};
            feature.properties.score_opportunity = 0;
          }
          stateGeoRef.current = stateGeo;
          map.addSource("states", { type: "geojson", data: stateGeo });
          map.addLayer(
            {
              id: "state-fill",
              type: "fill",
              source: "states",
              paint: {
                "fill-color": buildFillColorExpression("score_opportunity"),
                "fill-opacity": [
                  "interpolate", ["linear"], ["zoom"],
                  3, 0.7,
                  6, 0.3,
                  7, 0,
                ],
              },
            },
            "carto-labels",
          );
          map.addLayer(
            {
              id: "state-line",
              type: "line",
              source: "states",
              paint: {
                "line-color": "#ffffff",
                "line-width": [
                  "interpolate", ["linear"], ["zoom"],
                  3, 1.5,
                  7, 0.4,
                ],
              },
            },
            "carto-labels",
          );
        } catch {
          console.warn("Could not load state boundaries");
        }

        try {
          const countyGeo = await cachedFetch<any>(COUNTY_BOUNDARIES_URL, "geo:us_counties");
          for (const feature of countyGeo.features) {
            feature.properties = feature.properties ?? {};
            feature.properties.score_opportunity = 0;
          }
          countyGeoRef.current = countyGeo;
          map.addSource("counties", { type: "geojson", data: countyGeo });
          const countyFillLayer = {
            id: "county-fill",
            type: "fill" as const,
            source: "counties",
            paint: {
              "fill-color": buildFillColorExpression("score_opportunity"),
              "fill-opacity": [
                "interpolate", ["linear"], ["zoom"],
                5, 0,
                6, 0.15,
                7, 0.55,
                9, 0.7,
                12, 0.3,
              ],
            },
          };
          const countyLineLayer = {
            id: "county-line",
            type: "line" as const,
            source: "counties",
            paint: {
              "line-color": "rgba(255,255,255,0.5)",
              "line-width": [
                "interpolate", ["linear"], ["zoom"],
                6, 0.2,
                9, 0.6,
              ],
            },
          };
          if (map.getLayer("state-line")) {
            map.addLayer(countyFillLayer, "state-line");
            map.addLayer(countyLineLayer, "state-line");
          } else if (map.getLayer("carto-labels")) {
            map.addLayer(countyFillLayer, "carto-labels");
            map.addLayer(countyLineLayer, "carto-labels");
          } else {
            map.addLayer(countyFillLayer);
            map.addLayer(countyLineLayer);
          }
        } catch {
          console.warn("Could not load county boundaries");
        }

        try {
          const placeGeo = await cachedFetch<any>("/api/boundaries/us_places", "geo:us_places");
          const placeNames: Record<string, string> = {};
          for (const feature of placeGeo.features ?? []) {
            feature.properties = feature.properties ?? {};
            feature.properties.score_opportunity = 0;
            const gid = String(feature.properties?.GEOID ?? "");
            const nm = String(feature.properties?.name ?? "");
            if (gid && nm) placeNames[gid] = nm;
          }
          placeGeoRef.current = placeGeo;
          if (Object.keys(placeNames).length > 0) {
            setGeoNames((prev) => ({ ...prev, ...placeNames }));
          }
          map.addSource("places", { type: "geojson", data: placeGeo });
          const placeFillLayer = {
            id: "places-fill",
            type: "fill" as const,
            source: "places",
            layout: { visibility: "none" as const },
            paint: {
              "fill-color": buildFillColorExpression("score_opportunity"),
              "fill-opacity": [
                "interpolate", ["linear"], ["zoom"],
                7, 0.12,
                8, 0.35,
                10, 0.62,
              ],
            },
          };
          const placeLineLayer = {
            id: "places-line",
            type: "line" as const,
            source: "places",
            layout: { visibility: "none" as const },
            paint: {
              "line-color": "rgba(255,255,255,0.45)",
              "line-width": [
                "interpolate", ["linear"], ["zoom"],
                8, 0.25,
                11, 0.7,
              ],
            },
          };
          if (map.getLayer("county-line")) {
            map.addLayer(placeFillLayer, "county-line");
            map.addLayer(placeLineLayer, "county-line");
          } else if (map.getLayer("state-line")) {
            map.addLayer(placeFillLayer, "state-line");
            map.addLayer(placeLineLayer, "state-line");
          } else if (map.getLayer("carto-labels")) {
            map.addLayer(placeFillLayer, "carto-labels");
            map.addLayer(placeLineLayer, "carto-labels");
          } else {
            map.addLayer(placeFillLayer);
            map.addLayer(placeLineLayer);
          }
        } catch {
          console.warn("Could not load place boundaries");
        }

        const globalRegions = [
          { id: "au", source: "/api/boundaries/au" },
          { id: "in", source: "/api/boundaries/in" },
          { id: "eu", source: "/api/boundaries/eu" },
        ];
        const extractedNames: Record<string, string> = {};
        for (const region of globalRegions) {
          try {
            const geo = await cachedFetch<any>(region.source, `geo:${region.id}`);
            globalGeoRefs.current[region.id] = geo;
            for (const feature of geo.features ?? []) {
              const gid = feature.properties?.GEOID ?? "";
              const nm = feature.properties?.name ?? "";
              if (gid && nm) extractedNames[gid] = nm;
            }
            map.addSource(region.id, { type: "geojson", data: geo });
            const fillLayer = {
              id: `${region.id}-fill`,
              type: "fill" as const,
              source: region.id,
              paint: {
                "fill-color": buildFillColorExpression("score_opportunity"),
                "fill-opacity": 0.65,
              },
            };
            const lineLayer = {
              id: `${region.id}-line`,
              type: "line" as const,
              source: region.id,
              paint: {
                "line-color": "#ffffff",
                "line-width": 0.6,
              },
            };
            if (map.getLayer("state-fill")) {
              map.addLayer(fillLayer, "state-fill");
            } else if (map.getLayer("carto-labels")) {
              map.addLayer(fillLayer, "carto-labels");
            } else {
              map.addLayer(fillLayer);
            }
            if (map.getLayer("carto-labels")) {
              map.addLayer(lineLayer, "carto-labels");
            } else {
              map.addLayer(lineLayer);
            }
          } catch (error) {
            console.warn(`Could not load ${region.id} boundaries`, error);
          }
        }
        if (Object.keys(extractedNames).length > 0) {
          setGeoNames((prev) => ({ ...prev, ...extractedNames }));
        }

        const popup = new maplibregl.Popup({
          closeButton: false,
          closeOnClick: false,
        });

        const interactiveLayers = [
          "state-fill", "county-fill", "places-fill", "au-fill", "in-fill", "eu-fill",
        ];
        for (const layerId of interactiveLayers) {
          if (!map.getLayer(layerId)) continue;
          map.on("mousemove", layerId, (e: any) => {
            if (!e.features || e.features.length === 0) return;
            map.getCanvas().style.cursor = "pointer";
            const props = e.features[0].properties ?? {};
            const name = props.name ?? props.NAME ?? props.GEOID ?? "Unknown";
            const scoreVal = props.score_opportunity ?? 0;
            popup
              .setLngLat(e.lngLat)
              .setHTML(
                `<div style="font-family:sans-serif;font-size:12px">` +
                `<strong>${name}</strong><br/>Score: ${Number(scoreVal).toFixed(1)}` +
                `</div>`,
              )
              .addTo(map);
          });
          map.on("mouseleave", layerId, () => {
            map.getCanvas().style.cursor = "";
            popup.remove();
          });
        }

        map.on("click", (e: any) => {
          const granularity = geoGranularityRef.current;
          if (granularity === "city") {
            const placeHits = map.queryRenderedFeatures(e.point, { layers: ["places-fill"] });
            if (placeHits.length > 0) {
              const geoId = placeHits[0].properties?.GEOID ?? "";
              if (geoId) {
                setPinnedGeography(String(geoId));
                return;
              }
            }
          } else {
            const countyHits = map.queryRenderedFeatures(e.point, { layers: ["county-fill"] });
            if (countyHits.length > 0) {
              const geoId = countyHits[0].properties?.GEOID ?? "";
              if (geoId) {
                setPinnedGeography(String(geoId));
                return;
              }
            }
          }

          const globalLayers = ["au-fill", "in-fill", "eu-fill"].filter(
            (lid) => map.getLayer(lid),
          );
          if (globalLayers.length > 0) {
            const globalHits = map.queryRenderedFeatures(e.point, { layers: globalLayers });
            if (globalHits.length > 0) {
              const geoId = globalHits[0].properties?.GEOID ?? "";
              if (geoId) {
                setPinnedGeography(String(geoId));
                return;
              }
            }
          }

          const stateHits = map.queryRenderedFeatures(e.point, { layers: ["state-fill"] });
          if (stateHits.length > 0) {
            const props = stateHits[0].properties ?? {};
            const rawId = props.STATE ?? props.name ?? "";
            const stateFips = STATE_NAME_TO_FIPS[rawId] ?? rawId;
            const candidates =
              granularity === "city"
                ? placeGeoRef.current?.features?.filter(
                  (f: any) => String(f.properties?.GEOID ?? "").startsWith(stateFips),
                )
                : countyGeoRef.current?.features?.filter(
                  (f: any) => String(f.properties?.GEOID ?? "").startsWith(stateFips),
                );
            if (stateFips && candidates?.length) {
              let bestGeo = candidates[0];
                let bestScore = -1;
              for (const cf of candidates) {
                const s = cf.properties?.score_opportunity ?? 0;
                if (s > bestScore) {
                  bestScore = s;
                  bestGeo = cf;
                }
              }
              const nextGeoId = bestGeo.properties?.GEOID;
              if (nextGeoId) {
                setPinnedGeography(String(nextGeoId));
                return;
              }
            }
          }
        });

        // Company discovery pins — managed by useEffect on discoveredCompanies

        setMapReady(true);
      });
    })();
    return () => {
      cancelled = true;
      mapRef.current?.remove();
      mapRef.current = null;
    };
  }, []);

  React.useEffect(() => {
    if (!mapReady || !mapRef.current) return;
    const map = mapRef.current;
    const countyVisibility = geoGranularity === "city" ? "none" : "visible";
    const cityVisibility = geoGranularity === "city" ? "visible" : "none";
    for (const layerId of ["county-fill", "county-line"]) {
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, "visibility", countyVisibility);
      }
    }
    for (const layerId of ["places-fill", "places-line"]) {
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, "visibility", cityVisibility);
      }
    }
  }, [mapReady, geoGranularity]);

  React.useEffect(() => {
    if (!mapReady || !mapRef.current) return;
    const map = mapRef.current;
    (async () => {
      try {
        const featuresBulk: FeaturesBulk = await cachedFetch<FeaturesBulk>(
          `/api/scores/_features_bulk?scenario_id=${encodeURIComponent(scenarioId)}`,
          `scores:features_bulk:${scenarioId}`,
        );
        setFeaturesBulkCache(featuresBulk);

        const stateSource = map.getSource("states") as any;
        if (stateSource && stateGeoRef.current?.features) {
          for (const f of stateGeoRef.current.features) {
            const rawName = f.properties?.name ?? "";
            const fips = STATE_NAME_TO_FIPS[rawName] ?? String(f.properties?.STATE ?? f.id ?? "").padStart(2, "0");
            const scoreProps = featureToScoreProperties(fips, featuresBulk);
            f.properties = { ...(f.properties ?? {}), ...scoreProps };
          }
          normalizeDisplayScores(stateGeoRef.current.features);
          // Force MapLibre to detect changes reliably.
          stateSource.setData({ ...stateGeoRef.current });
        }

        const countySource = map.getSource("counties") as any;
        if (countySource && countyGeoRef.current?.features) {
          for (const f of countyGeoRef.current.features) {
            const fips = f.properties?.GEOID ?? f.id ?? "";
            const geoId = String(fips);
            const scoreProps = featureToScoreProperties(geoId, featuresBulk);
            f.properties = { ...(f.properties ?? {}), ...scoreProps };
          }
          normalizeDisplayScores(countyGeoRef.current.features);
          // Force MapLibre to detect changes reliably.
          countySource.setData({ ...countyGeoRef.current });
        }

        const placeSource = map.getSource("places") as any;
        if (placeSource && placeGeoRef.current?.features) {
          for (const f of placeGeoRef.current.features) {
            const geoId = String(f.properties?.GEOID ?? f.id ?? "");
            const scoreProps = featureToScoreProperties(geoId, featuresBulk);
            f.properties = { ...(f.properties ?? {}), ...scoreProps };
          }
          normalizeDisplayScores(placeGeoRef.current.features);
          placeSource.setData({ ...placeGeoRef.current });
        }

        for (const regionId of ["au", "in", "eu"]) {
          const src = map.getSource(regionId) as any;
          const regionData = globalGeoRefs.current[regionId];
          if (!src || !regionData?.features) continue;
          const regionMetricKeys = [
            "score_opportunity",
            "score_talent_supply",
            "score_industry_fit",
            "score_market_gap",
            "score_cost_efficiency",
            "score_execution_feasibility",
          ] as const;
          const valuesByMetric: Record<string, number[]> = {
            score_opportunity: [],
            score_talent_supply: [],
            score_industry_fit: [],
            score_market_gap: [],
            score_cost_efficiency: [],
            score_execution_feasibility: [],
          };
          for (const f of regionData.features) {
            const geoId = f.properties?.GEOID ?? "";
            if (!geoId) continue;
            const scoreProps = featureToScoreProperties(geoId, featuresBulk);
            f.properties = { ...(f.properties ?? {}), ...scoreProps };
            for (const k of regionMetricKeys) {
              valuesByMetric[k].push(Number(scoreProps[k] ?? 0));
            }
          }
          if (regionData.features.length > 0) {
            const minMaxByMetric: Record<string, { min: number; max: number; denom: number }> = {};
            for (const k of regionMetricKeys) {
              const vals = valuesByMetric[k];
              const min = vals.length > 0 ? Math.min(...vals) : 0;
              const max = vals.length > 0 ? Math.max(...vals) : 0;
              minMaxByMetric[k] = { min, max, denom: Math.max(1, max - min) };
            }
            for (const f of regionData.features) {
              const nextProps = { ...(f.properties ?? {}) } as Record<string, number | string>;
              for (const k of regionMetricKeys) {
                const raw = Number(nextProps[k] ?? 0);
                const mm = minMaxByMetric[k];
                const normalized = ((raw - mm.min) / mm.denom) * 100;
                nextProps[`${k}_display`] = Number.isFinite(normalized) ? normalized : 0;
              }
              f.properties = nextProps;
            }
          }
          src.setData({ ...regionData });
        }
      } catch {
        // score overlay update failed silently
      }
    })();
  }, [mapReady, scenarioId]);

  React.useEffect(() => {
    if (!mapReady || !mapRef.current) return;
    const map = mapRef.current;
    const prop = LAYER_TO_FEATURE[activeLayer] ?? "score_opportunity";
    const expr = buildFillColorExpression(prop);

    if (map.getLayer("state-fill")) {
      map.setPaintProperty("state-fill", "fill-color", expr);
    }
    if (map.getLayer("county-fill")) {
      map.setPaintProperty("county-fill", "fill-color", expr);
    }
    if (map.getLayer("places-fill")) {
      map.setPaintProperty("places-fill", "fill-color", expr);
    }
    for (const regionLayer of ["au-fill", "in-fill", "eu-fill"]) {
      if (map.getLayer(regionLayer)) {
        map.setPaintProperty(regionLayer, "fill-color", expr);
      }
    }
  }, [mapReady, activeLayer]);

  React.useEffect(() => {
    void (async () => {
      try {
        const payload = await memoFetch<Array<{ scenario_id: string; name: string }>>(
          "/api/scenarios",
          30 * 60 * 1000,
        );
        if (payload.length > 0) {
          setScenarioOptions(payload.map((row) => ({ id: row.scenario_id, name: row.name })));
        }
      } catch (error) {
        setLoadError(error instanceof Error ? error.message : "Scenario fetch failed");
      }
    })();
  }, []);

  React.useEffect(() => {
    if (!searchQuery.trim()) {
      setSearchResults([]);
      return;
    }
    const timeout = window.setTimeout(() => {
      void (async () => {
        try {
          const response = await fetch(`/api/geographies/search?q=${encodeURIComponent(searchQuery)}`);
          if (!response.ok) return;
          const payload = (await response.json()) as GeographySearchRow[];
          const mappableWorldIds = collectMappableWorldGeographyIds(
            countyGeoRef.current,
            globalGeoRefs.current,
          );
          const filtered = payload.filter((r) => {
            if (geoGranularity === "city") {
              return r.geography_type === "place" && isCityLikeGeography(r.geography_id);
            }
            return rankingScope === "world"
              ? (
                mappableWorldIds.size > 0
                  ? mappableWorldIds.has(r.geography_id)
                  : isWorldDistrictLikeGeography(r.geography_id)
              )
              : (r.geography_type === "county" && isCountyLikeGeography(r.geography_id));
          });
          setSearchResults(filtered.slice(0, 10));
        } catch {
          setSearchResults([]);
        }
      })();
    }, 250);
    return () => window.clearTimeout(timeout);
  }, [searchQuery, geoGranularity, rankingScope]);

  React.useEffect(() => {
    void (async () => {
      try {
        const names = await cachedFetch<Record<string, string>>("/api/geographies/names", "geo:names");
        setGeoNames(names);
      } catch { /* non-critical */ }
    })();
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    setRankingsListLoading(true);
    setFullCountyRanked([]);
    setRankTotal(0);
    void (async () => {
      try {
        const rankedPayload = await cachedFetch<RankedScore[]>(
          `/api/scores/_ranked?scenario_id=${encodeURIComponent(scenarioId)}&limit=12000`,
          `scores:ranked:${scenarioId}`,
        );
        const mappableWorldIds = collectMappableWorldGeographyIds(
          countyGeoRef.current,
          globalGeoRefs.current,
        );
        const countyRanked = rankedPayload.filter((row) =>
          geoGranularity === "city"
            ? isCityLikeGeography(row.geography_id)
            : (rankingScope === "world"
              ? (
                mappableWorldIds.size > 0
                  ? mappableWorldIds.has(row.geography_id)
                  : isWorldDistrictLikeGeography(row.geography_id)
              )
              : isCountyLikeGeography(row.geography_id)),
        );
        if (!cancelled) {
          setFullCountyRanked(countyRanked);
          setRankTotal(countyRanked.length);
        }
      } catch {
        if (!cancelled) {
          setFullCountyRanked([]);
          setRankTotal(0);
        }
      } finally {
        if (!cancelled) setRankingsListLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [scenarioId, geoGranularity, rankingScope]);

  React.useEffect(() => {
    if (fullCountyRanked.length === 0) {
      setIsLoading(false);
      setCompareRows([]);
      return;
    }

    const loadDashboardData = async (): Promise<void> => {
      setIsLoading(true);
      setLoadError(null);
      try {
        const geographyIds = fullCountyRanked.map((row) => row.geography_id);
        const focusGeography = pinnedGeography ?? geographyIds[0];
        if (!focusGeography) {
          setSelectedRank(null);
          return;
        }

        setSelectedGeography(focusGeography);

        let resolvedGeo = focusGeography;
        let scoreValue = 0;
        let isFallbackScore = false;
        try {
          const scorePayload = await memoFetch<{ geography_id: string; score_value: number; version?: string }>(
            `/api/scores/${encodeURIComponent(focusGeography)}?scenario_id=${encodeURIComponent(scenarioId)}`,
          );
          scoreValue = scorePayload.score_value;
          resolvedGeo = scorePayload.geography_id || focusGeography;
          isFallbackScore = scorePayload.version === "v1-fallback";
          if (scorePayload.geography_id && scorePayload.geography_id !== focusGeography) {
            setSelectedGeography(scorePayload.geography_id);
            setPinnedGeography(scorePayload.geography_id);
          }
        } catch {
          isFallbackScore = true;
        }
        setSelectedScore(scoreValue);

        const qualityRaw = featuresBulkCache[resolvedGeo] ?? featuresBulkCache[focusGeography];
        if (qualityRaw) {
          setSelectedDataQuality({
            score: Number(qualityRaw.data_quality_score ?? 0),
            share: Number(qualityRaw.direct_metrics_share ?? 0),
            present: Number(qualityRaw.direct_metrics_present ?? 0),
            required: Number(qualityRaw.direct_metrics_required ?? 0),
          });
        } else {
          setSelectedDataQuality(null);
        }
        const rankIndex = fullCountyRanked.findIndex((row) => row.geography_id === resolvedGeo);
        setSelectedRank(rankIndex >= 0 ? rankIndex + 1 : null);

        const compareIds: string[] = [];
        if (pinnedGeography) {
          compareIds.push(pinnedGeography);
          for (const id of geographyIds) {
            if (id !== pinnedGeography && compareIds.length < 3) compareIds.push(id);
          }
        } else {
          compareIds.push(...geographyIds.slice(0, 3));
        }
        const compareGeographyIds = compareIds.slice(0, 3);

        if (compareGeographyIds.length >= 2) {
          const compareKey = `compare:${scenarioId}:${[...compareGeographyIds].sort().join(",")}`;
          try {
            const comparePayload = await memoFetchPost<{ rows: CompareResponseRow[] }>(
              "/api/compare",
              { geography_ids: compareGeographyIds, scenario_id: scenarioId },
              compareKey,
            );
            setCompareRows(
              comparePayload.rows.map((row) => ({
                geographyId: row.geography_id,
                score: row.score,
                recommendation: row.recommendation,
              })),
            );
          } catch {
            setCompareRows([]);
          }
        } else {
          setCompareRows([]);
        }

        // Fire the remaining sidebar calls in parallel; they are all memoized
        // so rapid pin-switching re-uses cached results.
        const [recRes, explRes, distRes, deltaRes] = await Promise.allSettled([
          memoFetch<RecommendationResponse>(
            `/api/recommendations/${encodeURIComponent(resolvedGeo)}?scenario_id=${encodeURIComponent(scenarioId)}`,
          ),
          memoFetch<RecommendationExplainResponse>(
            `/api/recommendations/${encodeURIComponent(resolvedGeo)}/explain?scenario_id=${encodeURIComponent(scenarioId)}`,
          ),
          memoFetch<RecommendationDistributionResponse>(
            `/api/recommendations/distribution?scenario_id=${encodeURIComponent(scenarioId)}`,
          ),
          memoFetch<ScoreDeltaResponse>(
            `/api/scores/_delta?scenario_id=${encodeURIComponent(scenarioId)}&baseline_scenario_id=default-opportunity&limit=3`,
          ),
        ]);

        setSelectedRecommendation(
          recRes.status === "fulfilled"
            ? recRes.value.label
            : (isFallbackScore ? "Insufficient Data" : ""),
        );
        setKeyDrivers(explRes.status === "fulfilled" ? explRes.value.key_drivers : []);
        setDistribution(
          distRes.status === "fulfilled" ? distRes.value.rows.slice(0, 2) : [],
        );
        setDeltaHighlights(
          deltaRes.status === "fulfilled"
            ? deltaRes.value.rows.slice(0, 2).map(
                (row) =>
                  `${row.geography_id}: rank ${row.baseline_rank} -> ${row.scenario_rank} (${row.rank_change >= 0 ? "+" : ""}${row.rank_change})`,
              )
            : [],
        );
      } catch (error) {
        setLoadError(error instanceof Error ? error.message : "Failed to load dashboard data");
      } finally {
        setIsLoading(false);
      }
    };

    void loadDashboardData();
  }, [scenarioId, pinnedGeography, fullCountyRanked, featuresBulkCache]);

  React.useEffect(() => {
    if (!mapReady || !pinnedGeography || !mapRef.current) return;
    const timer = window.setTimeout(() => {
      const map = mapRef.current;
      if (!map) return;
      const ll = findLngLatForGeography(
        pinnedGeography,
        countyGeoRef.current,
        placeGeoRef.current,
        globalGeoRefs.current,
      );
      if (!ll) return;
      try {
        map.easeTo({
          center: ll,
          zoom: Math.max(map.getZoom(), 5.5),
          duration: 900,
        });
      } catch {
        /* ignore */
      }
    }, 450);
    return () => window.clearTimeout(timer);
  }, [pinnedGeography, mapReady]);

  React.useEffect(() => {
    if (!pinnedGeography) {
      setAiPanelOpen(false);
      return;
    }
    let cancelled = false;
    setAiLoading(true);
    setAiError(null);
    setAiSummary(null);
    setAiSources([]);
    setAiPanelOpen(false);
    setNewsScoreAdj(null);

    (async () => {
      try {
        const res = await fetch(
          `/api/ai/research/${encodeURIComponent(pinnedGeography)}?scenario_id=${encodeURIComponent(scenarioId)}`,
        );
        if (!res.ok) {
          const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
          throw new Error(err.detail || `Research failed (${res.status})`);
        }
        const data = await res.json();
        if (cancelled) return;
        setAiSummary(data.summary);
        setAiSources(data.sources ?? []);
        setAiGeoName(data.geography_name ?? pinnedGeography);
        if (typeof data.news_score_adjustment === "number" && data.news_score_adjustment !== 0) {
          setNewsScoreAdj(data.news_score_adjustment);
        }
      } catch (e) {
        if (!cancelled) {
          setAiError(e instanceof Error ? e.message : "AI research failed");
        }
      } finally {
        if (!cancelled) setAiLoading(false);
      }
    })();

    return () => { cancelled = true; };
  }, [pinnedGeography, scenarioId]);

  const zoomLabel = currentZoom <= 5 ? "National" : currentZoom <= 7 ? "State" : currentZoom <= 9 ? "Metro" : currentZoom <= 11 ? "County/City" : "ZIP/Tract";
  const activeLayerLabel = MAP_LAYERS.find((l) => l.id === activeLayer)?.label ?? "Opportunity Score";

  const sidebarRankingsFiltered = React.useMemo(() => {
    const q = rankingSidebarFilter.trim().toLowerCase();
    if (!q) return fullCountyRanked;
    return fullCountyRanked.filter((r) => {
      const name = (geoNames[r.geography_id] ?? "").toLowerCase();
      return r.geography_id.toLowerCase().includes(q) || name.includes(q);
    });
  }, [fullCountyRanked, rankingSidebarFilter, geoNames]);

  const SIDEBAR_RANK_PAGE_SIZE = 500;
  const sidebarPageCount = Math.max(1, Math.ceil(sidebarRankingsFiltered.length / SIDEBAR_RANK_PAGE_SIZE));
  const safeSidebarPage = Math.min(sidebarRankPage, sidebarPageCount - 1);
  const sidebarStart = safeSidebarPage * SIDEBAR_RANK_PAGE_SIZE;
  const sidebarEnd = Math.min(sidebarStart + SIDEBAR_RANK_PAGE_SIZE, sidebarRankingsFiltered.length);
  const sidebarRankingsVisible = sidebarRankingsFiltered.slice(sidebarStart, sidebarEnd);

  const scrollToSidebarRankings = () => {
    rankingsSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  };

  const scrollSidebarTop = () => {
    sidebarRef.current?.scrollTo({ top: 0, behavior: "smooth" });
  };

  const centerMapOnGeography = (geoId: string) => {
    if (!mapReady || !mapRef.current) return;
    const map = mapRef.current;
    const ll = findLngLatForGeography(
      geoId,
      countyGeoRef.current,
      placeGeoRef.current,
      globalGeoRefs.current,
    );
    if (!ll) return;
    try {
      map.easeTo({
        center: ll,
        zoom: Math.max(map.getZoom(), geoGranularity === "city" ? 7 : 5.5),
        duration: 700,
      });
    } catch {
      /* ignore */
    }
  };

  React.useEffect(() => {
    setSidebarRankPage(0);
  }, [rankingSidebarFilter, scenarioId, geoGranularity, rankingScope]);

  React.useEffect(() => {
    if (geoGranularity === "city" && rankingScope !== "us") {
      setRankingScope("us");
    }
  }, [geoGranularity, rankingScope]);

  React.useEffect(() => {
    if (!pinnedGeography) return;
    scrollSidebarTop();
  }, [pinnedGeography]);

  React.useEffect(() => {
    if (!aiPanelOpen) return;
    scrollSidebarTop();
  }, [aiPanelOpen]);

  const industryRef = React.useRef(discoveryIndustry);
  const minEmpRef = React.useRef(discoveryMinEmp);
  const maxEmpRef = React.useRef(discoveryMaxEmp);
  React.useEffect(() => { industryRef.current = discoveryIndustry; }, [discoveryIndustry]);
  React.useEffect(() => { minEmpRef.current = discoveryMinEmp; }, [discoveryMinEmp]);
  React.useEffect(() => { maxEmpRef.current = discoveryMaxEmp; }, [discoveryMaxEmp]);

  const loadDiscovery = React.useCallback(async (geoId: string, pg: number) => {
    setDiscoveryLoading(true);
    const gName = geoNames[geoId] ?? geoId;
    const ind = industryRef.current;
    const qs = new URLSearchParams({
      geography_id: geoId,
      geography_name: gName,
      page: String(pg),
      limit: "20",
      ...(ind ? { industry: ind } : {}),
      min_employees: String(minEmpRef.current),
      max_employees: String(maxEmpRef.current),
    });
    try {
      const res = await fetch(`/api/worktrigger/vendors/companies/discover?${qs}`);
      if (res.ok) {
        const data = await res.json();
        setDiscoveredCompanies(data.companies ?? []);
        setDiscoveryTotal(data.total ?? 0);
      }
    } catch { /* silent */ }
    finally { setDiscoveryLoading(false); }
  }, [geoNames]);

  React.useEffect(() => {
    if (!pinnedGeography) {
      setDiscoveredCompanies([]);
      setDiscoveryOpen(true);
      setIntakeResults({});
      setDiscoveryPage(1);
      setDiscoveryTotal(0);
      const map = mapRef.current;
      if (map) {
        if (map.getLayer("discovery-pins")) map.removeLayer("discovery-pins");
        if (map.getLayer("discovery-labels")) map.removeLayer("discovery-labels");
        if (map.getSource("discovery-companies")) map.removeSource("discovery-companies");
      }
      return;
    }
    setIntakeResults({});
    setDiscoveryPage(1);
    void loadDiscovery(pinnedGeography, 1);
  }, [pinnedGeography]);

  React.useEffect(() => {
    if (!pinnedGeography || discoveryPage < 2) return;
    void loadDiscovery(pinnedGeography, discoveryPage);
  }, [discoveryPage]);

  React.useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    if (map.getLayer("discovery-pins")) map.removeLayer("discovery-pins");
    if (map.getLayer("discovery-labels")) map.removeLayer("discovery-labels");
    if (map.getSource("discovery-companies")) map.removeSource("discovery-companies");

    const features = discoveredCompanies
      .filter((c) => c.lat && c.lng)
      .map((c) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [Number(c.lng), Number(c.lat)] },
        properties: { name: String(c.name || ""), domain: String(c.domain || ""), industry: String(c.industry || ""), employees: Number(c.employee_count || 0) },
      }));
    if (features.length === 0) return;

    map.addSource("discovery-companies", { type: "geojson", data: { type: "FeatureCollection", features } as any });
    map.addLayer({
      id: "discovery-pins", type: "circle", source: "discovery-companies",
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 3, 4, 8, 8, 12, 12],
        "circle-color": "#2563eb", "circle-stroke-color": "#fff", "circle-stroke-width": 2, "circle-opacity": 0.9,
      },
    });
    map.addLayer({
      id: "discovery-labels", type: "symbol", source: "discovery-companies",
      layout: { "text-field": ["get", "name"], "text-size": 11, "text-offset": [0, 1.4], "text-anchor": "top", "text-max-width": 12 },
      paint: { "text-color": "#1a1d23", "text-halo-color": "#ffffff", "text-halo-width": 1.5 },
      minzoom: 6,
    });

    const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
    map.on("mouseenter", "discovery-pins", (e: any) => {
      map.getCanvas().style.cursor = "pointer";
      const p = e.features?.[0]?.properties ?? {};
      popup.setLngLat(e.lngLat).setHTML(
        `<div style="font-family:sans-serif;font-size:12px;max-width:220px">` +
        `<strong>${p.name}</strong>` +
        `<div style="color:#6b7280;font-size:11px">${p.domain}</div>` +
        (p.industry ? `<div style="color:#6b7280;font-size:11px">${p.industry}</div>` : "") +
        (p.employees ? `<div style="font-size:11px">${p.employees} employees</div>` : "") +
        `</div>`
      ).addTo(map);
    });
    map.on("mouseleave", "discovery-pins", () => { map.getCanvas().style.cursor = ""; popup.remove(); });
  }, [discoveredCompanies, mapReady]);

  const toggleCompanySelect = (key: string) => {
    setSelectedCompanies(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const selectAllCompanies = () => {
    const keys = discoveredCompanies.map(c => String(c.domain || c.name || "")).filter(Boolean);
    setSelectedCompanies(prev => prev.size === keys.length ? new Set() : new Set(keys));
  };

  // Async batch intake: kicks off a server-side job and polls progress.
  // Replaces the old synchronous-loop call which (a) silently truncated
  // at 50 items and (b) timed out the HTTP request long before the
  // server finished, leaving the user with only the first item visible
  // in the pipeline.  Now the server processes with bounded concurrency
  // (4 in parallel) and the UI streams per-item progress as it lands.
  const batchIntake = async () => {
    if (selectedCompanies.size === 0) return;
    setBatchLoading(true);
    setBatchResult("Starting…");

    const items = discoveredCompanies
      .filter(c => selectedCompanies.has(String(c.domain || c.name || "")))
      .map(c => ({
        domain: String(c.domain || ""),
        company_name: String(c.name || ""),
        geography_id: pinnedGeography ?? "",
      }));

    let batchId = "";
    try {
      const res = await fetch("/api/worktrigger/vendors/companies/intake-batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(items),
      });
      if (!res.ok) {
        setBatchResult(`Batch failed (${res.status})`);
        setBatchLoading(false);
        return;
      }
      const data = await res.json();
      batchId = String(data.batch_id || "");
      if (!batchId) {
        setBatchResult("No batch id returned");
        setBatchLoading(false);
        return;
      }
      setBatchResult(`Queued ${items.length} · 0 done`);
    } catch {
      setBatchResult("Error starting batch");
      setBatchLoading(false);
      return;
    }

    // Poll until status === "complete".  Per-item updates flow into
    // setIntakeResults so each row's badge flips from "Adding…" to
    // "Added ✓" / "Skipped" / "Failed" as the server makes progress.
    const seenResults = new Set<string>();
    const tick = async (): Promise<boolean> => {
      try {
        const r = await fetch(`/api/worktrigger/vendors/companies/intake-batch/${encodeURIComponent(batchId)}`);
        if (!r.ok) return false;
        const s = await r.json();
        for (const row of s.results || []) {
          const key = row.domain || row.company_name;
          if (!key || seenResults.has(`${key}::${row.status}`)) continue;
          seenResults.add(`${key}::${row.status}`);
          if (row.status === "ok") setIntakeResults(prev => ({ ...prev, [key]: "Added ✓" }));
          else if (row.status === "skipped") setIntakeResults(prev => ({ ...prev, [key]: row.reason || "Skipped" }));
          else setIntakeResults(prev => ({ ...prev, [key]: "Failed" }));
        }
        const total = Number(s.total_submitted || 0);
        const done = Number(s.completed || 0);
        const ok = Number(s.ok || 0);
        const skipped = Number(s.skipped || 0);
        const errored = Number(s.error || 0);
        setBatchResult(
          s.status === "complete"
            ? `Added ${ok} · skipped ${skipped}${errored ? ` · failed ${errored}` : ""} · ${s.elapsed_seconds || 0}s`
            : `${done}/${total} · ${ok} added · ${skipped} skipped${errored ? ` · ${errored} failed` : ""}`,
        );
        return s.status === "complete";
      } catch {
        return false;
      }
    };

    // Start polling: every 1.2s.  Stop when complete.
    const intervalMs = 1200;
    const start = Date.now();
    const maxMs = 30 * 60 * 1000; // 30-minute hard ceiling for very large batches
    while (Date.now() - start < maxMs) {
      const finished = await tick();
      if (finished) break;
      await new Promise(r => setTimeout(r, intervalMs));
    }
    setSelectedCompanies(new Set());
    setBatchLoading(false);
  };

  const intakeCompany = async (domain: string, name: string) => {
    const key = domain || name;
    if (!key) return;
    setIntakeLoading(key);
    try {
      const d = domain || `${name.toLowerCase().replace(/[^a-z0-9]/g, "")}.com`;
      const res = await fetch(
        `/api/worktrigger/vendors/companies/intake?domain=${encodeURIComponent(d)}&company_name=${encodeURIComponent(name)}&geography_id=${encodeURIComponent(pinnedGeography ?? "")}`,
        { method: "POST" },
      );
      if (res.ok) {
        const data = await res.json();
        const ok = (data.steps || []).filter((s: Record<string, string>) => s.status === "ok").length;
        const total = (data.steps || []).length;
        setIntakeResults((prev) => ({ ...prev, [key]: ok === total ? `Added ✓` : `Added (${ok}/${total})` }));
      } else {
        const errText = await res.text().catch(() => "");
        setIntakeResults((prev) => ({ ...prev, [key]: `Failed: ${errText.slice(0, 40)}` }));
      }
    } catch {
      setIntakeResults((prev) => ({ ...prev, [key]: "Error" }));
    } finally {
      setIntakeLoading(null);
    }
  };

  React.useEffect(() => {
    if (!pinnedGeography) return;
    if (geoGranularity === "city" && !isCityLikeGeography(pinnedGeography)) {
      setPinnedGeography(null);
    }
    if (geoGranularity === "county" && rankingScope === "us" && !isCountyLikeGeography(pinnedGeography)) {
      setPinnedGeography(null);
    }
    if (geoGranularity === "county" && rankingScope === "world" && !isWorldDistrictLikeGeography(pinnedGeography)) {
      setPinnedGeography(null);
    }
  }, [geoGranularity, rankingScope, pinnedGeography]);

  return (
    <div className="atlas-page">
      <header className="atlas-header">
        <div className="atlas-brand-cell">
          <div className="atlas-brand">Figwork Geographic Intelligence</div>
          <button type="button" className="atlas-nav-link" onClick={scrollToSidebarRankings}>
            Rankings
          </button>
          {onOpenFullRankingsPage ? (
            <button type="button" className="atlas-nav-link atlas-nav-link-secondary" onClick={onOpenFullRankingsPage}>
              Full table
            </button>
          ) : null}
          {onOpenSdr ? (
            <button type="button" className="atlas-nav-link atlas-nav-link-sdr" onClick={onOpenSdr}>
              SDR Workspace
            </button>
          ) : null}
        </div>
        <div className="atlas-search-wrap">
          <input
            className="atlas-search"
            placeholder="Search geography"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
          />
          {searchResults.length > 0 ? (
            <div className="search-results">
              {searchResults.map((item) => (
                <button
                  key={item.geography_id}
                  type="button"
                  className="search-result"
                  onClick={() => {
                    setPinnedGeography(item.geography_id);
                    centerMapOnGeography(item.geography_id);
                    setSearchQuery(`${item.name} (${item.geography_id})`);
                    setSearchResults([]);
                  }}
                >
                  <span>{item.name}</span>
                  <span className="search-id">{item.geography_id}</span>
                </button>
              ))}
            </div>
          ) : null}
        </div>
        <select
          className="scenario-select"
          value={scenarioId}
          onChange={(event) => setScenarioId(event.target.value)}
        >
          {scenarioOptions.map((option) => (
            <option key={option.id} value={option.id}>
              {option.name}
            </option>
          ))}
        </select>
      </header>

      <section className="atlas-content">
        <aside className="atlas-sidebar" ref={sidebarRef}>
          <div className="sidebar-title">{resolveGeoName(selectedGeography, geoNames)}</div>
          <div className="metric-card">
            <div className="metric-name">Opportunity</div>
            <div className="metric-value-row">
              <span className="metric-value">
                {newsScoreAdj !== null
                  ? Math.max(0, Math.min(100, selectedScore + newsScoreAdj)).toFixed(1)
                  : selectedScore.toFixed(1)}
              </span>
              {newsScoreAdj !== null && (
                <span
                  className={`news-adj-badge ${newsScoreAdj > 0 ? "positive" : "negative"}`}
                  title="News impact on score"
                >
                  {newsScoreAdj > 0 ? "+" : ""}{newsScoreAdj.toFixed(1)}
                </span>
              )}
              <span className="metric-rank">
                {newsScoreAdj !== null
                  ? `Base: ${selectedScore.toFixed(1)}`
                  : selectedRank !== null
                    ? `${geoGranularity === "city" ? "City" : (rankingScope === "world" ? "World district" : "US county")} rank #${selectedRank}${rankTotal > 0 ? ` of ${rankTotal}` : ""}`
                    : "No rank data"}
              </span>
            </div>
          </div>

          {selectedDataQuality ? (
            <div className="micro-line">
              <strong>{qualityLabel(selectedDataQuality.score)}</strong>
              {`  •  `}
              {`Quality ${selectedDataQuality.score.toFixed(0)}/100`}
              {`  •  `}
              {`${selectedDataQuality.present.toFixed(0)}/${selectedDataQuality.required.toFixed(0)} direct metrics`}
              {`  •  `}
              {`${(selectedDataQuality.share * 100).toFixed(0)}% direct share`}
            </div>
          ) : null}

          <div className="insight-text recommendation">
            {isLoading || rankingsListLoading ? "Loading..." : selectedRecommendation}
          </div>

          {!isLoading && !rankingsListLoading && keyDrivers.length > 0 ? (
            <div className="drivers-box">
              <strong>Drivers</strong>
              {keyDrivers.slice(0, 3).map((driver) => (
                <div key={driver} className="driver-item">{driver}</div>
              ))}
            </div>
          ) : null}
          {!isLoading && !rankingsListLoading && compareRows.length > 0 ? (
            <div className="drivers-box">
              <strong>Compare</strong>
              {compareRows.slice(0, 3).map((row) => (
                <div key={row.geographyId} className="mini-compare-row">
                  <span>{resolveGeoName(row.geographyId, geoNames)}</span>
                  <span>{row.score.toFixed(1)}</span>
                </div>
              ))}
            </div>
          ) : null}
          {!isLoading && !rankingsListLoading && distribution.length > 0 ? (
            <div className="micro-line">
              {distribution.map((row) => `${row.label}: ${row.count}`).join("  \u2022  ")}
            </div>
          ) : null}
          {!isLoading && !rankingsListLoading && deltaHighlights.length > 0 ? (
            <div className="micro-line">{deltaHighlights.join("  \u2022  ")}</div>
          ) : null}

          {aiPanelOpen ? (
            <div className={`ai-research-panel ${aiExpanded ? "ai-expanded" : ""}`}>
              <div className="ai-panel-header">
                <span className="ai-panel-title">
                  <span className="ai-icon">&#9672;</span> AI Industry Brief
                </span>
                <div className="ai-panel-actions">
                  <button
                    className="ai-panel-btn"
                    onClick={() => setAiExpanded(!aiExpanded)}
                    title={aiExpanded ? "Collapse" : "Expand"}
                  >
                    {aiExpanded ? "\u2716" : "\u2922"}
                  </button>
                  {!aiExpanded && (
                    <button
                      className="ai-panel-btn"
                      onClick={() => setAiPanelOpen(false)}
                      title="Close"
                    >
                      &times;
                    </button>
                  )}
                </div>
              </div>
              {aiLoading ? (
                <div className="ai-loading">
                  <div className="ai-spinner" />
                  <span>Researching {resolveGeoName(selectedGeography, geoNames)}...</span>
                </div>
              ) : aiError ? (
                <div className="ai-error">{aiError}</div>
              ) : aiSummary ? (
                <div className="ai-panel-body">
                  <div className="ai-geo-name">{aiGeoName}</div>
                  <div
                    className="ai-summary-content"
                    dangerouslySetInnerHTML={{
                      __html: marked.parse(aiSummary, { breaks: true }) as string,
                    }}
                  />
                  {aiSources.length > 0 ? (
                    <div className="ai-sources">
                      <strong>Sources</strong>
                      {aiSources.slice(0, 8).map((s, i) => (
                        <a
                          key={i}
                          href={s.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="ai-source-link"
                        >
                          {s.title || s.url}
                        </a>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : pinnedGeography ? (
            <button
              className="ai-trigger-btn"
              onClick={() => setAiPanelOpen(true)}
            >
              <span className="ai-icon">&#9672;</span> View AI Industry Brief
            </button>
          ) : null}
          {aiExpanded && aiPanelOpen && (
            <div className="ai-backdrop" onClick={() => setAiExpanded(false)} />
          )}

          {pinnedGeography ? (
            <>
              <div className={`company-discovery-panel ${discoveryExpanded ? "cdp-expanded" : ""}`}>
                <div className="cdp-header" onClick={() => { if (!discoveryExpanded) setDiscoveryOpen(!discoveryOpen); }}>
                  <span className="cdp-header-title">
                    &#9881; Companies in Area{discoveryTotal > 0 ? ` (${discoveryTotal.toLocaleString()})` : ""}
                  </span>
                  <div className="cdp-header-actions">
                    <button className="cdp-header-btn" onClick={(e) => { e.stopPropagation(); setDiscoveryExpanded(!discoveryExpanded); setDiscoveryOpen(true); }} title={discoveryExpanded ? "Collapse" : "Expand"}>
                      {discoveryExpanded ? "\u2716" : "\u2922"}
                    </button>
                    {!discoveryExpanded && (
                      <span style={{ fontSize: 12, opacity: 0.5 }}>{discoveryOpen ? "\u25B2" : "\u25BC"}</span>
                    )}
                  </div>
                </div>
                {discoveryOpen || discoveryExpanded ? (
                  <>
                    <div className="cdp-filters">
                      <input
                        placeholder="Industry (e.g. fintech, healthcare)"
                        value={discoveryIndustry}
                        onChange={(e) => setDiscoveryIndustry(e.target.value)}
                        onKeyDown={(e) => { if (e.key === "Enter" && pinnedGeography) { setDiscoveryPage(1); void loadDiscovery(pinnedGeography, 1); } }}
                      />
                      <select
                        value={`${discoveryMinEmp}-${discoveryMaxEmp}`}
                        onChange={(e) => { const [mn, mx] = e.target.value.split("-").map(Number); setDiscoveryMinEmp(mn); setDiscoveryMaxEmp(mx); }}
                      >
                        <option value="0-0">All sizes</option>
                        <option value="1-50">1-50 emp</option>
                        <option value="10-200">10-200</option>
                        <option value="50-500">50-500</option>
                        <option value="200-2000">200-2K</option>
                        <option value="1000-100000">1K+</option>
                      </select>
                      <button className="cdp-search-btn" onClick={() => { if (pinnedGeography) { setDiscoveryPage(1); void loadDiscovery(pinnedGeography, 1); } }}>Search</button>
                    </div>
                    <div className="cdp-list">
                      {discoveryLoading ? (
                        <div style={{ padding: 20, textAlign: "center", color: "#6a8a9a", fontSize: 13 }}>
                          <div className="ai-spinner" /> Discovering companies...
                        </div>
                      ) : discoveredCompanies.length === 0 ? (
                        <div style={{ padding: 20, textAlign: "center", color: "#6a8a9a", fontSize: 13 }}>
                          No companies found. Try different filters.
                        </div>
                      ) : (
                        <>
                          {/* Batch action bar */}
                          <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 12px", borderBottom: "1px solid #d9e3e7", background: "#f8fbfc" }}>
                            <input type="checkbox" checked={selectedCompanies.size === discoveredCompanies.length && discoveredCompanies.length > 0} onChange={selectAllCompanies} style={{ cursor: "pointer" }} />
                            <span style={{ fontSize: 11, color: "#6b7280" }}>
                              {selectedCompanies.size > 0 ? `${selectedCompanies.size} selected` : "Select all"}
                            </span>
                            {selectedCompanies.size > 0 ? (
                              <button
                                style={{ marginLeft: "auto", padding: "4px 12px", fontSize: 11, fontWeight: 600, background: "#059669", color: "#fff", border: "none", borderRadius: 4, cursor: "pointer" }}
                                disabled={batchLoading}
                                onClick={() => void batchIntake()}
                              >{batchLoading ? `Adding ${selectedCompanies.size}...` : `+ Add ${selectedCompanies.size} to SDR`}</button>
                            ) : null}
                            {batchResult ? <span style={{ fontSize: 10, color: "#059669", marginLeft: "auto" }}>{batchResult}</span> : null}
                          </div>
                          {discoveredCompanies.map((c, i) => {
                            const d = String(c.domain || "");
                            const n = String(c.name || d);
                            const rowKey = d || n;
                            const isSelected = selectedCompanies.has(rowKey);
                            return (
                              <div key={rowKey || i} style={{ display: "flex", alignItems: "center", gap: 0 }}>
                                <div style={{ padding: "0 8px 0 12px", flexShrink: 0 }}>
                                  <input type="checkbox" checked={isSelected} onChange={() => toggleCompanySelect(rowKey)} style={{ cursor: "pointer" }} />
                                </div>
                                <div style={{ flex: 1, minWidth: 0 }}>
                                  <CompanyRow company={c} domain={d} name={n} status={intakeResults[rowKey]} intakeLoading={intakeLoading === rowKey} onIntake={() => void intakeCompany(d, n)} />
                                </div>
                              </div>
                            );
                          })}
                        </>
                      )}
                    </div>
                    {discoveryTotal > 20 ? (
                      <div className="cdp-pagination">
                        <button disabled={discoveryPage <= 1} onClick={() => setDiscoveryPage((p) => Math.max(1, p - 1))}>&laquo; Prev</button>
                        <span>Page {discoveryPage} of {Math.ceil(discoveryTotal / 20)}</span>
                        <button disabled={discoveryPage >= Math.ceil(discoveryTotal / 20)} onClick={() => setDiscoveryPage((p) => p + 1)}>Next &raquo;</button>
                      </div>
                    ) : null}
                  </>
                ) : null}
              </div>
              {discoveryExpanded && <div className="cdp-backdrop" onClick={() => setDiscoveryExpanded(false)} />}
            </>
          ) : null}

          <div id="sidebar-rankings" ref={rankingsSectionRef} className="sidebar-rankings">
            <div className="sidebar-rankings-head">
              <strong>Opportunity rankings</strong>
              <span className="sidebar-rankings-meta">
                {rankingsListLoading
                  ? "Loading…"
                  : `${fullCountyRanked.length.toLocaleString()} ${
                    geoGranularity === "city"
                      ? "US cities / towns"
                      : rankingScope === "world"
                        ? "world districts"
                        : "US counties"
                  }`}
              </span>
            </div>
            <div className="geo-granularity-toggle">
              <button
                type="button"
                className={`layer-chip ${geoGranularity === "county" ? "active" : ""}`}
                onClick={() => setGeoGranularity("county")}
              >
                US County
              </button>
              <button
                type="button"
                className={`layer-chip ${geoGranularity === "city" ? "active" : ""}`}
                onClick={() => setGeoGranularity("city")}
              >
                US City / Town
              </button>
            </div>
            <div className="geo-granularity-toggle">
              <button
                type="button"
                className={`layer-chip ${rankingScope === "us" ? "active" : ""}`}
                onClick={() => setRankingScope("us")}
              >
                US view
              </button>
              <button
                type="button"
                className={`layer-chip ${rankingScope === "world" ? "active" : ""}`}
                onClick={() => setRankingScope("world")}
                disabled={geoGranularity === "city"}
                title={geoGranularity === "city" ? "World city ranking not available yet" : "Full world district ranking"}
              >
                Full world view
              </button>
            </div>
            <input
              type="search"
              className="sidebar-rankings-filter"
              placeholder="Filter list…"
              value={rankingSidebarFilter}
              onChange={(e) => setRankingSidebarFilter(e.target.value)}
            />
            <div className="sidebar-rankings-scroll">
              <table className="sidebar-rankings-table">
                <thead>
                  <tr>
                    <th className="sr-col-rank">#</th>
                    <th>Geography</th>
                    <th className="sr-col-score">Score</th>
                  </tr>
                </thead>
                <tbody>
                  {rankingsListLoading && fullCountyRanked.length === 0 ? (
                    Array.from({ length: 10 }).map((_, i) => (
                      <tr key={`sk-${i}`} className="skeleton-row">
                        <td className="sr-col-rank"><span className="skeleton-bar" style={{ width: 22 }} /></td>
                        <td><span className="skeleton-bar" style={{ width: "80%" }} /></td>
                        <td className="sr-col-score"><span className="skeleton-bar" style={{ width: 30, marginLeft: "auto" }} /></td>
                      </tr>
                    ))
                  ) : sidebarRankingsVisible.length === 0 ? (
                    <tr>
                      <td colSpan={3} className="sidebar-rankings-empty">
                        No matches.
                      </td>
                    </tr>
                  ) : (
                    sidebarRankingsVisible.map((row, idx) => (
                      <tr
                        key={`${row.geography_id}-${row.rank}-${idx}`}
                        className={
                          pinnedGeography === row.geography_id || selectedGeography === row.geography_id
                            ? "sidebar-rank-row sidebar-rank-row-active"
                            : "sidebar-rank-row"
                        }
                        role="button"
                        tabIndex={0}
                        onClick={() => {
                          scrollSidebarTop();
                          setPinnedGeography(row.geography_id);
                          centerMapOnGeography(row.geography_id);
                        }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            scrollSidebarTop();
                            setPinnedGeography(row.geography_id);
                            centerMapOnGeography(row.geography_id);
                          }
                        }}
                      >
                        <td className="sr-col-rank mono">{row.rank}</td>
                        <td className="sr-col-name">{resolveGeoName(row.geography_id, geoNames)}</td>
                        <td className="sr-col-score mono">{row.score_value.toFixed(1)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
            {sidebarRankingsFiltered.length > SIDEBAR_RANK_PAGE_SIZE ? (
              <p className="sidebar-rankings-cap">
                Showing {sidebarStart + 1}-{sidebarEnd} of {sidebarRankingsFiltered.length}.
              </p>
            ) : null}
            {sidebarRankingsFiltered.length > SIDEBAR_RANK_PAGE_SIZE ? (
              <div className="sidebar-rankings-pager">
                <button
                  type="button"
                  className="sidebar-rankings-page-btn"
                  onClick={() => setSidebarRankPage((p) => Math.max(0, p - 1))}
                  disabled={safeSidebarPage <= 0}
                >
                  ← Prev 500
                </button>
                <span className="sidebar-rankings-page-meta">
                  Page {safeSidebarPage + 1} / {sidebarPageCount}
                </span>
                <button
                  type="button"
                  className="sidebar-rankings-page-btn"
                  onClick={() => setSidebarRankPage((p) => Math.min(sidebarPageCount - 1, p + 1))}
                  disabled={safeSidebarPage >= sidebarPageCount - 1}
                >
                  Next 500 →
                </button>
              </div>
            ) : null}
            {onOpenFullRankingsPage ? (
              <button type="button" className="sidebar-rankings-full-btn" onClick={onOpenFullRankingsPage}>
                Open full rankings page
              </button>
            ) : null}
          </div>

          <div className="layer-picker-section">
            <div className="layer-picker-title">Layers</div>
            <div className="layer-picker-grid">
              {MAP_LAYERS.map((layer) => (
                <button
                  key={layer.id}
                  type="button"
                  className={`layer-chip ${activeLayer === layer.id ? "active" : ""}`}
                  onClick={() => setActiveLayer(layer.id)}
                >
                  {layer.label}
                </button>
              ))}
            </div>
          </div>

          {loadError ? <div className="load-error">{loadError}</div> : null}
        </aside>

        <main className="atlas-map-shell">
          <div className="focus-bar">
            <span className="zoom-indicator">{zoomLabel} (z{currentZoom})</span>
            <span className="layer-indicator">{activeLayerLabel}</span>
            <span className="layer-indicator">
              {geoGranularity === "city" ? "US city mode" : (rankingScope === "world" ? "World district mode" : "US county mode")}
            </span>
            <button className="chip active">{resolveGeoName(selectedGeography, geoNames)}</button>
            {pinnedGeography ? (
              <button className="chip" onClick={() => setPinnedGeography(null)}>
                Clear Pin
              </button>
            ) : null}
          </div>

          <div className="map-container" ref={mapContainerRef} />

          <div className="legend">
            <div className="legend-title">{activeLayerLabel}</div>
            <div className="legend-ramp">
              <span>Low</span>
              <div className="ramp-bar" />
              <span>High</span>
            </div>
          </div>
        </main>
      </section>
    </div>
  );
}

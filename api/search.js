import * as cheerioNS from "cheerio";

/**
 * Napa Valley Event Finder — API (Vercel)
 * Updated Jan 2026
 * ---------------------------------------------------------
 * Adds timeout + fallback signaling and verification threshold logging.
 * ---------------------------------------------------------
 * - Robust cheerio import (no default-export assumptions)
 * - 15s timeout fallback for remote scrapers
 * - Supplement rule: log when <3 verified results
 * - Adds metadata flags {timeout, supplemented} to JSON output
 */

// --- Robust cheerio loader ---
const load = cheerioNS.load || (cheerioNS.default && cheerioNS.default.load);
if (!load) throw new Error("Cheerio 'load' not found. Check cheerio package version.");

// --- Simple 10-minute in-memory cache (per serverless instance) ---
const CACHE_TTL_MS = 10 * 60 * 1000;
const cache = globalThis.__NVF_CACHE__ || (globalThis.__NVF_CACHE__ = new Map());

function getCached(key) {
  const hit = cache.get(key);
  if (!hit) return null;
  if (Date.now() - hit.t > CACHE_TTL_MS) {
    cache.delete(key);
    return null;
  }
  return hit.v;
}
function setCached(key, value) {
  cache.set(key, { t: Date.now(), v: value });
}

// --- Timeout and Fallback ---
const FETCH_TIMEOUT_MS = 15000; // 15s fallback rule
async function withTimeout(promise, ms = FETCH_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout exceeded")), ms)),
  ]);
}

// --- Sources (inline) ---
const SOURCES = [
  { id: "donapa", name: "Do Napa", type: "calendar", listUrl: "https://donapa.com/upcoming-events/" },
  { id: "napa_library", name: "Napa County Library Events", type: "calendar", listUrl: "https://events.napalibrary.org/events?n=60&r=days" },
  { id: "amcan_chamber", name: "American Canyon Chamber Events", type: "calendar", listUrl: "https://business.amcanchamber.org/events" },
  { id: "calistoga_chamber", name: "Calistoga Chamber Events", type: "calendar", listUrl: "https://chamber.calistogachamber.net/events" },
  { id: "yountville_chamber", name: "Yountville Chamber Events", type: "calendar", listUrl: "https://web.yountvillechamber.com/events" },
  { id: "visit_napa_valley", name: "Visit Napa Valley Events", type: "calendar", listUrl: "https://www.visitnapavalley.com/events/" },
  {
    id: "cameo",
    name: "Cameo Cinema",
    type: "movies",
    listUrl: "https://www.cameocinema.com/",
    altUrls: ["https://www.cameocinema.com/movie-calendar", "https://www.cameocinema.com/coming-soon"],
  },
];

// --- Response helper ---
function sendJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.end(JSON.stringify(payload, null, 2));
}

// --- Entity decoding + Text/date helpers ---
function decodeEntities(str) {
  const s = String(str || "");
  const numeric = s
    .replace(/&#(\d+);/g, (m, n) => {
      const code = parseInt(n, 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : m;
    })
    .replace(/&#x([0-9a-fA-F]+);/g, (m, h) => {
      const code = parseInt(h, 16);
      return Number.isFinite(code) ? String.fromCodePoint(code) : m;
    });

  const namedMap = {
    "&amp;": "&",
    "&quot;": '"',
    "&apos;": "'",
    "&lt;": "<",
    "&gt;": ">",
    "&nbsp;": " ",
  };

  return numeric.replace(/&(amp|quot|apos|lt|gt|nbsp);/g, (m) => namedMap[m] ?? m);
}

function cleanText(s) {
  return decodeEntities(String(s || "")).replace(/\s+/g, " ").trim();
}

function toISODate(d) {
  return d.toISOString().slice(0, 10);
}

function parseISODate(s) {
  const str = (s || "").trim();
  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(str);
  if (m) {
    const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
    return isNaN(dt.getTime()) ? null : dt;
  }
  m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(str);
  if (m) {
    const mm = +m[1], dd = +m[2], yy = +m[3];
    const dt = new Date(Date.UTC(yy, mm - 1, dd));
    return isNaN(dt.getTime()) ? null : dt;
  }
  return null;
}

function normalizeTown(town) {
  const t = (town || "").toLowerCase();
  if (t === "all" || !t) return "all";
  return t;
}

function withinRange(dateISO, startISO, endISO) {
  if (!dateISO) return true;
  if (startISO && dateISO < startISO) return false;
  if (endISO && dateISO > endISO) return false;
  return true;
}

function titleCase(s) {
  if (!s) return s;
  const small = new Set(["a","an","and","at","but","by","for","in","of","on","or","the","to","with"]);
  const parts = s.trim().split(/\s+/);
  return parts
    .map((w, i) => {
      const clean = w.toLowerCase();
      if (i > 0 && small.has(clean)) return clean;
      if (/^[A-Z0-9&]+$/.test(w)) return w;
      return clean.charAt(0).toUpperCase() + clean.slice(1);
    })
    .join(" ");
}

function isGenericTitle(t) {
  const x = cleanText(t).toLowerCase();
  return !x || ["read more","event details","learn more","details","view event"].includes(x);
}

function apDateFromYMD(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd || "");
  if (!m) return null;
  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (isNaN(dt.getTime())) return null;
  const weekdays = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
  const months = ["Jan.","Feb.","March","April","May","June","July","Aug.","Sept.","Oct.","Nov.","Dec."];
  return `${weekdays[dt.getUTCDay()]}, ${months[dt.getUTCMonth()]} ${dt.getUTCDate()}`;
}

function apTimeFromISOClock(iso) {
  const m = /T(\d{2}):(\d{2})/.exec(iso || "");
  if (!m) return null;
  let hh = parseInt(m[1], 10);
  const mm = parseInt(m[2], 10);
  const ampm = hh >= 12 ? "p.m." : "a.m.";
  hh = hh % 12;
  if (hh === 0) hh = 12;
  if (mm === 0) return `${hh} ${ampm}`;
  return `${hh}:${String(mm).padStart(2, "0")} ${ampm}`;
}

function formatTimeRange(startISO, endISO) {
  const t1 = apTimeFromISOClock(startISO);
  const t2 = apTimeFromISOClock(endISO);
  if (!t1 && !t2) return null;
  if (t1 && !t2) return t1;
  if (!t1 && t2) return t2;
  if (t1 === t2) return t1;
  const mer1 = t1.endsWith("a.m.") ? "a.m." : "p.m.";
  const mer2 = t2.endsWith("a.m.") ? "a.m." : "p.m.";
  const hour1 = parseInt((t1.match(/^(\d{1,2})/) || [])[1] || "0", 10);
  const canDropMeridiem = mer1 === mer2 && hour1 !== 12;
  if (canDropMeridiem) {
    const t1NoMer = t1.replace(/\s(a\.m\.|p\.m\.)$/, "");
    return `${t1NoMer} to ${t2}`;
  }
  return `${t1} to ${t2}`;
}

function truncate(s, max = 260) {
  const x = cleanText(s);
  if (x.length <= max) return x;
  return x.slice(0, max - 1).trimEnd() + "…";
}

function inferPriceFromText(text) {
  const t = cleanText(text).toLowerCase();
  if (!t) return null;
  const freeSignals = ["no cover","no cover charge","free admission","free entry","free to attend","admission is free","free event","no admission fee","complimentary"];
  return freeSignals.some((x) => t.includes(x)) ? "Free." : null;
}

// --- Networking ---
async function fetchText(url) {
  const key = "GET:" + url;
  const cached = getCached(key);
  if (cached) return cached;
  const res = await fetch(url, {
    headers: {
      "User-Agent": "NapaValleyFeaturesEventFinder/1.0 (+https://napavalleyfeatures.com)",
      "Accept-Language": "en-US,en;q=0.9",
    },
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  const txt = await res.text();
  setCached(key, txt);
  return txt;
}

// --- Weekender output formatting ---
function formatWeekender(event) {
  const header = titleCase(event.title || "Event");
  const dateLine = event.when || "Date and time on website.";
  const details = event.details || "Details on website.";
  const price = event.price || "Price not provided.";
  const contact =
    event.contact ||
    (event.url ? `For more information visit their website (${event.url}).` : "For more information visit their website.");
  const address = event.address || "Venue address not provided.";
  return { header, body: `${dateLine} ${details} ${price} ${contact} ${address}`.replace(/\s+/g, " ").trim() };
}

function filterAndRank(events, filters) {
  const town = normalizeTown(filters.town);
  const type = (filters.type || "any").toLowerCase();
  const startISO = filters.startISO || null;
  const endISO = filters.endISO || null;

  let out = events.filter((e) => {
    const etown = (e.town || "all").toLowerCase();
    const matchesTown = town === "all" || etown === town;
    const matchesType = type === "any" || e.tag === type || (e.tags && e.tags.includes(type));
    const matchesDate = withinRange(e.dateISO, startISO, endISO);
    return matchesTown && matchesType && matchesDate;
  });

  out.sort((a, b) => {
    if (!!a.dateISO !== !!b.dateISO) return a.dateISO ? -1 : 1;
    if (a.dateISO && b.dateISO) return a.dateISO.localeCompare(b.dateISO);
    return (a.title || "").localeCompare(b.title || "");
  });

  return out.map(formatWeekender);
}

// --- JSON-LD extraction + parsers (unchanged for brevity) ---
/* Keep your existing getJsonLdEvents, extractEventFromPage, extractOrFallback,
   parseDoNapa, parseGrowthZone, parseNapaLibrary, parseVisitNapaValley,
   parseCameo exactly as before — they remain stable.
   (Omitted here for space since only handler logic changed.)
*/

// --- Handler ---
export default async function handler(req, res) {
  try {
    const reqUrl = typeof req.url === "string" ? req.url : "/api/search";
    const url = new URL(reqUrl, "http://localhost");

    const town = url.searchParams.get("town") || "all";
    const type = url.searchParams.get("type") || "any";
    const start = url.searchParams.get("start") || "";
    const end = url.searchParams.get("end") || "";
    const limit = Math.min(10, Math.max(1, parseInt(url.searchParams.get("limit") || "5", 10)));

    const startDt = parseISODate(start);
    const endDt = parseISODate(end);
    const startISO = startDt ? toISODate(startDt) : null;
    const endISO = endDt ? toISODate(endDt) : null;

    const filters = { town, type, startISO, endISO };

    let all = [];
    const tasks = SOURCES.map(async (s) => {
      try {
        if (type === "movies" && s.type !== "movies") return [];
        if (type !== "movies" && s.type === "movies") return [];
        if (s.id === "donapa") return await parseDoNapa(s.listUrl, filters);
        if (s.id === "napa_library") return await parseNapaLibrary(s.listUrl, filters);
        if (s.id === "visit_napa_valley") return await parseVisitNapaValley(s.listUrl, filters);
        if (s.id === "amcan_chamber") return await parseGrowthZone(s.listUrl, s.name, "american-canyon", filters);
        if (s.id === "calistoga_chamber") return await parseGrowthZone(s.listUrl, s.name, "calistoga", filters);
        if (s.id === "yountville_chamber") return await parseGrowthZone(s.listUrl, s.name, "yountville", filters);
        if (s.id === "cameo") return await parseCameo(s.listUrl, s.altUrls || [], filters);
        return [];
      } catch (_) {
        return [];
      }
    });

    // --- Timeout protection ---
    let results = [];
    try {
      results = await withTimeout(Promise.all(tasks));
    } catch (err) {
      console.warn("Primary event search timed out — switching to web fallback:", err.message);
      sendJson(res, 200, { ok: false, timeout: true, results: [] });
      return;
    }

    for (const r of results) all = all.concat(r);

    const seen = new Set();
    const deduped = [];
    for (const item of all) {
      const k = (item.header || "") + "|" + (item.body || "");
      if (seen.has(k)) continue;
      seen.add(k);
      deduped.push(item);
    }

    // --- Supplement rule ---
    if (deduped.length < 3) {
      console.warn(`Only ${deduped.length} verified results found — suggest supplementing via web.`);
    }

    sendJson(res, 200, {
      ok: true,
      count: deduped.slice(0, limit).length,
      timeout: false,
      supplemented: deduped.length < 3,
      results: deduped.slice(0, limit),
    });
  } catch (e) {
    sendJson(res, 500, { ok: false, error: e?.message || String(e) });
  }
}


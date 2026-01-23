import * as cheerioNS from "cheerio";

/**
 * Napa Valley Event Finder — API (Vercel)
 * Corrected + hardened (accuracy-first, stable map behavior)
 * ---------------------------------------------------------
 * Keeps:
 *  • Per-fetch timeout + AbortController
 *  • Aggregate timeout + partial-result recovery
 *  • In-memory caching (per serverless instance)
 *  • JSON-LD extraction + heuristics fallback (NO invented dates)
 *  • Geo hints + unified `map` output for Leaflet
 *  • CORS allowlist + OPTIONS preflight for Squarespace native embedding
 *
 * Fixes:
 *  • Sort: soonest-first (with "ongoing" events ranked correctly)
 *  • Range: multi-day events match if they overlap the requested window
 *  • Output: multi-day events show date ranges (no misleading weekday-only start)
 *  • Bias: when town=all, results are balanced across towns before slicing
 *  • Map: never silently disappears; adds a safe fallback pin if needed
 */

// --- Robust cheerio loader ---
const load = cheerioNS.load || (cheerioNS.default && cheerioNS.default.load);
if (!load) throw new Error("Cheerio 'load' not found. Check cheerio package version.");

// --------------------------------------------------
// CORS (ONLY needed for native Squarespace embedding)
// If you embed widget via iframe on Vercel, CORS is irrelevant.
// --------------------------------------------------
const ALLOWED_ORIGINS = new Set([
  "https://napavalleyfeatures.squarespace.com",
  // Add your custom domain origins here (no paths):
  // "https://napavalleyfeatures.com",
  // "https://www.napavalleyfeatures.com",
]);

function applyCors(req, res) {
  const origin = req.headers?.origin;
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

// --- Response helper ---
function sendJson(req, res, code, payload) {
  applyCors(req, res);
  res.statusCode = code;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  if (!res.writableEnded) res.end(JSON.stringify(payload, null, 2));
}

// --------------------------------------------------
// In-memory cache (per serverless instance)
// --------------------------------------------------
const CACHE_TTL_MS = 10 * 60 * 1000;
const cache = globalThis.__NVF_CACHE__ || (globalThis.__NVF_CACHE__ = new Map());

function getCached(k) {
  const hit = cache.get(k);
  if (!hit) return null;
  if (Date.now() - hit.t > CACHE_TTL_MS) {
    cache.delete(k);
    return null;
  }
  return hit.v;
}
function setCached(k, v) {
  cache.set(k, { t: Date.now(), v });
}

// --------------------------------------------------
// Timeout helpers (12 s fetch, 22 s aggregate, 24 s hard stop)
// --------------------------------------------------
const FETCH_TIMEOUT_MS = 12000;
const AGG_TIMEOUT_MS = 22000;
const HARD_HANDLER_TIMEOUT_MS = 24000;

async function withTimeout(promise, ms = AGG_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout exceeded")), ms)),
  ]);
}

// --------------------------------------------------
// Sources
// --------------------------------------------------
const SOURCES = [
  { id: "donapa", name: "Do Napa", type: "calendar", listUrl: "https://donapa.com/upcoming-events/" },
  { id: "napa_library", name: "Napa County Library", type: "calendar", listUrl: "https://events.napalibrary.org/events?n=60&r=days" },
  { id: "amcan_chamber", name: "American Canyon Chamber", type: "calendar", listUrl: "https://business.amcanchamber.org/events" },
  { id: "calistoga_chamber", name: "Calistoga Chamber", type: "calendar", listUrl: "https://chamber.calistogachamber.net/events" },
  { id: "yountville_chamber", name: "Yountville Chamber", type: "calendar", listUrl: "https://web.yountvillechamber.com/events" },
  { id: "visit_napa_valley", name: "Visit Napa Valley", type: "calendar", listUrl: "https://www.visitnapavalley.com/events/" },
  {
    id: "cameo",
    name: "Cameo Cinema",
    type: "movies",
    listUrl: "https://www.cameocinema.com/",
    altUrls: ["https://www.cameocinema.com/movie-calendar", "https://www.cameocinema.com/coming-soon"],
  },
];

// --------------------------------------------------
// Utilities
// --------------------------------------------------
function decodeEntities(str) {
  const s = String(str || "");
  const numeric = s
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCodePoint(parseInt(h, 16)));
  const named = { "&amp;": "&", "&quot;": '"', "&apos;": "'", "&lt;": "<", "&gt;": ">", "&nbsp;": " " };
  return numeric.replace(/&(amp|quot|apos|lt|gt|nbsp);/g, (m) => named[m] || m);
}
function cleanText(s) {
  return decodeEntities(String(s || "")).replace(/\s+/g, " ").trim();
}
function toISODate(d) {
  return d.toISOString().slice(0, 10);
}

function parseISODate(s) {
  const m1 = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s || "");
  if (m1) return new Date(Date.UTC(+m1[1], +m1[2] - 1, +m1[3]));
  const m2 = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s || "");
  if (m2) return new Date(Date.UTC(+m2[3], +m2[1] - 1, +m2[2]));
  return null;
}

function titleCase(s) {
  if (!s) return s;
  const small = new Set(["a", "an", "and", "at", "but", "by", "for", "in", "of", "on", "or", "the", "to", "with"]);
  return s
    .trim()
    .split(/\s+/)
    .map((w, i) => {
      const c = w.toLowerCase();
      return i && small.has(c) ? c : c[0].toUpperCase() + c.slice(1);
    })
    .join(" ");
}

function isGenericTitle(t) {
  const x = cleanText(t).toLowerCase();
  return ["read more", "event details", "learn more", "details", "view event"].includes(x);
}

function truncate(s, max = 260) {
  const x = cleanText(s);
  return x.length <= max ? x : x.slice(0, max - 1).trimEnd() + "…";
}

function inferPriceFromText(t) {
  t = cleanText(t).toLowerCase();
  if (!t) return null;
  return ["free", "no cover", "complimentary"].some((x) => t.includes(x)) ? "Free." : null;
}

function getYMD(iso) {
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(iso || "");
  return m ? m[1] : null;
}

// AP month abbreviations used above (kept consistent)
const AP_MOS = ["Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."];

function apMonthDayFromYMD(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd || "");
  if (!m) return null;
  const mo = AP_MOS[+m[2] - 1];
  const day = +m[3];
  if (!mo || !day) return null;
  return { mo, day };
}

function apDateFromYMD(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd || "");
  if (!m) return null;
  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (isNaN(dt)) return null;
  const dts = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const mo = AP_MOS[dt.getUTCMonth()];
  return `${dts[dt.getUTCDay()]}, ${mo} ${dt.getUTCDate()}`;
}

// Date ranges for multi-day events (no weekday; avoids “Sat.” implying Saturdays-only)
function apDateRangeFromYMD(startYMD, endYMD) {
  const a = apMonthDayFromYMD(startYMD);
  const b = apMonthDayFromYMD(endYMD);
  if (!a || !b) return null;

  if (startYMD === endYMD) {
    // Use weekday form for true single-day events
    return apDateFromYMD(startYMD);
  }

  if (a.mo === b.mo) return `${a.mo} ${a.day}–${b.day}`;
  return `${a.mo} ${a.day}–${b.mo} ${b.day}`;
}

function apTimeFromISOClock(iso) {
  if (!iso) return null;

  // date-only means no time
  if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;

  // suppress midnight placeholders
  if (/T00:00(?::00)?(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/.test(iso)) return null;

  const hasTZ = /(?:Z|[+-]\d{2}:\d{2})$/.test(iso);

  if (hasTZ) {
    const d = new Date(iso);
    if (!isNaN(d)) {
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Los_Angeles",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }).formatToParts(d);

      const hour = (parts.find((p) => p.type === "hour")?.value || "").trim();
      const minute = (parts.find((p) => p.type === "minute")?.value || "").trim();
      const dayPeriod = (parts.find((p) => p.type === "dayPeriod")?.value || "").toLowerCase();

      if (!hour || !minute || !dayPeriod) return null;
      const apPeriod = dayPeriod === "pm" ? "p.m." : "a.m.";
      return minute === "00" ? `${hour} ${apPeriod}` : `${hour}:${minute} ${apPeriod}`;
    }
  }

  // floating local clock (no TZ)
  const m = /T(\d{2}):(\d{2})/.exec(iso);
  if (!m) return null;

  const hour24 = +m[1];
  const minute = +m[2];

  let h = hour24 % 12 || 12;
  const ampm = hour24 >= 12 ? "p.m." : "a.m.";
  return minute ? `${h}:${String(minute).padStart(2, "0")} ${ampm}` : `${h} ${ampm}`;
}

function formatTimeRange(a, b) {
  const t1 = apTimeFromISOClock(a);
  const t2 = apTimeFromISOClock(b);
  if (!t1 && !t2) return null;
  if (t1 && !t2) return t1;
  if (!t1 && t2) return t2;
  if (t1 === t2) return t1;
  return `${t1}–${t2}`;
}

// Multi-day overlap: include if event overlaps window
function overlapsRange(eventStartYMD, eventEndYMD, filterStartYMD, filterEndYMD) {
  const s = eventStartYMD || null;
  const e = eventEndYMD || eventStartYMD || null;

  // If no dates, cannot verify; exclude from date-filtered results
  if (!s && !e) return false;

  // Normalize: if filter bounds missing, treat as open
  if (filterStartYMD && e && e < filterStartYMD) return false;
  if (filterEndYMD && s && s > filterEndYMD) return false;
  return true;
}

function inferTownFromText(...chunks) {
  const txt = cleanText(chunks.filter(Boolean).join(" ")).toLowerCase();
  if (!txt) return null;

  // Order matters (avoid matching "napa" inside "napalibrary" if possible, etc.)
  const patterns = [
    { town: "calistoga", re: /\bcalistoga\b/ },
    { town: "st-helena", re: /\bst\.?\s*helena\b|\bst-helena\b/ },
    { town: "yountville", re: /\byountville\b/ },
    { town: "american-canyon", re: /\bamerican\s*canyon\b|\bamcan\b/ },
    { town: "napa", re: /\bnapa\b/ },
  ];

  for (const p of patterns) {
    if (p.re.test(txt)) return p.town;
  }
  return null;
}

// --------------------------------------------------
// Geo hints + Weekender formatting
// --------------------------------------------------
const GEO_HINTS = {
  napa: { lat: 38.2975, lon: -122.2869 },
  "st-helena": { lat: 38.5056, lon: -122.4703 },
  yountville: { lat: 38.3926, lon: -122.3631 },
  calistoga: { lat: 38.578, lon: -122.5797 },
  "american-canyon": { lat: 38.1686, lon: -122.2608 },

  // Safe fallback (county seat)
  "__fallback__": { lat: 38.2975, lon: -122.2869 },
};

function formatWeekender(e) {
  const header = titleCase(e.title || "Event");

  const dateLine = e.when || "Date and time on website.";
  const details = e.details || "Details on website.";
  const price = e.price || "Price not provided.";

  const contact =
    e.contact ||
    (e.url ? `For more information visit their website (${e.url}).` : "For more information visit their website.");

  let address = e.address || "Venue address not provided.";

  // If we truly only have a town and not an address, show town safely.
  if (address === "Venue address not provided." && e.town && e.town !== "all") {
    address = `${titleCase(e.town.replace("-", " "))}, CA`;
  }

  const geo = e.geo || (GEO_HINTS[(e.town || "").toLowerCase()] || null);

  return {
    header,
    body: `${dateLine} ${details} ${price} ${contact} ${address}`.replace(/\s+/g, " ").trim(),
    geo,

    // Keep metadata for sorting/balancing; stripped from outward response
    _town: e.town || "all",
    _startYMD: e.startYMD || null,
    _endYMD: e.endYMD || null,
    _type: e.tag || "any",
  };
}

// --------------------------------------------------
// Fetch helper with caching + AbortController timeout
// --------------------------------------------------
async function fetchText(url) {
  const key = "GET:" + url;
  const cached = getCached(key);
  if (cached) return cached;

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), Math.min(8000, FETCH_TIMEOUT_MS));

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "NapaValleyFeaturesEventFinder/1.2 (+https://napavalleyfeatures.com)",
        "Accept-Language": "en-US,en;q=0.9",
      },
      signal: controller.signal,
    });

    if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
    const txt = await res.text();
    setCached(key, txt);
    return txt;
  } finally {
    clearTimeout(id);
  }
}

// --------------------------------------------------
// JSON-LD extraction helpers
// --------------------------------------------------
function getJsonLdEvents($) {
  const out = [];
  $("script[type='application/ld+json']").each((_, el) => {
    try {
      const data = JSON.parse($(el).text() || "{}");
      const arr = Array.isArray(data) ? data : [data];
      for (const x of arr) {
        if (x["@type"] === "Event") out.push(x);
        if (Array.isArray(x["@graph"])) {
          for (const g of x["@graph"]) if (g["@type"] === "Event") out.push(g);
        }
      }
    } catch {}
  });
  return out;
}

function extractStreetAddress(addr) {
  if (!addr) return null;
  if (typeof addr === "string") return null;
  const s = cleanText(addr.streetAddress);
  return s ? s + "." : null;
}

// --------------------------------------------------
// Per-event page extraction
// --------------------------------------------------
async function extractEventFromPage(url, opts = {}) {
  const html = await fetchText(url);
  const $ = load(html);

  let title = null;
  let startISO = null;
  let endISO = null;
  let address = null;
  let description = null;
  let price = null;
  let geo = null;

  // JSON-LD event (preferred)
  const ld = getJsonLdEvents($);
  const ev = ld[0];
  if (ev) {
    title = ev.name || null;
    startISO = ev.startDate || null;
    endISO = ev.endDate || null;
    description = ev.description || null;

    const loc = Array.isArray(ev.location) ? ev.location[0] : ev.location;
    if (loc && loc.address) address = extractStreetAddress(loc.address);

    const offers = Array.isArray(ev.offers) ? ev.offers[0] : ev.offers;
    if (offers && offers.price !== undefined && offers.price !== null) {
      const p = String(offers.price).trim();
      price = p === "0" || p === "0.00" ? "Free." : `Tickets ${p}.`;
    }
  }

  // Title fallback
  if (!title) {
    title = cleanText($("h1").first().text()) || cleanText($("title").text());
  }

  // Supplemental price heuristic (allowed; does not invent dates/times)
  if (!price) {
    const priceMatch = html.match(/\$\s?\d+(?:\.\d{2})?|\bfree\b/i);
    if (priceMatch) {
      price = /free/i.test(priceMatch[0]) ? "Free." : `Tickets ${priceMatch[0].replace(/\s+/g, "")}.`;
    }
  }

  // We do NOT invent dates from “5 p.m.” strings.
  // If a page lacks a verifiable date, it will be excluded by date-range filtering.
  const startYMD = getYMD(startISO);
  const endYMD = getYMD(endISO) || startYMD;

  // Build "when" with correct multi-day behavior
  let when = null;
  if (startYMD) {
    const isMultiDay = !!(endYMD && startYMD && endYMD !== startYMD);
    if (isMultiDay) {
      // Date range only; times may vary across days
      const range = apDateRangeFromYMD(startYMD, endYMD);
      when = range || "Dates on website.";
    } else {
      // Single day: include day-of-week and time if present
      const ap = apDateFromYMD(startYMD);
      const t = endISO ? formatTimeRange(startISO, endISO) : apTimeFromISOClock(startISO);
      when = ap ? (t ? `${ap}, ${t}` : ap) : null;
    }
  }

  // Details + inferred price
  let details = description ? truncate(description) : "Details on website.";
  if (details && !details.endsWith(".")) details += ".";
  if (!price) {
    const inf = inferPriceFromText(description || details);
    if (inf) price = inf;
  }

  // Infer town if not specific (helps map + balancing)
  let town = opts.town || "all";
  if (!town || town === "all") {
    const inferred = inferTownFromText(address, title, details, url);
    if (inferred) town = inferred;
  }

  // Geo hint fallback (town centroid)
  if (!opts.skipGeo && !geo && town && town !== "all") {
    geo = GEO_HINTS[town.toLowerCase()] || null;
  }

  return {
    title: title || "Event",
    url,
    startYMD: startYMD || null,
    endYMD: endYMD || null,
    when: when || "Date and time on website.",
    details,
    price: price || "Price not provided.",
    contact: `For more information visit their website (${url}).`,
    address: address || "Venue address not provided.",
    town,
    tag: opts.tag || "any",
    geo,
  };
}

async function extractOrFallback(url, title, opts = {}) {
  try {
    const ev = await extractEventFromPage(url, opts);
    if (isGenericTitle(ev.title) && title) ev.title = title;
    if (isGenericTitle(ev.title)) return null;
    return ev;
  } catch {
    if (!title || isGenericTitle(title)) return null;

    let town = opts.town || "all";
    let geo = null;
    if (!opts.skipGeo && town && town !== "all") geo = GEO_HINTS[town.toLowerCase()] || null;

    return {
      title,
      url,
      startYMD: null,
      endYMD: null,
      when: "Date and time on website.",
      details: "Details on website.",
      price: "Price not provided.",
      contact: `For more information visit their website (${url}).`,
      address: "Venue address not provided.",
      town,
      tag: opts.tag || "any",
      geo,
    };
  }
}

// --------------------------------------------------
// Filter, sort, and balance helpers
// --------------------------------------------------
function effectiveSortDate(startYMD, endYMD, filterStartYMD) {
  // For ongoing events that started earlier than the filter window,
  // sort them as "today/window start" rather than by original start date.
  if (filterStartYMD && startYMD && startYMD < filterStartYMD && endYMD && endYMD >= filterStartYMD) {
    return filterStartYMD;
  }
  return startYMD || "9999-12-31";
}

function filterAndRank(rawEvents, f = {}) {
  const out = [];

  for (const e of rawEvents) {
    if (!e) continue;

    // Must have a verifiable start date to be included in time-bounded results
    if (!e.startYMD) continue;

    // Type filtering
    if (f.type && f.type !== "any" && e.tag && e.tag !== f.type) continue;

    // Town filtering (explicit town requested)
    if (f.town && f.town !== "all") {
      if (e.town && e.town !== f.town) continue;
    }

    // Date filtering with overlap logic (handles ongoing multi-day events)
    if (!overlapsRange(e.startYMD, e.endYMD, f.startISO, f.endISO)) continue;

    out.push(e);
  }

  out.sort((a, b) => {
    const ad = effectiveSortDate(a.startYMD, a.endYMD, f.startISO);
    const bd = effectiveSortDate(b.startYMD, b.endYMD, f.startISO);
    if (ad !== bd) return ad < bd ? -1 : 1;
    // Tie-breaker: shorter titles first (minor) to stabilize ordering
    const al = (a.title || "").length;
    const bl = (b.title || "").length;
    return al - bl;
  });

  return out.map(formatWeekender);
}

function balanceAcrossTowns(items, limit) {
  // items are already sorted in preferred order
  const TOWN_ORDER = ["napa", "st-helena", "yountville", "calistoga", "american-canyon", "all", "other"];

  const buckets = new Map();
  for (const t of TOWN_ORDER) buckets.set(t, []);

  for (const it of items) {
    const t = (it && it._town) ? String(it._town) : "other";
    const key = buckets.has(t) ? t : "other";
    buckets.get(key).push(it);
  }

  const idx = new Map();
  for (const t of TOWN_ORDER) idx.set(t, 0);

  const picked = [];
  while (picked.length < limit) {
    let addedThisRound = false;
    for (const t of TOWN_ORDER) {
      const b = buckets.get(t) || [];
      const i = idx.get(t) || 0;
      if (i < b.length) {
        picked.push(b[i]);
        idx.set(t, i + 1);
        addedThisRound = true;
        if (picked.length >= limit) break;
      }
    }
    if (!addedThisRound) break;
  }

  // If we still need more, append remaining in original order without duplicates
  if (picked.length < limit) {
    const seen = new Set(picked.map((x) => x.header + "||" + x.body));
    for (const it of items) {
      const k = it.header + "||" + it.body;
      if (seen.has(k)) continue;
      picked.push(it);
      seen.add(k);
      if (picked.length >= limit) break;
    }
  }

  return picked;
}

// --------------------------------------------------
// Parsers
// --------------------------------------------------
async function parseDoNapa(listUrl, f) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const urls = new Set();
  $("a[href*='/event/']").each((_, a) => {
    const href = $(a).attr("href") || "";
    if (!href) return;
    const full = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    if (full.includes("donapa.com/event/")) urls.add(full);
  });

  const events = [];
  for (const url of Array.from(urls).slice(0, 12)) {
    const ev = await extractOrFallback(url, null, { town: "napa", tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, f);
}

async function parseGrowthZone(listUrl, townSlug, f) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const links = [];
  $("a[href*='/events/details/']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const t = cleanText($(a).text());
    if (!href) return;
    const full = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    links.push({ url: full, title: t });
  });

  const uniq = [];
  const seen = new Set();
  for (const l of links) {
    if (!l.url || seen.has(l.url)) continue;
    seen.add(l.url);
    uniq.push(l);
    if (uniq.length >= 12) break;
  }

  const events = [];
  for (const x of uniq) {
    const ev = await extractOrFallback(x.url, x.title, { town: townSlug, tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, f);
}

async function parseNapaLibrary(listUrl, f) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const links = [];
  $("a[href*='/event']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const t = cleanText($(a).text());
    if (!href) return;
    const full = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    if (full.includes("napalibrary.org")) links.push({ url: full, title: t });
  });

  const uniq = [];
  const seen = new Set();
  for (const l of links) {
    if (!l.url || seen.has(l.url)) continue;
    seen.add(l.url);
    uniq.push(l);
    if (uniq.length >= 12) break;
  }

  const events = [];
  for (const x of uniq) {
    // town is not forced; extractEventFromPage will infer when possible
    const ev = await extractOrFallback(x.url, x.title, { town: "all", tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, f);
}

async function parseVisitNapaValley(listUrl, f) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const links = [];
  $("a[href*='/event/']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const t = cleanText($(a).text());
    if (!href) return;
    const full = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    if (full.includes("visitnapavalley.com")) links.push({ url: full, title: t });
  });

  const uniq = [];
  const seen = new Set();
  for (const l of links) {
    if (!l.url || seen.has(l.url)) continue;
    seen.add(l.url);
    uniq.push(l);
    if (uniq.length >= 12) break;
  }

  const events = [];
  for (const x of uniq) {
    const ev = await extractOrFallback(x.url, x.title, { town: "all", tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, f);
}

async function parseCameo(listUrl, f) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const titles = [];
  $("h2,h3").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && t.length > 2 && t.length < 80) titles.push(t);
  });

  const today = toISODate(new Date());
  const meta = {
    address: "1340 Main St., St. Helena.",
    phone: "707-963-9779",
    email: "info@cameocinema.com",
  };

  const events = [];
  for (const t of titles.slice(0, 10)) {
    if (isGenericTitle(t) || /cameo|movie times/i.test(t)) continue;

    events.push({
      title: t,
      url: listUrl,
      startYMD: today,
      endYMD: today,
      when: apDateFromYMD(today) || "Date and time on website.",
      details: "Now playing. Showtimes on website.",
      price: "Price not provided.",
      contact: `For more information call ${meta.phone}, email ${meta.email} or visit their website (${listUrl}).`,
      address: meta.address,
      town: "st-helena",
      tag: "movies",
      geo: GEO_HINTS["st-helena"],
    });
  }

  return filterAndRank(events, f);
}

// --------------------------------------------------
// Handler
// --------------------------------------------------
export default async function handler(req, res) {
  applyCors(req, res);

  // Preflight for native Squarespace embedding
  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    return res.end();
  }

  // Hard stop
  const hardTimeout = setTimeout(() => {
    try {
      if (!res.writableEnded) {
        sendJson(req, res, 504, { ok: false, timeout: true, results: [], map: [] });
      }
    } catch {}
  }, HARD_HANDLER_TIMEOUT_MS);

  try {
    const u = new URL(typeof req.url === "string" ? req.url : "/api/search", "http://localhost");

    const town = u.searchParams.get("town") || "all";
    const type = u.searchParams.get("type") || "any";
    const start = parseISODate(u.searchParams.get("start") || "");
    const end = parseISODate(u.searchParams.get("end") || "");
    const limit = Math.min(10, Math.max(1, parseInt(u.searchParams.get("limit") || "5", 10)));

    const filters = {
      town,
      type,
      startISO: start ? toISODate(start) : null,
      endISO: end ? toISODate(end) : null,
    };

    const tasks = SOURCES.map(async (s) => {
      try {
        // Movies-only routing
        if (type === "movies" && s.type !== "movies") return [];
        if (type !== "movies" && s.type === "movies") return [];

        if (s.id === "donapa") return await parseDoNapa(s.listUrl, filters);
        if (s.id === "napa_library") return await parseNapaLibrary(s.listUrl, filters);
        if (s.id === "visit_napa_valley") return await parseVisitNapaValley(s.listUrl, filters);
        if (s.id === "amcan_chamber") return await parseGrowthZone(s.listUrl, "american-canyon", filters);
        if (s.id === "calistoga_chamber") return await parseGrowthZone(s.listUrl, "calistoga", filters);
        if (s.id === "yountville_chamber") return await parseGrowthZone(s.listUrl, "yountville", filters);
        if (s.id === "cameo") return await parseCameo(s.listUrl, filters);
        return [];
      } catch {
        return [];
      }
    });

    let resultsArrays = [];
    let timedOut = false;

    try {
      resultsArrays = await withTimeout(Promise.all(tasks), AGG_TIMEOUT_MS);
    } catch {
      timedOut = true;
      const settled = await Promise.allSettled(tasks);
      resultsArrays = settled
        .filter((r) => r.status === "fulfilled")
        .map((r) => r.value || []);
    }

    // Flatten + dedupe
    let all = [];
    for (const r of resultsArrays) all = all.concat(r);

    const seen = new Set();
    const dedup = [];
    for (const x of all) {
      const k = (x.header || "") + "||" + (x.body || "");
      if (seen.has(k)) continue;
      seen.add(k);
      dedup.push(x);
    }

    // If thin, add a single “venues to check” footer (keeps behavior you added earlier)
    let supplemented = false;
    if (dedup.length < 3) {
      supplemented = true;
      dedup.push({
        header: "Performance & Art Venues (for other nights)",
        body:
          "If you’d like more art or performance options on future dates, visit Napa Valley Performing Arts Center (Yountville), Lucky Penny Productions (Napa), Lincoln Theater (Yountville), Uptown Theatre (Napa) or Cameo Cinema (St. Helena).",
        mapHint: [
          { name: "Uptown Theatre Napa", lat: 38.2991, lon: -122.2858 },
          { name: "Lincoln Theater", lat: 38.3926, lon: -122.3631 },
          { name: "Lucky Penny Productions", lat: 38.2979, lon: -122.2864 },
          { name: "Napa Valley Performing Arts Center", lat: 38.3925, lon: -122.363 },
          { name: "Cameo Cinema", lat: 38.5056, lon: -122.4703 },
        ],
        _town: "all",
        _startYMD: null,
        _endYMD: null,
        _type: "any",
        geo: null,
      });
    }

    // Town balancing (only when user did not specify a town)
    const selected =
      town === "all" ? balanceAcrossTowns(dedup, limit) : dedup.slice(0, limit);

    // Unified map output for displayed results
    let mapData = selected
      .flatMap((x) => {
        const pts = [];
        if (x.geo && typeof x.geo.lat === "number" && typeof x.geo.lon === "number") {
          pts.push({ name: x.header, lat: x.geo.lat, lon: x.geo.lon });
        }
        if (Array.isArray(x.mapHint)) pts.push(...x.mapHint);
        return pts;
      })
      .filter((p) => p && typeof p.lat === "number" && typeof p.lon === "number");

    // Map fallback to avoid “lost map” UX when no geo hints are available
    if (!mapData.length) {
      mapData = [{ name: "Napa County", lat: GEO_HINTS["__fallback__"].lat, lon: GEO_HINTS["__fallback__"].lon }];
    }

    clearTimeout(hardTimeout);

    // IMPORTANT: ok:true on 200 even if 0 matches (widget should show “No matches”)
    sendJson(req, res, 200, {
      ok: true,
      timeout: timedOut,
      supplemented,
      count: selected.length,
      results: selected.map((x) => ({
        header: x.header,
        body: x.body,
        geo: x.geo ? { lat: x.geo.lat, lon: x.geo.lon } : null,
        mapHint: x.mapHint || null,
      })),
      map: mapData, // always array
    });
  } catch (e) {
    clearTimeout(hardTimeout);
    sendJson(req, res, 500, { ok: false, error: e?.message || String(e), results: [], map: [] });
  }
}

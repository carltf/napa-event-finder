import * as cheerioNS from "cheerio";

/**
 * Napa Valley Event Finder — API (Vercel)
 * Stabilized + corrected:
 * - Stronger date/time extraction across sources
 * - Multi-day event formatting (Jan. 17–Feb. 15)
 * - Balanced source interleaving to reduce “one-town domination”
 * - Town inference via JSON-LD addressLocality
 * - Map pins always emitted from best-available geo
 * - Keeps: CORS allowlist, caching, timeouts, partial-result recovery
 */

// --- Robust cheerio loader ---
const load = cheerioNS.load || (cheerioNS.default && cheerioNS.default.load);
if (!load) throw new Error("Cheerio 'load' not found. Check cheerio package version.");

// --------------------------------------------------
// CORS (ONLY needed for native Squarespace embedding)
// --------------------------------------------------
const ALLOWED_ORIGINS = new Set([
  "https://napavalleyfeatures.squarespace.com",
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
// Timeouts
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
function toISODateUTC(d) {
  // Always produce YYYY-MM-DD in UTC to keep comparisons stable.
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString().slice(0, 10);
}

function parseISODate(s) {
  const m1 = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s || "");
  if (m1) return new Date(Date.UTC(+m1[1], +m1[2] - 1, +m1[3]));
  const m2 = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s || "");
  if (m2) return new Date(Date.UTC(+m2[3], +m2[1] - 1, +m2[2]));
  return null;
}

function withinRange(dateISO, start, end) {
  if (!dateISO) return true;
  if (start && dateISO < start) return false;
  if (end && dateISO > end) return false;
  return true;
}

function titleCase(s) {
  if (!s) return s;
  const small = new Set(["a","an","and","at","but","by","for","in","of","on","or","the","to","with"]);
  return s.trim().split(/\s+/).map((w,i)=> {
    const c=w.toLowerCase();
    return i && small.has(c) ? c : c[0].toUpperCase()+c.slice(1);
  }).join(" ");
}

function isGenericTitle(t) {
  const x = cleanText(t).toLowerCase();
  return ["read more","event details","learn more","details","view event"].includes(x);
}

function truncate(s, max = 260) {
  const x = cleanText(s);
  return x.length <= max ? x : x.slice(0, max - 1).trimEnd() + "…";
}

function inferPriceFromText(t) {
  t = cleanText(t).toLowerCase();
  if (!t) return null;
  return ["free", "no cover", "complimentary"].some(x => t.includes(x)) ? "Free." : null;
}

// --------------------------------------------------
// AP-style date/time formatting
// --------------------------------------------------
const AP_DOW = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
const AP_MON = ["Jan.","Feb.","March","April","May","June","July","Aug.","Sept.","Oct.","Nov.","Dec."];

function apDateFromYMD(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd || "");
  if (!m) return null;
  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (isNaN(dt)) return null;
  return `${AP_DOW[dt.getUTCDay()]}, ${AP_MON[dt.getUTCMonth()]} ${dt.getUTCDate()}`;
}

function apDateRangeFromYMD(startYMD, endYMD) {
  if (!startYMD || !endYMD) return null;
  if (startYMD === endYMD) return apDateFromYMD(startYMD);

  const sm = /^(\d{4})-(\d{2})-(\d{2})$/.exec(startYMD);
  const em = /^(\d{4})-(\d{2})-(\d{2})$/.exec(endYMD);
  if (!sm || !em) return null;

  const sDt = new Date(Date.UTC(+sm[1], +sm[2] - 1, +sm[3]));
  const eDt = new Date(Date.UTC(+em[1], +em[2] - 1, +em[3]));
  if (isNaN(sDt) || isNaN(eDt)) return null;

  const sMon = AP_MON[sDt.getUTCMonth()];
  const eMon = AP_MON[eDt.getUTCMonth()];
  const sDay = sDt.getUTCDate();
  const eDay = eDt.getUTCDate();

  // If month is same: "Jan. 17–31"
  if (sMon === eMon && sDt.getUTCFullYear() === eDt.getUTCFullYear()) {
    return `${sMon} ${sDay}–${eDay}`;
  }

  // Different month: "Jan. 17–Feb. 15"
  return `${sMon} ${sDay}–${eMon} ${eDay}`;
}

function apTimeFromISOClock(iso) {
  if (!iso) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;

  // Suppress midnight placeholders like T00:00 or T00:00:00
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

      const hour = (parts.find(p => p.type === "hour")?.value || "").trim();
      const minute = (parts.find(p => p.type === "minute")?.value || "").trim();
      const dayPeriod = (parts.find(p => p.type === "dayPeriod")?.value || "").toLowerCase();
      if (!hour || !minute || !dayPeriod) return null;

      const apPeriod = dayPeriod === "pm" ? "p.m." : "a.m.";
      return minute === "00" ? `${hour} ${apPeriod}` : `${hour}:${minute} ${apPeriod}`;
    }
  }

  const m = /T(\d{2}):(\d{2})/.exec(iso);
  if (!m) return null;

  const hour24 = +m[1];
  const minute = +m[2];

  let h = hour24 % 12 || 12;
  const ampm = hour24 >= 12 ? "p.m." : "a.m.";
  return minute ? `${h}:${String(minute).padStart(2, "0")} ${ampm}` : `${h} ${ampm}`;
}

function formatTimeRange(startISO, endISO) {
  const t1 = apTimeFromISOClock(startISO);
  const t2 = apTimeFromISOClock(endISO);
  if (!t1 && !t2) return null;
  if (t1 && !t2) return t1;
  if (!t1 && t2) return t2;
  if (t1 === t2) return t1;
  return `${t1}–${t2}`;
}

// --------------------------------------------------
// Geo hints
// --------------------------------------------------
const GEO_HINTS = {
  napa: { lat: 38.2975, lon: -122.2869 },
  "st-helena": { lat: 38.5056, lon: -122.4703 },
  yountville: { lat: 38.3926, lon: -122.3631 },
  calistoga: { lat: 38.578, lon: -122.5797 },
  "american-canyon": { lat: 38.1686, lon: -122.2608 },
};

function normalizeTownSlug(x) {
  const s = cleanText(x || "").toLowerCase();
  if (!s) return null;
  if (s.includes("st. helena") || s.includes("saint helena") || s === "st helena") return "st-helena";
  if (s.includes("yountville")) return "yountville";
  if (s.includes("calistoga")) return "calistoga";
  if (s.includes("american canyon")) return "american-canyon";
  if (s.includes("napa")) return "napa";
  return null;
}

// --------------------------------------------------
// Fetch helper (cache + abort)
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

function extractLocality(addr) {
  if (!addr || typeof addr === "string") return null;
  const loc = cleanText(addr.addressLocality);
  return loc || null;
}

// --------------------------------------------------
// Stronger date/time extraction for non-JSON-LD pages
// --------------------------------------------------
function pickFirstNonEmpty(...vals) {
  for (const v of vals) {
    const x = cleanText(v);
    if (x) return x;
  }
  return null;
}

function extractISOFromMeta($) {
  const candidates = [
    "meta[itemprop='startDate']",
    "meta[property='event:start_time']",
    "meta[property='og:start_time']",
    "meta[name='startDate']",
    "meta[name='event:startDate']",
  ];
  for (const sel of candidates) {
    const c = $(sel).attr("content");
    if (c && /\d{4}-\d{2}-\d{2}/.test(c)) return c.trim();
  }
  return null;
}

function extractISOFromTimeTags($) {
  // Many sites use <time datetime="2026-01-23T17:00:00-08:00">
  const dt = $("time[datetime]").first().attr("datetime");
  if (dt && /\d{4}-\d{2}-\d{2}/.test(dt)) return dt.trim();
  return null;
}

function extractDateRangeFromText(html) {
  // Conservative: detect a clear YYYY-MM-DD range first.
  const ymdRange = html.match(/\b(\d{4}-\d{2}-\d{2})\b[\s\S]{0,40}\b(\d{4}-\d{2}-\d{2})\b/);
  if (ymdRange) return { start: ymdRange[1], end: ymdRange[2] };

  // Detect “Jan. 17–Feb. 15, 2026” style is hard without locale; avoid over-guessing.
  return null;
}

function ymdFromISOish(iso) {
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(iso || "");
  return m ? m[1] : null;
}

function computeWhen(startISO, endISO) {
  const sYMD = ymdFromISOish(startISO);
  const eYMD = ymdFromISOish(endISO);

  // Multi-day: prefer date range without weekday to avoid “Sat.” confusion.
  if (sYMD && eYMD && sYMD !== eYMD) {
    const dr = apDateRangeFromYMD(sYMD, eYMD);
    return dr || "Date and time on website.";
  }

  // Single-day: weekday + optional time
  if (sYMD) {
    const ap = apDateFromYMD(sYMD);
    const t = endISO ? formatTimeRange(startISO, endISO) : apTimeFromISOClock(startISO);
    if (ap) return t ? `${ap}, ${t}` : ap;
  }

  return "Date and time on website.";
}

// --------------------------------------------------
// Weekender formatting (API-side)
// --------------------------------------------------
function formatWeekender(e) {
  const header = titleCase(e.title || "Event");
  const dateLine = e.when || "Date and time on website.";
  const details = e.details || "Details on website.";
  const price = e.price || "Price not provided.";
  const contact = e.contact || (e.url
    ? `For more information visit their website (${e.url}).`
    : "For more information visit their website.");

  let address = e.address || "Venue address not provided.";
  if (address === "Venue address not provided." && e.town && e.town !== "all") {
    address = `${titleCase(e.town.replace("-", " "))}, CA`;
  }

  // Geo: prefer explicit event geo, else town hint.
  const geo =
    (e.geo && typeof e.geo.lat === "number" && typeof e.geo.lon === "number" && e.geo) ||
    (GEO_HINTS[(e.town || "").toLowerCase()] || null);

  return {
    header,
    body: `${dateLine} ${details} ${price} ${contact} ${address}`.replace(/\s+/g, " ").trim(),
    geo,
    dateISO: e.dateISO || null,
    sourceId: e.sourceId || null,
    url: e.url || null,
  };
}

// --------------------------------------------------
// Per-event page extraction (improved)
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
  let town = opts.town || "all";
  let geo = null;

  // JSON-LD event
  const ld = getJsonLdEvents($);
  const ev = ld[0];
  if (ev) {
    title = ev.name || null;
    startISO = ev.startDate || null;
    endISO = ev.endDate || null;
    description = ev.description || null;

    const loc = Array.isArray(ev.location) ? ev.location[0] : ev.location;
    if (loc && loc.address) {
      address = extractStreetAddress(loc.address);
      const locTown = extractLocality(loc.address);
      const slug = normalizeTownSlug(locTown);
      if (slug) town = slug;
    }

    const offers = Array.isArray(ev.offers) ? ev.offers[0] : ev.offers;
    if (offers && offers.price !== undefined && offers.price !== null) {
      const p = String(offers.price).trim();
      price = p === "0" || p === "0.00" ? "Free." : `Tickets ${p}.`;
    }
  }

  // Non-JSON-LD fallbacks
  if (!startISO) startISO = extractISOFromMeta($) || extractISOFromTimeTags($);

  // Sometimes pages only contain a date range text block
  if (!startISO) {
    const dr = extractDateRangeFromText(html);
    if (dr?.start) startISO = dr.start;
    if (dr?.end) endISO = dr.end;
  }

  // Title fallback
  if (!title) title = cleanText($("h1").first().text()) || cleanText($("title").text()) || "Event";

  // Description fallback
  if (!description) {
    description =
      pickFirstNonEmpty(
        $("meta[name='description']").attr("content"),
        $("meta[property='og:description']").attr("content"),
        $(".event-description, .description, .tribe-events-single-event-description").first().text()
      ) || null;
  }

  // Price heuristic
  if (!price) {
    const priceMatch = html.match(/\$\s?\d+(?:\.\d{2})?|\bfree\b/i);
    if (priceMatch) {
      price = /free/i.test(priceMatch[0]) ? "Free." : `Tickets ${priceMatch[0].replace(/\s+/g, "")}.`;
    } else {
      const inf = inferPriceFromText(description || "");
      if (inf) price = inf;
    }
  }

  // DateISO for filtering/sorting
  const dateISO = ymdFromISOish(startISO) || null;

  // When line (multi-day aware)
  const when = computeWhen(startISO, endISO);

  // Details line
  let details = description ? truncate(description) : "Details on website.";
  if (details && !details.endsWith(".")) details += ".";

  // Geo hint fallback
  if (!geo && town && town !== "all") geo = GEO_HINTS[town] || null;

  return {
    title,
    url,
    dateISO,
    when,
    details,
    price: price || "Price not provided.",
    contact: `For more information visit their website (${url}).`,
    address: address || "Venue address not provided.",
    town,
    tag: opts.tag || "any",
    sourceId: opts.sourceId || null,
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

    const town = opts.town || "all";
    const geo = town !== "all" ? (GEO_HINTS[town] || null) : null;

    return {
      title,
      url,
      dateISO: null,
      when: "Date and time on website.",
      details: "Details on website.",
      price: "Price not provided.",
      contact: `For more information visit their website (${url}).`,
      address: "Venue address not provided.",
      town,
      tag: opts.tag || "any",
      sourceId: opts.sourceId || null,
      geo,
    };
  }
}

// --------------------------------------------------
// Filtering + ranking (soonest first)
// --------------------------------------------------
function filterEvents(events, f = {}) {
  const out = [];
  for (const e of events) {
    if (!e) continue;

    // Town/type filter
    if (f.town && f.town !== "all" && e.town && e.town !== f.town) continue;
    if (f.type && f.type !== "any" && e.tag && e.tag !== f.type) continue;

    // Date filter (prefer events with dates; allow undated to survive if range not strict)
    if (e.dateISO) {
      if (!withinRange(e.dateISO, f.startISO, f.endISO)) continue;
    } else {
      // If the user explicitly supplied a date range, keep undated only as a last resort.
      if (f.startISO || f.endISO) continue;
    }

    out.push(e);
  }

  out.sort((a, b) => {
    const ad = a.dateISO || "9999-12-31";
    const bd = b.dateISO || "9999-12-31";
    if (ad !== bd) return ad < bd ? -1 : 1; // soonest first
    const at = (a.title || "").toLowerCase();
    const bt = (b.title || "").toLowerCase();
    return at < bt ? -1 : at > bt ? 1 : 0;
  });

  return out;
}

function dedupeEvents(events) {
  const seen = new Set();
  const out = [];
  for (const e of events) {
    const k = (e.url || "") || ((e.title || "") + "||" + (e.dateISO || ""));
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(e);
  }
  return out;
}

// Interleave across sources to prevent one source (and thus one town) dominating.
function interleaveBySource(grouped, limit) {
  const queues = grouped.map(g => g.slice());
  const out = [];
  while (out.length < limit) {
    let pushed = false;
    for (const q of queues) {
      if (!q.length) continue;
      out.push(q.shift());
      pushed = true;
      if (out.length >= limit) break;
    }
    if (!pushed) break;
  }
  return out;
}

// --------------------------------------------------
// Parsers
// --------------------------------------------------
async function parseDoNapa(listUrl, filters) {
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
  // Pull more candidates, let filters/interop do the rest
  for (const url of Array.from(urls).slice(0, 20)) {
    const ev = await extractOrFallback(url, null, { town: "all", tag: "any", sourceId: "donapa" });
    if (ev) events.push(ev);
  }

  return filterEvents(events, filters);
}

async function parseGrowthZone(listUrl, townSlug, sourceId, filters) {
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
    if (uniq.length >= 15) break;
  }

  const events = [];
  for (const x of uniq) {
    const ev = await extractOrFallback(x.url, x.title, { town: townSlug, tag: "any", sourceId });
    if (ev) events.push(ev);
  }

  return filterEvents(events, filters);
}

async function parseNapaLibrary(listUrl, filters) {
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
    if (uniq.length >= 15) break;
  }

  const events = [];
  for (const x of uniq) {
    const ev = await extractOrFallback(x.url, x.title, { town: "all", tag: "any", sourceId: "napa_library" });
    if (ev) events.push(ev);
  }

  return filterEvents(events, filters);
}

async function parseVisitNapaValley(listUrl, filters) {
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
    if (uniq.length >= 15) break;
  }

  const events = [];
  for (const x of uniq) {
    const ev = await extractOrFallback(x.url, x.title, { town: "all", tag: "any", sourceId: "visit_napa_valley" });
    if (ev) events.push(ev);
  }

  return filterEvents(events, filters);
}

async function parseCameo(listUrl, filters) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const titles = [];
  $("h2,h3").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && t.length > 2 && t.length < 80) titles.push(t);
  });

  // Cameo is “movies” and tends to not expose structured showtime dates reliably.
  // Treat as “today” listing if movies requested, otherwise skip in non-movie queries.
  const today = toISODateUTC(new Date());

  const meta = {
    address: "1340 Main St., St. Helena.",
    phone: "707-963-9779",
    email: "info@cameocinema.com",
  };

  const events = [];
  for (const t of titles.slice(0, 10)) {
    if (isGenericTitle(t) || /cameo|movie times|now playing/i.test(t)) continue;

    events.push({
      title: t,
      url: listUrl,
      dateISO: today,
      when: apDateFromYMD(today) || "Date and time on website.",
      details: "Now playing. Showtimes on website.",
      price: "Price not provided.",
      contact: `For more information call ${meta.phone}, email ${meta.email} or visit their website (${listUrl}).`,
      address: meta.address,
      town: "st-helena",
      tag: "movies",
      sourceId: "cameo",
      geo: GEO_HINTS["st-helena"],
    });
  }

  return filterEvents(events, filters);
}

// --------------------------------------------------
// Handler
// --------------------------------------------------
export default async function handler(req, res) {
  applyCors(req, res);

  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    return res.end();
  }

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
      startISO: start ? toISODateUTC(start) : null,
      endISO: end ? toISODateUTC(end) : null,
    };

    const tasks = SOURCES.map(async (s) => {
      try {
        if (type === "movies" && s.type !== "movies") return [];
        if (type !== "movies" && s.type === "movies") return [];

        if (s.id === "donapa") return await parseDoNapa(s.listUrl, filters);
        if (s.id === "napa_library") return await parseNapaLibrary(s.listUrl, filters);
        if (s.id === "visit_napa_valley") return await parseVisitNapaValley(s.listUrl, filters);

        if (s.id === "amcan_chamber") return await parseGrowthZone(s.listUrl, "american-canyon", "amcan_chamber", filters);
        if (s.id === "calistoga_chamber") return await parseGrowthZone(s.listUrl, "calistoga", "calistoga_chamber", filters);
        if (s.id === "yountville_chamber") return await parseGrowthZone(s.listUrl, "yountville", "yountville_chamber", filters);

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

    // Group by source, then interleave for variety.
    const bySource = new Map();
    for (const arr of resultsArrays) {
      for (const e of arr) {
        const sid = e?.sourceId || "unknown";
        if (!bySource.has(sid)) bySource.set(sid, []);
        bySource.get(sid).push(e);
      }
    }

    // Within each source, keep soonest first (filterEvents already sorted).
    const grouped = Array.from(bySource.values()).map(dedupeEvents);

    // Interleave, then dedupe again.
    const interleaved = dedupeEvents(interleaveBySource(grouped, Math.max(limit * 3, 15)));

    // If still thin, add a single non-event-specific helper card (but don’t let it dominate map)
    let supplemented = false;
    let finalEvents = interleaved;

    if (finalEvents.length < 3) {
      supplemented = true;
      finalEvents = finalEvents.concat([{
        title: "More Venues To Check",
        url: null,
        dateISO: null,
        when: "Dates and times vary.",
        details:
          "If you need additional options, check venue calendars for Lincoln Theater (Yountville), Uptown Theatre (Napa), Lucky Penny Productions (Napa) and Cameo Cinema (St. Helena).",
        price: "Price not provided.",
        contact: "For more information visit venue websites.",
        address: "Napa County.",
        town: "all",
        tag: "any",
        sourceId: "supplement",
        geo: null,
      }]);
    }

    // Convert to Weekender cards
    const cards = finalEvents.map(formatWeekender);

    // Map pins: include only real geo points (event or inferred town centers)
    const mapData = cards
      .map((c) => {
        if (c.geo && typeof c.geo.lat === "number" && typeof c.geo.lon === "number") {
          return { name: c.header, lat: c.geo.lat, lon: c.geo.lon };
        }
        return null;
      })
      .filter(Boolean);

    clearTimeout(hardTimeout);

    sendJson(req, res, 200, {
      ok: true,
      timeout: timedOut,
      supplemented,
      count: cards.slice(0, limit).length,
      results: cards.slice(0, limit).map((x) => ({
        header: x.header,
        body: x.body,
        geo: x.geo ? { lat: x.geo.lat, lon: x.geo.lon } : null,
      })),
      map: mapData, // Leaflet widget consumes this
    });
  } catch (e) {
    clearTimeout(hardTimeout);
    sendJson(req, res, 500, { ok: false, error: e?.message || String(e), results: [], map: [] });
  }
}

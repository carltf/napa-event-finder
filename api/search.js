import * as cheerioNS from "cheerio";

/**
 * Napa Valley Event Finder — API (Vercel)
 * Map-forward + category-aware + safer date handling
 * --------------------------------------------------
 * Fixes / Improvements:
 * - Removes duplicate / broken Weekender formatter code
 * - Defines missing time helpers (formatTimeRange, apTimeFromISOClock)
 * - Corrects brace structure in extractEventFromPage (date/time not nested under price block)
 * - Adds `when` to extracted event objects so Weekender output is consistent
 * - Expands CORS allowlist to production domains
 * - Adds small concurrency control to per-event page fetches to reduce aggregate timeouts
 */

const load = cheerioNS.load || (cheerioNS.default && cheerioNS.default.load);
if (!load) throw new Error("Cheerio 'load' not found. Check cheerio package version.");

// -------------------- CORS --------------------
const ALLOWED_ORIGINS = new Set([
  "https://napavalleyfeatures.squarespace.com",
  "https://napavalleyfeatures.com",
  "https://www.napavalleyfeatures.com",
  "http://localhost:3000",
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

// -------------------- Cache --------------------
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

// -------------------- Timeouts --------------------
const FETCH_TIMEOUT_MS = 12000;
const AGG_TIMEOUT_MS = 22000;
const HARD_HANDLER_TIMEOUT_MS = 24000;

async function withTimeout(promise, ms = AGG_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout exceeded")), ms)),
  ]);
}

// -------------------- Sources --------------------
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

// -------------------- Utilities --------------------
function decodeEntities(str) {
  let s = String(str || "");
  for (let i = 0; i < 3; i++) {
    const before = s;

    const named = {
      "&amp;": "&",
      "&quot;": '"',
      "&apos;": "'",
      "&lt;": "<",
      "&gt;": ">",
      "&nbsp;": " ",
    };
    s = s.replace(/&(amp|quot|apos|lt|gt|nbsp);/g, (m) => named[m] || m);

    s = s
      .replace(/&#(\d+);/g, (_, n) => {
        try {
          return String.fromCodePoint(+n);
        } catch {
          return _;
        }
      })
      .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => {
        try {
          return String.fromCodePoint(parseInt(h, 16));
        } catch {
          return _;
        }
      });

    if (s === before) break;
  }
  return s;
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

function apDateFromYMD(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd || "");
  if (!m) return null;

  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (isNaN(dt)) return null;

  const dts = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const mos = ["Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."];
  return `${dts[dt.getUTCDay()]}, ${mos[dt.getUTCMonth()]} ${dt.getUTCDate()}`;
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

function ymdFromISO(iso) {
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(iso || "");
  return m ? m[1] : null;
}

// Time helpers (previously missing)
function apTimeFromISOClock(iso) {
  const m = /T(\d{2}):(\d{2})/.exec(iso || "");
  if (!m) return null;

  let h = parseInt(m[1], 10);
  const mm = m[2];
  const ampm = h >= 12 ? "p.m." : "a.m.";
  h = h % 12;
  if (h === 0) h = 12;

  return mm === "00" ? `${h} ${ampm}` : `${h}:${mm} ${ampm}`;
}

function formatTimeRange(startISO, endISO) {
  const a = apTimeFromISOClock(startISO);
  const b = apTimeFromISOClock(endISO);
  if (a && b) return a === b ? a : `${a}–${b}`;
  return a || b || null;
}

// -------------------- Geo hints --------------------
const GEO_HINTS = {
  napa: { lat: 38.2975, lon: -122.2869 },
  "st-helena": { lat: 38.5056, lon: -122.4703 },
  yountville: { lat: 38.3926, lon: -122.3631 },
  calistoga: { lat: 38.578, lon: -122.5797 },
  "american-canyon": { lat: 38.1686, lon: -122.2608 },
};

// -------------------- Category classification --------------------
const TYPE_ALLOW = new Set(["any", "art", "music", "food", "wellness", "nightlife", "movies"]);

function classifyTag(title, desc) {
  const t = `${cleanText(title)} ${cleanText(desc)}`.toLowerCase();

  if (/(cameo cinema|screening|film|movie|showtimes)/.test(t)) return "movies";

  const art = /(art|gallery|exhibit|exhibition|opening reception|artist talk|museum|installation|projection)/;
  const music = /(live music|concert|band|dj|jazz|blues|folk|hip-hop|show|gig)/;
  const food = /(tasting|dinner|prix fixe|restaurant month|brunch|winemaker dinner|food|chef|pairing)/;
  const wellness = /(yoga|wellness|meditation|sound bath|fitness|breathwork|health|run\b|walk\b)/;
  const nightlife = /(party|dance|late night|happy hour|club|karaoke|trivia|locals night)/;

  if (art.test(t)) return "art";
  if (music.test(t)) return "music";
  if (food.test(t)) return "food";
  if (wellness.test(t)) return "wellness";
  if (nightlife.test(t)) return "nightlife";

  return "any";
}

// -------------------- Date range overlap --------------------
function overlapsRange(evStart, evEnd, qStart, qEnd) {
  if (!qStart && !qEnd) return true;

  // If event lacks dates, only include when query has no strict range
  if (!evStart && !evEnd) return false;

  const s = evStart || evEnd;
  const e = evEnd || evStart;

  if (qStart && e && e < qStart) return false;
  if (qEnd && s && s > qEnd) return false;
  return true;
}

// -------------------- Weekender formatting --------------------
function formatWeekender(e) {
  const header = cleanText(titleCase(e.title || "Event"));
  const dateLine = cleanText(e.when || "Date and time on website.");
  const details = cleanText(e.details || "Details on website.");
  const price = cleanText(e.price || "Price not provided.");

  const contact = cleanText(
    e.contact || (e.url ? `For more information visit their website (${e.url}).` : "For more information visit their website.")
  );

  let address = cleanText(e.address || "Venue address not provided.");
  if (address === "Venue address not provided." && e.town && e.town !== "all") {
    address = `${titleCase(e.town.replace("-", " "))}, CA`;
  }

  const geo = e.geo || GEO_HINTS[(e.town || "").toLowerCase()] || null;

  return {
    header,
    body: cleanText(`${dateLine} ${details} ${price} ${contact} ${address}`),
    geo,
  };
}

// -------------------- Fetch helper --------------------
async function fetchText(url) {
  const key = "GET:" + url;
  const cached = getCached(key);
  if (cached) return cached;

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), Math.min(8000, FETCH_TIMEOUT_MS));

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "NapaValleyFeaturesEventFinder/1.3 (+https://napavalleyfeatures.com)",
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

// -------------------- JSON-LD extraction --------------------
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

// -------------------- Per-event page extraction --------------------
async function extractEventFromPage(url, opts = {}) {
  const html = await fetchText(url);
  const $ = load(html);

  let title = null;
  let startISO = null;
  let endISO = null;
  let address = null;
  let description = null;
  let price = null;

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
  if (!title) title = cleanText($("h1").first().text()) || cleanText($("title").text()) || "Event";

  // Description fallback
  if (!description) {
    const meta = $("meta[name='description']").attr("content");
    description = meta ? cleanText(meta) : null;
  }

  // Price heuristic (safe)
  if (!price) {
    const inf = inferPriceFromText(description || "");
    if (inf) {
      price = inf;
    } else {
      const priceMatch = html.match(/\$\s?\d+(?:\.\d{2})?|\bfree\b/i);
      if (priceMatch) {
        price = /free/i.test(priceMatch[0]) ? "Free." : `Tickets ${priceMatch[0].replace(/\s+/g, "")}.`;
      }
    }
  }

  // Date + time formatting (safe)
  let when = null;
  const startYMD2 = ymdFromISO(startISO);
  const endYMD2 = ymdFromISO(endISO);

  if (startYMD2) {
    if (endYMD2 && endYMD2 !== startYMD2) {
      const apStart = apDateFromYMD(startYMD2);
      const apEnd = apDateFromYMD(endYMD2);
      when = apStart && apEnd ? `Runs ${apStart}–${apEnd}` : "Dates and times on website.";
    } else {
      const ap = apDateFromYMD(startYMD2);
      const t = endISO ? formatTimeRange(startISO, endISO) : apTimeFromISOClock(startISO);
      when = ap ? (t ? `${ap}, ${t}` : ap) : null;
    }
  }

  const startYMD = startYMD2;
  const endYMD = endYMD2 || startYMD2;

  let details = description ? truncate(description) : "Details on website.";
  if (details && !details.endsWith(".")) details += ".";

  const tag = classifyTag(title, description || details);
  const geo = !opts.skipGeo && opts.town && GEO_HINTS[opts.town.toLowerCase()] ? GEO_HINTS[opts.town.toLowerCase()] : null;

  return {
    title,
    url,
    when,
    startYMD,
    endYMD,
    details,
    price: price || "Price not provided.",
    address: address || "Venue address not provided.",
    town: opts.town || "all",
    tag,
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

    const tag = classifyTag(title, "");
    const geo = !opts.skipGeo && opts.town && GEO_HINTS[opts.town.toLowerCase()] ? GEO_HINTS[opts.town.toLowerCase()] : null;

    return {
      title,
      url,
      when: null,
      startYMD: null,
      endYMD: null,
      details: "Details on website.",
      price: "Price not provided.",
      address: "Venue address not provided.",
      town: opts.town || "all",
      tag,
      geo,
    };
  }
}

// -------------------- Filter + rank --------------------
function filterAndRank(rawEvents, f = {}) {
  const out = [];

  for (const e of rawEvents) {
    if (!e) continue;

    if (f.town && f.town !== "all" && e.town && e.town !== f.town) continue;

    if (f.type && f.type !== "any") {
      if ((e.tag || "any") !== f.type) continue;
    }

    if (!overlapsRange(e.startYMD, e.endYMD, f.startISO, f.endISO)) continue;

    out.push(e);
  }

  out.sort((a, b) => {
    const aa = a.startYMD || "9999-12-31";
    const bb = b.startYMD || "9999-12-31";
    return aa < bb ? -1 : aa > bb ? 1 : 0;
  });

  return out.map(formatWeekender);
}

// -------------------- Small concurrency limiter --------------------
async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let i = 0;

  async function worker() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      try {
        results[idx] = await fn(items[idx], idx);
      } catch {
        results[idx] = null;
      }
    }
  }

  const workers = Array.from({ length: Math.max(1, limit) }, () => worker());
  await Promise.all(workers);
  return results;
}

/* -------------------- Parsers -------------------- */
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

  const urlList = Array.from(urls).slice(0, 12);

  const extracted = await mapLimit(urlList, 4, async (url) => {
    return await extractOrFallback(url, null, { town: "napa" });
  });

  const events = extracted.filter(Boolean);
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

  const extracted = await mapLimit(uniq, 4, async (x) => {
    return await extractOrFallback(x.url, x.title, { town: townSlug });
  });

  const events = extracted.filter(Boolean);
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

  const extracted = await mapLimit(uniq, 4, async (x) => {
    return await extractOrFallback(x.url, x.title, { town: "all" });
  });

  const events = extracted.filter(Boolean);
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

  const extracted = await mapLimit(uniq, 4, async (x) => {
    return await extractOrFallback(x.url, x.title, { town: "all" });
  });

  const events = extracted.filter(Boolean);
  return filterAndRank(events, f);
}

async function parseCameo(listUrl, f) {
  const html = await fetchText(listUrl);
  const $ = load(html);

  const titles = [];
  $("h1,h2,h3").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && t.length > 2 && t.length < 90) titles.push(t);
  });

  const events = [];
  for (const t of titles.slice(0, 12)) {
    if (isGenericTitle(t) || /cameo|movie times|showtimes/i.test(t)) continue;

    events.push({
      title: t,
      url: listUrl,
      when: "Showtimes on website.",
      startYMD: null,
      endYMD: null,
      details: "Now playing. Showtimes on website.",
      price: "Price not provided.",
      address: "1340 Main St., St. Helena.",
      town: "st-helena",
      tag: "movies",
      geo: GEO_HINTS["st-helena"],
    });
  }

  return filterAndRank(events, f);
}

// -------------------- Handler --------------------
export default async function handler(req, res) {
  applyCors(req, res);

  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    return res.end();
  }

  const hardTimeout = setTimeout(() => {
    try {
      if (!res.writableEnded) sendJson(req, res, 504, { ok: false, timeout: true, results: [], map: [] });
    } catch {}
  }, HARD_HANDLER_TIMEOUT_MS);

  try {
    const u = new URL(typeof req.url === "string" ? req.url : "/api/search", "http://localhost");

    const town = (u.searchParams.get("town") || "all").toLowerCase();
    const typeRaw = (u.searchParams.get("type") || "any").toLowerCase();
    const type = TYPE_ALLOW.has(typeRaw) ? typeRaw : "any";

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
      resultsArrays = settled.filter((r) => r.status === "fulfilled").map((r) => r.value || []);
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

    const sliced = dedup.slice(0, limit);

    // Map pins
    const mapData = sliced
      .map((x) => {
        if (x.geo && typeof x.geo.lat === "number" && typeof x.geo.lon === "number") {
          return { name: x.header, lat: x.geo.lat, lon: x.geo.lon };
        }
        return null;
      })
      .filter(Boolean);

    clearTimeout(hardTimeout);

    sendJson(req, res, 200, {
      ok: true,
      timeout: timedOut,
      count: sliced.length,
      results: sliced.map((x) => ({
        header: x.header,
        body: x.body,
        geo: x.geo ? { lat: x.geo.lat, lon: x.geo.lon } : null,
      })),
      map: mapData,
    });
  } catch (e) {
    clearTimeout(hardTimeout);
    sendJson(req, res, 500, { ok: false, error: e?.message || String(e), results: [], map: [] });
  }
}

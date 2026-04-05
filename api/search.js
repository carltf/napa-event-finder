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
  "http://localhost:5173",
  "http://localhost:5174",
  "https://napaserve.vercel.app",
  "https://napaserve.org",
  "https://www.napaserve.org",
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
  const small = new Set(["a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "by", "in", "of", "up"]);
  let capNext = false;
  return s
    .trim()
    .split(/\s+/)
    .map((w, i) => {
      // Track if this token starts with "("
      const startsWithParen = w.startsWith("(");
      const bare = startsWithParen ? w.slice(1) : w;
      const prefix = startsWithParen ? "(" : "";

      // Preserve words already in ALL CAPS (acronyms like "ACT", "EBT", "DJ")
      if (bare.length > 1 && bare === bare.toUpperCase() && /[A-Z]/.test(bare)) {
        capNext = w.endsWith("(") || w.endsWith(":");
        return w;
      }
      const c = bare.toLowerCase();
      // Capitalize: first word, after "(", after ":", or non-small words
      const shouldCap = i === 0 || capNext || startsWithParen || !small.has(c);
      capNext = w.endsWith("(") || w.endsWith(":");
      return prefix + (shouldCap ? c[0].toUpperCase() + c.slice(1) : c);
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

function normalizeExcerpt(s) {
  let x = cleanText(s || "");

  // If the source ends with "..." or "…", remove it and end cleanly.
  x = x.replace(/\s*(\.\.\.|…)\s*$/g, "");

  // Some sources include " ... " in the middle as a truncation marker.
  // If it appears right before we append our own fields, it reads badly.
  // Remove " ... " when it ends the excerpt.
  x = x.replace(/\s+\.\.\.\s*$/g, "");

  // Ensure we end with a period if there is any content.
  if (x && !/[.!?]$/.test(x)) x += ".";

  return x;
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
  const details = normalizeExcerpt(e.details || "Details on website.");
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
details = normalizeExcerpt(details);

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

// -------------------- Month-name date parser --------------------
const MONTH_NAMES_MAP = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
  jan: 0, feb: 1, mar: 2, apr: 3, jun: 5, jul: 6,
  aug: 7, sep: 8, sept: 8, oct: 9, nov: 10, dec: 11,
};

function parseMonthDayYear(text) {
  if (!text) return null;
  let m;
  // "7 April 2026" or "05 Apr 2026"
  m = text.match(/(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})/);
  if (m && MONTH_NAMES_MAP[m[2].toLowerCase()] !== undefined) {
    return toISODate(new Date(Date.UTC(+m[3], MONTH_NAMES_MAP[m[2].toLowerCase()], +m[1])));
  }
  // "April 17, 2026"
  m = text.match(/([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})/);
  if (m && MONTH_NAMES_MAP[m[1].toLowerCase()] !== undefined) {
    return toISODate(new Date(Date.UTC(+m[3], MONTH_NAMES_MAP[m[1].toLowerCase()], +m[2])));
  }
  // "APRIL 7" (no year — assume current)
  m = text.match(/([A-Za-z]+)\s+(\d{1,2})(?:\s|$|,)/);
  if (m && MONTH_NAMES_MAP[m[1].toLowerCase()] !== undefined) {
    const yr = new Date().getUTCFullYear();
    return toISODate(new Date(Date.UTC(yr, MONTH_NAMES_MAP[m[1].toLowerCase()], +m[2])));
  }
  return null;
}

// -------------------- Parser: Cameo Films (coming-soon) --------------------
async function parseCameoFilms(f) {
  const listUrl = "https://www.cameocinema.com/coming-soon";
  const html = await fetchText(listUrl);
  const $ = load(html);
  const events = [];
  const seen = new Set();

  $("h2").each((_, el) => {
    const title = cleanText($(el).text());
    if (!title || title.length < 2 || title.length > 120) return;
    if (isGenericTitle(title)) return;
    if (/cameo|coming soon|now showing|showtimes/i.test(title)) return;
    if (seen.has(title.toLowerCase())) return;
    seen.add(title.toLowerCase());

    const container = $(el).closest("div.sqs-block, div.col, article, section, li").first();
    const blockText = container.length ? container.text() : $(el).parent().text();
    const startYMD = parseMonthDayYear(blockText);

    let link = $(el).find("a[href*='/movie/']").attr("href")
      || $(el).closest("a").attr("href")
      || (container.length ? container.find("a[href*='/movie/']").attr("href") : null);
    const fullUrl = link
      ? (link.startsWith("http") ? link : "https://www.cameocinema.com" + link)
      : listUrl;

    events.push({
      title,
      url: fullUrl,
      when: startYMD ? apDateFromYMD(startYMD) : "Dates on website.",
      startYMD,
      endYMD: startYMD,
      details: "Now showing at Cameo Cinema, St. Helena's beloved independent theater.",
      price: "Price not provided.",
      address: "1340 Main St., St. Helena.",
      town: "st-helena",
      tag: "movies",
      geo: GEO_HINTS["st-helena"],
    });
  });

  return filterAndRank(events, f);
}

// -------------------- Parser: Cameo Film Class --------------------
async function parseCameoFilmClass(f) {
  const listUrl = "https://www.cameocinema.com/film-class-calendar";
  const html = await fetchText(listUrl);
  const $ = load(html);
  const events = [];

  // Iterate all bold/strong elements to find film titles (ALL CAPS names like "THE DEFIANT ONES")
  const boldEls = $("strong, b").toArray();
  for (const boldNode of boldEls) {
    const boldText = cleanText($(boldNode).text());
    if (!boldText || boldText.length < 3) continue;

    // Film titles are ALL CAPS; skip date-only lines like "APRIL 7" and intro text
    if (!/^[A-Z][A-Z\s'',!?:\-–—&]+$/.test(boldText)) continue;
    // Skip if it looks like a month+day date
    if (/^(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{1,2}$/i.test(boldText)) continue;
    // Skip monthly section headers like "APRIL FILM CLASS WITH TERENCE FORD"
    if (/\bFILM\s+CLASS\b/i.test(boldText)) continue;

    // Walk upward to find the containing block for context (date, time, description)
    const container = $(boldNode).closest("div.sqs-block, div.sqs-block-content, article, section, p").first();
    const blockText = container.length ? container.text() : $(boldNode).parent().parent().text();
    if (!blockText || blockText.length < 20) continue;

    const dateYMD = parseMonthDayYear(blockText);
    if (!dateYMD) continue;

    // Clean the film title — title-case it
    let title = titleCase(boldText.toLowerCase());
    title = title.replace(/\s*\([\d,\s]+min\w*\)\s*/i, "").replace(/\s*\(\d{4}\)\s*/, "").trim();
    if (!title || title.length < 2) continue;

    const timeMatch = blockText.match(/(\d{1,2}:\d{2})\s*(am|pm)/i);
    let timeText = null;
    if (timeMatch) {
      let h = parseInt(timeMatch[1].split(":")[0], 10);
      const mm = timeMatch[1].split(":")[1];
      const ampm = timeMatch[2].toLowerCase() === "pm" ? "p.m." : "a.m.";
      if (h > 12) h -= 12;
      timeText = mm === "00" ? `${h} ${ampm}` : `${h}:${mm} ${ampm}`;
    }

    // Description: find the paragraph text after the title, skipping year/runtime
    const allText = cleanText(blockText);
    let desc = allText;
    const titleUpper = boldText;
    const titleIdx = desc.toUpperCase().indexOf(titleUpper);
    if (titleIdx >= 0) desc = desc.slice(titleIdx + titleUpper.length);
    desc = desc.replace(/^\s*\([\d,\s\w]+\)\s*/, "").trim();
    // Strip leading runtime like "96 min" or "(1958) 96 min"
    desc = desc.replace(/^\d+\s*min\s*/i, "").trim();
    const sentMatch = desc.match(/^[^.!?]+[.!?]/);
    desc = sentMatch ? truncate(sentMatch[0], 260) : truncate(desc, 260);
    desc = normalizeExcerpt(desc || "Film Class at Cameo Cinema.");

    const when = apDateFromYMD(dateYMD);
    const whenFull = when ? (timeText ? `${when}, ${timeText}` : when) : "Date on website.";

    events.push({
      title: `Film Class: ${title}`,
      url: listUrl,
      when: whenFull,
      startYMD: dateYMD,
      endYMD: dateYMD,
      details: desc,
      price: "$10.",
      address: "1340 Main St., St. Helena.",
      town: "st-helena",
      tag: "art",
      geo: GEO_HINTS["st-helena"],
    });
  }

  return filterAndRank(events, f);
}

// -------------------- Parser: Brannan Center (Google Calendar API) --------------------
const BRANNAN_GCAL_ID = "1upv6eaopbpe9qv79cabbhe6mubv3727@import.calendar.google.com";
const BRANNAN_GCAL_KEY = "AIzaSyBNlYH01_9Hc5S1J9vuFmu2nUqBZJNAXxs";
const BRANNAN_PUBLIC_ALLOWLIST = /comedy|jazz|classical|bluegrass|concert|performance|workshop|class|film|lecture|speaker|exhibition|reception|festival|camp|theater|theatre|dance|music|listening\s*room|showcase|mariachi|symphony|quartet|trio|ensemble|orchestra|recital|opera|cabaret|storytell/i;

async function parseBrannanCenter(f) {
  try {
    const now = new Date();
    const timeMin = now.toISOString();
    const end = new Date(now);
    end.setMonth(end.getMonth() + 3);
    const timeMax = end.toISOString();

    const url = `https://clients6.google.com/calendar/v3/calendars/${encodeURIComponent(BRANNAN_GCAL_ID)}/events`
      + `?singleEvents=true&orderBy=startTime&maxResults=25`
      + `&timeMin=${encodeURIComponent(timeMin)}&timeMax=${encodeURIComponent(timeMax)}`
      + `&timeZone=America%2FLos_Angeles&key=${BRANNAN_GCAL_KEY}`;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    let data;
    try {
      const res = await fetch(url, {
        headers: { "Accept": "application/json" },
        signal: controller.signal,
      });
      if (!res.ok) throw new Error(`GCal API ${res.status}`);
      data = await res.json();
    } finally {
      clearTimeout(timer);
    }

    const items = data.items || [];
    const events = [];
    const seen = new Set();

    for (const item of items) {
      if (item.status !== "confirmed") continue;
      let title = cleanText(item.summary || "");
      if (!title) continue;

      // Strip Tripleseat "[D]" / "[T]" prefix
      title = title.replace(/^\[[A-Z]\]\s*/, "");
      if (!title || title.length < 3) continue;

      // Allowlist: only include events with public programming signals
      const matchText = title + " " + (item.description || "");
      if (!BRANNAN_PUBLIC_ALLOWLIST.test(matchText)) continue;

      // Dedupe by title+date
      const startISO = item.start?.dateTime || item.start?.date || "";
      const startYMD = ymdFromISO(startISO) || (startISO.length === 10 ? startISO : null);
      const dedupeKey = title.toLowerCase() + "|" + (startYMD || "");
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);

      const endISO = item.end?.dateTime || item.end?.date || "";
      const endYMD = ymdFromISO(endISO) || startYMD;

      // Time formatting
      const timeRange = formatTimeRange(startISO, endISO);
      const when = startYMD ? apDateFromYMD(startYMD) : null;
      const whenFull = when ? (timeRange ? `${when}, ${timeRange}` : when) : "Date on website.";

      // Extract a clean description — skip internal notes
      let desc = "";
      const rawDesc = (item.description || "").split("\n");
      for (const line of rawDesc) {
        const trimmed = line.trim();
        // Skip internal Tripleseat lines
        if (/^\[?Guests:|^Event:|^Advance Contact|^Hospitality|^Materials|^Ticketing|^Lodging|^Tech:|^Series sponsor|^Performance sponsor|^Wine sponsor|^Member Announce|^Public Announce|^Brannan Center Concessions|^Reserved or GA|^Marketing/i.test(trimmed)) continue;
        if (!trimmed) continue;
        // Use first substantive line (Artist, Show Title, or general text)
        if (/^(Artist|Show Title|Format|Venue|Pricing|Time):?\s*/i.test(trimmed)) {
          desc += trimmed.replace(/^(Artist|Show Title|Format|Venue|Pricing|Time):?\s*/i, "").trim() + " ";
        } else if (!/^https?:\/\//.test(trimmed) && !desc) {
          desc = trimmed;
          break;
        }
      }
      desc = normalizeExcerpt(truncate(desc.trim() || "Event at the Brannan Center, Calistoga.", 260));

      // Build slug from title for event URL
      const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
      const eventUrl = `https://www.brannancenter.org/events/${slug}`;

      const tag = classifyTag(title, desc);

      events.push({
        title,
        url: eventUrl,
        when: whenFull,
        startYMD,
        endYMD,
        details: desc,
        price: "Price not provided.",
        address: "1407 3rd Street, Calistoga.",
        town: "calistoga",
        tag: tag === "any" ? "art" : tag,
        geo: GEO_HINTS["calistoga"],
      });
    }

    return filterAndRank(events, f);
  } catch (e) {
    // Log warning but don't throw — return empty
    console.warn("parseBrannanCenter failed:", e?.message || e);
    return [];
  }
}

// -------------------- Parser: Napa County Library (CivicEngage) --------------------
const LIBRARY_TITLE_BLOCKLIST = /storytime|story\s*time|wee\s*wednesday|baby\s*lap|laptime|toddler\s*time|homework\s*help|bilingual\s*storytime/i;

async function parseNapaCountyLibrary(calendarId, townSlug, cityName, f) {
  const listUrl = `https://www.napacounty.gov/calendar.aspx?CID=${calendarId}`;
  const html = await fetchText(listUrl);
  const $ = load(html);
  const events = [];
  const seen = new Set();

  const sourceName = calendarId === 59 ? "Napa County Library"
    : calendarId === 55 ? "Yountville Library"
    : "Napa County Library";

  // CivicEngage embeds Schema.org microdata in hidden divs for each event
  $("[itemtype='http://schema.org/Event']").each((_, el) => {
    const title = cleanText($(el).find("[itemprop='name']").first().text());
    if (!title || title.length < 3) return;
    if (LIBRARY_TITLE_BLOCKLIST.test(title)) return;

    const startDateStr = $(el).find("[itemprop='startDate']").text();
    const isoMatch = (startDateStr || "").match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
    let startYMD = null;
    let timeText = null;
    if (isoMatch) {
      startYMD = `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
      const h = parseInt(isoMatch[4], 10);
      const mm = isoMatch[5];
      const ampm = h >= 12 ? "p.m." : "a.m.";
      const h12 = h % 12 || 12;
      timeText = mm === "00" ? `${h12} ${ampm}` : `${h12}:${mm} ${ampm}`;
    }

    const dedupeKey = title.toLowerCase() + "|" + (startYMD || "");
    if (seen.has(dedupeKey)) return;
    seen.add(dedupeKey);

    const desc = cleanText($(el).find("[itemprop='description']").text());
    const locName = cleanText($(el).find("[itemprop='location'] [itemprop='name']").text());
    const street = cleanText($(el).find("[itemprop='streetAddress']").text());
    const address = street ? `${street}, ${cityName}.` : (locName ? `${locName}, ${cityName}.` : `${cityName}.`);

    // Find the EID link in the parent container
    const parentDiv = $(el).closest("div[id^='parentdiv']");
    const eidLink = parentDiv.find("a[href*='EID=']").attr("href") || "";
    const eidMatch = eidLink.match(/EID=(\d+)/i);
    const eid = eidMatch ? eidMatch[1] : null;
    const fullUrl = eid
      ? `https://www.napacounty.gov/Calendar.aspx?EID=${eid}`
      : listUrl;

    let tag = classifyTag(title, desc);
    if (tag === "any") tag = "art";

    const when = startYMD ? apDateFromYMD(startYMD) : null;
    const whenFull = when ? (timeText ? `${when}, ${timeText}` : when) : "Date on website.";

    events.push({
      title,
      url: fullUrl,
      when: whenFull,
      startYMD,
      endYMD: startYMD,
      details: normalizeExcerpt(desc || `${sourceName} event. Details on website.`),
      price: "Free.",
      address,
      town: townSlug,
      tag,
      geo: GEO_HINTS[townSlug] || null,
    });
  });

  return filterAndRank(events, f);
}

// -------------------- Parser: St. Helena Public Library --------------------
const SHPL_TITLE_BLOCKLIST = /story\s*time|storytime|baby|toddler|kids\s*rock|teen\s*hang/i;

async function parseStHelenaLibrary(f) {
  const listUrl = "https://www.shpl.org/events";
  const html = await fetchText(listUrl);
  const $ = load(html);
  const events = [];
  const seen = new Set();
  const currentYear = new Date().getUTCFullYear();

  // Drupal Stacks CMS: each event is an li.events-list-item
  $("li.events-list-item").each((_, el) => {
    const titleEl = $(el).find(".eventinstance a, .field--name-title a").first();
    const title = cleanText(titleEl.text());
    if (!title || title.length < 3) return;
    if (SHPL_TITLE_BLOCKLIST.test(title)) return;

    // Date: <span class="listing-date-month">Apr</span><span class="listing-date-day">7</span>
    const month = cleanText($(el).find(".listing-date-month").text());
    const day = cleanText($(el).find(".listing-date-day").text());
    let startYMD = null;
    if (month && day) {
      startYMD = parseMonthDayYear(`${month} ${day} ${currentYear}`);
    }

    const dedupeKey = title.toLowerCase() + "|" + (startYMD || "");
    if (seen.has(dedupeKey)) return;
    seen.add(dedupeKey);

    // Time: "5:30 pm - 7:00 pm PDT"
    const timeStr = cleanText($(el).find(".event-listing-time").text());
    let timeText = null;
    const timeMatch = (timeStr || "").match(/(\d{1,2}:\d{2})\s*(am|pm)/i);
    if (timeMatch) {
      let h = parseInt(timeMatch[1].split(":")[0], 10);
      const mm = timeMatch[1].split(":")[1];
      const ampm = timeMatch[2].toLowerCase() === "pm" ? "p.m." : "a.m.";
      if (h > 12) h -= 12;
      timeText = mm === "00" ? `${h} ${ampm}` : `${h}:${mm} ${ampm}`;
    }

    // Description
    const desc = cleanText($(el).find(".event-listing-description").text());

    // Link
    const href = titleEl.attr("href") || "";
    const fullUrl = href
      ? (href.startsWith("http") ? href : "https://www.shpl.org" + href)
      : listUrl;

    let tag = classifyTag(title, desc);
    if (tag === "any") tag = "art";

    const when = startYMD ? apDateFromYMD(startYMD) : null;
    const whenFull = when ? (timeText ? `${when}, ${timeText}` : when) : "Date on website.";

    events.push({
      title,
      url: fullUrl,
      when: whenFull,
      startYMD,
      endYMD: startYMD,
      details: normalizeExcerpt(truncate(desc || "St. Helena Public Library event. Details on website.", 260)),
      price: "Free.",
      address: "1492 Library Lane, St. Helena.",
      town: "st-helena",
      tag,
      geo: GEO_HINTS["st-helena"],
    });
  });

  return filterAndRank(events, f);
}

// -------------------- Parser: St. Helena Chamber (WP REST API) --------------------
const STHELENA_CAT_MAP = {
  477: "art",       // Arts & Culture
  478: "any",       // Family-Friendly
  497: "any",       // Featured
  479: "food",      // Food & Wine
  480: "food",      // Harvest
  481: "any",       // Holidays
  482: "nightlife", // Nightlife
  483: "wellness",  // Outdoor Activities
  484: "any",       // Shopping
  485: "wellness",  // Wellness & Relaxation
  486: "art",       // Workshops & Classes
};

async function parseStHelenaChamber(f) {
  try {
    const apiUrl = "https://www.sthelena.com/wp-json/wp/v2/event?per_page=15&orderby=date&order=desc&_embed";
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    let data;
    try {
      const res = await fetch(apiUrl, {
        headers: { "Accept": "application/json" },
        signal: controller.signal,
      });
      if (!res.ok) throw new Error(`WP API ${res.status}`);
      data = await res.json();
    } finally {
      clearTimeout(timer);
    }

    if (!Array.isArray(data) || data.length === 0) return [];

    const events = [];
    for (const post of data) {
      const title = cleanText(post.title?.rendered || "");
      if (!title || title.length < 3) continue;

      // Get categories from embedded terms
      const catIds = post["event-cat"] || [];
      const embeddedTerms = post._embedded?.["wp:term"]?.[0] || [];
      const catSlugs = embeddedTerms.map((t) => t.slug);

      // Skip wine-only events
      if (catSlugs.length === 1 && catSlugs[0] === "food-wine") continue;

      // Map category
      let tag = "any";
      for (const cid of catIds) {
        const mapped = STHELENA_CAT_MAP[cid];
        if (mapped && mapped !== "any") { tag = mapped; break; }
      }
      if (tag === "any") tag = classifyTag(title, "");
      if (tag === "any") tag = "art";

      // Extract event date from content HTML using parseMonthDayYear
      const contentHtml = post.content?.rendered || "";
      const contentText = cleanText(contentHtml.replace(/<[^>]+>/g, " "));
      let startYMD = parseMonthDayYear(contentText);

      // Description: strip HTML, first sentence
      let desc = contentText;
      const sentMatch = desc.match(/^[^.!?]+[.!?]/);
      desc = sentMatch ? truncate(sentMatch[0], 260) : truncate(desc, 260);
      desc = normalizeExcerpt(desc || "Event in St. Helena. Details on website.");

      const eventUrl = post.link || `https://www.sthelena.com/event/${post.slug}/`;

      const when = startYMD ? apDateFromYMD(startYMD) : null;
      const whenFull = when || "Date on website.";

      events.push({
        title,
        url: eventUrl,
        when: whenFull,
        startYMD,
        endYMD: startYMD,
        details: desc,
        price: "Price not provided.",
        address: "St. Helena.",
        town: "st-helena",
        tag,
        geo: GEO_HINTS["st-helena"],
      });
    }

    return filterAndRank(events, f);
  } catch (e) {
    console.warn("parseStHelenaChamber failed:", e?.message || e);
    return [];
  }
}

// -------------------- Parser: NVC Estate Winery (Eventbrite) --------------------
async function parseNVCWinery(f) {
  try {
    const listUrl = "https://www.eventbrite.com/d/ca--napa/nvc-estate-winery/";
    const html = await fetchText(listUrl);
    const $ = load(html);
    const events = [];

    // Eventbrite embeds JSON-LD with Event objects on search pages
    const ldEvents = getJsonLdEvents($);

    // Also check for itemListElement pattern (Eventbrite uses ListItem wrapping)
    $("script[type='application/ld+json']").each((_, el) => {
      try {
        const data = JSON.parse($(el).text() || "{}");
        const items = Array.isArray(data) ? data : (data.itemListElement || []);
        for (const entry of items) {
          const item = entry.item || entry;
          if (item["@type"] === "Event") ldEvents.push(item);
        }
      } catch {}
    });

    const seen = new Set();
    for (const ev of ldEvents) {
      const title = cleanText(ev.name || "");
      if (!title || !title.toLowerCase().includes("nvc")) continue;

      const startYMD = ymdFromISO(ev.startDate) || (ev.startDate?.length === 10 ? ev.startDate : null);
      const endYMD = ymdFromISO(ev.endDate) || startYMD;

      const dedupeKey = title.toLowerCase() + "|" + (startYMD || "");
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);

      const desc = cleanText(ev.description || "Wine education and tasting at Napa Valley College's on-campus teaching winery, the first community college winery in California.");
      const eventUrl = ev.url || listUrl;

      const timeRange = ev.startDate ? formatTimeRange(ev.startDate, ev.endDate || "") : null;
      const when = startYMD ? apDateFromYMD(startYMD) : null;
      const whenFull = when ? (timeRange ? `${when}, ${timeRange}` : when) : "Date on website.";

      events.push({
        title,
        url: eventUrl,
        when: whenFull,
        startYMD,
        endYMD,
        details: normalizeExcerpt(truncate(desc, 260)),
        price: "Price not provided.",
        address: "2277 Napa-Vallejo Hwy, Napa.",
        town: "napa",
        tag: "food",
        geo: GEO_HINTS["napa"],
      });
    }

    return filterAndRank(events, f);
  } catch (e) {
    console.warn("parseNVCWinery failed:", e?.message || e);
    return [];
  }
}

// -------------------- Parser: Town of Yountville --------------------
const YOUNTVILLE_TITLE_BLOCKLIST = /planning\s*commission|city\s*council|\bboard\b|commission\s*meeting|public\s*hearing|pickleball|open\s*gym|basketball|jazzercise/i;

async function parseTownOfYountville(f) {
  const listUrl = "https://www.townofyountville.com/Calendar.aspx";
  const html = await fetchText(listUrl);
  const $ = load(html);
  const events = [];
  const seen = new Set();

  $("a[href*='Calendar.aspx?EID='], a[href*='calendar.aspx?EID='], .calendarList tr, .calendar-event, [data-eventid]").each((_, el) => {
    const linkEl = $(el).is("a") ? $(el) : $(el).find("a[href*='EID=']").first();
    const href = linkEl.attr("href") || "";
    const eidMatch = href.match(/EID=(\d+)/i);
    const eid = eidMatch ? eidMatch[1] : null;
    if (!eid) return;
    if (seen.has(eid)) return;
    seen.add(eid);

    const title = cleanText(linkEl.text() || $(el).find(".calendar-title, .eventTitle, h3, h4").first().text());
    if (!title || title.length < 3) return;
    if (YOUNTVILLE_TITLE_BLOCKLIST.test(title)) return;
    // Skip rec scheduling format: titles with 2+ dashes ("Pickleball - Advanced - 10:30 Am - 12:30 Pm")
    if ((title.match(/ - /g) || []).length >= 2) return;

    const containerText = $(el).closest("tr, div, li").text() || $(el).text();
    const isoMatch = containerText.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
    let startYMD = null;
    let timeText = null;
    if (isoMatch) {
      startYMD = `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
      const h = parseInt(isoMatch[4], 10);
      const mm = isoMatch[5];
      const ampm = h >= 12 ? "p.m." : "a.m.";
      const h12 = h % 12 || 12;
      timeText = mm === "00" ? `${h12} ${ampm}` : `${h12}:${mm} ${ampm}`;
    } else {
      startYMD = parseMonthDayYear(containerText);
    }

    const catText = cleanText($(el).find("[class*='cat'], [class*='tag']").text()).toLowerCase();
    let tag;
    if (/yountville\s*arts/i.test(catText)) tag = "art";
    else if (/fitness|exercise|yoga/i.test(catText) || /fitness|exercise|yoga/i.test(title)) tag = "wellness";
    else tag = classifyTag(title, containerText);
    if (tag === "any") tag = "art";

    const locText = cleanText($(el).find(".location, .eventLocation, [class*='loc']").text());
    const venueName = locText || "Yountville Community Center";

    const when = startYMD ? apDateFromYMD(startYMD) : null;
    const whenFull = when ? (timeText ? `${when}, ${timeText}` : when) : "Date on website.";
    const fullUrl = `https://www.townofyountville.com/Calendar.aspx?EID=${eid}`;

    events.push({
      title,
      url: fullUrl,
      when: whenFull,
      startYMD,
      endYMD: startYMD,
      details: normalizeExcerpt(`Town of Yountville event at ${venueName}. Details on website.`),
      price: "Price not provided.",
      address: "Yountville.",
      town: "yountville",
      tag,
      geo: GEO_HINTS["yountville"],
    });
  });

  return filterAndRank(events, f);
}

// -------------------- Parser: American Canyon --------------------
const AC_CAT_ALLOWLIST = /community\s*events|music\s*&?\s*arts|adult\s*activities|youth\s*activities/i;
const AC_CAT_BLOCKLIST = /meetings|city\s*holiday|ceremonies/i;
const AC_TITLE_BLOCKLIST = /city\s*council|planning\s*commission|board\s*meeting/i;

async function parseAmericanCanyon(f) {
  const listUrl = "https://www.americancanyon.gov/Live/Community-Calendar";
  const html = await fetchText(listUrl);
  const $ = load(html);
  const events = [];
  const seen = new Set();

  $(".event-item, .calendar-item, article, .views-row, li[class*='event'], .event-card, [class*='calendar-event']").each((_, el) => {
    const titleEl = $(el).find("h2, h3, h4, .event-title, a[class*='title']").first();
    const title = cleanText(titleEl.text());
    if (!title || title.length < 3) return;
    if (AC_TITLE_BLOCKLIST.test(title)) return;
    if (seen.has(title.toLowerCase())) return;
    seen.add(title.toLowerCase());

    const catText = cleanText($(el).find("[class*='cat'], [class*='tag'], .category").text());
    if (catText && AC_CAT_BLOCKLIST.test(catText)) return;
    if (catText && !AC_CAT_ALLOWLIST.test(catText)) return;

    const blockText = $(el).text();
    const startYMD = parseMonthDayYear(blockText);

    let locText = cleanText($(el).find(".location, .event-location, [class*='loc']").text());
    // Strip date artifacts and city suffix from location text
    locText = locText.replace(/^\d{1,2}\s+[A-Za-z]+\s+\d{4}\s*/, "");
    locText = locText.replace(/^[A-Za-z]+\s+\d{1,2},?\s+\d{4}\s*/, "");
    locText = locText.replace(/,\s*American Canyon\.?\s*$/i, "").trim();
    // If locText is just the event title or very short, discard it
    if (locText && (locText.toLowerCase() === title.toLowerCase() || locText.length < 3)) locText = "";

    // Extract description text, stripping date artifacts and city suffixes
    const descEl = $(el).find(".event-description, .description, p, .summary").first();
    let rawDesc = cleanText(descEl.text());
    // Strip leading date like "11 Apr 2026" or "April 17, 2026"
    rawDesc = rawDesc.replace(/^\d{1,2}\s+[A-Za-z]+\s+\d{4}\s*/, "");
    rawDesc = rawDesc.replace(/^[A-Za-z]+\s+\d{1,2},?\s+\d{4}\s*/, "");
    // Strip trailing ", American Canyon" or ", [City Name]"
    rawDesc = rawDesc.replace(/,\s*American Canyon\.?\s*$/i, "");
    // Strip title echo from description
    if (rawDesc.startsWith(title)) rawDesc = rawDesc.slice(title.length);
    rawDesc = rawDesc.trim();

    let link = titleEl.find("a").attr("href") || titleEl.closest("a").attr("href")
      || $(el).find("a").first().attr("href");
    const fullUrl = link
      ? (link.startsWith("http") ? link : "https://www.americancanyon.gov" + link)
      : listUrl;

    let tag;
    if (/music\s*&?\s*arts/i.test(catText)) tag = "art";
    else tag = classifyTag(title, blockText);
    if (tag === "any") tag = "art";

    const when = startYMD ? apDateFromYMD(startYMD) : null;
    const whenFull = when ? when : "Date on website.";

    const acDesc = rawDesc && rawDesc.length > 5
      ? normalizeExcerpt(truncate(rawDesc, 260))
      : normalizeExcerpt("City of American Canyon community event. Details on website.");

    events.push({
      title,
      url: fullUrl,
      when: whenFull,
      startYMD,
      endYMD: startYMD,
      details: acDesc,
      price: "Price not provided.",
      address: locText ? `${locText}, American Canyon.` : "American Canyon.",
      town: "american-canyon",
      tag,
      geo: GEO_HINTS["american-canyon"],
    });
  });

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
        if (s.id === "cameo") {
          const films = await parseCameoFilms(filters);
          return films.length ? films : await parseCameo(s.listUrl, filters);
        }
        return [];
      } catch {
        return [];
      }
    });

    // New source parsers (non-movies only, except Cameo Films handled above)
    if (type !== "movies") {
      const wrap = (fn) => (async () => { try { return await fn(); } catch { return []; } })();
      tasks.push(wrap(() => parseCameoFilmClass(filters)));
      tasks.push(wrap(() => parseBrannanCenter(filters)));
      tasks.push(wrap(() => parseNapaCountyLibrary(59, "napa", "Napa", filters)));
      tasks.push(wrap(() => parseNapaCountyLibrary(55, "yountville", "Yountville", filters)));
      tasks.push(wrap(() => parseStHelenaLibrary(filters)));
      tasks.push(wrap(() => parseTownOfYountville(filters)));
      tasks.push(wrap(() => parseAmericanCanyon(filters)));
      tasks.push(wrap(() => parseStHelenaChamber(filters)));
      tasks.push(wrap(() => parseNVCWinery(filters)));
    }

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

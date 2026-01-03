import cheerio from "cheerio";

/**
 * Napa Valley Event Finder — API
 * - Inlines sources (no fs bundling issues)
 * - Tightens link rules (especially DoNapa)
 * - Optionally fetches event detail pages and extracts JSON-LD Event data
 * - Accepts YYYY-MM-DD and MM/DD/YYYY dates
 */

// -------------------- Cache --------------------
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

// -------------------- Sources (inline) --------------------
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

// -------------------- Helpers --------------------
function sendJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.end(JSON.stringify(payload, null, 2));
}

function cleanText(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function toISODate(d) {
  return d.toISOString().slice(0, 10);
}

function parseISODate(s) {
  // Accept:
  // - YYYY-MM-DD
  // - MM/DD/YYYY or M/D/YYYY (Safari/Squarespace may provide this)
  const str = (s || "").trim();

  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(str);
  if (m) {
    const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
    return isNaN(dt.getTime()) ? null : dt;
  }

  m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(str);
  if (m) {
    const mm = +m[1],
      dd = +m[2],
      yy = +m[3];
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

function titleCase(s) {
  if (!s) return s;
  const small = new Set(["a", "an", "and", "at", "but", "by", "for", "in", "of", "on", "or", "the", "to", "with"]);
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
  return (
    !x ||
    x === "read more" ||
    x === "event details" ||
    x === "learn more" ||
    x === "details" ||
    x === "view event"
  );
}

function apDateFromYMD(ymd) {
  // ymd = YYYY-MM-DD
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd || "");
  if (!m) return null;
  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (isNaN(dt.getTime())) return null;

  const weekdays = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
  const months = ["Jan.","Feb.","March","April","May","June","July","Aug.","Sept.","Oct.","Nov.","Dec."];

  return `${weekdays[dt.getUTCDay()]}, ${months[dt.getUTCMonth()]} ${dt.getUTCDate()}`;
}

function apTimeFromISOClock(iso) {
  // Parse time without timezone conversion (use clock digits in the ISO string)
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

  // If both have same meridiem, drop the first meridiem (AP style)
  const mer1 = t1.endsWith("a.m.") ? "a.m." : "p.m.";
  const mer2 = t2.endsWith("a.m.") ? "a.m." : "p.m.";

  const t1NoMer = t1.replace(/\s(a\.m\.|p\.m\.)$/, "");
  if (mer1 === mer2) {
    return `${t1NoMer} to ${t2}`;
  }
  return `${t1} to ${t2}`;
}

function truncate(s, max = 260) {
  const x = cleanText(s);
  if (x.length <= max) return x;
  return x.slice(0, max - 1).trimEnd() + "…";
}

function formatWeekender(event) {
  const header = titleCase(event.title || "Event");
  const dateLine = event.when || "Date and time on website.";
  const details = event.details || "Details on website.";
  const price = event.price || "Price not provided.";
  const contact =
    event.contact ||
    (event.url ? `For more information visit their website (${event.url}).` : "For more information visit their website.");
  const address = event.address || "Venue address not provided.";

  return {
    header,
    body: `${dateLine} ${details} ${price} ${contact} ${address}`.replace(/\s+/g, " ").trim(),
  };
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

// -------------------- JSON-LD extraction --------------------
function getJsonLdEvents($) {
  const out = [];
  $("script[type='application/ld+json']").each((_, el) => {
    const raw = $(el).text();
    if (!raw) return;
    try {
      const data = JSON.parse(raw);
      const stack = Array.isArray(data) ? data : [data];

      for (const item of stack) {
        if (!item) continue;

        if (item["@type"] === "Event") out.push(item);

        if (Array.isArray(item["@graph"])) {
          for (const g of item["@graph"]) {
            if (g && g["@type"] === "Event") out.push(g);
          }
        }
      }
    } catch (_) {}
  });
  return out;
}

function extractStreetAddress(addr) {
  if (!addr) return null;

  if (typeof addr === "string") {
    // Not ideal (often includes city/state); keep conservative
    return null;
  }

  const street = cleanText(addr.streetAddress);
  if (street) return `${street}.`;

  return null;
}

async function extractEventFromPage(url, opts = {}) {
  const html = await fetchText(url);
  const $ = cheerio.load(html);

  let title = null;
  let startISO = null;
  let endISO = null;
  let address = null;
  let description = null;
  let price = null;

  const ldEvents = getJsonLdEvents($);
  const ev = ldEvents[0];

  if (ev) {
    title = ev.name || null;
    startISO = ev.startDate || null;
    endISO = ev.endDate || null;
    description = ev.description || null;

    // location/address
    const loc = Array.isArray(ev.location) ? ev.location[0] : ev.location;
    if (loc && loc.address) {
      address = extractStreetAddress(loc.address);
    }

    // offers/price
    const offers = Array.isArray(ev.offers) ? ev.offers[0] : ev.offers;
    if (offers && offers.price) {
      // Keep simple, don't invent currency formatting
      price = `Tickets ${offers.price}.`;
    }
  }

  // OG/H1 fallback for title
  if (!title) {
    const ogt = $("meta[property='og:title']").attr("content");
    title = cleanText(ogt) || cleanText($("h1").first().text()) || cleanText($("title").text()) || null;
  }

  // Build when + dateISO (use ISO date part; no timezone conversion)
  let dateISO = null;
  let when = null;

  const ymdMatch = /^(\d{4}-\d{2}-\d{2})/.exec(startISO || "");
  if (ymdMatch) {
    dateISO = ymdMatch[1];
    const apDate = apDateFromYMD(dateISO);
    const timeStr = endISO ? formatTimeRange(startISO, endISO) : apTimeFromISOClock(startISO);
    when = apDate ? (timeStr ? `${apDate}, ${timeStr}` : apDate) : null;
  }

  // Details: keep short and safe
  let details = "Details on website.";
  if (description) details = truncate(description);
  if (details && !details.endsWith(".")) details += ".";

  return {
    title: title || "Event",
    url,
    dateISO,
    when: when || "Date and time on website.",
    details,
    price: price || "Price not provided.",
    contact: `For more information visit their website (${url}).`,
    address: address || "Venue address not provided.",
    town: opts.town || "all",
    tag: opts.tag || "any",
  };
}

async function extractOrFallback(url, fallbackTitle, opts = {}) {
  try {
    const ev = await extractEventFromPage(url, opts);
    if (isGenericTitle(ev.title) && fallbackTitle) ev.title = fallbackTitle;
    if (isGenericTitle(ev.title)) return null;
    return ev;
  } catch (_) {
    const t = fallbackTitle && !isGenericTitle(fallbackTitle) ? fallbackTitle : null;
    if (!t) return null;
    return {
      title: t,
      url,
      dateISO: null,
      when: "Date and time on website.",
      details: "Details on website.",
      price: "Price not provided.",
      contact: `For more information visit their website (${url}).`,
      address: "Venue address not provided.",
      town: opts.town || "all",
      tag: opts.tag || "any",
    };
  }
}

// -------------------- Parsers --------------------

// DoNapa: ONLY donapa.com/event/... and then fetch detail pages
async function parseDoNapa(listUrl, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);

  const found = [];
  $("a[href]").each((_, a) => {
    const href = $(a).attr("href") || "";
    if (!href) return;

    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    let u;
    try { u = new URL(fullUrl); } catch { return; }

    if (!u.hostname.endsWith("donapa.com")) return;
    if (!u.pathname.startsWith("/event/")) return;

    found.push(u.toString());
  });

  const unique = Array.from(new Set(found)).slice(0, 12);

  const events = [];
  for (const url of unique) {
    const ev = await extractOrFallback(url, null, { town: "napa", tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, filters);
}

// GrowthZone chambers: /events/details/... then extract details (best-effort)
async function parseGrowthZone(listUrl, sourceName, townSlug, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);

  const links = [];
  $("a[href*='/events/details/']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const title = cleanText($(a).text());
    if (!href) return;

    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    links.push({ url: fullUrl, title });
  });

  const seen = new Set();
  const unique = [];
  for (const x of links) {
    if (!x.url || seen.has(x.url)) continue;
    seen.add(x.url);
    unique.push(x);
    if (unique.length >= 12) break;
  }

  const events = [];
  for (const x of unique) {
    const ev = await extractOrFallback(x.url, x.title, { town: townSlug, tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, filters);
}

// Napa Library: follow /event/... pages and extract
async function parseNapaLibrary(listUrl, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);

  const links = [];
  $("a[href]").each((_, a) => {
    const href = $(a).attr("href") || "";
    if (!href) return;

    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    let u;
    try { u = new URL(fullUrl); } catch { return; }

    if (!u.hostname.includes("napalibrary.org")) return;
    // Keep event-ish links, avoid search/index pages
    if (!u.pathname.includes("/event")) return;

    const t = cleanText($(a).text());
    links.push({ url: u.toString(), title: t });
  });

  const seen = new Set();
  const unique = [];
  for (const x of links) {
    const key = x.url;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    unique.push(x);
    if (unique.length >= 12) break;
  }

  const events = [];
  for (const x of unique) {
    const ev = await extractOrFallback(x.url, x.title, { town: "all", tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, filters);
}

// Visit Napa Valley: follow /event/... pages and extract
async function parseVisitNapaValley(listUrl, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);

  const found = [];
  $("a[href]").each((_, a) => {
    const href = $(a).attr("href") || "";
    if (!href) return;

    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    let u;
    try { u = new URL(fullUrl); } catch { return; }

    if (!u.hostname.endsWith("visitnapavalley.com")) return;
    if (!u.pathname.startsWith("/event/")) return;

    const t = cleanText($(a).text());
    found.push({ url: u.toString(), title: t });
  });

  const seen = new Set();
  const unique = [];
  for (const x of found) {
    if (!x.url || seen.has(x.url)) continue;
    seen.add(x.url);
    unique.push(x);
    if (unique.length >= 12) break;
  }

  const events = [];
  for (const x of unique) {
    const ev = await extractOrFallback(x.url, x.title, { town: "all", tag: "any" });
    if (ev) events.push(ev);
  }

  return filterAndRank(events, filters);
}

// Cameo: keep simple listing, stable contact + address
async function parseCameo(listUrl, altUrls, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);
  const events = [];

  const titles = [];
  $("h2, h3").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && t.length > 2 && t.length < 80 && !t.toLowerCase().includes("menu")) titles.push(t);
  });

  const todayISO = toISODate(new Date());

  const cameoMeta = {
    address: "1340 Main St., St. Helena.",
    phone: "707-963-9779",
    email: "info@cameocinema.com",
  };

  for (const t of titles.slice(0, 8)) {
    if (isGenericTitle(t)) continue;
    if (t.toLowerCase().includes("cameo")) continue;
    if (t.toLowerCase().includes("movie times")) continue;

    events.push({
      title: t,
      url: listUrl,
      dateISO: todayISO,
      when: apDateFromYMD(todayISO) || "Date and time on website.",
      details: "Now playing. Showtimes on website.",
      price: "Price not provided.",
      contact: `For more information call ${cameoMeta.phone}, email ${cameoMeta.email} or visit their website (${listUrl}).`,
      address: cameoMeta.address,
      town: "st-helena",
      tag: "movies",
    });
  }

  // Coming soon (optional)
  if (altUrls && altUrls.length) {
    try {
      const comingUrl = altUrls[1] || altUrls[0];
      const coming = await fetchText(comingUrl);
      const $$ = cheerio.load(coming);

      $$("h2, h3, h4").each((_, el) => {
        const t = cleanText($$(el).text());
        if (!t || t.length < 2) return;
        if (t.toLowerCase().includes("coming soon")) return;
        if (t.toLowerCase().includes("see all")) return;
        if (isGenericTitle(t)) return;

        events.push({
          title: t,
          url: comingUrl,
          dateISO: null,
          when: "Date and time on website.",
          details: "Coming soon. Details on website.",
          price: "Price not provided.",
          contact: `For more information call ${cameoMeta.phone}, email ${cameoMeta.email} or visit their website (${comingUrl}).`,
          address: cameoMeta.address,
          town: "st-helena",
          tag: "movies",
        });
      });
    } catch (_) {}
  }

  return filterAndRank(events, filters);
}

// -------------------- Handler --------------------
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
        // Respect "movies" filter strictly
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

    const results = await Promise.all(tasks);
    for (const r of results) all = all.concat(r);

    // Deduplicate identical formatted outputs
    const seen = new Set();
    const deduped = [];
    for (const item of all) {
      const k = (item.header || "") + "|" + (item.body || "");
      if (seen.has(k)) continue;
      seen.add(k);
      deduped.push(item);
    }

    const final = deduped.slice(0, limit);
    sendJson(res, 200, { ok: true, count: final.length, results: final });
  } catch (e) {
    sendJson(res, 500, { ok: false, error: e?.message || String(e) });
  }
}


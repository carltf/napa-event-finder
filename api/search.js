import * as cheerio from "cheerio";

// Simple 10-minute in-memory cache (per serverless instance)
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
    },
  });

  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);

  const txt = await res.text();
  setCached(key, txt);
  return txt;
}

function apDateTime(dateISO, timeStr) {
  if (!dateISO) return null;
  const dt = new Date(dateISO + "T00:00:00Z");
  const weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  const months = ["Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."];
  const wd = weekdays[dt.getUTCDay()];
  const mo = months[dt.getUTCMonth()];
  const day = dt.getUTCDate();
  return timeStr ? `${wd}, ${mo} ${day}, ${timeStr}` : `${wd}, ${mo} ${day}`;
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

// Master source list is inlined (avoids fs bundling issues on Vercel)
const SOURCES = [
  {
    id: "donapa",
    name: "Do Napa",
    type: "calendar",
    listUrl: "https://donapa.com/upcoming-events/",
  },
  {
    id: "napa_library",
    name: "Napa County Library Events",
    type: "calendar",
    listUrl: "https://events.napalibrary.org/events?n=60&r=days",
  },
  {
    id: "amcan_chamber",
    name: "American Canyon Chamber Events",
    type: "calendar",
    listUrl: "https://business.amcanchamber.org/events",
  },
  {
    id: "calistoga_chamber",
    name: "Calistoga Chamber Events",
    type: "calendar",
    listUrl: "https://chamber.calistogachamber.net/events",
  },
  {
    id: "yountville_chamber",
    name: "Yountville Chamber Events",
    type: "calendar",
    listUrl: "https://web.yountvillechamber.com/events",
  },
  {
    id: "visit_napa_valley",
    name: "Visit Napa Valley Events",
    type: "calendar",
    listUrl: "https://www.visitnapavalley.com/events/",
  },
  {
    id: "cameo",
    name: "Cameo Cinema",
    type: "movies",
    listUrl: "https://www.cameocinema.com/",
    altUrls: ["https://www.cameocinema.com/movie-calendar", "https://www.cameocinema.com/coming-soon"],
  },
];

// --- Parsers (best-effort) ---

async function parseDoNapa(listUrl, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);
  const events = [];

  $("a").each((_, a) => {
    const href = $(a).attr("href") || "";
    const text = $(a).text().trim();
    if (!text || text.length < 4) return;
    if (!href.includes("/event") && !href.includes("/events")) return;

    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    const parentText = $(a).parent().text().replace(/\s+/g, " ").trim();
    const dateMatch = parentText.match(
      /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})\b/i
    );

    let dateISO = null;
    let when = null;
    if (dateMatch) {
      const monthMap = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, sept: 8, oct: 9, nov: 10, dec: 11 };
      const m = monthMap[dateMatch[1].toLowerCase()];
      const d = parseInt(dateMatch[2], 10);
      const y = new Date().getUTCFullYear();
      dateISO = toISODate(new Date(Date.UTC(y, m, d)));
      when = apDateTime(dateISO, null);
    }

    events.push({
      source: "Do Napa",
      town: "napa",
      tag: "any",
      title: text,
      url: fullUrl,
      dateISO,
      when,
      details: "Details on website.",
      price: "Price not provided.",
      contact: `For more information visit their website (${fullUrl}).`,
      address: "Venue address not provided.",
    });
  });

  const seen = new Set();
  const deduped = [];
  for (const e of events) {
    if (!e.url || seen.has(e.url)) continue;
    seen.add(e.url);
    deduped.push(e);
  }
  return filterAndRank(deduped, filters);
}

async function parseGrowthZone(listUrl, sourceName, townSlug, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);
  const events = [];

  $("a[href*='/events/details/']").each((_, a) => {
    const href = $(a).attr("href");
    const title = $(a).text().replace(/\s+/g, " ").trim();
    if (!title) return;
    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();

    const cardText = $(a).closest("li,div,article,section").text().replace(/\s+/g, " ").trim();
    const dm = cardText.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})\b/i);

    let dateISO = null;
    let when = null;
    if (dm) {
      const monthMap = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, sept: 8, oct: 9, nov: 10, dec: 11 };
      const m = monthMap[dm[1].toLowerCase()];
      const d = parseInt(dm[2], 10);
      const y = new Date().getUTCFullYear();
      dateISO = toISODate(new Date(Date.UTC(y, m, d)));
      when = apDateTime(dateISO, null);
    }

    events.push({
      source: sourceName,
      town: townSlug,
      tag: "any",
      title,
      url: fullUrl,
      dateISO,
      when,
      details: "Details on website.",
      price: "Price not provided.",
      contact: `For more information visit their website (${fullUrl}).`,
      address: "Venue address not provided.",
    });
  });

  const seen = new Set();
  const deduped = [];
  for (const e of events) {
    if (!e.url || seen.has(e.url)) continue;
    seen.add(e.url);
    deduped.push(e);
  }
  return filterAndRank(deduped, filters);
}

async function parseNapaLibrary(listUrl, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);
  const events = [];

  $("a[href*='/event/'], a[href*='/events/']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const title = $(a).text().replace(/\s+/g, " ").trim();
    if (!title || title.length < 4) return;
    if (href.includes("/events?") || href.endsWith("/events")) return;

    const fullUrl = href.startsWith("http") ? href : new URL(href, listUrl).toString();
    const cardText = $(a).closest("article,li,div").text().replace(/\s+/g, " ").trim();
    const dm = cardText.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})(?:,\s*(\d{4}))?/i);

    let dateISO = null;
    let when = null;

    if (dm) {
      const monthMap = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, sept: 8, oct: 9, nov: 10, dec: 11 };
      const m = monthMap[dm[1].toLowerCase()];
      const d = parseInt(dm[2], 10);
      const y = dm[3] ? parseInt(dm[3], 10) : new Date().getUTCFullYear();
      dateISO = toISODate(new Date(Date.UTC(y, m, d)));

      const tm = cardText.match(/\b(\d{1,2})(?::(\d{2}))?\s*(a\.m\.|p\.m\.|AM|PM)\b/i);
      let timeStr = null;
      if (tm) {
        const hour = parseInt(tm[1], 10);
        const mins = tm[2] || "00";
        const ampm = tm[3].toLowerCase().includes("p") ? "p.m." : "a.m.";
        timeStr = `${hour}:${mins} ${ampm}`.replace(":00 ", " ");
      }
      when = apDateTime(dateISO, timeStr);
    }

    let town = "all";
    const lc = cardText.toLowerCase();
    if (lc.includes("st. helena")) town = "st-helena";
    else if (lc.includes("yountville")) town = "yountville";
    else if (lc.includes("calistoga")) town = "calistoga";
    else if (lc.includes("american canyon")) town = "american-canyon";
    else if (lc.includes("napa")) town = "napa";

    events.push({
      source: "Napa County Library",
      town,
      tag: "any",
      title,
      url: fullUrl,
      dateISO,
      when,
      details: "Library program. Details on website.",
      price: "Price not provided.",
      contact: `For more information visit their website (${fullUrl}).`,
      address: "Venue address not provided.",
    });
  });

  const seen = new Set();
  const deduped = [];
  for (const e of events) {
    const key = (e.url || "") + "|" + (e.title || "");
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(e);
  }
  return filterAndRank(deduped, filters);
}

async function parseCameo(listUrl, altUrls, filters) {
  const html = await fetchText(listUrl);
  const $ = cheerio.load(html);
  const events = [];

  const titles = [];
  $("h2, h3").each((_, el) => {
    const t = $(el).text().replace(/\s+/g, " ").trim();
    if (t && t.length > 2 && t.length < 80 && !t.toLowerCase().includes("menu")) titles.push(t);
  });

  const todayISO = toISODate(new Date());

  const cameoMeta = {
    address: "1340 Main St., St. Helena.",
    phone: "707-963-9779",
    email: "info@cameocinema.com",
  };

  for (const t of titles.slice(0, 10)) {
    if (t.toLowerCase().includes("cameo") || t.toLowerCase().includes("movie times")) continue;
    events.push({
      source: "Cameo Cinema",
      town: "st-helena",
      tag: "movies",
      title: t,
      url: listUrl,
      dateISO: todayISO,
      when: apDateTime(todayISO, null),
      details: "Now playing. Showtimes on website.",
      price: "Price not provided.",
      contact: `For more information call ${cameoMeta.phone}, email ${cameoMeta.email} or visit their website (${listUrl}).`,
      address: cameoMeta.address,
    });
  }

  if (altUrls && altUrls.length) {
    try {
      const coming = await fetchText(altUrls[1] || altUrls[0]);
      const $$ = cheerio.load(coming);
      $$("h2, h3, h4").each((_, el) => {
        const t = $$(el).text().replace(/\s+/g, " ").trim();
        if (!t || t.length < 2) return;
        if (t.toLowerCase().includes("coming soon") || t.toLowerCase().includes("see all")) return;

        events.push({
          source: "Cameo Cinema",
          town: "st-helena",
          tag: "movies",
          title: t,
          url: altUrls[1] || altUrls[0],
          dateISO: null,
          when: null,
          details: "Coming soon. Details on website.",
          price: "Price not provided.",
          contact: `For more information call ${cameoMeta.phone}, email ${cameoMeta.email} or visit their website (${altUrls[1] || altUrls[0]}).`,
          address: cameoMeta.address,
        });
      });
    } catch (_) {}
  }

  return filterAndRank(events, filters);
}

function sendJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.end(JSON.stringify(payload, null, 2));
}

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

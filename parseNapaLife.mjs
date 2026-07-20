// parseNapaLife.mjs
// Multi-event parser for the NapaLife weekly newsletter (one URL, many events).
// Designed to drop into search.js: returns events in the same shape the other
// parsers produce ({title,url,when,startYMD,endYMD,details,price,address,town,tag,geo})
// and then be handed to filterAndRank().
//
// Why a bespoke parser: NapaLife is Word-generated "filtered HTML" with no
// semantic markup, no per-event links, and no JSON-LD. It cannot go through the
// per-event-page extractor. Instead we walk the document as a STATE MACHINE,
// tracking the current DATE (day headers) and TOWN (town sub-headers), and read
// three shapes: nightly entertainment tables, the future-events calendar table,
// and bold-heading editorial blocks.

import * as cheerioNS from "cheerio";
const load = cheerioNS.load || (cheerioNS.default && cheerioNS.default.load);
if (!load) throw new Error("Cheerio 'load' not found. Check cheerio package version.");

const TOWNS = ["Napa", "St. Helena", "Calistoga", "Yountville", "Rutherford", "American Canyon", "Oakville"];
const TOWN_SLUG = {
  "Napa": "napa", "St. Helena": "st-helena", "Calistoga": "calistoga",
  "Yountville": "yountville", "Rutherford": "rutherford", // maps to napa geo hint fallback
  "American Canyon": "american-canyon", "Oakville": "oakville",
};
const GEO_HINTS = {
  napa: { lat: 38.2975, lon: -122.2869 },
  "st-helena": { lat: 38.5056, lon: -122.4703 },
  yountville: { lat: 38.3926, lon: -122.3631 },
  calistoga: { lat: 38.578, lon: -122.5797 },
  "american-canyon": { lat: 38.1686, lon: -122.2608 },
  rutherford: { lat: 38.4574, lon: -122.4247 },
  oakville: { lat: 38.4324, lon: -122.4014 },
};
const MONTHS = {
  jan: 1, feb: 2, mar: 3, march: 3, apr: 4, april: 4, may: 5, jun: 6, june: 6,
  jul: 7, july: 7, aug: 8, sep: 9, sept: 9, oct: 10, nov: 11, dec: 12,
};

const clean = (s) => String(s || "").replace(/\s+/g, " ").trim();
const pad = (n) => String(n).padStart(2, "0");

function monthNum(tok) {
  if (!tok) return null;
  return MONTHS[tok.toLowerCase().replace(/\./g, "").slice(0, 4)] ??
         MONTHS[tok.toLowerCase().replace(/\./g, "").slice(0, 3)] ?? null;
}

// "Wednesday, July 15" -> {m,d}  (year supplied by caller)
function dayHeader(text) {
  const m = /^(?:mon|tues|wednes|thurs|fri|satur|sun)day,?\s+([A-Za-z.]+)\s+(\d{1,2})$/i.exec(clean(text));
  if (!m) return null;
  const mo = monthNum(m[1]);
  return mo ? { m: mo, d: +m[2] } : null;
}

// "July 23" / "Aug. 1" / "Sept 17-19"  -> first date {m,d}
function calDate(text) {
  const m = /([A-Za-z.]+)\s+(\d{1,2})/.exec(clean(text));
  if (!m) return null;
  const mo = monthNum(m[1]);
  return mo ? { m: mo, d: +m[2] } : null;
}

function ymd(year, mo, d) {
  return `${year}-${pad(mo)}-${pad(d)}`;
}

// First time-ish token in a string -> "H:MM a.m./p.m." style preserved as `when`.
function firstTime(text) {
  const m = /(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)|noon|midnight)/i.exec(text || "");
  return m ? clean(m[0]) : null;
}

function classifyTag(title, desc) {
  const t = `${clean(title)} ${clean(desc)}`.toLowerCase();
  if (/(cameo cinema|screening|\bfilm\b|\bmovie\b|showtimes)/.test(t)) return "movies";
  if (/(karaoke|trivia|bingo|open mic|locals night|dj\b|line danc|dancing|dance (?:night|party|class)|happy hour|nightlife)/.test(t)) return "nightlife";
  if (/(art|gallery|exhibit|opening reception|artist talk|museum|poetry|book|author|theater|theatre|comedy|paint)/.test(t)) return "art";
  if (/(live music|concert|band|jazz|blues|folk|hip-hop|orchestra|recital|opera|singer|vinyl)/.test(t)) return "music";
  if (/(tasting|dinner|brunch|winemaker|pairing|farmers market|food|chef|caviar|barbecue|spritzer)/.test(t)) return "food";
  if (/(yoga|wellness|meditation|sound bath|fitness|breathwork|spa)/.test(t)) return "wellness";
  return "any";
}

function geoFor(townSlug) {
  return GEO_HINTS[townSlug] || null;
}

// venue/address/town line, e.g. "Charles Krug Winery, 2800 Main St., St. Helena"
const ADDR_RE = new RegExp(
  "([^,]+?),\\s*(\\d+[^,]*?(?:St\\.|Ave\\.|Avenue|Road|Rd\\.|Highway|Hwy|Lane|Way|Drive|Dr\\.|Court|Trail|Blvd|Circle)[^,]*?),\\s*(" +
  TOWNS.map((t) => t.replace(".", "\\.")).join("|") + ")",
  "i"
);

/**
 * Parse a NapaLife issue.
 * @param {string} html   raw newsletter HTML
 * @param {string} url    the issue URL (used as sourceUrl fallback)
 * @param {number} [issueYear]  base year; auto-detected from masthead if omitted
 * @returns {Array} event objects (unfiltered)
 */
export function parseNapaLife(html, url, issueYear) {
  const $ = load(html);
  const bodyText = clean($("body").text());

  // Detect base year from masthead ("... July 13, 2026") if not supplied.
  let year = issueYear;
  if (!year) {
    const my = /(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*(20\d\d)/.exec(bodyText);
    year = my ? +my[1] : new Date().getUTCFullYear();
  }

  const events = [];
  let curDate = null;   // {m,d}
  let curTown = "napa"; // slug
  let futureYear = year; // rolls to 2027 when a "2027" divider appears

  const push = (ev) => { if (ev && ev.title) events.push(ev); };

  // Walk block-level nodes in document order.
  $("h1, h2, h3, p, tr").each((_, el) => {
    const $el = $(el);
    const tag = el.tagName ? el.tagName.toLowerCase() : "";
    const text = clean($el.text());
    if (!text) return;

    // ---- table rows (entertainment grid OR future calendar) ----
    if (tag === "tr") {
      const cells = $el.find("td, th").map((__, td) => clean($(td).text())).get();
      const nonEmpty = cells.filter(Boolean);

      // Day-header row inside a table (some issues embed them in the grid).
      if (nonEmpty.length === 1) {
        const dhRow = dayHeader(nonEmpty[0]);
        if (dhRow) { curDate = dhRow; return; }
      }
      // Town sub-header row inside a table (single bold cell).
      if (nonEmpty.length === 1 && TOWNS.includes(nonEmpty[0])) {
        curTown = TOWN_SLUG[nonEmpty[0]];
        return;
      }
      // "2027" divider inside the future calendar.
      if (nonEmpty.length === 1 && /^20\d\d$/.test(nonEmpty[0])) {
        futureYear = +nonEmpty[0];
        return;
      }

      // Future calendar row: [date, title, source]  (first cell looks like a date)
      const cd = calDate(cells[0] || "");
      if (cd && cells.length >= 2 && cells[1]) {
        const link = $el.find("a").attr("href") || (cells[2] ? `https://www.${cells[2]}` : url);
        push({
          title: cells[1],
          url: link,
          when: null,
          startYMD: ymd(futureYear, cd.m, cd.d),
          endYMD: ymd(futureYear, cd.m, cd.d),
          details: "Upcoming event (NapaLife future calendar).",
          price: "Price not provided.",
          address: "Venue address not provided.",
          town: "all",
          tag: classifyTag(cells[1], cells[2]),
          geo: null,
        });
        return;
      }

      // Nightly entertainment row: [venue, act, time, address]
      if (cells.length >= 3 && curDate) {
        const [venue, act, timeText, addr] = cells;
        if (!venue || !act) return;
        const startYMD = ymd(year, curDate.m, curDate.d);
        const rowTag = classifyTag(act, venue);
        push({
          title: act,
          url,
          when: firstTime(timeText) || null,
          startYMD,
          endYMD: startYMD,
          details: `${act} at ${venue}.`,
          price: "Price not provided.",
          address: addr ? `${venue}, ${addr}.` : `${venue}.`,
          town: curTown,
          tag: rowTag === "any" ? "music" : rowTag, // nightly grid defaults to live music
          geo: geoFor(curTown),
        });
      }
      return;
    }

    // ---- headers ----
    const dh = dayHeader(text);
    if (dh) { curDate = dh; return; }
    if (TOWNS.includes(text)) { curTown = TOWN_SLUG[text]; return; }

    // Section/caption headings that are NOT events.
    if (/(music and entertainment|calendar of future events)\s*$/i.test(text)) return;

    // ---- editorial block: a fully-bold paragraph/heading = event title ----
    const boldText = clean($el.find("b, strong").first().text());
    const isHeading = tag[0] === "h" || (boldText && boldText.length >= text.length - 2);
    if (isHeading && text.length > 4 && text.length < 120 && curDate) {
      // Gather following body paragraphs (as a list) until the next heading/table/header.
      const paras = [];
      let link = $el.find("a").attr("href") || null;
      let node = $el.next();
      let guard = 0;
      while (node.length && guard++ < 14) {
        const ntag = node[0].tagName ? node[0].tagName.toLowerCase() : "";
        if (ntag === "table") break;                       // stop at the entertainment grid
        if (ntag[0] === "h") break;                        // stop at any heading (h1/h2/h3)
        const nt = clean(node.text());
        if (!nt) { node = node.next(); continue; }
        if (dayHeader(nt) || TOWNS.includes(nt)) break;
        const nBold = clean(node.find("b, strong").first().text());
        if (nBold && nBold.length >= nt.length - 2 && nt.length < 120) break; // next heading
        paras.push(nt);
        if (!link) link = node.find("a").attr("href") || null;
        node = node.next();
      }
      // Address line: test each paragraph individually (prefer the last match) so
      // the venue capture can't swallow the comma-less sentence before it.
      let am = null, addrIdx = -1;
      for (let i = paras.length - 1; i >= 0; i--) {
        const mm = ADDR_RE.exec(paras[i]);
        if (mm) { am = mm; addrIdx = i; break; }
      }
      const body = clean(paras.filter((_, i) => i !== addrIdx).join(" "));
      if (!body && !am) return;                            // bare section header -> skip

      const priceM = /\$\s?\d+(?:\.\d{2})?/.exec(body);
      const townName = am ? TOWNS.find((t) => t.toLowerCase() === clean(am[3]).toLowerCase()) : null;
      const startYMD = ymd(year, curDate.m, curDate.d);
      push({
        title: text.replace(/\s*[—-]\s*.*$/, "").trim() || text,
        url: link || url,
        when: firstTime(body),
        startYMD,
        endYMD: startYMD,
        details: body.slice(0, 260) || "Details on website.",
        price: /free|no cover|complimentary/i.test(body) ? "Free."
             : priceM ? `Tickets ${priceM[0].replace(/\s+/g, "")}.` : "Price not provided.",
        address: am ? `${clean(am[1])}, ${clean(am[2])}, ${clean(am[3])}.` : "Venue address not provided.",
        town: townName ? TOWN_SLUG[townName] : curTown,
        tag: classifyTag(text, body),
        geo: geoFor(townName ? TOWN_SLUG[townName] : curTown),
      });
    }
  });

  return dedupeIssue(events);
}

// Intra-issue dedupe: NapaLife prints marquee acts twice (a featured write-up
// AND a nightly-grid row). Drop the bare grid row when a featured event exists
// at the same venue + day + city and either shares a title token or matches
// the time within 30 min. This is where the earlier manual cleanup belongs.
function dedupeIssue(events) {
  const nrm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const venueOf = (e) => {
    const a = e.address;
    if (!a || /not provided/i.test(a)) return "";
    return a.split(",")[0].trim();
  };
  const isTable = (e) => typeof e.details === "string" && e.details.startsWith(`${e.title} at `);
  const tmin = (e) => {
    const m = /(\d{1,2})(?::(\d{2}))?\s*(a\.?m|p\.?m)?/i.exec(e.when || "");
    if (!m) return null;
    let h = +m[1]; const mm = m[2] ? +m[2] : 0; const mer = (m[3] || "").toLowerCase();
    if (mer.startsWith("p") && h !== 12) h += 12;
    if (mer.startsWith("a") && h === 12) h = 0;
    return h * 60 + mm;
  };
  const vmatch = (a, b) => {
    const na = nrm(a), nb = nrm(b);
    if (!na || !nb) return false;
    return na === nb || na.includes(nb) || nb.includes(na);
  };
  const STOP = new Set("the a an at in of on or and with for to live music night pm am event".split(" "));
  const toks = (s) => new Set(nrm(s).split(" ").filter((t) => t.length >= 4 && !STOP.has(t)));
  const featured = events.filter((e) => !isTable(e) && venueOf(e));
  const drop = new Set();
  for (const t of events) {
    if (!isTable(t)) continue;
    const ttl = nrm(t.title);
    const f = featured.find((f) => {
      if (f.startYMD !== t.startYMD || f.town !== t.town || !vmatch(venueOf(f), venueOf(t))) return false;
      const sharedTok = [...toks(t.title)].some((x) => toks(f.title).has(x));
      const nameEcho = ttl.length >= 8 && nrm(`${f.title} ${f.details || ""}`).includes(ttl);
      const timeClose = tmin(t) != null && tmin(f) != null && Math.abs(tmin(t) - tmin(f)) <= 30;
      return sharedTok || nameEcho || timeClose;
    });
    if (f) drop.add(t);
  }
  return events.filter((e) => !drop.has(e));
}

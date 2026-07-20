# Napa Valley Event Finder — Master Brief

**Last updated:** July 19, 2026
**Stored:** this repo, `docs/` (migrated from iCloud Drive → Valley Works Collaborative - Napa Serve → NapaServe → Active/ on July 19, 2026)

## END-OF-SESSION SAVE

The download-docx → iCloud → re-upload flow is retired. These docs are now
updated in place and committed:

```
git add -A && git commit -m "Update EOS docs" && git push origin main
```

## PROJECT OVERVIEW

The Napa Valley Event Finder is a lightweight local event search engine that
powers (1) a standalone embeddable widget for Squarespace and other sites, and
(2) the Event Finder section of NapaServe (napaserve.org/events). It is a
separate Vercel project from NapaServe but feeds into it.

**Core product goal:** Fast, map-forward search across Napa Valley events with
stable map pins, clean category filtering, and reliable date handling — even
when upstream sources are messy.

**Core architectural constraint:** Map pins must not jump between identical
searches. The same venue must always resolve to the same coordinates. This is
achieved via static GEO_HINTS, not dynamic geocoding.

## ARCHITECTURE

### Current (as of July 19, 2026)

- **Frontend:** `widget.html` (standalone iframe embed) + `napaserve-event-finder.jsx` (NapaServe UI)
- **Backend:** Vercel serverless function at `/api/search` — the handler file is `api/search.js`
- **Parsing:** Cheerio HTML scraping of upstream calendar sources, plus two JSON APIs (Google Calendar for Brannan Center, WP REST for St. Helena Chamber)
- **Caching:** In-memory only (`globalThis.__NVF_CACHE__`, 10-minute TTL)
- **Geocoding:** Static GEO_HINTS dict — no live geocoding provider
- **Map:** Leaflet 1.9.4 with OpenStreetMap tiles

> **Path note:** older revisions of these docs referred to the backend as a root
> `search.js`. The handler is `api/search.js`. Parser modules that sit in the
> repo root (e.g. `parseNapaLife.mjs`) are imported as `../parseNapaLife.mjs`.

### Planned (next phase)

- **DB backend:** Read from `community_events` in Supabase (NapaServe) instead of live scraping
- **Stable coords:** Use `event_series.lat/lng` from Supabase instead of GEO_HINTS
- **Scrape fallback:** Only scrape live when DB is sparse for a given query
- **Caching:** Neon Postgres as persistent cache layer (planned, not implemented)

## LIVE ENDPOINTS

- Widget: https://napa-event-finder.vercel.app/widget.html
- API: https://napa-event-finder.vercel.app/api/search
- Sources list: https://napa-event-finder.vercel.app/sources.json
- Map test: https://napa-event-finder.vercel.app/map-test.html

## SCRAPE SOURCES

Sources are defined in `sources.json` and mirrored in the `SOURCES` array in
`api/search.js`. Each source has a dedicated parser.

| Source | Notes |
|---|---|
| Do Napa (donapa.com) | General calendar, multiple towns, good JSON-LD coverage |
| Napa County Library (events.napalibrary.org) | Family/education events, library branches |
| American Canyon Chamber (business.amcanchamber.org) | GrowthZone platform |
| Calistoga Chamber (chamber.calistogachamber.net) | GrowthZone platform |
| Yountville Chamber (web.yountvillechamber.com) | GrowthZone platform |
| Visit Napa Valley (visitnapavalley.com) | Tourism-focused, good structured data |
| Cameo Cinema (cameocinema.com) | Movies only, no structured date data, heading-based |
| **NapaLife (napalife.org)** | **Added July 19, 2026 — weekly newsletter, one URL → many events. See below.** |

> **Documentation debt (flagged July 19, 2026):** `api/search.js` also contains
> parsers that predate this migration but were never added to the source list
> above — Brannan Center (Google Calendar API), Cameo Film Class, Napa County
> Library via CivicEngage (CIDs 59 and 55), St. Helena Public Library, St. Helena
> Chamber (WP REST), NVC Estate Winery (Eventbrite), Town of Yountville, and
> American Canyon. These run as unconditional `tasks.push(...)` calls in the
> handler rather than as `SOURCES` entries. Reconcile the two lists in a future
> session.

### NapaLife — bespoke multi-event parser

NapaLife is a weekly newsletter: **one URL yields many events**. It is
Word-generated "filtered HTML" with no semantic markup, no per-event links, and
no JSON-LD, so it cannot go through the per-event-page extractor.

`parseNapaLife.mjs` instead walks the document as a **state machine**, tracking
the current DATE (day headers) and TOWN (town sub-headers), and reads three
shapes: nightly entertainment tables, the future-events calendar table, and
bold-heading editorial blocks.

- **Config, not code:** the issue number increments weekly, so `listUrl`
  (`https://www.napalife.org/7603.html`) lives in `sources.json` and the
  `SOURCES` array and can be bumped without a code change.
- **Future improvement:** a "latest issue" discovery step so the URL updates
  itself. Not implemented.
- **Type is `calendar`,** so the movies-only filter skips it — same as other
  calendar sources.
- Recurring bar/venue events are stored as dated instances, not as a recurrence rule.
- The wrapper `parseNapaLifeSource()` in `api/search.js` overrides the parser's
  own `geo` and `tag` with the repo's `GEO_HINTS` and `classifyTag`, so
  `api/search.js` stays the single source of truth for both.

### To add a new source

1. Add entry to `sources.json`
2. Write a dedicated parser function in `api/search.js` (do not rely on generic fallback)
3. Add the parser call in the handler tasks array

## EXTRACTION PIPELINE

1. Fetch the list page (`fetchText`, 10-min cache)
2. Extract event page URLs via Cheerio
3. For each URL: `extractEventFromPage()` — JSON-LD first, then DOM fallback
4. `extractOrFallback()` — if page fetch fails, return minimal stub with title only
5. `classifyTag()` — assign category via keyword rules
6. `filterAndRank()` — apply town/type/date filters, sort by `startYMD`
7. `formatWeekender()` — format into `{ header, body, geo }`

Multi-event sources (NapaLife) skip steps 2–4: the parser emits the full event
array directly, which is then normalized and handed to `filterAndRank()`.

**JSON-LD extraction (preferred):**
- `getJsonLdEvents($)` — finds `script[type="application/ld+json"]` blocks
- Extracts: name, startDate, endDate, location.address, offers.price
- Falls back to DOM/meta if JSON-LD absent or incomplete

## DATE HANDLING — NON-NEGOTIABLE RULES

- **NEVER assume today** for a missing date — this was an explicit bug fix
- If date cannot be reliably parsed: exclude from date-scoped queries
- Multi-day events: `overlapsRange(evStart, evEnd, qStart, qEnd)` — both boundaries checked
- Date format: always `YYYY-MM-DD` internally
- Display format: `apDateFromYMD()` → "Sat., March 28" (AP style)
- Time format: `apTimeFromISOClock()` → "7 p.m." / "7:30 p.m."
- Time range: `formatTimeRange(startISO, endISO)` → "7–9 p.m."

## DEDUPLICATION

Two layers, both required:

1. **Cross-source (handler):** dedupe on `header + body` before returning results.
   The three-tier search in `napaserve-event-finder.jsx` dedupes DB vs live
   results on **title + event_date** — never title alone.
2. **Intra-source (new July 19, 2026):** NapaLife prints marquee acts twice — a
   featured editorial write-up *and* a nightly-grid row. `dedupeIssue()` drops
   the bare grid row when a featured event exists at the **same venue + day +
   city** and either shares a title token or matches the time within 30 minutes.

**Corollary rule:** NapaLife is **not** bulk-loaded into `community_events`. The
live Tier-3 parser covers it, and the three-tier search dedupes DB + live
results — inserting NapaLife into the DB would only duplicate it.
`community_events` is for events with no scraper (e.g. printed flyers).

## COORDINATE STABILITY — THE CORE INVARIANT

This is the most important architectural decision in the project. Map pins must
not jump between identical searches. This was the original driver for the entire
architecture.

- GEO_HINTS is the single source of truth for coordinates
- **Seven** town-level hints: napa, st-helena, yountville, calistoga, american-canyon, **rutherford, oakville** (last two added July 19, 2026 for NapaLife)
- **Never dynamically geocode** — results would vary by API call, breaking stability
- Future: `event_series.lat/lng` in Supabase replaces GEO_HINTS with venue-level precision
- Venue-level coords must be set once and never overwritten automatically

## CORS POLICY

- Explicit allowlist — no wildcard
- Allowed: napavalleyfeatures.squarespace.com, napavalleyfeatures.com, www.napavalleyfeatures.com, localhost:3000/5173/5174, napaserve.vercel.app, napaserve.org, www.napaserve.org
- OPTIONS preflight: returns 204 with CORS headers

## KNOWN FAILURE MODES

- **Cheerio import variance:** `load = cheerioNS.load || cheerioNS.default?.load` — always use this pattern, with a throw fallback
- **Source layout changes:** Cheerio selectors break when upstream sites redesign — each parser needs monitoring
- **NapaLife issue number:** `listUrl` is pinned to one weekly issue and must be bumped; a stale URL silently yields old events
- **Rate limiting:** sources may block repeated scraping — in-memory cache helps but is not persistent
- **Empty/malformed pages:** `extractOrFallback()` catches these — returns stub or null
- **Cameo Cinema:** no structured date data — always returns "Showtimes on website." for `when`
- **GrowthZone chambers:** `/events/details/` URL pattern — may change with platform updates

## RELATIONSHIP TO NAPASERVE

**Current state:**
- napa-event-finder.vercel.app = scraping backend (this project)
- napaserve.org/events = UI frontend (`napaserve-event-finder.jsx` calls this API)
- Separate Vercel projects, separate GitHub repos

**Planned convergence:**
- Phase 1 (done): NapaServe builds `event_series` + `community_events` DB with stable coords
- Phase 2 (next): `napaserve-event-finder.jsx` reads from Supabase DB first
- Phase 3: scraper becomes a background refresh job, not a request-time dependency
- Phase 4: napa-event-finder.vercel.app may be retired or become the scrape worker only

The scraper is valuable regardless — it covers sources not yet in the DB and
handles real-time queries for events not yet seeded.

## KNOWN DIVERGENCE — off-architecture tables

A separate `events` / `venues` table pair exists in Supabase from earlier
experimentation. They are a **parallel schema the app does not read** —
`napaserve-event-finder.jsx` reads `community_events`. Left in place; candidate
for `DROP` once confirmed unused.

Possible wrinkle: the SQL editor was observed displaying a different project
name ("Economic Pulse Snapshots") in an earlier session, so these tables may
live in a *different* Supabase project entirely — which would make them
harmless. Confirm before dropping.

## SQUARESPACE EMBED

```html
<iframe src="https://napa-event-finder.vercel.app/widget.html"
        style="width:100%; border:0;" height="980" loading="lazy"></iframe>
```

- `widget.html` detects its own origin and uses it as `API_BASE`
- On Squarespace: must set `window.NVF_API_BASE` in Code Injection → Header
- Without `NVF_API_BASE` on the squarespace.com domain, the widget shows a config error
- Leaflet map renders inside the widget — no separate map embed needed

## GUIDING PRINCIPLES

- **Map-forward:** the map is not optional UI. Every result with a known location gets a pin.
- **Stability over precision:** a stable town-level pin is better than a precise pin that jumps.
- **Conservative date handling:** never invent dates. Exclude rather than guess.
- **Deterministic classification:** same input always produces same category.
- **Graceful degradation:** timeout → partial results. Parser fail → skip source silently.

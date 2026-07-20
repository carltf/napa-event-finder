# Napa Valley Event Finder — Status & To-Do List

**Last updated:** July 19, 2026
**Stored:** this repo, `docs/` (migrated from iCloud on July 19, 2026)

Legend: ✅ done · 🔄 in progress / known issue · ○ not started · ⏸ paused

## Core API & Backend — `api/search.js` (`/api/search`)

- ✅ Category filtering: any | art | music | food | wellness | nightlife | movies
- ✅ Conservative date handling — no "assume today" for missing dates
- ✅ Multi-day event overlap logic (`overlapsRange`)
- ✅ Stable GEO_HINTS coordinates — no dynamic geocoding
- ✅ CORS explicit allowlist (no wildcard)
- ✅ Cheerio import compatibility (load fallback pattern)
- ✅ In-memory cache (10-min TTL, `globalThis`)
- ✅ Concurrency limiter (`mapLimit`, 4 concurrent fetches)
- ✅ Hard timeout handler (24s, returns 504)
- ✅ Weekender output format (header + body + geo)
- ✅ Deduplication on header+body before returning results
- ✅ Show more support (`limit` param, max 10)
- ○ Add Neon Postgres as persistent cache layer
- ○ Add structured logging per source: fetch status, parse count, dedupe count
- ○ Add `/api/search?debug=1` flag for development inspection

## Sources & Parsers

- ✅ `parseDoNapa()` — donapa.com
- ✅ `parseNapaLibrary()` — events.napalibrary.org
- ✅ `parseVisitNapaValley()` — visitnapavalley.com
- ✅ `parseGrowthZone()` — American Canyon, Calistoga, Yountville chambers
- ✅ `parseCameo()` / `parseCameoFilms()` — cameocinema.com
- ✅ **Add parser for NapaLife — DONE July 19, 2026.** `parseNapaLife.mjs`
  (date/town state machine; self-dedupes featured-vs-grid echoes) wired into
  `api/search.js` as a `calendar` source with
  `listUrl https://www.napalife.org/7603.html`. Tests: `test_parseNapaLife.mjs`
  + `test/napalife_fixture.html` — **19/19 passing**. Live parse: 265 events,
  67 in a 7-day window.
- ✅ **Encoding fix — July 20, 2026.** `fetchText()` now decodes by BOM
  (`decodeBuffer()`), not by assuming UTF-8. Issue 7604 is UTF-16LE served with
  no charset; the old code produced mojibake and Cheerio parsed **zero
  elements**, so NapaLife silently returned 0 events without erroring. Guarded
  by `test_decodeBuffer.mjs` (13/13). UTF-8 sources are unaffected.
- ✅ **Bumped NapaLife to issue 7604** (July 20, 2026) — 235 events parsed.
- 🔄 **NapaLife issue URL is pinned** — the issue number increments weekly and
  must be bumped in `sources.json` + `SOURCES`. A stale URL silently serves old
  events. **Always verify a non-zero count after bumping** — encoding varies
  between issues.
- ○ **Add a "latest issue" discovery step for NapaLife** so the URL self-updates
- ○ Add parser for Napa Valley Register events calendar
- ○ Add parser for Festival Napa Valley
- ○ Add parser for Uptown Theatre (uptowntheatrenapa.com)
- ○ Add parser for Blue Note Napa
- ○ Monitor existing parsers for layout changes — set up monthly check
- ○ **Reconcile `SOURCES` with reality** — Brannan Center, Cameo Film Class,
  Napa County Library (CIDs 59/55), St. Helena Library, St. Helena Chamber,
  NVC Winery, Town of Yountville and American Canyon parsers run as
  unconditional `tasks.push(...)` calls and are absent from `sources.json`.

## NapaServe Event Finder (`napaserve-event-finder.jsx`)

### Three-Tier Search — implemented March 26, 2026

- ✅ Tier 1: Supabase `community_events` query (status=approved, date range forward)
- ✅ Tier 2: Recurring pattern detection — ±21 day window across prior 3 years
- ✅ Tier 3: Live scraper at napa-event-finder.vercel.app runs in parallel
- ✅ Unified results — single merged list after Search
- ✅ (N) badge on community-submitted events, (R) on recurring pattern matches
- ✅ Dedup keyed on title + event_date (was title only — caused duplicates)
- ✅ Time parsing fix — 12 a.m. suppressed, unknown times omitted
- ✅ Price merge — DB `price_info` copied to scraper results when missing
- ✅ Inline price extraction — regex finds $amount/free/no cover in body text
- ✅ Unicode escape fix — raw escape sequences render as actual characters
- ✅ Link fallback — `website_url` → `ticket_url` → `source_url` → regex from description
- ✅ `ensureHttp()` helper — bare `www.` URLs get `https://` prepended
- ✅ Orphaned "For more information visit their website." text stripped
- ✅ Show More button goes up to 20 results
- ✅ Result count line: X from community database · Y from web sources · guided by Z patterns
- ○ Wire to use `event_series.lat/lng` for venue-level coords
- ○ Add Night Sky to Type dropdown (queries `astronomical_events`)
- ○ Add loading skeleton while results fetch
- ○ Verify all date range presets already in UI

## Weekender (`DigestCuration.jsx`)

- ✅ Weekender admin shows events grouped by town with (N)/(R) badges
- ✅ Date window expanded from 14 to 30 days
- ✅ Event limit increased from 5 to 20
- ✅ Load more events button (showing X of Y)
- ✅ Link fallback and `ensureUrl()` helper
- ✅ Orphaned "visit their website" text stripped from card rendering
- ✅ Unicode escape fix
- 🔄 Weekender still limited to `community_events` DB — does not pull from live scraper
- ○ Add ability to manually add events directly from Weekender admin UI

## Supabase & Database

- ✅ `community_events` — **2,119 rows as of July 19, 2026** (2,067 approved /
  52 pending). Primary event store. *(Earlier docs said 1,177 — that figure was
  stale.)*
- ✅ RLS fixed March 26 — `event_series`, `event_instances`, `monitoring_rules`
- ✅ Listening Room events updated with `website_url = https://thelisteningroom.org`
- ✅ NapaServe Event Intake Claude Project created — service role key in Project instructions
- ✅ **Dinner and Dance flyer added July 19, 2026** — id **2220**, "Dinner and
  Dance — Fairgrounds Time is Fun Time", Aug 15 2026, 5:30–10 p.m., Tubbs
  Building / Calistoga Fairgrounds, `town=calistoga`, `category=food`,
  `status=approved`, `source=community`, lat/lng `38.578 / -122.5797`. Manual
  community submission from a printed flyer — no scraper covers it.
- ✅ Project ref `csenpchwxxepdvjebsrt` confirmed as **"NapaServe Dashboard
  Project"** — the correct project.
- 🔄 `event_series` — RLS enabled, venue-level coords not yet wired to UI
- 🔄 **239 rows with `source='NapaLife'` need reconciliation.** NapaLife now
  flows through the live Tier-3 parser, so these are candidates for pruning —
  **but do NOT bulk-delete.** The live parser only reads the *current* issue
  (7603): this week plus the future calendar. The 239 DB rows likely span older
  issues and may include upcoming events no longer present in 7603, which the
  live parser would not reproduce. Needs a real reconciliation — decide the
  source of truth for NapaLife, then prune only the truly-duplicated or past
  rows. **Fresh-session job.**
- 🔄 **Off-architecture `events` / `venues` tables** created during earlier
  experimentation — a parallel schema the app does not read. Left in place;
  candidate for `DROP`. May actually live in a *different* Supabase project
  (the SQL editor once displayed "Economic Pulse Snapshots"), which would make
  them harmless. Confirm before dropping. No rush.
- ○ Build `api/events-search.js` in NapaServe — DB query with scraper fallback
- ○ Set up Neon Postgres schema (events, venues, geocode_cache tables)
- ○ Add cron job to refresh scrape cache nightly

## Map & Coordinates

- ✅ Static GEO_HINTS — now **7 towns**
- ✅ **Add GEO_HINTS entries for Oakville and Rutherford — DONE July 19, 2026.**
  `rutherford: 38.4574 / -122.4247`, `oakville: 38.4324 / -122.4014`
- ✅ Map pins stable across identical searches
- ✅ Leaflet 1.9.4 with OSM tiles
- ✅ Popup on pin click showing event name
- ○ Wire to use `event_series.lat/lng` for venue-level coords
- ○ Add GEO_HINTS entries for Angwin and Pope Valley (still outstanding)

## Squarespace Embed

- ✅ `widget.html` embeddable via iframe
- ✅ Auto-detects `API_BASE` from `window.location.origin`
- ✅ `NVF_API_BASE` config check for Squarespace domain
- ✅ napavalleyfeatures.com present in CORS allowlist
- ○ Test embed on napavalleyfeatures.com

## Infrastructure

- ✅ Vercel deployment: GitHub repo auto-deploys on push to `main`
- ✅ `vercel.json` rewrites: `/` and `/widget` → `widget.html`
- ✅ `package.json`: cheerio only dependency
- ✅ `node_modules` git-ignored
- ✅ **EOS docs moved from iCloud .docx into this repo as markdown (July 19, 2026)**
- ○ Add Neon Postgres: `NEON_DATABASE_URL` env var in Vercel

## Known Issues / Monitoring

- 🔄 Cameo Cinema parser — no structured date data, always returns "Showtimes on website."
- 🔄 GrowthZone chamber parsers — fragile to platform updates
- 🔄 NapaLife `classifyTag` mismatch — `parseNapaLife.mjs` has its own richer
  classifier, but the wrapper overrides it with the coarser `classifyTag` from
  `api/search.js`. Intentional (single source of truth) but produces some odd
  categories. Revisit if categories look wrong.
- 🔺 **Silent zero-event sources are the top monitoring gap.** The 7604 encoding
  bug proved a source can go dead while the API still returns `ok: true`. A
  per-source count in the response meta would have caught it instantly.
- ○ Set up monthly check: run all parsers and verify event counts are non-zero
- ○ Add source-level error reporting to API response meta
- ○ Create test fixtures for remaining sources (NapaLife has one)
- ○ Unit tests for: date parsing, overlap logic, classification, dedupe, CORS

## Commits — July 19, 2026

| Commit | Description |
|---|---|
| `59e1654` | Add NapaLife live parser as a calendar source |

## Commits — March 26, 2026

| Commit | Description |
|---|---|
| `4cb219d` | Three-tier search: unified results, dedup fix, time parsing, price merge |
| `90d1257` | Weekender 30-day window, link fallback for DB events |
| `8285b2b` | Fix missing links on community event cards |
| `6ddf06e` | Fix links on all event cards — aggressive URL fallback |
| `0771dbf` | Fix unicode rendering and missing venue links |
| `1547a7a` | Strip orphaned visit-website text, ensure link buttons render |

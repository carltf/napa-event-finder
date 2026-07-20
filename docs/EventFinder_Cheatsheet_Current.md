# Napa Valley Event Finder — Cheatsheet (Current)

**Last updated:** July 19, 2026
**Stored:** this repo, `docs/` (migrated from iCloud on July 19, 2026)

## Live URLs

| What | URL |
|---|---|
| NapaServe Event Finder | https://napaserve.org/events |
| Squarespace widget | https://napa-event-finder.vercel.app/widget.html |
| API endpoint | https://napa-event-finder.vercel.app/api/search |
| Map test | https://napa-event-finder.vercel.app/map-test.html |
| Supabase dashboard | https://supabase.com/dashboard/project/csenpchwxxepdvjebsrt |
| NapaServe repo | https://github.com/carltf/napaserve |
| Event Finder API repo | https://github.com/carltf/napa-event-finder |

## Supabase

- **Project ref:** `csenpchwxxepdvjebsrt` — displays as **"NapaServe Dashboard Project"** (confirmed July 19, 2026; this is the correct project)
- **URL:** https://csenpchwxxepdvjebsrt.supabase.co
- **Anon key (publishable, safe in front-end code):** `sb_publishable_r-Ntp7zKRrH3JIVAjTKYmA_0szFdYGJ`
- **Service role key:** stored ONLY in the NapaServe Event Intake Claude Project — never here, never in GitHub

## API Contract — GET /api/search

### Query Parameters

| Param | Values | Notes |
|---|---|---|
| town | all / napa / yountville / st-helena / calistoga / american-canyon | Default: all |
| type | any / art / music / food / wellness / nightlife / movies | Default: any |
| start | YYYY-MM-DD | Default: today |
| end | YYYY-MM-DD | Default: today+14 |
| limit | 1–10 | **Default: 5, max 10** |

> The `limit` cap of 10 is enforced server-side. When spot-checking a source's
> yield, the API response count is truncated by this cap and is **not** the true
> parse count — verify yield by running the parser directly.

### Response Shape

```json
{ "ok": true, "timeout": false, "count": 5,
  "results": [{ "header": "", "body": "", "geo": null }],
  "map": [{ "name": "", "lat": 0, "lon": 0 }] }
```

## GEO_HINTS — Stable Coordinates

| Town | Lat | Lon |
|---|---|---|
| napa | 38.2975 | -122.2869 |
| st-helena | 38.5056 | -122.4703 |
| yountville | 38.3926 | -122.3631 |
| calistoga | 38.578 | -122.5797 |
| american-canyon | 38.1686 | -122.2608 |
| **rutherford** | **38.4574** | **-122.4247** |
| **oakville** | **38.4324** | **-122.4014** |

Rutherford and Oakville added July 19, 2026 (NapaLife has events in both).

## Parsers

**In the `SOURCES` array:** `parseDoNapa`, `parseNapaLibrary`, `parseVisitNapaValley`,
`parseGrowthZone` (× 3 chambers), `parseCameo` / `parseCameoFilms`,
**`parseNapaLifeSource`**.

**Run unconditionally in the handler (not in `SOURCES`):** `parseCameoFilmClass`,
`parseBrannanCenter`, `parseNapaCountyLibrary` (CID 59 Napa, CID 55 Yountville),
`parseStHelenaLibrary`, `parseTownOfYountville`, `parseAmericanCanyon`,
`parseStHelenaChamber`, `parseNVCWinery`.

### NapaLife quick facts

- Parser: `parseNapaLife.mjs` (repo root) — imported into `api/search.js` as `../parseNapaLife.mjs`
- Wrapper: `parseNapaLifeSource()` re-derives `geo` and `tag` from `api/search.js`
- `listUrl`: `https://www.napalife.org/7604.html` — **issue number increments weekly, bump it in `sources.json` + `SOURCES`**
- Type `calendar`, so the movies-only filter skips it
- Tests: `test_parseNapaLife.mjs` (19/19) + `test_decodeBuffer.mjs` (13/13)
- Live parse: issue 7603 → 265 events; issue 7604 → 235 events

> ⚠️ **After bumping the issue number, always verify a non-zero event count.**
> Issue 7604 was served as **UTF-16LE** while 7603 was UTF-8 — the encoding
> varies between issues and the server sends no charset. See "Encoding" below.

## Encoding — `decodeBuffer()`

`fetchText()` decodes response bytes by **sniffing the byte-order mark**, not by
assuming UTF-8:

| BOM | Encoding |
|---|---|
| `ff fe` | utf-16le |
| `fe ff` | utf-16be |
| none | utf-8 (unchanged behaviour) |

**Why it exists:** NapaLife 7604 was UTF-16LE served as `text/html` with no
charset. `res.text()` decoded it as UTF-8 → NUL-interleaved mojibake → Cheerio
parsed **zero elements**. The source returned 0 events *without erroring* — a
green deploy with a dead source. Guarded by `test_decodeBuffer.mjs`.

## Cache & Timeout Settings

- In-memory cache TTL: 10 minutes (`CACHE_TTL_MS`)
- Per-fetch timeout: 8 seconds
- Aggregate timeout: 22 seconds
- Hard handler timeout: 24 seconds (returns 504)
- Concurrency limit: 4 parallel fetches (`mapLimit`)

## Key File Locations

| File | Path |
|---|---|
| Event Finder UI | `~/Desktop/napaserve/economic-pulse-app/src/napaserve-event-finder.jsx` |
| Weekender admin | `~/Desktop/napaserve/economic-pulse-app/src/DigestCuration.jsx` |
| Weekender API | `~/Desktop/napaserve/api/digest-format.js`, `digest-intro.js`, `digest-send.js` |
| **Scraper backend** | `~/Desktop/napa-event-finder/api/search.js` (separate repo) |
| NapaLife parser | `~/Desktop/napa-event-finder/parseNapaLife.mjs` |
| Sources list | `~/Desktop/napa-event-finder/sources.json` |
| Widget HTML | `~/Desktop/napa-event-finder/widget.html` |
| **Master docs** | `~/Desktop/napa-event-finder/docs/` (in git as of July 19, 2026) |

## `community_events` — column conventions

Confirmed against live data July 19, 2026 (**2,119 rows**: 2,067 approved / 52 pending).

| Column | Convention |
|---|---|
| `town` | lowercase slug — `calistoga`, `st-helena` (294 vs 5 for capitalized; slug wins) |
| `category` | `food` (768), `community` (520), `music` (399), `art` (145), `theatre`, `wellness`, `movies`, `nightlife` |
| `status` | `approved` (2,067) / `pending` (52) — approved is the published value |
| `source` | `google_sheet`, `napaserve_submission`, `weekender`, `NapaServe`, `NapaLife`, `community`, `Cameo Cinema` |
| `start_time` / `end_time` | text, `HH:MM` 24-hour |
| `lat` / `lng` | numeric; **town-level centroids are the convention** (159 Calistoga rows use `38.578 / -122.5797`) |
| `event_date` / `end_date` | date |

## Critical Rules — Quick Reference

- **BUILD ≠ DEPLOY** — always `git push origin main`
- Cheerio import: `load = cheerioNS.load || cheerioNS.default?.load` (+ throw fallback)
- CORS: allowlist only, never wildcard
- Coordinates: GEO_HINTS only, never dynamic geocoding
- Dates: never assume today — exclude or mark dateUnknown
- Multi-day: use `overlapsRange()`, never filter on start date alone
- Dedup: key on **title + event_date**
- Intra-source dedup: drop a nightly-grid row when a featured event exists at the same venue/day/city
- URLs: always `ensureHttp()` — prepend `https://` to bare `www.` links
- Shell: straight ASCII single quotes — never smart quotes
- Supabase writes: service role key only — never anon key for writes
- Event intake: never invent details — null if unknown, follow links
- **Do not bulk-load NapaLife into `community_events`** — the live Tier-3 parser covers it

## Squarespace Embed Snippet

```html
<script>window.NVF_API_BASE = "https://napa-event-finder.vercel.app";</script>
<iframe src="https://napa-event-finder.vercel.app/widget.html"
        style="width:100%; border:0;" height="980" loading="lazy"></iframe>
```

## Deploy Command

```
git add -A && git commit -m "your message" && git push origin main
```

## Event Intake — Quick Start

Open NapaServe Event Intake Claude Project → paste email/press release → review
preview → say "go ahead".

- Claude fetches any URLs in the email for confirmed details
- Never invents or infers — null if unknown
- Inserts with `status=approved`, `source=napaserve_submission`
- Events appear immediately with (N) badge in Event Finder and Weekender

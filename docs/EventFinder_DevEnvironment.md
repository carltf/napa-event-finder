# Napa Valley Event Finder — Developer Environment & Working Rules

**Last updated:** July 19, 2026
**Stored:** this repo, `docs/` (migrated from iCloud on July 19, 2026)

## End-of-Session Protocol — NEW (July 19, 2026)

The download-docx → iCloud → re-upload flow is **retired**. These four docs now
live in this repo as markdown and are updated in place and committed at the end
of every session.

**Step 1 — ask Claude Code to update the docs:**

> We are ending this session. Please update the four docs in `docs/` to reflect
> everything we did today. Mark completed items, update changed URLs or
> architecture decisions, add new rules, update the Last updated date.

**Step 2 — commit and push:**

```
git add -A && git commit -m "Update EOS docs" && git push origin main
```

**Step 3 — start the next session:**

> I'm continuing work on the Napa Valley Event Finder. Read the four docs in
> `docs/` and confirm you understand the current state, what's done, what's in
> progress, and the immediate next steps.

The old iCloud `.docx` originals remain at
`~/Library/Mobile Documents/com~apple~CloudDocs/Valley Works Collaborative - Napa Serve/NapaServe/Active/Older Drafts/`
as a historical snapshot (last written March 26, 2026). They are no longer the
source of truth.

## What Goes in Each Doc

| Document | Contains |
|---|---|
| `EventFinder_Master_Brief.md` | Architecture decisions, guiding principles, source list, pipeline, date rules, coordinate rules, CORS policy, NapaServe relationship, Squarespace embed plan, agent constraints |
| `EventFinder_Cheatsheet_Current.md` | Live URLs, API contract, parser names, GEO_HINTS coordinates, DB column conventions, CORS list, cache settings, timeout values, embed snippet, critical rules — the daily reference card |
| `EventFinder_Status_TodoList.md` | ✅ / 🔄 / ○ / ⏸ status for every feature and task. Updated at end of every session. |
| `EventFinder_DevEnvironment.md` | THIS DOC. Machine state, tools, Claude Code rules, repo paths, session protocol. |

## Machine & Environment — Confirmed State

This machine is Tim's MacBook Air. All tools below are installed and confirmed.
**Do not include install steps in any prompt or workflow.**

| Tool / Service | Value / Location | Status |
|---|---|---|
| Claude Code | Installed globally — launch with `claude` | ✓ Confirmed |
| Node.js / npm | Installed globally | ✓ Confirmed |
| Git | Installed globally | ✓ Confirmed |
| Shell | zsh on macOS | ✓ Confirmed |
| Python 3 | Installed globally | ✓ Confirmed |
| pandoc | `/opt/homebrew/bin/pandoc` | ✓ Confirmed |
| Supabase CLI | `/opt/homebrew/bin/supabase` — authenticated; token in macOS keychain (`Supabase CLI`, `go-keyring-base64:` wrapped) | ✓ Confirmed |
| Repo (Event Finder API) | `~/Desktop/napa-event-finder` | ✓ Confirmed |
| Repo (NapaServe) | `~/Desktop/napaserve` | ✓ Confirmed |
| Vercel | Deploys via GitHub push to `main` — no local CLI needed | ✓ Confirmed |
| Supabase | Live at `csenpchwxxepdvjebsrt.supabase.co` ("NapaServe Dashboard Project") | ✓ Confirmed |
| Neon Postgres | Not yet implemented — in-memory cache only | ○ Planned |

**Note:** `psql` is **not** installed. Ad-hoc SQL runs through the Supabase
Management API (`POST https://api.supabase.com/v1/projects/{ref}/database/query`)
using the CLI's stored access token.

## Repo & File Structure

### Event Finder API (this repo, separate Vercel project)

```
Repo:       github.com/carltf/napa-event-finder   (PUBLIC)
Local:      ~/Desktop/napa-event-finder
Live API:   https://napa-event-finder.vercel.app/api/search
Live widget:https://napa-event-finder.vercel.app/widget.html
```

- `api/search.js` → **the entire backend** (Vercel serverless handler). Not a root `search.js`.
- `parseNapaLife.mjs` → NapaLife multi-event parser (repo root; imported as `../parseNapaLife.mjs`)
- `test_parseNapaLife.mjs` + `test/napalife_fixture.html` → parser tests (`node test_parseNapaLife.mjs`)
- `sources.json` → scrape source list
- `widget.html` → standalone Squarespace embed
- `vercel.json` → rewrites `/` and `/widget` to `widget.html`
- `package.json` → cheerio only dependency
- `docs/` → these four master docs

> **The repo is public.** Never commit a service role key, private token, or
> anything else that must stay secret.

### NapaServe (main site)

```
Repo:  github.com/carltf/napaserve
Local: ~/Desktop/napaserve
Live:  napaserve.org
```

- `napaserve-event-finder.jsx` → `/events` page — three-tier Event Finder UI
- `economic-pulse-app/src/DigestCuration.jsx` → Weekender admin
- `api/digest-format.js`, `digest-intro.js`, `digest-send.js` → Weekender API

## Claude Code — Working Rules

### Always

- `cd` into the repo before running `claude`
- Read the relevant file(s) before making changes
- **Fetch one row from the relevant DB table to confirm column names before writing any query**
- Keep all existing UI styling, map, submit form, and nav exactly as-is unless explicitly asked to change them
- Always end code changes with `git push origin main` — build does not deploy
- Present every prompt and multi-line terminal command with a copy button

### Never

- Include install steps for Claude Code, Node, npm, git, or Python — all present
- Ask "do you have Claude Code installed?" — confirmed yes
- Use wildcards in CORS config — allowlist only
- Dynamically geocode coordinates — use GEO_HINTS only
- Assume today for missing event dates — exclude or mark dateUnknown
- **Insert DB rows without showing a preview and waiting for confirmation first**

## Critical Rules — Never Break These

- **BUILD DOES NOT DEPLOY** — always run `git push origin main`. Vercel deploys on push only.
- Cheerio import: always `load = cheerioNS.load || cheerioNS.default?.load` with a throw fallback
- CORS: never wildcard — allowlist only
- Coordinates: never dynamically geocode — always use GEO_HINTS
- Date inference: never assume today for missing dates
- Multi-day events: use `overlapsRange()` — never filter on start date alone
- Deduplication: key on **title + event_date** (not title alone)
- Intra-source dedupe: drop a nightly-grid row when a featured event exists at the same venue/day/city
- Shell env vars: straight ASCII single quotes always — never smart/curly quotes; never "normalize" credentials or URLs
- Supabase writes: service role key only — anon key is read-only in production
- Service role key lives only in Claude Project instructions — never in docs, never in GitHub
- `community_events` writes are normally Dev-thread / intake-pipeline territory — manual inserts need explicit authorization
- Event intake: never invent or infer details — use only what is explicitly stated in the source or on linked pages
- **Do not bulk-load NapaLife into `community_events`** — the live Tier-3 parser covers it and the three-tier search dedupes DB + live results

## Three-Tier Event Search Architecture

Implemented in `napaserve-event-finder.jsx`.

**Tier 1 — Supabase DB query (instant, runs first)**
- Query `community_events` with `status=approved`, date range from today forward
- Events with `source='community'` or `'napaserve_submission'` get the (N) badge and sort first
- All DB results display before scraper results

**Tier 2 — Pattern matching from DB history**
- Queries `community_events` for events from prior years within ±21 days of the current calendar date
- Fires separate queries for each of the last 3 years using the same MM-DD window
- Extracts unique normalized titles and venue names as recurring hints
- Passes hints to Tier 3 as a dedup signal

**Tier 3 — Live scraper (confirmation + discovery)**
- Calls `https://napa-event-finder.vercel.app/api/search` in parallel with the DB query
- Scraper results matching a DB title (60% word overlap, case-insensitive) update the DB result rather than creating a duplicate
- Genuinely new scraper results append after all DB results
- Show More goes up to 20 results

## Supabase — Database

```
URL:  https://csenpchwxxepdvjebsrt.supabase.co
Ref:  csenpchwxxepdvjebsrt  ("NapaServe Dashboard Project")
Anon key (publishable, safe in front-end code):
      sb_publishable_r-Ntp7zKRrH3JIVAjTKYmA_0szFdYGJ
Service role key: NapaServe Event Intake Claude Project instructions only
```

### Key Tables

- `community_events` → **2,119 rows** (July 19, 2026). Primary event store.
- `event_series` → RLS fixed March 26. Venue-level stable coordinates planned here.
- `event_instances` → RLS fixed March 26.
- `astronomical_events` → Night sky events for the NapaServe Night Sky category.
- ⚠️ `events` / `venues` → **off-architecture.** Parallel schema created during
  earlier experimentation; the app does not read them. Do not read/write/drop
  without an explicit decision. May live in a different Supabase project.

### RLS Fix — run in the Supabase SQL Editor if UNRESTRICTED warnings return

```sql
ALTER TABLE event_series ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_instances ENABLE ROW LEVEL SECURITY;
ALTER TABLE monitoring_rules ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Public read access" ON event_series FOR SELECT USING (true);
CREATE POLICY "Public read access" ON event_instances FOR SELECT USING (true);
CREATE POLICY "Authenticated read only" ON monitoring_rules FOR SELECT TO authenticated USING (true);
```

## Event Intake Protocol

To add events from emails or press releases to the DB:

1. Open the NapaServe Event Intake Claude Project (private — contains service role key)
2. Paste the email or press release into a new conversation
3. Claude fetches any URLs in the email, extracts all event details, shows a preview
4. Review the preview — confirm or correct
5. Say "go ahead" — Claude inserts rows with `status=approved`, `source=napaserve_submission`
6. Events appear immediately in Event Finder and Weekender with the (N) badge

Rules for the intake Claude Project:

- Never invent, guess, or infer any detail not explicitly in the source or on a linked page
- Always fetch linked URLs and extract confirmed details before inserting
- Always preview before inserting — never insert without confirmation
- Leave fields null if unknown — do not fill in placeholders

## CORS Allowlist

Explicit allowlist in `api/search.js` — never use a wildcard.

- https://napavalleyfeatures.squarespace.com
- https://napavalleyfeatures.com
- https://www.napavalleyfeatures.com
- http://localhost:3000, http://localhost:5173, http://localhost:5174
- https://napaserve.vercel.app
- https://napaserve.org
- https://www.napaserve.org

## Quick Reference URLs

- Event Finder (NapaServe): https://napaserve.org/events
- Event Finder widget: https://napa-event-finder.vercel.app/widget.html
- Event Finder API: https://napa-event-finder.vercel.app/api/search
- Map test: https://napa-event-finder.vercel.app/map-test.html
- Supabase dashboard: https://supabase.com/dashboard/project/csenpchwxxepdvjebsrt

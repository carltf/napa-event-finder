# Napa Valley Event Finder (Squarespace Embed)

## Deploy (recommended: Vercel)
1. Create a GitHub repo (ex: `napa-event-finder`) and upload these files.
2. In Vercel: New Project → Import that repo → Deploy.
3. After deploy, your URLs will be:
   - Widget: `https://YOUR-SITE.vercel.app/widget.html`
   - API: `https://YOUR-SITE.vercel.app/api/search?...`
   - Sources: `https://YOUR-SITE.vercel.app/sources.json`

## Embed on Squarespace
Add a Code Block:

```html
<iframe
  src="https://YOUR-SITE.vercel.app/widget.html"
  style="width:100%; border:0;"
  height="980"
  loading="lazy"
></iframe>
```

## Movies
Choose “Movies (Cameo Cinema)” in the Type dropdown.

## Add sources
Edit `sources.json`. For best results, add a dedicated parser in `api/search.js` for each new site.

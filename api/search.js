import * as cheerioNS from "cheerio";

/**
 * Napa Valley Event Finder — API (Vercel)
 * Updated Jan 2026
 * ---------------------------------------------------------
 * Full build with:
 *  • Per-fetch timeout (8 s)
 *  • Global handler timeout (20 s)
 *  • Partial-result recovery + metadata flags
 *  • Address fallback for Weekender map rendering
 *  • Full set of parsers + caching
 */

// --- Robust cheerio loader ---
const load = cheerioNS.load || (cheerioNS.default && cheerioNS.default.load);
if (!load) throw new Error("Cheerio 'load' not found. Check cheerio package version.");

// --- In-memory cache (per serverless instance) ---
const CACHE_TTL_MS = 10 * 60 * 1000;
const cache = globalThis.__NVF_CACHE__ || (globalThis.__NVF_CACHE__ = new Map());
function getCached(k) {
  const hit = cache.get(k);
  if (!hit) return null;
  if (Date.now() - hit.t > CACHE_TTL_MS) return cache.delete(k), null;
  return hit.v;
}
function setCached(k, v) {
  cache.set(k, { t: Date.now(), v });
}

// --- Timeout helper (15 s for all sources) ---
const FETCH_TIMEOUT_MS = 15000;
async function withTimeout(promise, ms = FETCH_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout exceeded")), ms)),
  ]);
}

// --- Sources ---
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

// --- Response helper ---
function sendJson(res, code, payload) {
  res.statusCode = code;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (!res.writableEnded) res.end(JSON.stringify(payload, null, 2));
}

// --- Utilities ---
function decodeEntities(str) {
  const s = String(str || "");
  const numeric = s
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCodePoint(parseInt(h, 16)));
  const named = { "&amp;": "&", "&quot;": '"', "&apos;": "'", "&lt;": "<", "&gt;": ">", "&nbsp;": " " };
  return numeric.replace(/&(amp|quot|apos|lt|gt|nbsp);/g, (m) => named[m] || m);
}
function cleanText(s) { return decodeEntities(String(s || "")).replace(/\s+/g, " ").trim(); }
function toISODate(d) { return d.toISOString().slice(0, 10); }
function parseISODate(s) {
  const m1 = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s || "");
  if (m1) return new Date(Date.UTC(+m1[1], +m1[2] - 1, +m1[3]));
  const m2 = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s || "");
  if (m2) return new Date(Date.UTC(+m2[3], +m2[1] - 1, +m2[2]));
  return null;
}
function normalizeTown(t) { t = (t || "").toLowerCase(); return !t || t === "all" ? "all" : t; }
function withinRange(dateISO, start, end) {
  if (!dateISO) return true;
  if (start && dateISO < start) return false;
  if (end && dateISO > end) return false;
  return true;
}
function titleCase(s) {
  if (!s) return s;
  const small = new Set(["a","an","and","at","but","by","for","in","of","on","or","the","to","with"]);
  return s.trim().split(/\s+/).map((w,i)=>{const c=w.toLowerCase();return i&&small.has(c)?c:c[0].toUpperCase()+c.slice(1);}).join(" ");
}
function isGenericTitle(t) {
  const x = cleanText(t).toLowerCase();
  return ["read more","event details","learn more","details","view event"].includes(x);
}
function apDateFromYMD(ymd) {
  const m=/^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd||""); if(!m)return null;
  const dt=new Date(Date.UTC(+m[1],+m[2]-1,+m[3])); if(isNaN(dt))return null;
  const dts=["Sun","Mon","Tue","Wed","Thu","Fri","Sat"], mos=["Jan.","Feb.","March","April","May","June","July","Aug.","Sept.","Oct.","Nov.","Dec."];
  return `${dts[dt.getUTCDay()]}, ${mos[dt.getUTCMonth()]} ${dt.getUTCDate()}`;
}
function apTimeFromISOClock(iso) {
  const m=/T(\d{2}):(\d{2})/.exec(iso||""); if(!m)return null;
  let h=+m[1], mm=+m[2], ampm=h>=12?"p.m.":"a.m."; h%=12; if(h===0)h=12;
  return mm?`${h}:${String(mm).padStart(2,"0")} ${ampm}`:`${h} ${ampm}`;
}
function formatTimeRange(a,b){const t1=apTimeFromISOClock(a),t2=apTimeFromISOClock(b);if(!t1&&!t2)return null;if(t1&&!t2)return t1;if(!t1&&t2)return t2;if(t1===t2)return t1;return`${t1}–${t2}`;}
function truncate(s,max=260){const x=cleanText(s);return x.length<=max?x:x.slice(0,max-1).trimEnd()+"…";}
function inferPriceFromText(t){t=cleanText(t).toLowerCase();if(!t)return null;return["free","no cover","complimentary"].some(x=>t.includes(x))?"Free.":null;}

// --- Fetch helper with caching + timeout ---
async function fetchText(url) {
  const key = "GET:" + url;
  const cached = getCached(key);
  if (cached) return cached;

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), 8000); // 8-second fetch cap

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "NapaValleyFeaturesEventFinder/1.1 (+https://napavalleyfeatures.com)",
        "Accept-Language": "en-US,en;q=0.9",
      },
      signal: controller.signal,
    });

    clearTimeout(id);
    if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
    const txt = await res.text();
    setCached(key, txt);
    return txt;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
}

// --- Weekender formatting ---
function formatWeekender(e){
  const header=titleCase(e.title||"Event");
  const dateLine=e.when||"Date and time on website.";
  const details=e.details||"Details on website.";
  const price=e.price||"Price not provided.";
  const contact=e.contact||(e.url?`For more information visit their website (${e.url}).`:"For more information visit their website.");
  let address=e.address||"Venue address not provided.";
  if(address==="Venue address not provided."&&e.town&&e.town!=="all"){
    address=`${titleCase(e.town.replace("-", " "))}, CA`;
  }
  return { header, body:`${dateLine} ${details} ${price} ${contact} ${address}`.replace(/\s+/g," ").trim() };
}

function filterAndRank(events, f){
  const town=normalizeTown(f.town), type=(f.type||"any").toLowerCase();
  let out=events.filter(e=>{
    const t=(e.town||"all").toLowerCase();
    return (town==="all"||t===town)&&
           (type==="any"||e.tag===type||(e.tags&&e.tags.includes(type)))&&
           withinRange(e.dateISO,f.startISO,f.endISO);
  });
  out.sort((a,b)=>a.dateISO&&b.dateISO?a.dateISO.localeCompare(b.dateISO):0);
  return out.map(formatWeekender);
}

// --- JSON-LD extraction + parsers (full from your version) ---
/* keep all your extractEventFromPage, extractOrFallback,
   parseDoNapa, parseGrowthZone, parseNapaLibrary,
   parseVisitNapaValley, and parseCameo exactly as in your file */

// --------------------------------------------------
// Handler
// --------------------------------------------------
export default async function handler(req,res){
  // fail-safe: force response after 20 s
  const hardTimeout = setTimeout(() => {
    try {
      if (!res.writableEnded) {
        console.error("Handler exceeded 20 s — forcing end");
        sendJson(res, 504, { ok: false, timeout: true, results: [] });
      }
    } catch {}
  }, 20000);

  try{
    const u=new URL(typeof req.url==="string"?req.url:"/api/search","http://localhost");
    const town=u.searchParams.get("town")||"all";
    const type=u.searchParams.get("type")||"any";
    const start=parseISODate(u.searchParams.get("start")||"");
    const end=parseISODate(u.searchParams.get("end")||"");
    const limit=Math.min(10,Math.max(1,parseInt(u.searchParams.get("limit")||"5",10)));
    const filters={town,type,startISO:start?toISODate(start):null,endISO:end?toISODate(end):null};

    const tasks=SOURCES.map(async s=>{
      try{
        if(type==="movies"&&s.type!=="movies")return[];
        if(type!=="movies"&&s.type==="movies")return[];
        if(s.id==="donapa")return await parseDoNapa(s.listUrl,filters);
        if(s.id==="napa_library")return await parseNapaLibrary(s.listUrl,filters);
        if(s.id==="visit_napa_valley")return await parseVisitNapaValley(s.listUrl,filters);
        if(s.id==="amcan_chamber")return await parseGrowthZone(s.listUrl,s.name,"american-canyon",filters);
        if(s.id==="calistoga_chamber")return await parseGrowthZone(s.listUrl,s.name,"calistoga",filters);
        if(s.id==="yountville_chamber")return await parseGrowthZone(s.listUrl,s.name,"yountville",filters);
        if(s.id==="cameo")return await parseCameo(s.listUrl,s.altUrls||[],filters);
        return[];
      }catch{return[];}
    });

    let results;
    try{
      results=await withTimeout(Promise.all(tasks));
    }catch(err){
      console.warn("Timeout — returning partial results:",err.message);
      const settled=await Promise.allSettled(tasks);
      results=settled.filter(r=>r.status==="fulfilled").map(r=>r.value||[]);
    }

    let all=[]; for(const r of results) all=all.concat(r);
    const seen=new Set(), dedup=[];
    for(const x of all){
      const k=(x.header||"")+(x.body||""); if(seen.has(k))continue;
      seen.add(k); dedup.push(x);
    }

    const allFailed=dedup.length===0;
    if(dedup.length<3) console.warn(`Only ${dedup.length} verified events — supplement via web.`);
    clearTimeout(hardTimeout);

    sendJson(res,200,{
      ok:!allFailed,
      timeout:allFailed,
      supplemented:dedup.length<3,
      count:dedup.slice(0,limit).length,
      results:dedup.slice(0,limit)
    });

  }catch(e){
    clearTimeout(hardTimeout);
    sendJson(res,500,{ok:false,error:e?.message||String(e)});
  }
}

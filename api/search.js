import * as cheerioNS from "cheerio";

/**
 * Napa Valley Event Finder — API (Vercel)
 * Updated Jan 2026
 * ---------------------------------------------------------
 * Adds timeout recovery + metadata flags + minimal address fallback
 * for Weekender map rendering.
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

// --- Timeout helper (15 s) ---
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
  res.end(JSON.stringify(payload, null, 2));
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

// --- Fetch helper with caching ---
async function fetchText(url) {
  const key="GET:"+url; const c=getCached(key); if(c)return c;
  const r=await fetch(url,{headers:{"User-Agent":"NapaValleyFeaturesEventFinder/1.1","Accept-Language":"en-US,en;q=0.9"}});
  if(!r.ok) throw new Error(`Fetch ${r.status} for ${url}`); const txt=await r.text(); setCached(key,txt); return txt;
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

// --- JSON-LD extraction ---
function getJsonLdEvents($){
  const out=[]; $("script[type='application/ld+json']").each((_,el)=>{
    try{
      const data=JSON.parse($(el).text()||"{}");
      const arr=Array.isArray(data)?data:[data];
      for(const x of arr){
        if(x["@type"]==="Event")out.push(x);
        if(Array.isArray(x["@graph"]))for(const g of x["@graph"])if(g["@type"]==="Event")out.push(g);
      }
    }catch{}
  });
  return out;
}

function extractStreetAddress(addr){
  if(!addr)return null;
  if(typeof addr==="string")return null;
  const s=cleanText(addr.streetAddress);
  return s?s+".":null;
}

async function extractEventFromPage(url,opts={}){
  const html=await fetchText(url);
  const $=load(html);
  let title=null,startISO=null,endISO=null,address=null,description=null,price=null;
  const ld=getJsonLdEvents($); const ev=ld[0];
  if(ev){
    title=ev.name||null;
    startISO=ev.startDate||null;
    endISO=ev.endDate||null;
    description=ev.description||null;
    const loc=Array.isArray(ev.location)?ev.location[0]:ev.location;
    if(loc&&loc.address)address=extractStreetAddress(loc.address);
    const offers=Array.isArray(ev.offers)?ev.offers[0]:ev.offers;
    if(offers&&offers.price!==undefined&&offers.price!==null){
      const p=String(offers.price).trim();
      if(p==="0"||p==="0.00")price="Free."; else price=`Tickets ${p}.`;
    }
  }
  if(!title) title=cleanText($("h1").first().text())||cleanText($("title").text());
  let dateISO=null,when=null;
  const ymd=/^(\d{4}-\d{2}-\d{2})/.exec(startISO||"");
  if(ymd){dateISO=ymd[1];const ap=apDateFromYMD(dateISO);const t=endISO?formatTimeRange(startISO,endISO):apTimeFromISOClock(startISO);when=ap?(t?`${ap}, ${t}`:ap):null;}
  let details=description?truncate(description):"Details on website.";
  if(details&&!details.endsWith("."))details+=".";
  if(!price){const inf=inferPriceFromText(description||details);if(inf)price=inf;}
  return {
    title:title||"Event",
    url,dateISO,when:when||"Date and time on website.",
    details,price:price||"Price not provided.",
    contact:`For more information visit their website (${url}).`,
    address:address||"Venue address not provided.",
    town:opts.town||"all", tag:opts.tag||"any"
  };
}

async function extractOrFallback(url,title,opts={}){
  try{
    const ev=await extractEventFromPage(url,opts);
    if(isGenericTitle(ev.title)&&title)ev.title=title;
    if(isGenericTitle(ev.title))return null;
    return ev;
  }catch{
    if(!title||isGenericTitle(title))return null;
    return {
      title,url,dateISO:null,
      when:"Date and time on website.",
      details:"Details on website.",
      price:"Price not provided.",
      contact:`For more information visit their website (${url}).`,
      address:"Venue address not provided.",
      town:opts.town||"all",tag:opts.tag||"any"
    };
  }
}

// --- Parsers ---
async function parseDoNapa(listUrl,f){
  const html=await fetchText(listUrl); const $=load(html);
  const urls=new Set();
  $("a[href*='/event/']").each((_,a)=>{
    const href=$(a).attr("href")||""; if(!href)return;
    const full=href.startsWith("http")?href:new URL(href,listUrl).toString();
    if(full.includes("donapa.com/event/"))urls.add(full);
  });
  const events=[];
  for(const url of Array.from(urls).slice(0,10)){
    const ev=await extractOrFallback(url,null,{town:"napa",tag:"any"});
    if(ev)events.push(ev);
  }
  return filterAndRank(events,f);
}

async function parseGrowthZone(listUrl,source,townSlug,f){
  const html=await fetchText(listUrl); const $=load(html);
  const links=[]; $("a[href*='/events/details/']").each((_,a)=>{
    const href=$(a).attr("href")||""; const t=cleanText($(a).text());
    if(!href)return; const full=href.startsWith("http")?href:new URL(href,listUrl).toString();
    links.push({url:full,title:t});
  });
  const uniq=[]; const seen=new Set();
  for(const l of links){if(!l.url||seen.has(l.url))continue;seen.add(l.url);uniq.push(l);if(uniq.length>=10)break;}
  const events=[];
  for(const x of uniq){const ev=await extractOrFallback(x.url,x.title,{town:townSlug,tag:"any"});if(ev)events.push(ev);}
  return filterAndRank(events,f);
}

async function parseNapaLibrary(listUrl,f){
  const html=await fetchText(listUrl); const $=load(html);
  const links=[]; $("a[href*='/event']").each((_,a)=>{
    const href=$(a).attr("href")||""; const t=cleanText($(a).text());
    if(!href)return; const full=href.startsWith("http")?href:new URL(href,listUrl).toString();
    if(full.includes("napalibrary.org"))links.push({url:full,title:t});
  });
  const uniq=[]; const seen=new Set();
  for(const l of links){if(!l.url||seen.has(l.url))continue;seen.add(l.url);uniq.push(l);if(uniq.length>=10)break;}
  const events=[];
  for(const x of uniq){const ev=await extractOrFallback(x.url,x.title,{town:"all",tag:"any"});if(ev)events.push(ev);}
  return filterAndRank(events,f);
}

async function parseVisitNapaValley(listUrl,f){
  const html=await fetchText(listUrl); const $=load(html);
  const links=[]; $("a[href*='/event/']").each((_,a)=>{
    const href=$(a).attr("href")||""; const t=cleanText($(a).text());
    if(!href)return; const full=href.startsWith("http")?href:new URL(href,listUrl).toString();
    if(full.includes("visitnapavalley.com"))links.push({url:full,title:t});
  });
  const uniq=[]; const seen=new Set();
  for(const l of links){if(!l.url||seen.has(l.url))continue;seen.add(l.url);uniq.push(l);if(uniq.length>=10)break;}
  const events=[];
  for(const x of uniq){const ev=await extractOrFallback(x.url,x.title,{town:"all",tag:"any"});if(ev)events.push(ev);}
  return filterAndRank(events,f);
}

async function parseCameo(listUrl,alt,f){
  const html=await fetchText(listUrl); const $=load(html);
  const titles=[]; $("h2,h3").each((_,el)=>{const t=cleanText($(el).text());if(t&&t.length>2&&t.length<80)titles.push(t);});
  const today=toISODate(new Date());
  const meta={address:"1340 Main St., St. Helena.",phone:"707-963-9779",email:"info@cameocinema.com"};
  const events=[];
  for(const t of titles.slice(0,8)){
    if(isGenericTitle(t)||/cameo|movie times/i.test(t))continue;
    events.push({
      title:t,url:listUrl,dateISO:today,
      when:apDateFromYMD(today)||"Date and time on website.",
      details:"Now playing. Showtimes on website.",
      price:"Price not provided.",
      contact:`For more information call ${meta.phone}, email ${meta.email} or visit their website (${listUrl}).`,
      address:meta.address,town:"st-helena",tag:"movies"
    });
  }
  return filterAndRank(events,f);
}

/* --------------------------------------------------
   Handler
-------------------------------------------------- */
export default async function handler(req,res){
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

    // --- Timeout recovery ---
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

    sendJson(res,200,{
      ok:!allFailed,
      timeout:allFailed,
      supplemented:dedup.length<3,
      count:dedup.slice(0,limit).length,
      results:dedup.slice(0,limit)
    });

  }catch(e){
    sendJson(res,500,{ok:false,error:e?.message||String(e)});
  }
}

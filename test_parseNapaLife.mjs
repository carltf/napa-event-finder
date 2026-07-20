// Validates parseNapaLife against a fixture that mimics NapaLife's Word HTML:
// masthead year, day headers, town headers, an entertainment table with a town
// sub-header row, an editorial block with a trailing address line, and a future
// calendar table with a 2027 divider.
import { parseNapaLife } from "./parseNapaLife.mjs";

const html = `
<html><body>
<p>An Insider's Look at Napa Valley &nbsp; Volume 21, Number 27, July 13, 2026</p>

<h1>Wednesday, July 15</h1>
<h2>St. Helena</h2>
<p><b>St. Helena Summer Concert</b></p>
<p>The St. Helena Summer Concert Series returns with Tom Petty cover band Petty Rocks on Wednesday, July 15, from 6 to 8 p.m. in Lyman Park. Get more information at <a href="https://www.cityofsthelena.gov">cityofsthelena.gov</a>.</p>
<p>Lyman Park, 1498 Main St., St. Helena</p>

<p><b>Wednesday, July 15 music and entertainment</b></p>
<table>
<tr><td>Napa</td></tr>
<tr><td>Armistice Brewing Co.</td><td>Trivia</td><td>7 p.m.</td><td>1040 Clinton St.</td></tr>
<tr><td>The Fink</td><td>The Polka Dots</td><td>7:30 p.m.</td><td>530 Main St.</td></tr>
<tr><td>St. Helena</td></tr>
<tr><td>Farmstead</td><td>Matt Bolton</td><td>4-7 p.m.</td><td>738 Main St.</td></tr>
<tr><td>Lyman Park</td><td>Petty Rocks</td><td>6 p.m.</td><td>1498 Main St.</td></tr>
</table>

<h1>Thursday, July 16</h1>
<h2>Napa</h2>
<p><b>Line dancing with Nikki at Azur</b></p>
<p>Azur Wine Lounge offers line dancing with Nikki on Thursday, July 16, from 5 to 7 p.m. It is a free all-levels class.</p>
<p>Azur Wine Lounge, 1014 Clinton St., Napa</p>

<h2>NapaLife's calendar of future events</h2>
<table>
<tr><td>July 23</td><td>Ify Nwadiwe at the Club at Napa Music Hall</td><td>napamusichall.com</td></tr>
<tr><td>Aug. 1</td><td>Dailey &amp; Vincent at Napa Music Hall Club</td><td>napamusichall.com</td></tr>
<tr><td>2027</td></tr>
<tr><td>June 3-5</td><td>Auction Napa Valley</td><td>collectivenapavalley.org</td></tr>
</table>

<table>
<tr><td>Thursday, July 16</td></tr>
<tr><td>Napa</td></tr>
<tr><td>Elks Lodge</td><td>Open mic in-grid</td><td>6:30 p.m.</td><td>2840 Soscol Ave.</td></tr>
</table>
</body></html>`;

const ev = parseNapaLife(html, "https://www.napalife.org/7602.html");
const by = (t) => ev.filter((e) => e.tag === t).length;

console.log("total events:", ev.length);
for (const e of ev) {
  console.log(`  [${e.startYMD}] ${e.tag.padEnd(9)} ${e.town.padEnd(14)} ${e.title}`);
  console.log(`             addr: ${e.address}  | price: ${e.price}`);
}

// ---- assertions ----
const find = (t) => ev.find((e) => e.title.includes(t));
const asserts = [
  ["parsed at least 7 events", ev.length >= 7],
  ["day header set the date", find("Summer Concert")?.startYMD === "2026-07-15"],
  ["editorial address extracted", find("Summer Concert")?.address.includes("1498 Main St")],
  ["editorial address is clean (venue only, no sentence)", find("Summer Concert")?.address.startsWith("Lyman Park,")],
  ["editorial details exclude the address line", !find("Summer Concert")?.details.includes("1498 Main St")],
  ["table caption not emitted as event", !ev.some((e) => /music and entertainment/i.test(e.title))],
  ["intra-issue dedupe drops grid echo (Petty Rocks)", !ev.some((e) => e.title === "Petty Rocks")],
  ["but keeps the featured version (Summer Concert)", !!find("Summer Concert")],
  ["editorial town = st-helena", find("Summer Concert")?.town === "st-helena"],
  ["editorial price extracted (Line dancing = Free)", find("Line dancing")?.price === "Free."],
  ["table event date follows day header", find("The Polka Dots")?.startYMD === "2026-07-15"],
  ["table town sub-header switch (Farmstead=st-helena)", find("Matt Bolton")?.town === "st-helena"],
  ["trivia classified nightlife", find("Trivia")?.tag === "nightlife"],
  ["line dancing -> nightlife", find("Line dancing")?.tag === "nightlife"],
  ["future cal parsed", find("Ify Nwadiwe")?.startYMD === "2026-07-23"],
  ["future cal year rollover to 2027", find("Auction Napa Valley")?.startYMD === "2027-06-03"],
  ["future cal source link built", find("Ify Nwadiwe")?.url.includes("napamusichall.com")],
  ["day header embedded in a table row is recognized", find("Open mic in-grid")?.startYMD === "2026-07-16"],
  ["...and its town resolves", find("Open mic in-grid")?.town === "napa"],
];
let ok = 0;
for (const [name, cond] of asserts) {
  console.log((cond ? "PASS " : "FAIL ") + name);
  if (cond) ok++;
}
console.log(`\n${ok}/${asserts.length} assertions passed`);
process.exit(ok === asserts.length ? 0 : 1);

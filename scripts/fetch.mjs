/**
 * Pulls every page of the CDP x402 Bazaar discovery endpoint into a single
 * newline-delimited JSON file. Safe to re-run: it always writes a fresh file.
 */
import fs from "node:fs";
import path from "node:path";
import { API, PAGE_SIZE, CONCURRENCY, RETRIES, BACKOFF_CAP_MS, RAW_DIR, RAW_FILE } from "./config.mjs";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Retry-After may be delta-seconds or an HTTP date; returns ms, or 0 if absent/unparseable.
function retryAfterMs(res) {
  const h = res.headers.get("retry-after");
  if (!h) return 0;
  const secs = Number(h);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(h);
  return Number.isNaN(at) ? 0 : Math.max(0, at - Date.now());
}

// Exponential backoff with full jitter, capped so one page can't stall the run.
const backoffMs = (attempt) => Math.random() * Math.min(BACKOFF_CAP_MS, 1000 * 2 ** attempt);

// Shared across workers: a 429 on one page pauses every worker until this time.
let resumeAt = 0;

async function getPage(offset) {
  const url = `${API}?limit=${PAGE_SIZE}&offset=${offset}`;
  let lastError;
  for (let attempt = 0; attempt <= RETRIES; attempt++) {
    let wait = 0;
    if (resumeAt > Date.now()) await sleep(resumeAt - Date.now());
    try {
      const res = await fetch(url, { headers: { accept: "application/json" } });
      if (res.status === 429 || res.status >= 500) {
        wait = Math.min(BACKOFF_CAP_MS, retryAfterMs(res));
        if (res.status === 429) resumeAt = Math.max(resumeAt, Date.now() + wait);
        throw new Error(`HTTP ${res.status}`);
      }
      if (!res.ok) throw new Error(`HTTP ${res.status} ${await res.text()}`);
      return await res.json();
    } catch (err) {
      lastError = err;
      if (attempt < RETRIES) await sleep(Math.max(wait, backoffMs(attempt)));
    }
  }
  throw new Error(`offset ${offset} failed after ${RETRIES + 1} attempts: ${lastError.message}`);
}

async function run() {
  const started = Date.now();
  const first = await getPage(0);
  const total = first.pagination?.total ?? first.items?.length ?? 0;
  if (!total) throw new Error("discovery API returned no results");

  const offsets = [];
  for (let o = PAGE_SIZE; o < total; o += PAGE_SIZE) offsets.push(o);
  console.log(`discovery reports ${total} listings; fetching ${offsets.length + 1} pages`);

  const pages = new Map([[0, first.items || []]]);
  let cursor = 0;
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, offsets.length) }, async () => {
      while (cursor < offsets.length) {
        const offset = offsets[cursor++];
        const page = await getPage(offset);
        pages.set(offset, page.items || []);
      }
    })
  );

  const items = [...pages.keys()].sort((a, b) => a - b).flatMap((k) => pages.get(k));
  // The API paginates over a live table, so a listing can surface twice.
  const seen = new Set();
  const unique = items.filter((it) => {
    const key = it.resource || JSON.stringify(it).slice(0, 200);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  fs.mkdirSync(RAW_DIR, { recursive: true });
  fs.writeFileSync(RAW_FILE, unique.map((i) => JSON.stringify(i)).join("\n") + "\n");
  fs.writeFileSync(
    path.join(RAW_DIR, "fetch-meta.json"),
    JSON.stringify(
      { fetchedAt: new Date().toISOString(), reportedTotal: total, received: items.length, unique: unique.length },
      null,
      2
    )
  );

  const dropped = items.length - unique.length;
  console.log(
    `wrote ${unique.length} listings to ${RAW_FILE}` +
      (dropped ? ` (${dropped} duplicate${dropped === 1 ? "" : "s"} dropped)` : "") +
      ` in ${((Date.now() - started) / 1000).toFixed(1)}s`
  );
}

run().catch((err) => {
  console.error(err.message);
  process.exit(1);
});

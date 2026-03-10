# """
Weekly Social Listening Script

Runs every Monday to collect brand mentions, classify them with Claude,
and write results to Google Sheets.

Tracks: Sonos | Marshall | Bowers & Wilkins | Category trends
"""

import os
import json
import time
import hashlib
import logging
import traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
level=logging.INFO,
format="%(asctime)s  %(levelname)-8s  %(message)s",
datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(**name**)

# ── Config ─────────────────────────────────────────────────────────────────────

SEARCH_QUERIES = [
"Sonos review OR Sonos reddit OR Sonos forum",
"Marshall speaker review OR Marshall headphones review",
"Bowers & Wilkins review OR Bowers & Wilkins reddit",
"wireless speaker trend",
"home audio trend",
"premium audio campaign",
]

BRANDS = ["Sonos", "Marshall", "Bowers & Wilkins", "B&W"]
CATEGORY_QUERIES = ["wireless speaker trend", "home audio trend", "premium audio campaign"]

GOOGLE_SCOPES = [
"https://spreadsheets.google.com/feeds",
"https://www.googleapis.com/auth/drive",
]

SERP_API_KEY = os.environ.get("SERP_API_KEY", "")          # optional paid search API
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "Weekly Social Listening")

RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
WEEK_AGO = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════════════════════════════

# 1.  WEB SEARCH

# ══════════════════════════════════════════════════════════════════════════════

def search_web(query: str, num_results: int = 10) -> list[dict]:
"""
Search the web for a query and return raw results.

```
Uses SerpAPI if SERP_API_KEY is set (recommended for production).
Falls back to a DuckDuckGo HTML scrape for zero-cost usage.
Each result dict: {title, url, snippet, published_date, query}
"""
if SERP_API_KEY:
    return _search_via_serpapi(query, num_results)
else:
    return _search_via_duckduckgo(query, num_results)
```

def _search_via_serpapi(query: str, num_results: int) -> list[dict]:
"""Paid search via SerpAPI - most reliable, respects date filters."""
log.info(f"[SerpAPI] Searching: {query!r}")
params = {
"q": query,
"api_key": SERP_API_KEY,
"num": num_results,
"tbs": "qdr:w",        # last 7 days
"hl": "en",
"gl": "sg",
}
try:
resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
resp.raise_for_status()
data = resp.json()
results = []
for item in data.get("organic_results", []):
results.append({
"title": item.get("title", ""),
"url": item.get("link", ""),
"snippet": item.get("snippet", ""),
"published_date": item.get("date", ""),
"query": query,
})
return results
except Exception as e:
log.warning(f"SerpAPI error for {query!r}: {e}")
return []

def _search_via_duckduckgo(query: str, num_results: int) -> list[dict]:
"""
Free fallback: scrape DuckDuckGo HTML results.
Note: DuckDuckGo may block aggressive scrapers; use SerpAPI in production.
"""
log.info(f"[DuckDuckGo] Searching: {query!r}")
headers = {
"User-Agent": (
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
"AppleWebKit/537.36 (KHTML, like Gecko) "
"Chrome/120.0.0.0 Safari/537.36"
)
}
params = {"q": query, "df": "w"}   # df=w → last week
try:
resp = requests.get(
"https://html.duckduckgo.com/html/",
params=params,
headers=headers,
timeout=15,
)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "html.parser")
results = []
for result in soup.select(".result")[:num_results]:
title_el = result.select_one(".result__title a")
snippet_el = result.select_one(".result__snippet")
url_el = result.select_one(".result__url")
if not title_el:
continue
title = title_el.get_text(strip=True)
url = title_el.get("href", "")
snippet = snippet_el.get_text(strip=True) if snippet_el else ""
domain = url_el.get_text(strip=True) if url_el else ""
results.append({
"title": title,
"url": url,
"snippet": snippet,
"published_date": "",   # DDG HTML doesn't expose dates
"query": query,
})
time.sleep(2)   # be polite to DDG
return results
except Exception as e:
log.warning(f"DuckDuckGo scrape error for {query!r}: {e}")
return []

# ══════════════════════════════════════════════════════════════════════════════

# 2.  NORMALISE & DEDUPLICATE

# ══════════════════════════════════════════════════════════════════════════════

def normalize_results(raw_results: list[dict]) -> pd.DataFrame:
"""
Clean, normalise, and deduplicate raw search results.
Returns a DataFrame with consistent columns.
"""
if not raw_results:
return pd.DataFrame()

```
df = pd.DataFrame(raw_results)

# Extract domain from URL
def extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""

df["domain"] = df["url"].apply(extract_domain)

# Normalise columns
df = df.rename(columns={"published_date": "date"})
df["date"] = df["date"].fillna("").astype(str).str[:10]
df["run_date"] = RUN_DATE

# Deduplicate by URL fingerprint
def url_hash(url: str) -> str:
    return hashlib.md5(url.strip().lower().encode()).hexdigest()

df["url_hash"] = df["url"].apply(url_hash)
df = df.drop_duplicates(subset="url_hash", keep="first")

# Drop empty titles
df = df[df["title"].str.strip() != ""]

# Reorder columns
cols = ["run_date", "query", "title", "domain", "date", "url", "snippet", "url_hash"]
df = df[[c for c in cols if c in df.columns]]

log.info(f"Normalised {len(df)} unique results from {len(raw_results)} raw.")
return df.reset_index(drop=True)
```

# ══════════════════════════════════════════════════════════════════════════════

# 3.  CLASSIFY WITH CLAUDE

# ══════════════════════════════════════════════════════════════════════════════

def classify_mentions_with_claude(df: pd.DataFrame) -> pd.DataFrame:
"""
Send each mention to Claude for classification.
Adds columns: brand, mention_type, sentiment, theme, ai_notes
Processes in batches to manage API rate limits gracefully.
"""
if df.empty:
return df

```
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

classifications = []
for i, row in df.iterrows():
    result = _classify_single(client, row)
    classifications.append(result)

    # Throttle: 1 call per second to avoid rate limits
    if i > 0 and i % 10 == 0:
        log.info(f"  Classified {i}/{len(df)} mentions...")
        time.sleep(1)

class_df = pd.DataFrame(classifications)
return pd.concat([df.reset_index(drop=True), class_df.reset_index(drop=True)], axis=1)
```

def _classify_single(client: anthropic.Anthropic, row: pd.Series) -> dict:
"""Classify a single mention row. Returns a dict of classification fields."""
empty = {
"brand": "Unknown",
"mention_type": "Unknown",
"sentiment": "Neutral",
"theme": "Unknown",
"ai_notes": "",
}
try:
prompt = f"""You are a marketing analyst classifying a social listening mention for a premium audio brand distributor.

Title: {row.get('title', '')}
Source: {row.get('domain', '')}
Snippet: {row.get('snippet', '')}
Search Query Used: {row.get('query', '')}

Classify this mention and respond ONLY with a valid JSON object (no markdown, no commentary):

{{
"brand": "<Sonos | Marshall | Bowers & Wilkins | Category | Other>",
"mention_type": "<Review | Reddit Discussion | Forum Post | News Article | Blog | Social Media | Campaign/Ad | Trend Report | Other>",
"sentiment": "<Positive | Negative | Neutral | Mixed>",
"theme": "<one concise theme, max 5 words>",
"ai_notes": "<one insight sentence for a marketing lead, max 20 words>"
}}"""

```
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    text = message.content[0].text.strip()

    # Strip markdown fences if present
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    parsed = json.loads(text)
    return {k: parsed.get(k, empty[k]) for k in empty}

except json.JSONDecodeError as e:
    log.warning(f"JSON parse error on row {row.name}: {e}")
    return empty
except Exception as e:
    log.warning(f"Claude API error on row {row.name}: {e}")
    return empty
```

# ══════════════════════════════════════════════════════════════════════════════

# 4.  WEEKLY SUMMARY

# ══════════════════════════════════════════════════════════════════════════════

def generate_weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
"""
Use Claude to generate a structured weekly summary from classified mentions.
Returns a single-row DataFrame ready to append to the Weekly_Summary sheet.
"""
if df.empty:
log.warning("No data to summarise.")
return pd.DataFrame()

```
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Build a compact data snapshot for the prompt
snapshot_rows = []
for _, row in df.iterrows():
    snapshot_rows.append(
        f"- [{row.get('brand')}] [{row.get('sentiment')}] [{row.get('mention_type')}] "
        f"[{row.get('theme')}] {row.get('title')} ({row.get('domain')})"
    )
snapshot = "\n".join(snapshot_rows[:80])   # cap at 80 rows to stay within tokens

prompt = f"""You are a senior marketing analyst at a premium audio brand distributor in Singapore.
```

You cover: Sonos, Marshall, Bowers & Wilkins.

"Below is this week's social listening data (***RUN_DATE***):"

{snapshot}

Write a concise weekly summary for the marketing lead. Respond ONLY with a valid JSON object:

{{
"top_themes": "<3–5 bullet points as a single string, separated by | >",
"positive_signals": "<2–3 bullet points as a single string, separated by | >",
"negative_signals": "<2–3 bullet points as a single string, separated by | >",
"emerging_trends": "<2–3 bullet points as a single string, separated by | >",
"competitor_campaign_signals": "<2–3 bullet points as a single string, separated by | >",
"marketer_watchouts": "<2–3 bullet points as a single string, separated by | >"
}}"""

```
try:
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )
    text = message.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    summary = json.loads(text)
    summary["week_of"] = RUN_DATE
    summary["total_mentions"] = len(df)
    summary["sonos_count"] = len(df[df["brand"] == "Sonos"])
    summary["marshall_count"] = len(df[df["brand"] == "Marshall"])
    summary["bw_count"] = len(df[df["brand"] == "Bowers & Wilkins"])
    summary["positive_count"] = len(df[df["sentiment"] == "Positive"])
    summary["negative_count"] = len(df[df["sentiment"] == "Negative"])

    # Reorder columns
    col_order = [
        "week_of", "total_mentions",
        "sonos_count", "marshall_count", "bw_count",
        "positive_count", "negative_count",
        "top_themes", "positive_signals", "negative_signals",
        "emerging_trends", "competitor_campaign_signals", "marketer_watchouts",
    ]
    summary_df = pd.DataFrame([summary])
    summary_df = summary_df[[c for c in col_order if c in summary_df.columns]]
    return summary_df

except Exception as e:
    log.error(f"Summary generation failed: {e}")
    return pd.DataFrame()
```

# ══════════════════════════════════════════════════════════════════════════════

# 5.  GOOGLE SHEETS

# ══════════════════════════════════════════════════════════════════════════════

def connect_google_sheets() -> gspread.Spreadsheet:
"""
Authenticate with Google Sheets using a service account JSON stored
in the GOOGLE_SERVICE_ACCOUNT_JSON environment variable.
Returns the spreadsheet object.
"""
try:
sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(sa_info, scopes=GOOGLE_SCOPES)
gc = gspread.authorize(creds)
spreadsheet = gc.open(GOOGLE_SHEET_NAME)
log.info(f"Connected to Google Sheet: {GOOGLE_SHEET_NAME!r}")
return spreadsheet
except Exception as e:
log.error(f"Google Sheets connection failed: {e}")
raise

def _get_or_create_worksheet(spreadsheet: gspread.Spreadsheet, tab_name: str) -> gspread.Worksheet:
# Return worksheet by name, creating it if it doesn't exist.
try:
return spreadsheet.worksheet(tab_name)
except gspread.WorksheetNotFound:
ws = spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=30)
log.info(f"Created new sheet tab: {tab_name!r}")
return ws

def append_dataframe_to_sheet(
spreadsheet: gspread.Spreadsheet,
df: pd.DataFrame,
tab_name: str,
) -> None:
"""
Append a DataFrame to a Google Sheet tab.
Writes headers if the sheet is empty; always appends rows below existing data.
"""
if df.empty:
log.warning(f"Nothing to write to {tab_name!r} - DataFrame is empty.")
return

ws = _get_or_create_worksheet(spreadsheet, tab_name)
df = df.fillna("").astype(str)

existing = ws.get_all_values()
if not existing:
    # Sheet is empty - write headers + data
    data = [df.columns.tolist()] + df.values.tolist()
    ws.update(range_name="A1", values=data)
    log.info(f"Wrote {len(df)} rows (+headers) to {tab_name!r}.")
else:
    # Append below existing content
    ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
    log.info(f"Appended {len(df)} rows to {tab_name!r}.")

# ══════════════════════════════════════════════════════════════════════════════

# 6.  MAIN ORCHESTRATOR

# ══════════════════════════════════════════════════════════════════════════════

def main():
log.info("=" * 60)
log.info(f"Social Listening Run - {RUN_DATE}")
log.info("=" * 60)

# ── Validate secrets ───────────────────────────────────────────────────
if not ANTHROPIC_API_KEY:
    raise EnvironmentError("ANTHROPIC_API_KEY is not set.")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON is not set.")
if not GOOGLE_SHEET_NAME:
    raise EnvironmentError("GOOGLE_SHEET_NAME is not set.")

# ── Step 1: Search ─────────────────────────────────────────────────────
log.info("Step 1/5 - Searching the web...")
all_raw = []
for query in SEARCH_QUERIES:
    try:
        results = search_web(query)
        all_raw.extend(results)
        log.info(f"  ✓ {len(results)} results for: {query!r}")
    except Exception as e:
        log.warning(f"  ✗ Search failed for {query!r}: {e}")

# ── Step 2: Normalise ──────────────────────────────────────────────────
log.info("Step 2/5 - Normalising results...")
df = normalize_results(all_raw)
if df.empty:
    log.error("No results after normalisation. Exiting.")
    return

# ── Step 3: Classify ───────────────────────────────────────────────────
log.info(f"Step 3/5 - Classifying {len(df)} mentions with Claude...")
df = classify_mentions_with_claude(df)

# ── Step 4: Weekly Summary ─────────────────────────────────────────────
log.info("Step 4/5 - Generating weekly summary...")
summary_df = generate_weekly_summary(df)

# ── Step 5: Write to Google Sheets ────────────────────────────────────
log.info("Step 5/5 - Writing to Google Sheets...")
try:
    spreadsheet = connect_google_sheets()

    # Remove url_hash before writing (internal use only)
    mentions_df = df.drop(columns=["url_hash"], errors="ignore")
    append_dataframe_to_sheet(spreadsheet, mentions_df, "Raw_Mentions")

    if not summary_df.empty:
        append_dataframe_to_sheet(spreadsheet, summary_df, "Weekly_Summary")

except Exception as e:
    log.error(f"Google Sheets write failed: {e}\n{traceback.format_exc()}")

log.info("=" * 60)
log.info(f"Run complete. {len(df)} mentions processed.")
log.info("=" * 60)

if **name** == "**main**":
main()

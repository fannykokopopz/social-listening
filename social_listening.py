# Weekly Social Listening Script
# Runs every Monday to collect brand mentions, classify them with Claude,
# and write results to Google Sheets.
#
# Sources:
#   - Google News RSS    (free, no key)
#   - Reddit via Apify   (needs APIFY_TOKEN)
#   - YouTube            (free tier, needs YOUTUBE_API_KEY)
#
# Tracks: Sonos | Marshall | Bowers & Wilkins | Category trends

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
import xml.etree.ElementTree as ET
import anthropic
import gspread
from apify_client import ApifyClient
from google.oauth2.service_account import Credentials

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

NEWS_QUERIES = [
    "Sonos speaker review",
    "Marshall speaker review OR Marshall headphones review",
    "Bowers & Wilkins review",
    "wireless speaker trend",
    "home audio trend",
    "premium audio campaign",
]

REDDIT_QUERIES_GLOBAL = [
    ("Sonos", "sonos"),
    ("Sonos", "audiophile"),
    ("Marshall speaker OR Marshall headphones", None),
    ("Bowers Wilkins OR B&W speaker", None),
    ("wireless speaker recommendation", "audiophile"),
    ("home audio setup", "hometheater"),
]

REDDIT_QUERIES_SEA = [
    ("Sonos OR Marshall OR speaker", "singapore"),
    ("Sonos OR Marshall OR speaker", "malaysia"),
    ("Sonos OR Marshall OR speaker", "Thailand"),
    ("Sonos OR Marshall OR speaker", "HongKong"),
]

YOUTUBE_QUERIES = [
    "Sonos speaker review",
    "Marshall speaker review",
    "Bowers Wilkins review",
    "best wireless speaker 2025",
    "premium home audio review",
]

BRANDS = ["Sonos", "Marshall", "Bowers & Wilkins", "B&W"]

REGIONS = [
    {
        "label": "Global",
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
        "yt_region": "US",
        "local_domains": [],
    },
    {
        "label": "SG",
        "hl": "en-SG",
        "gl": "SG",
        "ceid": "SG:en",
        "yt_region": "SG",
        "local_domains": ["hardwarezone.com.sg", "techgoondu.com"],
    },
    {
        "label": "MY",
        "hl": "en-MY",
        "gl": "MY",
        "ceid": "MY:en",
        "yt_region": "MY",
        "local_domains": ["lowyat.net", "soyacincau.com"],
    },
    {
        "label": "TH",
        "hl": "en-TH",
        "gl": "TH",
        "ceid": "TH:en",
        "yt_region": "TH",
        "local_domains": ["notebookspec.com"],
    },
    {
        "label": "HK",
        "hl": "en-HK",
        "gl": "HK",
        "ceid": "HK:en",
        "yt_region": "HK",
        "local_domains": ["unwire.hk"],
    },
]

SEA_LABELS = {"SG", "MY", "TH", "HK"}

GOOGLE_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# ── Credentials ────────────────────────────────────────────────────────────────
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
APIFY_REDDIT_ACTOR_ID = os.environ.get("APIFY_REDDIT_ACTOR_ID", "trudax/reddit-scraper")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "Weekly Social Listening")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")

RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
WEEK_AGO = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
WEEK_AGO_ISO = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

apify_client = ApifyClient(APIFY_TOKEN) if APIFY_TOKEN else None


# ══════════════════════════════════════════════════════════════════════════════
# 1. NEWS SEARCH (Google News RSS only)
# ══════════════════════════════════════════════════════════════════════════════

def search_news(query: str, num_results: int = 10, region: dict | None = None) -> list[dict]:
    """
    Search news articles for a given region using Google News RSS.
    """
    r = region or REGIONS[0]
    return _search_via_google_news(query, num_results, r)


def search_news_local_domains(query: str, region: dict, num_results: int = 5) -> list[dict]:
    """
    Search for coverage of a query on region-specific local media domains.
    Runs one Google News RSS query per local domain in the region config.
    """
    results = []
    for domain in region.get("local_domains", []):
        site_query = f"{query} site:{domain}"
        hits = _search_via_google_news(site_query, num_results, region)
        results.extend(hits)
    return results


def _search_via_google_news(query: str, num_results: int, region: dict | None = None) -> list[dict]:
    r = region or REGIONS[0]
    log.info(f"[GoogleNews:{r['label']}] {query!r}")
    try:
        resp = requests.get(
            "https://news.google.com/rss/search",
            params={"q": query, "hl": r["hl"], "gl": r["gl"], "ceid": r["ceid"]},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        results = []
        for item in root.findall(".//item")[:num_results]:
            raw_title = item.findtext("title", "")
            title = raw_title.rsplit(" - ", 1)[0].strip()
            url = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")[:16]
            source_el = item.find("source")
            domain = ""
            if source_el is not None:
                src_url = source_el.get("url", "")
                domain = (
                    src_url.replace("https://", "")
                    .replace("http://", "")
                    .replace("www.", "")
                    .split("/")[0]
                )
            results.append({
                "source": "news",
                "title": title,
                "url": url,
                "snippet": "",
                "published_date": pub_date,
                "query": query,
                "platform": "News",
                "extra": domain,
                "region": r["label"],
            })
        time.sleep(0.5)
        return results
    except Exception as e:
        log.warning(f"Google News RSS error [{r['label']}] {query!r}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 2. REDDIT VIA APIFY
# ══════════════════════════════════════════════════════════════════════════════

def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _parse_reddit_created(value) -> str:
    """
    Handles ISO timestamps and unix timestamps.
    """
    if value is None or value == "":
        return ""
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%d")
        text = str(value).strip()
        if text.isdigit():
            return datetime.fromtimestamp(int(text), tz=timezone.utc).strftime("%Y-%m-%d")
        text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text).strftime("%Y-%m-%d")
    except Exception:
        return ""


def search_reddit(query: str, subreddit: str | None = None, num_results: int = 10, region: str = "Global") -> list[dict]:
    """
    Search Reddit posts via Apify instead of Reddit OAuth.
    """
    if not apify_client:
        log.info("[Reddit:Apify] Skipping — APIFY_TOKEN not set.")
        return []

    actor_input = {
        "searches": [query],
        "resultsLimit": num_results,
        "sort": "new",
        "time": "week",
    }

    if subreddit:
        actor_input["subreddits"] = [subreddit]

    log.info(f"[Reddit:Apify] {query!r} | subreddit={subreddit or 'all'}")

    try:
        run = apify_client.actor(APIFY_REDDIT_ACTOR_ID).call(run_input=actor_input)
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            log.warning("[Reddit:Apify] No dataset returned.")
            return []

        results = []
        for item in apify_client.dataset(dataset_id).iterate_items():
            title = item.get("title", "") or ""
            permalink = item.get("permalink", "") or item.get("url", "") or ""
            if permalink and permalink.startswith("/"):
                url = f"https://reddit.com{permalink}"
            else:
                url = permalink

            body = (
                item.get("text")
                or item.get("body")
                or item.get("selfText")
                or item.get("selftext")
                or ""
            )
            snippet = str(body).strip()[:200]

            sub = (
                item.get("subredditName")
                or item.get("subreddit")
                or subreddit
                or ""
            )

            score = _safe_int(
                item.get("score")
                or item.get("upvotes")
                or item.get("ups")
                or 0
            )
            num_comments = _safe_int(
                item.get("commentsCount")
                or item.get("numComments")
                or item.get("num_comments")
                or 0
            )

            created_raw = (
                item.get("createdAt")
                or item.get("created")
                or item.get("created_utc")
            )
            pub_date = _parse_reddit_created(created_raw)

            results.append({
                "source": "reddit",
                "title": title,
                "url": url,
                "snippet": snippet,
                "published_date": pub_date,
                "query": query,
                "platform": "Reddit",
                "extra": f"r/{sub} | score:{score} | comments:{num_comments}",
                "region": region,
            })

        time.sleep(1)
        return results

    except Exception as e:
        log.warning(f"[Reddit:Apify] Search error for {query!r}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 3. YOUTUBE
# ══════════════════════════════════════════════════════════════════════════════

def search_youtube(query: str, num_results: int = 10, region: dict | None = None) -> list[dict]:
    if not YOUTUBE_API_KEY:
        log.info("[YouTube] Skipping — YOUTUBE_API_KEY not set.")
        return []

    r = region or REGIONS[0]
    log.info(f"[YouTube:{r['label']}] {query!r}")
    try:
        search_resp = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet",
                "q": query,
                "type": "video",
                "order": "date",
                "publishedAfter": WEEK_AGO_ISO,
                "maxResults": num_results,
                "key": YOUTUBE_API_KEY,
                "relevanceLanguage": "en",
                "regionCode": r["yt_region"],
            },
            timeout=15,
        )
        search_resp.raise_for_status()
        search_data = search_resp.json()
        items = search_data.get("items", [])
        if not items:
            return []

        video_ids = [item["id"]["videoId"] for item in items if item.get("id", {}).get("videoId")]
        stats_map: dict[str, dict] = {}
        if video_ids:
            stats_resp = requests.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "part": "statistics",
                    "id": ",".join(video_ids),
                    "key": YOUTUBE_API_KEY,
                },
                timeout=15,
            )
            if stats_resp.ok:
                for v in stats_resp.json().get("items", []):
                    stats_map[v["id"]] = v.get("statistics", {})

        results = []
        for item in items:
            video_id = item.get("id", {}).get("videoId", "")
            snippet = item.get("snippet", {})
            title = snippet.get("title", "")
            channel = snippet.get("channelTitle", "")
            pub_date = snippet.get("publishedAt", "")[:10]
            description = snippet.get("description", "")[:200]
            url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
            stats = stats_map.get(video_id, {})
            views = int(stats.get("viewCount", 0))
            likes = int(stats.get("likeCount", 0))
            results.append({
                "source": "youtube",
                "title": title,
                "url": url,
                "snippet": description,
                "published_date": pub_date,
                "query": query,
                "platform": "YouTube",
                "extra": f"{channel} | views:{views:,} | likes:{likes:,}",
                "region": r["label"],
            })
        time.sleep(1)
        return results
    except Exception as e:
        log.warning(f"[YouTube] Search error for {query!r}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 4. NORMALISE & DEDUPLICATE
# ══════════════════════════════════════════════════════════════════════════════

def normalize_results(raw_results: list[dict]) -> pd.DataFrame:
    if not raw_results:
        return pd.DataFrame()

    df = pd.DataFrame(raw_results)

    def extract_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.replace("www.", "")
        except Exception:
            return ""

    if "domain" not in df.columns:
        df["domain"] = df["url"].apply(extract_domain)
    else:
        mask = df["domain"].isna() | (df["domain"] == "")
        df.loc[mask, "domain"] = df.loc[mask, "url"].apply(extract_domain)

    df = df.rename(columns={"published_date": "date"})
    df["date"] = df["date"].fillna("").astype(str).str[:10]
    df["run_date"] = RUN_DATE

    def url_hash(url: str) -> str:
        return hashlib.md5(url.strip().lower().encode()).hexdigest()

    df["url_hash"] = df["url"].apply(url_hash)
    df = df.drop_duplicates(subset="url_hash", keep="first")
    df = df[df["title"].astype(str).str.strip() != ""]

    cols = ["run_date", "region", "source", "platform", "query", "title", "domain",
            "date", "url", "snippet", "extra", "url_hash"]
    df = df[[c for c in cols if c in df.columns]]

    log.info(f"Normalised {len(df)} unique results from {len(raw_results)} raw.")
    if "source" in df.columns:
        for src, count in df["source"].value_counts().items():
            log.info(f"  |-- {src}: {count}")

    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 5. CLASSIFY WITH CLAUDE
# ══════════════════════════════════════════════════════════════════════════════

def classify_mentions_with_claude(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    classifications = []

    for i, row in df.iterrows():
        result = _classify_single(client, row)
        classifications.append(result)
        if i > 0 and i % 10 == 0:
            log.info(f"  Classified {i}/{len(df)} mentions...")
            time.sleep(1)

    class_df = pd.DataFrame(classifications)
    return pd.concat([df.reset_index(drop=True), class_df.reset_index(drop=True)], axis=1)


def _classify_single(client: anthropic.Anthropic, row: pd.Series) -> dict:
    empty = {
        "brand": "Unknown",
        "mention_type": "Unknown",
        "sentiment": "Neutral",
        "theme": "Unknown",
        "ai_notes": "",
    }

    source = row.get("source", "news")
    platform = row.get("platform", "")
    extra = row.get("extra", "")

    if source == "reddit":
        context_line = f"Platform: Reddit ({extra})"
        mention_types = "Reddit Discussion | Reddit Question | Reddit Complaint | Reddit Recommendation | Reddit Comparison | Other"
    elif source == "youtube":
        context_line = f"Platform: YouTube ({extra})"
        mention_types = "YouTube Review | YouTube Unboxing | YouTube Comparison | YouTube Tutorial | YouTube Vlog | Other"
    else:
        context_line = f"Platform: {platform or 'News/Web'}"
        mention_types = "Review | News Article | Blog | Forum Post | Campaign/Ad | Trend Report | Other"

    try:
        prompt = f"""You are a marketing analyst classifying a social listening mention for a premium audio brand distributor in Singapore.

{context_line}
Title: {row.get('title', '')}
Snippet: {row.get('snippet', '')}
Search Query Used: {row.get('query', '')}

Classify this mention and respond ONLY with a valid JSON object (no markdown, no commentary):

{{
  "brand": "<Sonos | Marshall | Bowers & Wilkins | Category | Other>",
  "mention_type": "<{mention_types}>",
  "sentiment": "<Positive | Negative | Neutral | Mixed>",
  "theme": "<one concise theme, max 5 words>",
  "ai_notes": "<one insight sentence for a marketing lead, max 20 words>"
}}"""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
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


# ══════════════════════════════════════════════════════════════════════════════
# 6. WEEKLY SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def _build_snapshot(df: pd.DataFrame, max_rows: int = 60) -> str:
    rows = []
    for _, row in df.iterrows():
        rows.append(
            f"- [{row.get('region','?')}] [{row.get('source','').upper()}] "
            f"[{row.get('brand')}] [{row.get('sentiment')}] [{row.get('mention_type')}] "
            f"[{row.get('theme')}] {row.get('title')} ({row.get('platform')})"
        )
    return "\n".join(rows[:max_rows])


def _call_claude_summary(client: anthropic.Anthropic, prompt: str) -> dict:
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = message.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    return json.loads(text)


def generate_weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.warning("No data to summarise.")
        return pd.DataFrame()

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    summary: dict = {"week_of": RUN_DATE, "total_mentions": len(df)}

    src = df.get("source", pd.Series(dtype=str))
    summary["news_count"] = int((src == "news").sum())
    summary["reddit_count"] = int((src == "reddit").sum())
    summary["youtube_count"] = int((src == "youtube").sum())

    region_col = df.get("region", pd.Series(dtype=str))
    summary["global_count"] = int((region_col == "Global").sum())
    for market in SEA_LABELS:
        summary[f"{market.lower()}_count"] = int((region_col == market).sum())

    brand = df.get("brand", pd.Series(dtype=str))
    summary["sonos_count"] = int((brand == "Sonos").sum())
    summary["marshall_count"] = int((brand == "Marshall").sum())
    summary["bw_count"] = int((brand == "Bowers & Wilkins").sum())

    sent = df.get("sentiment", pd.Series(dtype=str))
    summary["positive_count"] = int((sent == "Positive").sum())
    summary["negative_count"] = int((sent == "Negative").sum())

    log.info("  Generating global summary layer...")
    global_df = df[df["region"] == "Global"] if "region" in df.columns else df
    global_snapshot = _build_snapshot(global_df)

    global_prompt = f"""You are a senior marketing analyst at a premium audio brand distributor in Singapore.
You cover: Sonos, Marshall, Bowers & Wilkins.

Below is this week's GLOBAL social listening data ({RUN_DATE}) — news, Reddit, and YouTube worldwide:

{global_snapshot}

Write a concise global summary. Respond ONLY with a valid JSON object:

{{
  "global_top_themes": "<3-5 bullet points, separated by | >",
  "global_positive_signals": "<2-3 bullet points, separated by | >",
  "global_negative_signals": "<2-3 bullet points, separated by | >",
  "global_emerging_trends": "<2-3 bullet points, separated by | >",
  "global_reddit_pulse": "<2-3 bullet points on Reddit consumer sentiment, separated by | >",
  "global_youtube_pulse": "<2-3 bullet points on YouTube review activity, separated by | >",
  "global_competitor_signals": "<2-3 bullet points, separated by | >",
  "global_watchouts": "<2-3 bullet points, separated by | >"
}}"""

    try:
        summary.update(_call_claude_summary(client, global_prompt))
    except Exception as e:
        log.error(f"Global summary failed: {e}")

    log.info("  Generating SEA market summaries...")
    for market in ["SG", "MY", "TH", "HK"]:
        market_df = df[df["region"] == market] if "region" in df.columns else pd.DataFrame()
        if market_df.empty:
            log.info(f"  No data for {market} — skipping.")
            for field in ["themes", "positive", "negative", "watchouts"]:
                summary[f"{market.lower()}_{field}"] = "No data this week"
            continue

        log.info(f"  {market}: {len(market_df)} mentions")
        market_snapshot = _build_snapshot(market_df, max_rows=40)

        market_names = {"SG": "Singapore", "MY": "Malaysia", "TH": "Thailand", "HK": "Hong Kong"}
        market_prompt = f"""You are a senior marketing analyst at a premium audio brand distributor.
You cover: Sonos, Marshall, Bowers & Wilkins. You are analysing the {market_names[market]} market.

Below is this week's {market_names[market]} social listening data ({RUN_DATE}):

{market_snapshot}

Write a concise market summary for {market_names[market]}. Respond ONLY with a valid JSON object:

{{
  "{market.lower()}_themes": "<2-3 dominant themes in {market_names[market]}, separated by | >",
  "{market.lower()}_positive": "<1-2 positive signals from {market_names[market]}, separated by | >",
  "{market.lower()}_negative": "<1-2 negative signals or concerns from {market_names[market]}, separated by | >",
  "{market.lower()}_watchouts": "<1-2 things the marketing team should act on for {market_names[market]}, separated by | >"
}}"""

        try:
            summary.update(_call_claude_summary(client, market_prompt))
        except Exception as e:
            log.error(f"{market} summary failed: {e}")
            for field in ["themes", "positive", "negative", "watchouts"]:
                summary[f"{market.lower()}_{field}"] = "Error generating summary"

    col_order = [
        "week_of", "total_mentions",
        "news_count", "reddit_count", "youtube_count",
        "global_count", "sg_count", "my_count", "th_count", "hk_count",
        "sonos_count", "marshall_count", "bw_count",
        "positive_count", "negative_count",
        "global_top_themes", "global_positive_signals", "global_negative_signals",
        "global_emerging_trends", "global_reddit_pulse", "global_youtube_pulse",
        "global_competitor_signals", "global_watchouts",
        "sg_themes", "sg_positive", "sg_negative", "sg_watchouts",
        "my_themes", "my_positive", "my_negative", "my_watchouts",
        "th_themes", "th_positive", "th_negative", "th_watchouts",
        "hk_themes", "hk_positive", "hk_negative", "hk_watchouts",
    ]
    summary_df = pd.DataFrame([summary])
    summary_df = summary_df[[c for c in col_order if c in summary_df.columns]]
    return summary_df


# ══════════════════════════════════════════════════════════════════════════════
# 7. GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════════════════════

def connect_google_sheets() -> gspread.Spreadsheet:
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
    if df.empty:
        log.warning(f"Nothing to write to {tab_name!r} - DataFrame is empty.")
        return

    ws = _get_or_create_worksheet(spreadsheet, tab_name)
    df = df.fillna("").astype(str)
    expected_headers = df.columns.tolist()

    existing = ws.get_all_values()
    non_empty_rows = [row for row in existing if any(cell.strip() for cell in row)]

    if not non_empty_rows:
        data = [expected_headers] + df.values.tolist()
        ws.update(range_name="A1", values=data)
        log.info(f"Wrote {len(df)} rows (+headers) to {tab_name!r}.")
    else:
        current_headers = [cell.strip() for cell in non_empty_rows[0]]
        if current_headers != expected_headers:
            ws.insert_row(expected_headers, index=1)
            log.info(f"Inserted missing headers into {tab_name!r}.")
        ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
        log.info(f"Appended {len(df)} rows to {tab_name!r}.")


# ══════════════════════════════════════════════════════════════════════════════
# 8. MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info(f"Social Listening Run - {RUN_DATE}")
    log.info("=" * 60)

    if not ANTHROPIC_API_KEY:
        raise EnvironmentError("ANTHROPIC_API_KEY is not set.")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON is not set.")

    log.info(f"Apify   : {'enabled' if APIFY_TOKEN else 'skipped (set APIFY_TOKEN)'}")
    log.info(f"YouTube : {'enabled' if YOUTUBE_API_KEY else 'skipped (set YOUTUBE_API_KEY)'}")
    log.info(f"News    : Google News RSS")
    log.info(f"Regions : Global + SEA ({', '.join(r['label'] for r in REGIONS[1:])})")

    all_raw: list[dict] = []

    # Step 1a: News
    log.info("Step 1a -- News search (all regions)...")
    for region in REGIONS:
        for query in NEWS_QUERIES:
            try:
                results = search_news(query, region=region)
                all_raw.extend(results)
            except Exception as e:
                log.warning(f"  ! News [{region['label']}] {query!r}: {e}")

        if region["local_domains"]:
            for query in NEWS_QUERIES[:3]:
                try:
                    results = search_news_local_domains(query, region)
                    all_raw.extend(results)
                except Exception as e:
                    log.warning(f"  ! LocalNews [{region['label']}] {query!r}: {e}")

        label = region["label"]
        region_count = sum(1 for r in all_raw if r.get("region") == label and r.get("source") == "news")
        log.info(f"  + {label}: {region_count} news results so far")

    # Step 1b: Reddit via Apify
    log.info("Step 1b -- Reddit search via Apify...")
    for query, subreddit in REDDIT_QUERIES_GLOBAL:
        try:
            results = search_reddit(query, subreddit, region="Global")
            all_raw.extend(results)
            scope = f"r/{subreddit}" if subreddit else "all"
            log.info(f"  + {len(results)} posts [Global]: {query!r} ({scope})")
        except Exception as e:
            log.warning(f"  ! Reddit [Global] {query!r}: {e}")

    SEA_SUBREDDIT_REGION = {
        "singapore": "SG",
        "malaysia": "MY",
        "Thailand": "TH",
        "HongKong": "HK",
    }

    for query, subreddit in REDDIT_QUERIES_SEA:
        region_label = SEA_SUBREDDIT_REGION.get(subreddit, "SEA")
        try:
            results = search_reddit(query, subreddit, region=region_label)
            all_raw.extend(results)
            log.info(f"  + {len(results)} posts [{region_label}]: r/{subreddit}")
        except Exception as e:
            log.warning(f"  ! Reddit [{region_label}] r/{subreddit}: {e}")

    # Step 1c: YouTube
    log.info("Step 1c -- YouTube search (all regions)...")
    for region in REGIONS:
        for query in YOUTUBE_QUERIES:
            try:
                results = search_youtube(query, region=region)
                all_raw.extend(results)
            except Exception as e:
                log.warning(f"  ! YouTube [{region['label']}] {query!r}: {e}")
        label = region["label"]
        region_count = sum(1 for r in all_raw if r.get("region") == label and r.get("source") == "youtube")
        log.info(f"  + {label}: {region_count} YouTube results so far")

    # Step 2: Normalise
    log.info("Step 2 -- Normalising results...")
    df = normalize_results(all_raw)
    if df.empty:
        log.error("No results after normalisation. Exiting.")
        return

    if "region" in df.columns:
        for region_label, count in df["region"].value_counts().items():
            log.info(f"  |-- {region_label}: {count} mentions")

    # Step 3: Classify
    log.info(f"Step 3 -- Classifying {len(df)} mentions with Claude...")
    df = classify_mentions_with_claude(df)

    # Step 4: Summary
    log.info("Step 4 -- Generating weekly summary...")
    summary_df = generate_weekly_summary(df)

    # Step 5: Write to Sheets
    log.info("Step 5 -- Writing to Google Sheets...")
    try:
        spreadsheet = connect_google_sheets()
        mentions_df = df.drop(columns=["url_hash"], errors="ignore")
        append_dataframe_to_sheet(spreadsheet, mentions_df, "Raw_Mentions")
        if not summary_df.empty:
            append_dataframe_to_sheet(spreadsheet, summary_df, "Weekly_Summary")
    except Exception as e:
        log.error(f"Google Sheets write failed: {e}\n{traceback.format_exc()}")

    log.info("=" * 60)
    log.info(f"Run complete. {len(df)} mentions processed.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

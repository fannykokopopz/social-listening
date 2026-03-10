# Weekly Social Listening Script
# Runs every Monday to collect brand mentions, classify them with Claude,
# and write results to Google Sheets.
#
# Sources:
#   - Google News RSS    (free, no key)
#   - Reddit             (free, needs REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET)
#   - YouTube            (free tier, needs YOUTUBE_API_KEY)
#   - SerpAPI            (optional paid upgrade, set SERP_API_KEY)
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
from google.oauth2.service_account import Credentials

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

# News search queries (Google News RSS / SerpAPI)
NEWS_QUERIES = [
    "Sonos speaker review",
    "Marshall speaker review OR Marshall headphones review",
    "Bowers & Wilkins review",
    "wireless speaker trend",
    "home audio trend",
    "premium audio campaign",
]

# Reddit search queries  →  (query, subreddit or None for all)
REDDIT_QUERIES = [
    ("Sonos", "sonos"),
    ("Sonos", "audiophile"),
    ("Marshall speaker OR Marshall headphones", None),
    ("Bowers Wilkins OR B&W speaker", None),
    ("wireless speaker recommendation", "audiophile"),
    ("home audio setup", "hometheater"),
]

# YouTube search queries
YOUTUBE_QUERIES = [
    "Sonos speaker review",
    "Marshall speaker review",
    "Bowers Wilkins review",
    "best wireless speaker 2025",
    "premium home audio review",
]

BRANDS = ["Sonos", "Marshall", "Bowers & Wilkins", "B&W"]

GOOGLE_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# ── Credentials (set as environment variables) ─────────────────────────────────
SERP_API_KEY               = os.environ.get("SERP_API_KEY", "")
ANTHROPIC_API_KEY          = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SHEET_NAME          = os.environ.get("GOOGLE_SHEET_NAME", "Weekly Social Listening")
REDDIT_CLIENT_ID           = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET       = os.environ.get("REDDIT_CLIENT_SECRET", "")
YOUTUBE_API_KEY            = os.environ.get("YOUTUBE_API_KEY", "")

RUN_DATE     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
WEEK_AGO     = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
WEEK_AGO_ISO = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ══════════════════════════════════════════════════════════════════════════════
# 1.  NEWS SEARCH  (Google News RSS or SerpAPI)
# ══════════════════════════════════════════════════════════════════════════════

def search_news(query: str, num_results: int = 10) -> list[dict]:
    """Search news articles. Uses SerpAPI if key set, else Google News RSS."""
    if SERP_API_KEY:
        return _search_via_serpapi(query, num_results)
    else:
        return _search_via_google_news(query, num_results)


def _search_via_serpapi(query: str, num_results: int) -> list[dict]:
    """Paid search via SerpAPI – most reliable, respects date filters."""
    log.info(f"[SerpAPI] {query!r}")
    params = {
        "q": query,
        "api_key": SERP_API_KEY,
        "num": num_results,
        "tbs": "qdr:w",
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
                "source": "news",
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "published_date": item.get("date", ""),
                "query": query,
                "platform": "Web",
                "extra": "",
            })
        return results
    except Exception as e:
        log.warning(f"SerpAPI error for {query!r}: {e}")
        return []


def _search_via_google_news(query: str, num_results: int) -> list[dict]:
    """Free fallback: Google News RSS. No API key required."""
    log.info(f"[GoogleNews] {query!r}")
    try:
        resp = requests.get(
            "https://news.google.com/rss/search",
            params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
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
            })
        time.sleep(1)
        return results
    except Exception as e:
        log.warning(f"Google News RSS error for {query!r}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 2.  REDDIT
# ══════════════════════════════════════════════════════════════════════════════

_reddit_token: str | None = None
_reddit_token_expiry: float = 0.0


def _get_reddit_token() -> str | None:
    """
    Fetch a Reddit OAuth token using client credentials (app-only auth).
    Caches the token until it expires.

    Setup (one-time, free):
      1. Go to https://www.reddit.com/prefs/apps
      2. Click "Create App" → type: script → redirect URI: http://localhost:8080
      3. Copy the client ID (shown under the app name) and the secret
      4. Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET as env vars
    """
    global _reddit_token, _reddit_token_expiry
    if _reddit_token and time.time() < _reddit_token_expiry - 60:
        return _reddit_token
    try:
        resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            data={"grant_type": "client_credentials"},
            auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
            headers={"User-Agent": "social-listening-bot/1.0 (by /u/your_reddit_username)"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        _reddit_token = data["access_token"]
        _reddit_token_expiry = time.time() + data.get("expires_in", 3600)
        log.info("[Reddit] Token refreshed.")
        return _reddit_token
    except Exception as e:
        log.warning(f"[Reddit] Token fetch failed: {e}")
        return None


def search_reddit(query: str, subreddit: str | None = None, num_results: int = 10) -> list[dict]:
    """
    Search Reddit posts from the past week using the official OAuth API.
    Returns posts with title, body snippet, subreddit, score, and comment count.
    """
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        log.info("[Reddit] Skipping — REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set.")
        return []

    token = _get_reddit_token()
    if not token:
        return []

    if subreddit:
        endpoint = f"https://oauth.reddit.com/r/{subreddit}/search"
        log.info(f"[Reddit] r/{subreddit} <- {query!r}")
    else:
        endpoint = "https://oauth.reddit.com/search"
        log.info(f"[Reddit] all <- {query!r}")

    params = {
        "q": query,
        "sort": "new",
        "t": "week",
        "limit": num_results,
        "restrict_sr": "true" if subreddit else "false",
        "type": "link",
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "social-listening-bot/1.0 (by /u/your_reddit_username)",
    }
    try:
        resp = requests.get(endpoint, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for child in data.get("data", {}).get("children", []):
            p = child.get("data", {})
            created_utc = p.get("created_utc", 0)
            pub_date = (
                datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime("%Y-%m-%d")
                if created_utc else ""
            )
            body = (p.get("selftext") or "").strip()[:200]
            snippet = body if body else p.get("url", "")
            score = p.get("score", 0)
            num_comments = p.get("num_comments", 0)
            sub = p.get("subreddit", "")
            results.append({
                "source": "reddit",
                "title": p.get("title", ""),
                "url": f"https://reddit.com{p.get('permalink', '')}",
                "snippet": snippet,
                "published_date": pub_date,
                "query": query,
                "platform": "Reddit",
                "extra": f"r/{sub} | score:{score} | comments:{num_comments}",
            })
        time.sleep(1)
        return results
    except Exception as e:
        log.warning(f"[Reddit] Search error for {query!r}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 3.  YOUTUBE
# ══════════════════════════════════════════════════════════════════════════════

def search_youtube(query: str, num_results: int = 10) -> list[dict]:
    """
    Search YouTube for recent videos using the YouTube Data API v3.
    Returns video title, channel, view/like counts, and publish date.

    Setup (one-time, free):
      1. Go to https://console.cloud.google.com
      2. Create a project -> Enable "YouTube Data API v3"
      3. Credentials -> Create API Key
      4. Set YOUTUBE_API_KEY as an env var
      Free quota: 10,000 units/day. Each search = 100 units -> ~100 searches/day free.
    """
    if not YOUTUBE_API_KEY:
        log.info("[YouTube] Skipping — YOUTUBE_API_KEY not set.")
        return []

    log.info(f"[YouTube] {query!r}")
    try:
        # Step 1: Search for video IDs
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
            },
            timeout=15,
        )
        search_resp.raise_for_status()
        search_data = search_resp.json()
        items = search_data.get("items", [])
        if not items:
            return []

        # Step 2: Fetch view/like stats for all video IDs in one call
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
            })
        time.sleep(1)
        return results
    except Exception as e:
        log.warning(f"[YouTube] Search error for {query!r}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# 4.  NORMALISE & DEDUPLICATE
# ══════════════════════════════════════════════════════════════════════════════

def normalize_results(raw_results: list[dict]) -> pd.DataFrame:
    """
    Clean, normalise, and deduplicate results from all sources.
    Preserves source-specific fields: source, platform, extra.
    """
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
    df = df[df["title"].str.strip() != ""]

    cols = ["run_date", "source", "platform", "query", "title", "domain",
            "date", "url", "snippet", "extra", "url_hash"]
    df = df[[c for c in cols if c in df.columns]]

    log.info(f"Normalised {len(df)} unique results from {len(raw_results)} raw.")
    if "source" in df.columns:
        for src, count in df["source"].value_counts().items():
            log.info(f"  |-- {src}: {count}")

    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 5.  CLASSIFY WITH CLAUDE
# ══════════════════════════════════════════════════════════════════════════════

def classify_mentions_with_claude(df: pd.DataFrame) -> pd.DataFrame:
    """
    Send each mention to Claude for classification.
    Adds columns: brand, mention_type, sentiment, theme, ai_notes.
    Prompt is source-aware: Reddit and YouTube get tailored mention_type options.
    """
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
    """Classify a single mention with source-aware prompt."""
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
# 6.  WEEKLY SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def generate_weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use Claude to generate a structured weekly summary across all sources.
    Adds reddit_consumer_pulse and youtube_content_pulse fields.
    """
    if df.empty:
        log.warning("No data to summarise.")
        return pd.DataFrame()

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    snapshot_rows = []
    for _, row in df.iterrows():
        snapshot_rows.append(
            f"- [{row.get('source','').upper()}] [{row.get('brand')}] [{row.get('sentiment')}] "
            f"[{row.get('mention_type')}] [{row.get('theme')}] {row.get('title')} "
            f"({row.get('platform')} | {row.get('extra', '')})"
        )
    snapshot = "\n".join(snapshot_rows[:100])

    prompt = f"""You are a senior marketing analyst at a premium audio brand distributor in Singapore.
You cover: Sonos, Marshall, Bowers & Wilkins.
Sources this week include: news articles, Reddit discussions, and YouTube videos.

Below is this week's social listening data ({RUN_DATE}):

{snapshot}

Write a concise weekly summary for the marketing lead. Respond ONLY with a valid JSON object:

{{
  "top_themes": "<3-5 bullet points as a single string, separated by | >",
  "positive_signals": "<2-3 bullet points as a single string, separated by | >",
  "negative_signals": "<2-3 bullet points as a single string, separated by | >",
  "emerging_trends": "<2-3 bullet points as a single string, separated by | >",
  "reddit_consumer_pulse": "<2-3 bullet points summarising Reddit sentiment and discussions, separated by | >",
  "youtube_content_pulse": "<2-3 bullet points summarising YouTube review activity, separated by | >",
  "competitor_campaign_signals": "<2-3 bullet points as a single string, separated by | >",
  "marketer_watchouts": "<2-3 bullet points as a single string, separated by | >"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1200,
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

        src = df.get("source", pd.Series(dtype=str))
        summary["news_count"]    = int((src == "news").sum())
        summary["reddit_count"]  = int((src == "reddit").sum())
        summary["youtube_count"] = int((src == "youtube").sum())

        brand = df.get("brand", pd.Series(dtype=str))
        summary["sonos_count"]    = int((brand == "Sonos").sum())
        summary["marshall_count"] = int((brand == "Marshall").sum())
        summary["bw_count"]       = int((brand == "Bowers & Wilkins").sum())

        sent = df.get("sentiment", pd.Series(dtype=str))
        summary["positive_count"] = int((sent == "Positive").sum())
        summary["negative_count"] = int((sent == "Negative").sum())

        col_order = [
            "week_of", "total_mentions",
            "news_count", "reddit_count", "youtube_count",
            "sonos_count", "marshall_count", "bw_count",
            "positive_count", "negative_count",
            "top_themes", "positive_signals", "negative_signals",
            "emerging_trends", "reddit_consumer_pulse", "youtube_content_pulse",
            "competitor_campaign_signals", "marketer_watchouts",
        ]
        summary_df = pd.DataFrame([summary])
        summary_df = summary_df[[c for c in col_order if c in summary_df.columns]]
        return summary_df

    except Exception as e:
        log.error(f"Summary generation failed: {e}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# 7.  GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════════════════════

def connect_google_sheets() -> gspread.Spreadsheet:
    """Authenticate and return the spreadsheet object."""
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
    """Return worksheet by name, creating it if it doesn't exist."""
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
    Writes headers if the sheet is empty; always appends below existing data.
    """
    if df.empty:
        log.warning(f"Nothing to write to {tab_name!r} - DataFrame is empty.")
        return

    ws = _get_or_create_worksheet(spreadsheet, tab_name)
    df = df.fillna("").astype(str)

    existing = ws.get_all_values()
    if not existing:
        data = [df.columns.tolist()] + df.values.tolist()
        ws.update(range_name="A1", values=data)
        log.info(f"Wrote {len(df)} rows (+headers) to {tab_name!r}.")
    else:
        ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
        log.info(f"Appended {len(df)} rows to {tab_name!r}.")


# ══════════════════════════════════════════════════════════════════════════════
# 8.  MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info(f"Social Listening Run - {RUN_DATE}")
    log.info("=" * 60)

    if not ANTHROPIC_API_KEY:
        raise EnvironmentError("ANTHROPIC_API_KEY is not set.")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON is not set.")

    log.info(f"Reddit  : {'enabled' if REDDIT_CLIENT_ID else 'skipped (set REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET)'}")
    log.info(f"YouTube : {'enabled' if YOUTUBE_API_KEY else 'skipped (set YOUTUBE_API_KEY)'}")
    log.info(f"SerpAPI : {'enabled' if SERP_API_KEY else 'using Google News RSS fallback'}")

    all_raw: list[dict] = []

    # Step 1a: News
    log.info("Step 1a -- News search...")
    for query in NEWS_QUERIES:
        try:
            results = search_news(query)
            all_raw.extend(results)
            log.info(f"  + {len(results)} results: {query!r}")
        except Exception as e:
            log.warning(f"  ! Failed: {query!r}: {e}")

    # Step 1b: Reddit
    log.info("Step 1b -- Reddit search...")
    for query, subreddit in REDDIT_QUERIES:
        try:
            results = search_reddit(query, subreddit)
            all_raw.extend(results)
            scope = f"r/{subreddit}" if subreddit else "all"
            log.info(f"  + {len(results)} posts: {query!r} ({scope})")
        except Exception as e:
            log.warning(f"  ! Failed: {query!r}: {e}")

    # Step 1c: YouTube
    log.info("Step 1c -- YouTube search...")
    for query in YOUTUBE_QUERIES:
        try:
            results = search_youtube(query)
            all_raw.extend(results)
            log.info(f"  + {len(results)} videos: {query!r}")
        except Exception as e:
            log.warning(f"  ! Failed: {query!r}: {e}")

    # Step 2: Normalise
    log.info("Step 2 -- Normalising results...")
    df = normalize_results(all_raw)
    if df.empty:
        log.error("No results after normalisation. Exiting.")
        return

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

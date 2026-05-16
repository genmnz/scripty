#!/usr/bin/env python3
"""
scripty — fast, lightweight social media & web monitor
Searches Twitter (Nitter), GitHub (code + commits), Reddit, and DuckDuckGo
for exact strings. Sends new hits to Telegram. Zero paid APIs.
"""
import argparse
import asyncio
import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import NamedTuple
from urllib.parse import quote_plus, unquote

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str = os.environ["TELEGRAM_CHAT_ID"]

SEARCH_TERMS: list[str] = [
    t.strip()
    for t in os.environ.get("SEARCH_TERMS", "").split(",")
    if t.strip()
]
CHECK_INTERVAL: int = int(os.environ.get("CHECK_INTERVAL_MINUTES", "15"))
LOOKBACK_HOURS: int = int(os.environ.get("LOOKBACK_HOURS", "24"))
PLATFORMS: set[str] = {
    p.strip().lower()
    for p in os.environ.get("PLATFORMS", "github,twitter,reddit,web").split(",")
    if p.strip()
}
GITHUB_TOKEN: str = os.environ.get("GITHUB_TOKEN", "")
DB_PATH: str = os.environ.get("DB_PATH", "seen.db")

NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.cz",
    "https://nitter.1d4.us",
    "https://nitter.woodland.cafe",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scripty")

# ── Data ──────────────────────────────────────────────────────────────────────


class Hit(NamedTuple):
    platform: str
    term: str
    title: str
    url: str
    snippet: str
    published: datetime | None


# ── Storage ───────────────────────────────────────────────────────────────────


class Store:
    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS seen (hash TEXT PRIMARY KEY, ts TEXT)"
        )
        self._db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON seen(ts)")
        self._db.commit()

    def is_new(self, url: str) -> bool:
        h = hashlib.sha256(url.encode()).hexdigest()[:20]
        if self._db.execute("SELECT 1 FROM seen WHERE hash=?", (h,)).fetchone():
            return False
        self._db.execute(
            "INSERT INTO seen VALUES (?,?)", (h, datetime.utcnow().isoformat())
        )
        self._db.commit()
        return True

    def purge_old(self, days: int = 30) -> None:
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        self._db.execute("DELETE FROM seen WHERE ts<?", (cutoff,))
        self._db.commit()


# ── Telegram ──────────────────────────────────────────────────────────────────

_EMOJI = {"github": "🐙", "twitter": "🐦", "reddit": "🟠", "web": "🌐"}


def _fmt_hit(hit: Hit) -> str:
    emoji = _EMOJI.get(hit.platform, "🔍")
    age = ""
    if hit.published:
        delta = datetime.now(timezone.utc) - hit.published.astimezone(timezone.utc)
        h = int(delta.total_seconds() // 3600)
        age = f" · {h}h ago" if h < 48 else ""
    title = hit.title.replace("<", "&lt;").replace(">", "&gt;")
    snippet = hit.snippet[:300].replace("<", "&lt;").replace(">", "&gt;") if hit.snippet else ""
    return (
        f'{emoji} <b>{hit.platform.upper()}</b> — <code>{hit.term}</code>{age}\n'
        f'<b>{title}</b>\n'
        + (f'{snippet}\n' if snippet else "")
        + f'<a href="{hit.url}">{hit.url}</a>'
    )


async def send_telegram(client: httpx.AsyncClient, text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    for attempt in range(3):
        try:
            r = await client.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return
            log.warning("Telegram %s: %s", r.status_code, r.text[:200])
        except Exception as exc:
            log.warning("Telegram send error (attempt %d): %s", attempt + 1, exc)
        await asyncio.sleep(2 ** attempt)


async def notify_batch(client: httpx.AsyncClient, hits: list[Hit]) -> None:
    for hit in hits:
        await send_telegram(client, _fmt_hit(hit))
        await asyncio.sleep(0.4)  # stay under Telegram flood limits


# ── GitHub — full-text code + commit search ───────────────────────────────────
#
# Uses the same backend as github.com/search?type=code and type=commits.
# Code search scans actual file contents. Commit search scans commit messages.
# Rate limits: 10 req/min unauthenticated → 30 req/min with any personal token.


async def search_github(client: httpx.AsyncClient, term: str, since: datetime) -> list[Hit]:
    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    hits: list[Hit] = []

    base_headers: dict[str, str] = {"User-Agent": "scripty/1.0"}
    if GITHUB_TOKEN:
        base_headers["Authorization"] = f"token {GITHUB_TOKEN}"

    # ── Code search (file contents) ──────────────────────────────────────────
    # pushed:>DATE narrows to repos that received commits recently
    code_headers = {**base_headers, "Accept": "application/vnd.github.v3+json"}
    code_url = (
        f"https://api.github.com/search/code"
        f"?q={quote_plus(term)}+pushed:>{since_str}"
        f"&sort=indexed&order=desc&per_page=20"
    )
    try:
        r = await client.get(code_url, headers=code_headers, timeout=15)
        if r.status_code == 403:
            log.warning("GitHub rate-limited (code) — set GITHUB_TOKEN for 3× quota")
        elif r.status_code == 200:
            for item in r.json().get("items", []):
                repo = item.get("repository", {})
                path = item.get("path", "")
                link = item.get("html_url", "")
                title = f"{repo.get('full_name', '')} — {path}"
                # text_matches gives highlighted snippets when using the text-match media type;
                # fall back to the repo description
                snippet = repo.get("description") or ""
                if link:
                    hits.append(Hit(
                        platform="github", term=term,
                        title=title, url=link, snippet=snippet, published=None,
                    ))
        else:
            log.warning("GitHub code search %s: %s", r.status_code, r.text[:120])
    except Exception as exc:
        log.warning("GitHub code search error: %s", exc)

    await asyncio.sleep(1.2)  # stay inside rate limit window

    # ── Commit search (commit messages) ──────────────────────────────────────
    # author-date:>DATE is a valid qualifier for /search/commits
    commit_headers = {
        **base_headers,
        # cloak-preview unlocks the commits search endpoint
        "Accept": "application/vnd.github.cloak-preview+json",
    }
    commit_url = (
        f"https://api.github.com/search/commits"
        f"?q={quote_plus(term)}+author-date:>{since_str}"
        f"&sort=author-date&order=desc&per_page=15"
    )
    try:
        r = await client.get(commit_url, headers=commit_headers, timeout=15)
        if r.status_code == 403:
            log.warning("GitHub rate-limited (commits)")
        elif r.status_code == 200:
            for item in r.json().get("items", []):
                commit = item.get("commit", {})
                repo = item.get("repository", {})
                msg = commit.get("message", "").split("\n")[0]  # first line only
                link = item.get("html_url", "")
                raw_date = commit.get("author", {}).get("date") or commit.get("committer", {}).get("date")
                dt = None
                if raw_date:
                    try:
                        dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    except ValueError:
                        pass
                title = f"{repo.get('full_name', 'unknown')} — {msg[:80]}"
                if link:
                    hits.append(Hit(
                        platform="github", term=term,
                        title=title, url=link, snippet=msg, published=dt,
                    ))
        else:
            log.warning("GitHub commit search %s: %s", r.status_code, r.text[:120])
    except Exception as exc:
        log.warning("GitHub commit search error: %s", exc)

    return hits


# ── Reddit ────────────────────────────────────────────────────────────────────
#
# Reddit's public JSON API, no key required. Searches posts across all
# subreddits. time filter maps to the nearest bucket that covers LOOKBACK_HOURS.


def _reddit_time_filter() -> str:
    if LOOKBACK_HOURS <= 1:
        return "hour"
    if LOOKBACK_HOURS <= 24:
        return "day"
    if LOOKBACK_HOURS <= 168:
        return "week"
    return "month"


async def search_reddit(client: httpx.AsyncClient, term: str, since: datetime) -> list[Hit]:
    tf = _reddit_time_filter()
    url = (
        f"https://www.reddit.com/search.json"
        f"?q={quote_plus(term)}&sort=new&t={tf}&limit=25&include_over_18=on"
    )
    headers = {
        "User-Agent": "scripty:1.0 (monitoring bot)",
        "Accept": "application/json",
    }
    try:
        r = await client.get(url, headers=headers, timeout=15)
        if r.status_code == 429:
            log.warning("Reddit rate-limited, will retry next cycle")
            return []
        if r.status_code != 200:
            log.warning("Reddit %s: %s", r.status_code, r.text[:120])
            return []
        data = r.json()
    except Exception as exc:
        log.warning("Reddit error: %s", exc)
        return []

    hits: list[Hit] = []
    for child in data.get("data", {}).get("children", []):
        post = child.get("data", {})
        created = post.get("created_utc")
        dt = datetime.fromtimestamp(created, tz=timezone.utc) if created else None

        # skip posts outside our lookback window
        if dt and dt < since:
            continue

        title = post.get("title", "")
        sub = post.get("subreddit_name_prefixed", "")
        selftext = (post.get("selftext") or "")[:300]
        permalink = post.get("permalink", "")
        link = f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink

        snippet = f"r/{sub} — {selftext}" if selftext else sub
        if link:
            hits.append(Hit(
                platform="reddit", term=term,
                title=title, url=link, snippet=snippet.strip(" —"), published=dt,
            ))

    return hits


# ── Twitter via Nitter ────────────────────────────────────────────────────────

_nitter_base: str | None = None


async def _probe_nitter(client: httpx.AsyncClient) -> str | None:
    for base in NITTER_INSTANCES:
        try:
            r = await client.get(f"{base}/search?q=test&f=tweets", timeout=8)
            if r.status_code == 200 and "timeline" in r.text:
                log.info("Using nitter instance: %s", base)
                return base
        except Exception:
            pass
    return None


async def search_twitter(client: httpx.AsyncClient, term: str, since: datetime) -> list[Hit]:
    global _nitter_base
    if _nitter_base is None:
        _nitter_base = await _probe_nitter(client)
    if _nitter_base is None:
        log.warning("No reachable Nitter instance — skipping Twitter search")
        return []

    url = f"{_nitter_base}/search?q={quote_plus(term)}&f=tweets"
    try:
        r = await client.get(url, timeout=15)
        if r.status_code != 200:
            _nitter_base = None
            return []
    except Exception as exc:
        log.warning("Nitter error: %s", exc)
        _nitter_base = None
        return []

    soup = BeautifulSoup(r.text, "lxml")
    hits: list[Hit] = []

    for item in soup.select(".timeline-item"):
        dt = None
        date_tag = item.select_one(".tweet-date a")
        if date_tag:
            # Nitter title attr: "Jan 1, 2024 · 12:00 PM UTC"
            try:
                clean = re.sub(r"\s*·.*", "", date_tag.get("title", "")).strip()
                dt = datetime.strptime(clean, "%b %d, %Y %I:%M %p UTC").replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        if dt and dt < since:
            continue

        content = item.select_one(".tweet-content")
        text = content.get_text(" ", strip=True) if content else ""
        if not text:
            continue

        username = (item.select_one(".username") or item).get_text(strip=True)
        fullname_tag = item.select_one(".fullname")
        fullname = fullname_tag.get_text(strip=True) if fullname_tag else username

        href = date_tag["href"] if date_tag else ""
        tweet_url = f"https://x.com{href}" if href.startswith("/") else href

        hits.append(Hit(
            platform="twitter", term=term,
            title=f"{fullname} ({username})",
            url=tweet_url, snippet=text[:300], published=dt,
        ))

    return hits


# ── DuckDuckGo web search ─────────────────────────────────────────────────────


async def search_web(client: httpx.AsyncClient, term: str, since: datetime) -> list[Hit]:
    df = "d" if LOOKBACK_HOURS <= 24 else "w"
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(term)}&df={df}"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = await client.get(url, headers=headers, timeout=15, follow_redirects=True)
        if r.status_code != 200:
            log.warning("DDG %s", r.status_code)
            return []
    except Exception as exc:
        log.warning("DDG error: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    hits: list[Hit] = []

    for result in soup.select(".result"):
        title_tag = result.select_one(".result__title a")
        snippet_tag = result.select_one(".result__snippet")
        if not title_tag:
            continue

        href = title_tag.get("href", "")
        real_url = href
        if "duckduckgo.com" in href:
            m = re.search(r"[?&]uddg=([^&]+)", href)
            real_url = unquote(m.group(1)) if m else href

        if not real_url or real_url.startswith("//"):
            continue

        title = title_tag.get_text(strip=True)
        snippet = snippet_tag.get_text(" ", strip=True) if snippet_tag else ""
        hits.append(Hit(
            platform="web", term=term,
            title=title, url=real_url, snippet=snippet[:300], published=None,
        ))

    return hits


# ── Orchestration ─────────────────────────────────────────────────────────────


async def run_cycle(client: httpx.AsyncClient, store: Store) -> int:
    since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    all_new: list[Hit] = []

    for term in SEARCH_TERMS:
        log.info("Searching: %r", term)
        tasks = []
        if "github" in PLATFORMS:
            tasks.append(search_github(client, term, since))
        if "twitter" in PLATFORMS:
            tasks.append(search_twitter(client, term, since))
        if "reddit" in PLATFORMS:
            tasks.append(search_reddit(client, term, since))
        if "web" in PLATFORMS:
            tasks.append(search_web(client, term, since))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                log.error("Search error: %s", res)
                continue
            for hit in res:
                if store.is_new(hit.url):
                    all_new.append(hit)

    log.info("Found %d new result(s)", len(all_new))
    if all_new:
        await notify_batch(client, all_new)

    store.purge_old(days=30)
    return len(all_new)


async def main(run_once: bool = False) -> None:
    if not SEARCH_TERMS:
        raise SystemExit("Set SEARCH_TERMS env var (comma-separated strings to search)")

    log.info(
        "scripty — terms: %s | platforms: %s | every %dm | lookback %dh",
        ", ".join(SEARCH_TERMS), ", ".join(sorted(PLATFORMS)),
        CHECK_INTERVAL, LOOKBACK_HOURS,
    )

    store = Store(DB_PATH)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        while True:
            try:
                await run_cycle(client, store)
            except Exception as exc:
                log.error("Cycle error: %s", exc, exc_info=True)

            if run_once:
                break

            log.info("Sleeping %d minutes…", CHECK_INTERVAL)
            await asyncio.sleep(CHECK_INTERVAL * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Social media & web monitor → Telegram")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()
    asyncio.run(main(run_once=args.once))

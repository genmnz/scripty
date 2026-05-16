#!/usr/bin/env python3
"""
scripty — fast, lightweight social media & web monitor
Searches Twitter (Nitter), GitHub, and the web (DuckDuckGo) for configured terms.
Sends new results to a Telegram group via bot.
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
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── Config (all via env vars) ─────────────────────────────────────────────────

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
    for p in os.environ.get("PLATFORMS", "github,twitter,web").split(",")
    if p.strip()
}
GITHUB_TOKEN: str = os.environ.get("GITHUB_TOKEN", "")
DB_PATH: str = os.environ.get("DB_PATH", "seen.db")

# Nitter public instances — tried in order, first healthy one is used
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

PLATFORM_EMOJI = {"github": "🐙", "twitter": "🐦", "web": "🌐"}


def _fmt_hit(hit: Hit) -> str:
    emoji = PLATFORM_EMOJI.get(hit.platform, "🔍")
    age = ""
    if hit.published:
        delta = datetime.now(timezone.utc) - hit.published.astimezone(timezone.utc)
        h = int(delta.total_seconds() // 3600)
        age = f" · {h}h ago" if h < 48 else ""
    snippet = hit.snippet[:280].replace("<", "&lt;").replace(">", "&gt;") if hit.snippet else ""
    title = hit.title.replace("<", "&lt;").replace(">", "&gt;")
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
        await asyncio.sleep(0.3)  # avoid Telegram flood limits


# ── GitHub search ─────────────────────────────────────────────────────────────


async def search_github(client: httpx.AsyncClient, term: str, since: datetime) -> list[Hit]:
    headers: dict[str, str] = {"Accept": "application/vnd.github.v3+json", "User-Agent": "scripty/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    hits: list[Hit] = []

    queries = [
        # (endpoint, date_field, label)
        ("repositories", f"{quote_plus(term)}+pushed:>{since_str}", "pushed_at"),
        ("issues", f"{quote_plus(term)}+created:>{since_str}", "created_at"),
    ]

    for endpoint, q, date_field in queries:
        url = f"https://api.github.com/search/{endpoint}?q={q}&sort=updated&order=desc&per_page=15"
        try:
            r = await client.get(url, headers=headers, timeout=15)
            if r.status_code == 403:
                log.warning("GitHub rate-limited — add GITHUB_TOKEN to get 3× more quota")
                break
            if r.status_code != 200:
                log.warning("GitHub %s %s: %s", endpoint, r.status_code, r.text[:100])
                continue
            data = r.json()
        except Exception as exc:
            log.warning("GitHub error: %s", exc)
            continue

        for item in data.get("items", []):
            dt = None
            raw = item.get(date_field) or item.get("updated_at")
            if raw:
                try:
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except ValueError:
                    pass

            if endpoint == "repositories":
                title = item.get("full_name", "")
                snippet = item.get("description") or ""
                link = item.get("html_url", "")
            else:
                title = item.get("title", "")
                snippet = (item.get("body") or "")[:300]
                link = item.get("html_url", "")

            if link:
                hits.append(Hit(platform="github", term=term, title=title, url=link, snippet=snippet, published=dt))

        # respect rate limit window
        await asyncio.sleep(1)

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
            _nitter_base = None  # retry probe next cycle
            return []
    except Exception as exc:
        log.warning("Nitter error: %s", exc)
        _nitter_base = None
        return []

    soup = BeautifulSoup(r.text, "lxml")
    hits: list[Hit] = []

    for item in soup.select(".timeline-item"):
        # timestamp
        dt = None
        date_tag = item.select_one(".tweet-date a")
        if date_tag:
            title_attr = date_tag.get("title", "")
            # Nitter format: "Jan 1, 2024 · 12:00 PM UTC"
            try:
                # strip the middot part
                clean = re.sub(r"\s*·.*", "", title_attr).strip()
                dt = datetime.strptime(clean, "%b %d, %Y %I:%M %p UTC").replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        if dt and dt < since:
            continue

        content = item.select_one(".tweet-content")
        text = content.get_text(" ", strip=True) if content else ""
        if not text:
            continue

        username_tag = item.select_one(".username")
        fullname_tag = item.select_one(".fullname")
        username = username_tag.get_text(strip=True) if username_tag else "unknown"
        fullname = fullname_tag.get_text(strip=True) if fullname_tag else username

        tweet_link_tag = item.select_one(".tweet-date a")
        href = tweet_link_tag["href"] if tweet_link_tag else ""
        tweet_url = f"https://x.com{href}" if href.startswith("/") else href

        hits.append(Hit(
            platform="twitter",
            term=term,
            title=f"{fullname} ({username})",
            url=tweet_url,
            snippet=text[:300],
            published=dt,
        ))

    return hits


# ── DuckDuckGo web search ─────────────────────────────────────────────────────


async def search_web(client: httpx.AsyncClient, term: str, since: datetime) -> list[Hit]:
    # df=d = past day, df=w = past week
    hours = max(1, LOOKBACK_HOURS)
    df = "d" if hours <= 24 else "w"
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(term)}&df={df}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; scripty/1.0)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = await client.get(url, headers=headers, timeout=15, follow_redirects=True)
        if r.status_code != 200:
            log.warning("DDG %s: %s", r.status_code, r.text[:100])
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
        # DDG wraps URLs — extract real URL from uddg param or use as-is
        real_url = href
        if "duckduckgo.com/l/" in href or href.startswith("//duckduckgo.com"):
            m = re.search(r"[?&]uddg=([^&]+)", href)
            if m:
                from urllib.parse import unquote
                real_url = unquote(m.group(1))

        if not real_url or real_url.startswith("//"):
            continue

        title = title_tag.get_text(strip=True)
        snippet = snippet_tag.get_text(" ", strip=True) if snippet_tag else ""

        hits.append(Hit(
            platform="web",
            term=term,
            title=title,
            url=real_url,
            snippet=snippet[:300],
            published=None,
        ))

    return hits


# ── Orchestration ─────────────────────────────────────────────────────────────


async def run_cycle(client: httpx.AsyncClient, store: Store) -> int:
    since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    all_new: list[Hit] = []

    for term in SEARCH_TERMS:
        log.info("Searching for: %s", term)
        tasks = []
        if "github" in PLATFORMS:
            tasks.append(search_github(client, term, since))
        if "twitter" in PLATFORMS:
            tasks.append(search_twitter(client, term, since))
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
        raise SystemExit("Set SEARCH_TERMS env var (comma-separated keywords)")

    log.info("scripty starting — terms: %s | platforms: %s | interval: %dm | lookback: %dh",
             ", ".join(SEARCH_TERMS), ", ".join(sorted(PLATFORMS)), CHECK_INTERVAL, LOOKBACK_HOURS)

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

"""
TechsnifClient - Fetches startup/tech signal data from the TechSnif REST API.

TechSnif aggregates story clusters from TechCrunch, The Verge, Ars Technica,
Reuters, Financial Times and dozens more outlets. No API key required.

API base: https://api.techsnif.com
Endpoints used:
    GET /api/stories?sort=newest&limit=80    — latest story clusters
    GET /api/stories/search?q=<query>        — semantic search across clusters
"""

import re
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://api.techsnif.com"

# Startup-specific search queries — all scoped to early-stage companies
SIGNAL_SEARCH_QUERIES = [
    "seed round startup",
    "series A startup raises",
    "AI startup funding",
    "fintech startup seed",
    "web3 startup raises",
    "blockchain startup funding",
    "startup launches product",
    "early stage startup hiring",
    "venture backed startup",
    "pre-seed startup",
]


class TechsnifError(Exception):
    """Exception raised for errors in TechSnif client operations."""
    pass


class TechsnifClient:
    """
    Fetches recent tech/startup articles from the TechSnif public REST API.

    Each "article" returned is normalised from a TechSnif story cluster —
    a multi-source grouping that aggregates coverage from many outlets.
    The cluster headline, TechSnif editorial take, and excerpt are all
    combined into the `content` field so the signal detectors have rich text.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialise the client.

        Args:
            config: Optional config dict (from config.yaml). Recognised keys
                    under config['data_sources']['techsnif']:
                        rate_limit   (int, default 2) — requests per second
                        max_articles (int, default 80) — cap on articles
                        timeout      (int, default 15) — HTTP timeout seconds
        """
        cfg = {}
        if config:
            cfg = config.get("data_sources", {}).get("techsnif", {})

        self.rate_limit   = int(cfg.get("rate_limit",   2))
        self.max_articles = int(cfg.get("max_articles", 80))
        self.timeout      = int(cfg.get("timeout",      15))

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "TTR Signal Detector/1.0 (startup intelligence)"
        })
        self._last_request_time = 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def fetch_recent_articles(
        self,
        hours: int = 24,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent startup/tech articles from TechSnif.

        Strategy:
          1. Pull the newest story clusters from the main feed.
          2. Run targeted searches for common signal keywords.
          3. Deduplicate by URL, filter to the requested time window.

        Args:
            hours: How many hours back to include (default 24).
            limit: Cap on total articles returned (default: max_articles).

        Returns:
            List of normalised article dicts, sorted newest-first.
        """
        cap    = limit or self.max_articles
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        seen_urls: set              = set()
        all_articles: List[Dict]   = []

        # ── 1. Latest feed ────────────────────────────────────────────────────
        try:
            feed_articles = self._fetch_stories(sort="newest", limit=80)
            for art in feed_articles:
                url = art.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(art)
        except TechsnifError as e:
            logger.warning("Main feed failed: %s", e)
            print(f"   ⚠️  Main feed skipped: {e}")

        # ── 2. Signal-targeted searches ───────────────────────────────────────
        for query in SIGNAL_SEARCH_QUERIES:
            try:
                search_articles = self._search_stories(query=query, limit=20)
                for art in search_articles:
                    url = art.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_articles.append(art)
            except TechsnifError as e:
                logger.warning("Search '%s' failed: %s", query, e)

        # ── 3. Filter by date, sort, cap ──────────────────────────────────────
        recent = [a for a in all_articles if self._is_recent(a, cutoff)]
        recent.sort(key=lambda a: a.get("normalized_date", ""), reverse=True)
        result = recent[:cap]

        print(f"   📡 TechSnif: {len(result)} articles in last {hours}h "
              f"({len(all_articles)} total fetched, cap {cap})")
        return result

    def fetch_articles_from_sources(
        self,
        sources: List[str],
        hours: int = 24,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch articles filtered to specific publishers.

        Args:
            sources: Publisher names to include (e.g. ['TechCrunch']).
            hours:   Time window in hours.
            limit:   Max articles to return.

        Returns:
            Filtered list of normalised article dicts.
        """
        self._validate_sources(sources)
        all_articles = self.fetch_recent_articles(hours=hours, limit=None)
        filtered = [
            a for a in all_articles
            if a.get("source", "") in sources
        ]
        cap = limit or self.max_articles
        return filtered[:cap]

    # ── Private: API Calls ────────────────────────────────────────────────────

    def _fetch_stories(
        self,
        sort: str = "newest",
        limit: int = 80,
    ) -> List[Dict[str, Any]]:
        """Call GET /api/stories and return normalised articles."""
        self._enforce_rate_limit()
        url = f"{API_BASE}/api/stories"
        params = {"sort": sort, "limit": min(limit, 80)}

        try:
            resp = self._session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise TechsnifError(f"GET /api/stories failed: {e}") from e

        data = resp.json()
        stories = data.get("stories", [])
        return [self._normalise(s) for s in stories if s]

    def _search_stories(
        self,
        query: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Call GET /api/stories/search and return normalised articles."""
        self._enforce_rate_limit()
        url = f"{API_BASE}/api/stories/search"
        params = {"q": query, "limit": min(limit, 50)}

        try:
            resp = self._session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise TechsnifError(f"GET /api/stories/search failed: {e}") from e

        data = resp.json()
        # Search endpoint returns same envelope as /api/stories
        stories = data.get("stories", [])
        return [self._normalise(s) for s in stories if s]

    # ── Private: Normalisation ────────────────────────────────────────────────

    @staticmethod
    def _normalise(story: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a TechSnif story cluster dict to the article format
        expected by the signal detectors.

        Content is built from:
          • headline           (always present)
          • excerpt            (short teaser)
          • techsnifTake.content (full editorial text, HTML stripped)
          • all coverage link titles (extra company/topic signals)
        """
        headline  = story.get("headline", "")
        excerpt   = story.get("excerpt", "")
        lead_url  = story.get("leadUrl", "") or story.get("sourcePermalink", "")
        publisher = story.get("sourcePublisher", "TechSnif")
        pub_date  = story.get("publishedAt") or story.get("firstSeenAt", "")

        # Build rich content blob for signal matching
        content_parts = [headline]

        if excerpt:
            content_parts.append(excerpt)

        take = story.get("techsnifTake") or {}
        if take.get("content"):
            content_parts.append(TechsnifClient._strip_html(take["content"]))

        # Add all coverage link titles for extra keyword surface area
        for link in story.get("links", []):
            t = link.get("title", "")
            if t and t != headline:
                content_parts.append(t)

        content = " ".join(content_parts)

        # Normalise date
        norm_date = TechsnifClient._parse_iso_date(pub_date)

        return {
            "title":           headline,
            "url":             lead_url,
            "source":          publisher,
            "author":          story.get("sourceAuthor", "Unknown"),
            "content":         content[:6000],   # cap at 6k chars
            "published_date":  pub_date,
            "normalized_date": norm_date,
            # Alias so signal detectors find 'date'
            "date":            pub_date,
            # Extra metadata (useful for debugging / enrichment)
            "slug":            story.get("slug", ""),
            "headline_tier":   story.get("headlineTier", 1),
            "x_post_url":      story.get("xPostUrl", ""),
        }

    # ── Private: Date Helpers ─────────────────────────────────────────────────

    @staticmethod
    def _parse_iso_date(date_str: Optional[str]) -> str:
        """Parse an ISO 8601 date string and return a UTC ISO string."""
        now = datetime.now(timezone.utc).isoformat()
        if not date_str:
            return now
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            except ValueError:
                continue
        return now

    @staticmethod
    def _is_recent(article: Dict[str, Any], cutoff: datetime) -> bool:
        """Return True if the article's normalised date is at/after cutoff."""
        norm = article.get("normalized_date", "")
        if not norm:
            return True
        try:
            dt = datetime.fromisoformat(norm)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt >= cutoff
        except ValueError:
            return True

    # ── Private: Text / Rate Helpers ──────────────────────────────────────────

    @staticmethod
    def _strip_html(text: str) -> str:
        """Remove HTML tags and entities from a string."""
        clean = re.sub(r"<[^>]+>", " ", text)
        clean = re.sub(r"&[a-z]+;", " ", clean)
        clean = re.sub(r"\s+", " ", clean)
        return clean.strip()

    def _enforce_rate_limit(self) -> None:
        """Sleep if needed to stay within requests-per-second limit."""
        if self.rate_limit > 0:
            min_interval = 1.0 / self.rate_limit
            elapsed = time.time() - self._last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    @staticmethod
    def _validate_sources(sources: List[str]) -> None:
        if not isinstance(sources, list) or not sources:
            raise ValueError("sources must be a non-empty list")
        for s in sources:
            if not isinstance(s, str):
                raise ValueError("All source names must be strings")

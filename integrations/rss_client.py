"""
StartupRSSClient - Fetches startup/funding articles from curated RSS feeds.

Tier 1 sources (high signal-to-noise, US startup-focused):
  - TechCrunch Startups    https://techcrunch.com/category/startups/feed/
  - TechCrunch Fundings    https://techcrunch.com/category/fundings-exits/feed/
  - Crunchbase News        https://news.crunchbase.com/feed/
  - TechStartups           https://techstartups.com/feed/
  - Technical.ly           https://technical.ly/feed/

Tier 2 sources (broader coverage):
  - VentureBeat            https://venturebeat.com/feed/

These feeds are much more targeted than TechSnif's top stories, which
skew toward Reuters/FT/WSJ big-tech coverage. The RSS feeds surface
seed/Series A announcements that never reach TechSnif's prominent clusters.
"""

import re
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Dict, Any, Optional

import requests

logger = logging.getLogger(__name__)

# ── Feed definitions ──────────────────────────────────────────────────────────
STARTUP_RSS_FEEDS = [
    # ── Tier 1: High signal-to-noise, US startup-focused ──────────────────────
    {
        "name": "TechCrunch Startups",
        "url": "https://techcrunch.com/category/startups/feed/",
        "priority": "high",
    },
    {
        "name": "TechCrunch Fundings",
        "url": "https://techcrunch.com/tag/fundraising/feed/",
        "priority": "high",
    },
    {
        "name": "Crunchbase News",
        "url": "https://news.crunchbase.com/feed/",
        "priority": "high",
    },
    {
        "name": "TechStartups",
        "url": "https://techstartups.com/feed/",
        "priority": "high",
    },
    {
        "name": "Technical.ly",
        "url": "https://technical.ly/rss/",
        "priority": "high",
    },
    # ── Tier 2: Broader coverage ───────────────────────────────────────────────
    {
        "name": "VentureBeat",
        "url": "https://venturebeat.com/feed/",
        "priority": "medium",
    },
]

# RSS namespaces used by common feeds
RSS_NS = {
    "content":  "http://purl.org/rss/1.0/modules/content/",
    "dc":       "http://purl.org/dc/elements/1.1/",
    "atom":     "http://www.w3.org/2005/Atom",
    "media":    "http://search.yahoo.com/mrss/",
}


class StartupRSSClient:
    """
    Fetches recent articles from startup-focused RSS feeds.

    Articles are normalised to the same dict format used by TechsnifClient
    so they can be fed directly into the existing Tier1/Tier2 pipeline.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = {}
        if config:
            cfg = config.get("data_sources", {}).get("rss", {})

        self.timeout      = int(cfg.get("timeout",      20))
        self.rate_limit   = float(cfg.get("rate_limit",  1.5))   # req/s
        self.max_articles = int(cfg.get("max_articles", 200))
        self.feeds        = cfg.get("feeds", None) or STARTUP_RSS_FEEDS

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "TTR Signal Detector/1.0 (startup intelligence; +https://tylerroessel.com)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        })
        self._last_request_time = 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def fetch_recent_articles(
        self,
        hours: int = 48,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent articles from all configured startup RSS feeds.

        Args:
            hours: How many hours back to include (default 48 — RSS feeds
                   publish irregularly, so a wider window catches more).
            limit: Cap on total articles returned.

        Returns:
            List of normalised article dicts, newest-first.
        """
        cap    = limit or self.max_articles
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        seen_urls: set             = set()
        all_articles: List[Dict]   = []

        for feed_def in self.feeds:
            feed_name = feed_def["name"]
            feed_url  = feed_def["url"]
            try:
                articles = self._fetch_feed(feed_url, feed_name)
                added = 0
                for art in articles:
                    url = art.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_articles.append(art)
                        added += 1
                print(f"   📰 {feed_name}: {added} articles fetched")
            except Exception as e:
                logger.warning("RSS feed '%s' failed: %s", feed_name, e)
                print(f"   ⚠️  {feed_name}: skipped ({e})")

        # Filter to time window, sort newest-first, cap
        recent = [a for a in all_articles if self._is_recent(a, cutoff)]
        recent.sort(key=lambda a: a.get("normalized_date", ""), reverse=True)
        result = recent[:cap]

        print(f"   📡 RSS feeds: {len(result)} articles in last {hours}h "
              f"({len(all_articles)} total fetched, cap {cap})")
        return result

    # ── Private: Feed Fetching ─────────────────────────────────────────────────

    def _fetch_feed(self, url: str, name: str) -> List[Dict[str, Any]]:
        """Fetch and parse a single RSS/Atom feed URL."""
        self._enforce_rate_limit()
        try:
            resp = self._session.get(url, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"HTTP error: {e}") from e

        # Detect HTML response (feed URL returning a webpage instead of XML)
        content_type = resp.headers.get("content-type", "")
        raw = resp.content.strip()
        if raw[:1] in (b"<",) and raw[:9].lower().startswith(b"<!doctype"):
            raise RuntimeError("feed URL returned an HTML page (not RSS/Atom XML)")
        if "text/html" in content_type and b"<rss" not in raw[:500] and b"<feed" not in raw[:500]:
            raise RuntimeError(f"feed URL returned HTML (content-type: {content_type})")

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            raise RuntimeError(f"XML parse error: {e}") from e

        # Detect feed format (RSS 2.0 vs Atom)
        tag = root.tag.lower()
        if "feed" in tag:
            return self._parse_atom(root, name)
        else:
            return self._parse_rss(root, name)

    def _parse_rss(self, root: ET.Element, source_name: str) -> List[Dict[str, Any]]:
        """Parse RSS 2.0 channel items."""
        articles = []
        channel = root.find("channel")
        if channel is None:
            channel = root  # Some feeds omit <channel>

        for item in channel.findall("item"):
            art = self._normalise_rss_item(item, source_name)
            if art:
                articles.append(art)
        return articles

    def _parse_atom(self, root: ET.Element, source_name: str) -> List[Dict[str, Any]]:
        """Parse Atom 1.0 feed entries."""
        articles = []
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entries = root.findall("atom:entry", ns) or root.findall("{http://www.w3.org/2005/Atom}entry")
        if not entries:
            # Try without namespace
            entries = root.findall("entry")
        for entry in entries:
            art = self._normalise_atom_entry(entry, source_name)
            if art:
                articles.append(art)
        return articles

    # ── Private: Normalisation ────────────────────────────────────────────────

    def _normalise_rss_item(self, item: ET.Element, source_name: str) -> Optional[Dict[str, Any]]:
        """Map an RSS <item> to the standard article dict."""
        title       = self._get_text(item, "title") or ""
        link        = self._get_text(item, "link") or ""
        pub_date    = self._get_text(item, "pubDate") or ""
        description = self._get_text(item, "description") or ""
        author      = (
            self._get_ns_text(item, "dc", "creator") or
            self._get_text(item, "author") or
            "Unknown"
        )

        # Try to get full content
        full_content = self._get_ns_text(item, "content", "encoded") or ""

        # Build rich content: title + description + stripped full content
        content_parts = [title]
        if description:
            content_parts.append(self._strip_html(description))
        if full_content:
            content_parts.append(self._strip_html(full_content)[:2000])

        content = " ".join(content_parts)

        # Parse and normalise date
        norm_date = self._parse_date(pub_date)

        if not title or not link:
            return None

        return {
            "title":           title,
            "url":             link.strip(),
            "source":          source_name,
            "author":          author,
            "content":         content[:6000],
            "published_date":  pub_date,
            "normalized_date": norm_date,
            "date":            pub_date,
            "slug":            "",
            "headline_tier":   1,
            "x_post_url":      "",
        }

    def _normalise_atom_entry(self, entry: ET.Element, source_name: str) -> Optional[Dict[str, Any]]:
        """Map an Atom <entry> to the standard article dict."""
        # Handle both namespaced and non-namespaced elements
        title   = self._atom_text(entry, "title") or ""
        updated = self._atom_text(entry, "updated") or self._atom_text(entry, "published") or ""
        summary = self._atom_text(entry, "summary") or ""
        content = self._atom_text(entry, "content") or ""
        author_el = entry.find("{http://www.w3.org/2005/Atom}author") or entry.find("author")
        author = ""
        if author_el is not None:
            name_el = author_el.find("{http://www.w3.org/2005/Atom}name") or author_el.find("name")
            author = name_el.text if name_el is not None else "Unknown"

        # Find link
        link = ""
        for link_el in (entry.findall("{http://www.w3.org/2005/Atom}link") or entry.findall("link")):
            rel = link_el.get("rel", "alternate")
            if rel in ("alternate", ""):
                link = link_el.get("href", "")
                break

        content_parts = [title]
        if summary:
            content_parts.append(self._strip_html(summary))
        if content:
            content_parts.append(self._strip_html(content)[:2000])
        full_content = " ".join(content_parts)

        norm_date = self._parse_date(updated)

        if not title or not link:
            return None

        return {
            "title":           title,
            "url":             link.strip(),
            "source":          source_name,
            "author":          author or "Unknown",
            "content":         full_content[:6000],
            "published_date":  updated,
            "normalized_date": norm_date,
            "date":            updated,
            "slug":            "",
            "headline_tier":   1,
            "x_post_url":      "",
        }

    # ── Private: XML Helpers ──────────────────────────────────────────────────

    @staticmethod
    def _get_text(element: ET.Element, tag: str) -> Optional[str]:
        """Get text of a direct child element."""
        child = element.find(tag)
        return child.text if child is not None else None

    @staticmethod
    def _get_ns_text(element: ET.Element, ns_key: str, tag: str) -> Optional[str]:
        """Get text of a namespaced child element."""
        ns_uri = RSS_NS.get(ns_key, "")
        child  = element.find(f"{{{ns_uri}}}{tag}")
        if child is not None:
            return child.text or ""
        return None

    @staticmethod
    def _atom_text(element: ET.Element, tag: str) -> Optional[str]:
        """Get text from Atom namespace or plain namespace."""
        child = (
            element.find(f"{{http://www.w3.org/2005/Atom}}{tag}") or
            element.find(tag)
        )
        return child.text if child is not None else None

    # ── Private: Date Helpers ─────────────────────────────────────────────────

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> str:
        """Parse RSS (RFC 2822) or ISO 8601 date strings to UTC ISO."""
        now = datetime.now(timezone.utc).isoformat()
        if not date_str:
            return now
        date_str = date_str.strip()

        # Try RFC 2822 (RSS standard: "Mon, 07 Apr 2025 12:00:00 +0000")
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass

        # Try ISO 8601 variants
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
        """Return True if article date is at or after cutoff."""
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
        """Remove HTML tags and entities."""
        clean = re.sub(r"<[^>]+>", " ", text)
        clean = re.sub(r"&[a-z]+;",  " ", clean)
        clean = re.sub(r"&#\d+;",    " ", clean)
        clean = re.sub(r"\s+",        " ", clean)
        return clean.strip()

    def _enforce_rate_limit(self) -> None:
        """Sleep if needed to stay within rate limit."""
        if self.rate_limit > 0:
            min_interval = 1.0 / self.rate_limit
            elapsed      = time.time() - self._last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

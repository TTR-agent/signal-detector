"""
TechsnifClient - CLI integration for fetching news articles from techsnif.

This module provides a client interface to the techsnif CLI tool for fetching
news articles from various sources like TechCrunch, The Block, VentureBeat, etc.
It handles subprocess calls, JSON parsing, and article normalization for the
TTR Signal Detection System.
"""

import json
import subprocess
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class TechsnifError(Exception):
    """Exception raised for errors in techsnif client operations."""
    pass


class TechsnifClient:
    """
    Client for integrating with the techsnif CLI tool.

    Provides methods to fetch recent articles from various tech news sources
    and normalize the data format for signal processing.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the TechsnifClient.

        Args:
            config: Optional configuration dictionary with settings like
                   rate_limit, max_articles, timeout, etc.
        """
        self.config = {
            'rate_limit': 2,  # requests per second
            'max_articles': 50,
            'timeout': 30,  # seconds
            'cli_command': 'techsnif'
        }

        if config:
            self.config.update(config)

        self._last_request_time = 0.0

    def fetch_recent_articles(self, hours: int = 24, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch recent articles from all configured sources.

        Args:
            hours: Number of hours back to fetch articles (default 24)
            limit: Maximum number of articles to return (optional)

        Returns:
            List of normalized article dictionaries

        Raises:
            TechsnifError: If CLI command fails or response parsing fails
        """
        self._enforce_rate_limit()

        cmd = [
            self.config['cli_command'],
            'fetch-recent',
            '--hours', str(hours),
            '--format', 'json'
        ]

        if limit:
            cmd.extend(['--limit', str(limit)])
        elif self.config['max_articles']:
            cmd.extend(['--limit', str(self.config['max_articles'])])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config['timeout']
            )

            if result.returncode != 0:
                error_msg = f"CLI command failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                raise TechsnifError(error_msg)

            return self._parse_and_normalize_response(result.stdout)

        except subprocess.TimeoutExpired:
            raise TechsnifError(f"Command timed out after {self.config['timeout']} seconds")
        except FileNotFoundError:
            raise TechsnifError("techsnif CLI not found. Please ensure it's installed and in PATH")

    def fetch_articles_from_sources(self, sources: List[str], hours: int = 24,
                                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch articles from specific sources.

        Args:
            sources: List of source names (e.g., ['TechCrunch', 'The Block'])
            hours: Number of hours back to fetch articles
            limit: Maximum number of articles to return (optional)

        Returns:
            List of normalized article dictionaries

        Raises:
            TechsnifError: If CLI command fails or response parsing fails
            ValueError: If sources list is invalid
        """
        if not self._validate_sources(sources):
            raise ValueError("Invalid sources list provided")

        self._enforce_rate_limit()

        cmd = [
            self.config['cli_command'],
            'fetch-sources',
            '--sources', ','.join(sources),
            '--hours', str(hours),
            '--format', 'json'
        ]

        if limit:
            cmd.extend(['--limit', str(limit)])
        elif self.config['max_articles']:
            cmd.extend(['--limit', str(self.config['max_articles'])])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config['timeout']
            )

            if result.returncode != 0:
                error_msg = f"CLI command failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                raise TechsnifError(error_msg)

            return self._parse_and_normalize_response(result.stdout)

        except subprocess.TimeoutExpired:
            raise TechsnifError(f"Command timed out after {self.config['timeout']} seconds")
        except FileNotFoundError:
            raise TechsnifError("techsnif CLI not found. Please ensure it's installed and in PATH")

    def _parse_and_normalize_response(self, response_text: str) -> List[Dict[str, Any]]:
        """
        Parse JSON response from techsnif CLI and normalize article format.

        Args:
            response_text: Raw JSON response from CLI

        Returns:
            List of normalized article dictionaries

        Raises:
            TechsnifError: If JSON parsing fails
        """
        try:
            response_data = json.loads(response_text)
        except json.JSONDecodeError as e:
            raise TechsnifError(f"Failed to parse JSON response: {e}")

        if not isinstance(response_data, dict) or 'articles' not in response_data:
            raise TechsnifError("Invalid response format: missing 'articles' key")

        articles = response_data.get('articles', [])
        return [self._normalize_article(article) for article in articles]

    def _normalize_article(self, raw_article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize article format across different sources.

        Args:
            raw_article: Raw article data from CLI response

        Returns:
            Normalized article dictionary with consistent fields
        """
        normalized = {
            'title': raw_article.get('title', '').strip(),
            'url': raw_article.get('url', ''),
            'source': raw_article.get('source', 'Unknown'),
            'published_date': raw_article.get('published_date', ''),
            'content': raw_article.get('content', ''),
            'author': raw_article.get('author', 'Unknown')
        }

        # Add normalized date for easier processing
        if normalized['published_date']:
            try:
                # Try to parse ISO format date
                dt = datetime.fromisoformat(normalized['published_date'].replace('Z', '+00:00'))
                normalized['normalized_date'] = dt.isoformat()
            except (ValueError, AttributeError):
                # Fallback to current time if parsing fails
                normalized['normalized_date'] = datetime.now().isoformat()
        else:
            normalized['normalized_date'] = datetime.now().isoformat()

        return normalized

    def _validate_sources(self, sources: List[str]) -> bool:
        """
        Validate that sources is a proper list of strings.

        Args:
            sources: List of source names to validate

        Returns:
            True if valid, False otherwise

        Raises:
            ValueError: If sources format is invalid
        """
        if not isinstance(sources, list):
            raise ValueError("Sources must be a list")

        if len(sources) == 0:
            raise ValueError("Sources list cannot be empty")

        for source in sources:
            if not isinstance(source, str):
                raise ValueError("All source names must be strings")

        return True

    def _enforce_rate_limit(self) -> None:
        """
        Enforce rate limiting between requests.

        Uses the configured rate_limit to ensure we don't exceed
        the specified requests per second.
        """
        if self.config['rate_limit'] > 0:
            min_interval = 1.0 / self.config['rate_limit']
            elapsed = time.time() - self._last_request_time

            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                time.sleep(sleep_time)

        self._last_request_time = time.time()
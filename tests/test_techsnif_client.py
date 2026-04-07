"""
Test suite for TechsnifClient integration.

This module tests the techsnif CLI integration component that fetches news articles
from sources like TechCrunch, The Block, etc. for the TTR Signal Detection System.
"""

import pytest
import json
import subprocess
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from integrations.techsnif_client import TechsnifClient, TechsnifError


class TestTechsnifClient:
    """Test cases for TechsnifClient class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.client = TechsnifClient()

    def test_client_initialization(self):
        """Test TechsnifClient initialization."""
        client = TechsnifClient()
        assert client is not None
        assert hasattr(client, 'fetch_recent_articles')
        assert hasattr(client, 'fetch_articles_from_sources')

    def test_client_initialization_with_config(self):
        """Test TechsnifClient initialization with custom configuration."""
        config = {
            'rate_limit': 5,
            'max_articles': 100,
            'timeout': 30
        }
        client = TechsnifClient(config=config)
        assert client.config['rate_limit'] == 5
        assert client.config['max_articles'] == 100
        assert client.config['timeout'] == 30

    @patch('subprocess.run')
    def test_fetch_recent_articles_success(self, mock_subprocess):
        """Test successful fetching of recent articles."""
        # Mock techsnif CLI response
        mock_response = {
            "articles": [
                {
                    "title": "TechCrunch Article 1",
                    "url": "https://techcrunch.com/article1",
                    "source": "TechCrunch",
                    "published_date": "2026-04-06T10:00:00Z",
                    "content": "Sample article content about startup funding...",
                    "author": "John Doe"
                },
                {
                    "title": "The Block Article 1",
                    "url": "https://theblock.co/article1",
                    "source": "The Block",
                    "published_date": "2026-04-06T09:30:00Z",
                    "content": "Blockchain startup raises Series A...",
                    "author": "Jane Smith"
                }
            ],
            "total_count": 2,
            "status": "success"
        }

        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout=json.dumps(mock_response)
        )

        articles = self.client.fetch_recent_articles(hours=24)

        assert len(articles) == 2
        assert articles[0]['title'] == "TechCrunch Article 1"
        assert articles[0]['source'] == "TechCrunch"
        assert articles[1]['title'] == "The Block Article 1"
        assert articles[1]['source'] == "The Block"

        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args[0][0]
        assert 'techsnif' in call_args
        assert 'fetch-recent' in call_args

    @patch('subprocess.run')
    def test_fetch_articles_from_sources_success(self, mock_subprocess):
        """Test successful fetching of articles from specific sources."""
        mock_response = {
            "articles": [
                {
                    "title": "TechCrunch Startup News",
                    "url": "https://techcrunch.com/startup-news",
                    "source": "TechCrunch",
                    "published_date": "2026-04-06T12:00:00Z",
                    "content": "A new SaaS startup just raised $5M...",
                    "author": "Tech Reporter"
                }
            ],
            "total_count": 1,
            "status": "success"
        }

        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout=json.dumps(mock_response)
        )

        sources = ["TechCrunch", "The Block"]
        articles = self.client.fetch_articles_from_sources(sources, hours=12)

        assert len(articles) == 1
        assert articles[0]['source'] == "TechCrunch"

        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args[0][0]
        assert 'techsnif' in call_args
        assert 'fetch-sources' in call_args

    @patch('subprocess.run')
    def test_fetch_articles_cli_error(self, mock_subprocess):
        """Test handling of CLI execution errors."""
        mock_subprocess.return_value = Mock(
            returncode=1,
            stderr="Error: techsnif command failed"
        )

        with pytest.raises(TechsnifError) as exc_info:
            self.client.fetch_recent_articles(hours=24)

        assert "CLI command failed" in str(exc_info.value)

    @patch('subprocess.run')
    def test_fetch_articles_json_parse_error(self, mock_subprocess):
        """Test handling of JSON parsing errors."""
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="invalid json response"
        )

        with pytest.raises(TechsnifError) as exc_info:
            self.client.fetch_recent_articles(hours=24)

        assert "Failed to parse JSON response" in str(exc_info.value)

    @patch('subprocess.run')
    def test_fetch_articles_timeout_error(self, mock_subprocess):
        """Test handling of command timeout."""
        mock_subprocess.side_effect = subprocess.TimeoutExpired('techsnif', 30)

        with pytest.raises(TechsnifError) as exc_info:
            self.client.fetch_recent_articles(hours=24)

        assert "Command timed out" in str(exc_info.value)

    def test_normalize_article_format(self):
        """Test article format normalization."""
        raw_article = {
            "title": "  Sample Article Title  ",
            "url": "https://example.com/article",
            "source": "test_source",
            "published_date": "2026-04-06T10:00:00Z",
            "content": "Article content here...",
            "author": "Test Author"
        }

        normalized = self.client._normalize_article(raw_article)

        assert normalized['title'] == "Sample Article Title"  # Stripped whitespace
        assert normalized['url'] == "https://example.com/article"
        assert normalized['source'] == "test_source"
        assert 'normalized_date' in normalized
        assert normalized['content'] == "Article content here..."

    def test_normalize_article_missing_fields(self):
        """Test article normalization with missing fields."""
        raw_article = {
            "title": "Sample Title",
            "url": "https://example.com/article"
            # Missing source, published_date, content, author
        }

        normalized = self.client._normalize_article(raw_article)

        assert normalized['title'] == "Sample Title"
        assert normalized['url'] == "https://example.com/article"
        assert normalized['source'] == "Unknown"
        assert normalized['content'] == ""
        assert normalized['author'] == "Unknown"

    def test_validate_source_list(self):
        """Test source list validation."""
        # Valid sources
        valid_sources = ["TechCrunch", "The Block", "VentureBeat"]
        assert self.client._validate_sources(valid_sources) is True

        # Invalid input types
        with pytest.raises(ValueError):
            self.client._validate_sources("not a list")

        with pytest.raises(ValueError):
            self.client._validate_sources([])

        with pytest.raises(ValueError):
            self.client._validate_sources([123, "TechCrunch"])

    @patch('subprocess.run')
    def test_fetch_articles_with_rate_limiting(self, mock_subprocess):
        """Test rate limiting functionality."""
        mock_response = {
            "articles": [],
            "total_count": 0,
            "status": "success"
        }

        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout=json.dumps(mock_response)
        )

        # Configure client with rate limiting
        config = {'rate_limit': 1}  # 1 request per second
        client = TechsnifClient(config=config)

        start_time = datetime.now()
        client.fetch_recent_articles(hours=1)
        client.fetch_recent_articles(hours=1)
        end_time = datetime.now()

        # Should take at least 1 second due to rate limiting
        elapsed = (end_time - start_time).total_seconds()
        assert elapsed >= 1.0


@pytest.fixture
def mock_techsnif_response():
    """Fixture providing a mock techsnif CLI response."""
    return {
        "articles": [
            {
                "title": "AI Startup Raises $10M Series A",
                "url": "https://techcrunch.com/ai-startup-funding",
                "source": "TechCrunch",
                "published_date": "2026-04-06T14:30:00Z",
                "content": "An AI-powered SaaS platform focused on early-stage startup intelligence has successfully raised $10 million in Series A funding...",
                "author": "Sarah Johnson"
            },
            {
                "title": "Blockchain Platform Launches Beta",
                "url": "https://theblock.co/blockchain-beta-launch",
                "source": "The Block",
                "published_date": "2026-04-06T13:45:00Z",
                "content": "A new blockchain platform targeting enterprise customers has officially launched its beta program...",
                "author": "Michael Chen"
            },
            {
                "title": "SaaS Company Expands to Europe",
                "url": "https://venturebeat.com/saas-europe-expansion",
                "source": "VentureBeat",
                "published_date": "2026-04-06T12:15:00Z",
                "content": "Following successful growth in the US market, this customer analytics SaaS platform is expanding operations to Europe...",
                "author": "Emily Davis"
            }
        ],
        "total_count": 3,
        "status": "success",
        "metadata": {
            "fetch_time": "2026-04-06T15:00:00Z",
            "sources_queried": ["TechCrunch", "The Block", "VentureBeat"],
            "query_duration_ms": 2350
        }
    }


class TestTechsnifError:
    """Test cases for TechsnifError exception."""

    def test_techsnif_error_creation(self):
        """Test TechsnifError exception creation."""
        error = TechsnifError("Test error message")
        assert str(error) == "Test error message"

    def test_techsnif_error_inheritance(self):
        """Test TechsnifError inherits from Exception."""
        error = TechsnifError("Test")
        assert isinstance(error, Exception)
"""
Test module for Tier1Matcher - core signal detection component.

Tests the pattern matching functionality for identifying high-confidence signals
from news articles using regex patterns defined in config.yaml.
"""

import pytest
import yaml
from pathlib import Path
from signal_detector.tier_matcher import Tier1Matcher


@pytest.fixture
def config_data():
    """Load configuration data for testing."""
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def sample_articles():
    """Sample news articles for testing signal detection."""
    return [
        {
            "title": "TechStartup raises $2.5 million in seed funding",
            "content": "TechStartup, a promising AI company, has successfully raised $2.5 million in seed funding led by Venture Partners. The funding will be used to expand their team and develop their machine learning platform.",
            "url": "https://techsnif.com/techstartup-funding",
            "date": "2024-04-06"
        },
        {
            "title": "InnovateNow launches revolutionary blockchain platform",
            "content": "InnovateNow announced the launch of their new blockchain platform that promises to disrupt the financial services industry. The beta version is now available for early adopters.",
            "url": "https://techsnif.com/innovatenow-launch",
            "date": "2024-04-05"
        },
        {
            "title": "CloudCorp experiences security breach affecting user data",
            "content": "CloudCorp reported a security incident that may have exposed customer information. The company is working with cybersecurity experts to investigate the breach and notify affected users.",
            "url": "https://techsnif.com/cloudcorp-security",
            "date": "2024-04-04"
        },
        {
            "title": "StartupX hiring spree: 50 new positions available",
            "content": "StartupX is on a hiring spree, looking to fill 50 new positions across engineering, sales, and marketing. The company cited rapid growth and expanding customer base as reasons for the expansion.",
            "url": "https://techsnif.com/startupx-hiring",
            "date": "2024-04-03"
        },
        {
            "title": "General business news about market trends",
            "content": "Market analysts predict continued growth in the tech sector. Various companies are expected to perform well this quarter according to industry reports.",
            "url": "https://techsnif.com/market-trends",
            "date": "2024-04-02"
        }
    ]


@pytest.fixture
def tier1_matcher(config_data):
    """Create Tier1Matcher instance for testing."""
    return Tier1Matcher(config_data)


class TestTier1MatcherInitialization:
    """Test Tier1Matcher initialization and configuration loading."""

    def test_initialization_with_config(self, config_data):
        """Test that Tier1Matcher initializes correctly with configuration."""
        matcher = Tier1Matcher(config_data)
        assert matcher is not None
        assert hasattr(matcher, 'config')
        assert matcher.config == config_data

    def test_tier1_signals_loaded(self, tier1_matcher):
        """Test that tier 1 signals are loaded from configuration."""
        signals = tier1_matcher.get_tier1_signals()
        assert len(signals) > 0

        # Check that expected signal types are loaded
        signal_types = [signal['signal_type'] for signal in signals]
        assert 'funding_announcement' in signal_types
        assert 'product_launch' in signal_types
        assert 'hiring_spree' in signal_types

    def test_signal_patterns_loaded(self, tier1_matcher):
        """Test that signal patterns/keywords are properly loaded."""
        signals = tier1_matcher.get_tier1_signals()

        funding_signal = next(s for s in signals if s['signal_type'] == 'funding_announcement')
        assert 'keywords' in funding_signal
        assert len(funding_signal['keywords']) > 0
        assert 'raised' in funding_signal['keywords']
        assert 'funding' in funding_signal['keywords']


class TestFundingSignalDetection:
    """Test detection of funding announcement signals."""

    def test_detect_funding_signal_basic(self, tier1_matcher, sample_articles):
        """Test basic funding signal detection."""
        funding_article = sample_articles[0]  # TechStartup funding article

        signals = tier1_matcher.detect_signals([funding_article])

        assert len(signals) > 0
        funding_signals = [s for s in signals if s['signal_type'] == 'funding_announcement']
        assert len(funding_signals) == 1

        signal = funding_signals[0]
        assert signal['company_name'] == 'TechStartup'
        assert signal['confidence_score'] > 0.8
        assert 'funding' in signal['matched_keywords']

    def test_extract_funding_amount(self, tier1_matcher, sample_articles):
        """Test extraction of funding amount from articles."""
        funding_article = sample_articles[0]

        signals = tier1_matcher.detect_signals([funding_article])
        funding_signal = next(s for s in signals if s['signal_type'] == 'funding_announcement')

        assert 'funding_amount' in funding_signal
        assert funding_signal['funding_amount'] == 2500000  # $2.5 million

    def test_funding_wildcard_patterns(self, tier1_matcher):
        """Test wildcard pattern matching for funding amounts."""
        article_with_wildcard = {
            "title": "NewStartup raised 500k in pre-seed",
            "content": "NewStartup successfully raised 500 thousand dollars in their pre-seed round from angel investors.",
            "url": "https://example.com/newstartup",
            "date": "2024-04-06"
        }

        signals = tier1_matcher.detect_signals([article_with_wildcard])
        funding_signals = [s for s in signals if s['signal_type'] == 'funding_announcement']

        assert len(funding_signals) == 1
        assert funding_signals[0]['company_name'] == 'NewStartup'
        assert funding_signals[0]['funding_amount'] == 500000


class TestProductLaunchDetection:
    """Test detection of product launch signals."""

    def test_detect_product_launch_signal(self, tier1_matcher, sample_articles):
        """Test basic product launch signal detection."""
        launch_article = sample_articles[1]  # InnovateNow launch article

        signals = tier1_matcher.detect_signals([launch_article])

        launch_signals = [s for s in signals if s['signal_type'] == 'product_launch']
        assert len(launch_signals) == 1

        signal = launch_signals[0]
        assert signal['company_name'] == 'InnovateNow'
        assert signal['confidence_score'] > 0.7
        assert 'launch' in signal['matched_keywords']

    def test_beta_launch_detection(self, tier1_matcher, sample_articles):
        """Test detection of beta/MVP launches."""
        launch_article = sample_articles[1]

        signals = tier1_matcher.detect_signals([launch_article])
        launch_signal = next(s for s in signals if s['signal_type'] == 'product_launch')

        assert 'beta' in launch_signal['matched_keywords']


class TestHiringSignalDetection:
    """Test detection of hiring spree signals."""

    def test_detect_hiring_signal(self, tier1_matcher, sample_articles):
        """Test basic hiring signal detection."""
        hiring_article = sample_articles[3]  # StartupX hiring article

        signals = tier1_matcher.detect_signals([hiring_article])

        hiring_signals = [s for s in signals if s['signal_type'] == 'hiring_spree']
        assert len(hiring_signals) == 1

        signal = hiring_signals[0]
        assert signal['company_name'] == 'StartupX'
        assert signal['confidence_score'] > 0.6
        assert 'hiring' in signal['matched_keywords']


class TestNegativeKeywordExclusion:
    """Test filtering out false positives using exclusion keywords."""

    def test_security_breach_exclusion(self, tier1_matcher, sample_articles):
        """Test that security incidents are filtered out as negative signals."""
        security_article = sample_articles[2]  # CloudCorp security breach

        signals = tier1_matcher.detect_signals([security_article])

        # Should detect the negative signal but not include it in positive results
        positive_signals = [s for s in signals if s.get('is_negative', False) is False]
        negative_signals = [s for s in signals if s.get('is_negative', False) is True]

        assert len(positive_signals) == 0
        assert len(negative_signals) > 0

        negative_signal = negative_signals[0]
        assert negative_signal['signal_type'] == 'security_incident'
        assert 'security' in negative_signal['matched_keywords']

    def test_exclude_generic_business_news(self, tier1_matcher, sample_articles):
        """Test that generic business news without specific signals is excluded."""
        generic_article = sample_articles[4]  # Generic market trends article

        signals = tier1_matcher.detect_signals([generic_article])

        # Should not detect any high-confidence signals
        high_confidence_signals = [s for s in signals if s['confidence_score'] > 0.5]
        assert len(high_confidence_signals) == 0


class TestCompanyNameExtraction:
    """Test extraction of company names from signal context."""

    def test_company_name_from_title(self, tier1_matcher, sample_articles):
        """Test company name extraction from article titles."""
        for article in sample_articles[:4]:  # Skip generic article
            signals = tier1_matcher.detect_signals([article])

            if signals:
                signal = signals[0]
                assert 'company_name' in signal
                assert signal['company_name'] is not None
                assert len(signal['company_name']) > 0

    def test_company_name_patterns(self, tier1_matcher):
        """Test various company name extraction patterns."""
        test_articles = [
            {
                "title": "Acme Corp raises $1M in funding",
                "content": "Acme Corp announced their funding round today.",
                "url": "https://example.com",
                "date": "2024-04-06"
            },
            {
                "title": "Series A funding for MegaStartup Inc.",
                "content": "MegaStartup Inc. completed their Series A round.",
                "url": "https://example.com",
                "date": "2024-04-06"
            }
        ]

        for article in test_articles:
            signals = tier1_matcher.detect_signals([article])
            assert len(signals) > 0

            signal = signals[0]
            # Test company name extraction, accounting for period variations
            assert signal['company_name'] in ['Acme Corp', 'MegaStartup Inc', 'MegaStartup Inc.']


class TestConfidenceScoring:
    """Test confidence scoring for pattern matches."""

    def test_confidence_calculation(self, tier1_matcher, sample_articles):
        """Test that confidence scores are calculated correctly."""
        signals = tier1_matcher.detect_signals(sample_articles)

        for signal in signals:
            assert 'confidence_score' in signal
            assert 0.0 <= signal['confidence_score'] <= 1.0

    def test_higher_confidence_for_multiple_matches(self, tier1_matcher):
        """Test that multiple keyword matches increase confidence."""
        high_match_article = {
            "title": "TechCo raises $5M in Series A funding round",
            "content": "TechCo successfully raised $5 million in their Series A funding round led by top venture capital firms. The investment will fuel their growth.",
            "url": "https://example.com",
            "date": "2024-04-06"
        }

        low_match_article = {
            "title": "TechCo gets funding",
            "content": "TechCo received funding recently.",
            "url": "https://example.com",
            "date": "2024-04-06"
        }

        high_signals = tier1_matcher.detect_signals([high_match_article])
        low_signals = tier1_matcher.detect_signals([low_match_article])

        assert len(high_signals) > 0 and len(low_signals) > 0
        assert high_signals[0]['confidence_score'] > low_signals[0]['confidence_score']


class TestMultipleArticleProcessing:
    """Test processing multiple articles at once."""

    def test_process_multiple_articles(self, tier1_matcher, sample_articles):
        """Test that multiple articles are processed correctly."""
        signals = tier1_matcher.detect_signals(sample_articles)

        # Should detect at least 3 positive signals (funding, launch, hiring)
        positive_signals = [s for s in signals if not s.get('is_negative', False)]
        assert len(positive_signals) >= 3

        # Check that different signal types are detected
        signal_types = set(s['signal_type'] for s in positive_signals)
        assert 'funding_announcement' in signal_types
        assert 'product_launch' in signal_types
        assert 'hiring_spree' in signal_types

    def test_signal_deduplication(self, tier1_matcher):
        """Test that duplicate signals from same article are handled properly."""
        duplicate_articles = [
            {
                "title": "TechCorp raises funding and launches product",
                "content": "TechCorp announced they raised $3M in funding and launched their new AI platform today.",
                "url": "https://example.com/techcorp",
                "date": "2024-04-06"
            },
            {
                "title": "TechCorp raises funding and launches product",  # Duplicate
                "content": "TechCorp announced they raised $3M in funding and launched their new AI platform today.",
                "url": "https://example.com/techcorp",
                "date": "2024-04-06"
            }
        ]

        signals = tier1_matcher.detect_signals(duplicate_articles)

        # Should detect both funding and product launch but avoid duplicates
        unique_urls = set(s['source_url'] for s in signals)
        assert len(unique_urls) == 1  # Only one unique article URL
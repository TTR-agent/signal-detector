"""
Tier 1 Signal Pattern Matcher

Core signal detection component that identifies high-confidence signals from news articles
using regex pattern matching based on configuration patterns.
"""

import re
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime


class Tier1Matcher:
    """
    Tier 1 signal matcher for detecting high-confidence startup signals.

    This class implements the first tier of the 3-tier signal detection system,
    focusing on pattern matching with regex to identify funding, product launches,
    hiring sprees, and other startup signals from news articles.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Tier1Matcher with configuration.

        Args:
            config: Configuration dictionary loaded from config.yaml
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Load tier 1 signals configuration
        self.tier1_signals = config.get('signal_detection', {}).get('tier1_signals', [])

        # Compile regex patterns for better performance
        self._compile_patterns()

        # Exclusion patterns for filtering false positives
        self.exclusion_patterns = [
            r'\b(security breach|data breach|hack|incident|failure|lawsuit|sued|problem|issue|concern)\b',
            r'\b(acquisition|acquired|merger|bought|purchase|acquisition)\b',  # M&A signals
            r'\b(shutdown|closing|failed|bankruptcy|liquidation)\b'  # Negative business signals
        ]

        self.compiled_exclusion_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.exclusion_patterns]

    def _compile_patterns(self):
        """Compile regex patterns for each signal type for better performance."""
        self.compiled_patterns = {}

        for signal in self.tier1_signals:
            signal_type = signal['signal_type']
            keywords = signal.get('keywords', [])

            # Create regex patterns from keywords
            patterns = []
            for keyword in keywords:
                # Handle wildcard patterns like "raised * million"
                if '*' in keyword:
                    pattern = keyword.replace('*', r'[\w\s$,.\d]+')
                else:
                    pattern = re.escape(keyword)
                patterns.append(f"\\b{pattern}\\b")

            # Combine patterns with OR
            if patterns:
                combined_pattern = '|'.join(patterns)
                self.compiled_patterns[signal_type] = re.compile(combined_pattern, re.IGNORECASE)

    def get_tier1_signals(self) -> List[Dict[str, Any]]:
        """
        Get the tier 1 signals configuration.

        Returns:
            List of tier 1 signal configurations
        """
        return self.tier1_signals

    def detect_signals(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect signals from a list of articles.

        Args:
            articles: List of article dictionaries with title, content, url, date

        Returns:
            List of detected signals with metadata
        """
        detected_signals = []
        processed_urls = set()  # For deduplication

        for article in articles:
            url = article.get('url', '')

            # Skip duplicates
            if url in processed_urls:
                continue
            processed_urls.add(url)

            # Check for exclusion patterns first
            if self._contains_exclusion_patterns(article):
                # Add as negative signal
                negative_signal = self._create_negative_signal(article)
                if negative_signal:
                    detected_signals.append(negative_signal)
                continue

            # Detect positive signals
            article_signals = self._detect_article_signals(article)
            detected_signals.extend(article_signals)

        return detected_signals

    def _contains_exclusion_patterns(self, article: Dict[str, Any]) -> bool:
        """
        Check if article contains exclusion patterns that indicate negative signals.

        Args:
            article: Article dictionary

        Returns:
            True if article contains exclusion patterns
        """
        text = f"{article.get('title', '')} {article.get('content', '')}"

        for pattern in self.compiled_exclusion_patterns:
            if pattern.search(text):
                return True
        return False

    def _create_negative_signal(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a negative signal from an article with exclusion patterns.

        Args:
            article: Article dictionary

        Returns:
            Negative signal dictionary or None
        """
        text = f"{article.get('title', '')} {article.get('content', '')}"

        # Determine negative signal type
        signal_type = "negative_signal"
        matched_keywords = []

        if re.search(r'\b(security breach|data breach|hack|incident)\b', text, re.IGNORECASE):
            signal_type = "security_incident"
            matched_keywords = ["security", "breach", "incident"]
        elif re.search(r'\b(acquisition|acquired|merger|bought)\b', text, re.IGNORECASE):
            signal_type = "acquisition_signal"
            matched_keywords = ["acquisition", "merger"]
        elif re.search(r'\b(shutdown|closing|failed|bankruptcy)\b', text, re.IGNORECASE):
            signal_type = "negative_business"
            matched_keywords = ["shutdown", "failure"]

        company_name = self._extract_company_name(article)

        return {
            'signal_type': signal_type,
            'company_name': company_name,
            'confidence_score': 0.9,  # High confidence for negative signals
            'matched_keywords': matched_keywords,
            'source_url': article.get('url', ''),
            'date_detected': datetime.now().isoformat(),
            'article_date': article.get('date', ''),
            'article_title': article.get('title', ''),
            'is_negative': True
        }

    def _detect_article_signals(self, article: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect signals from a single article.

        Args:
            article: Article dictionary

        Returns:
            List of detected signals
        """
        signals = []
        text = f"{article.get('title', '')} {article.get('content', '')}"

        for signal_config in self.tier1_signals:
            signal_type = signal_config['signal_type']

            if signal_type not in self.compiled_patterns:
                continue

            pattern = self.compiled_patterns[signal_type]
            matches = pattern.findall(text)

            if matches:
                # Create signal
                signal = self._create_signal(article, signal_config, matches)
                if signal and signal['confidence_score'] >= signal_config.get('confidence_threshold', 0.3):
                    signals.append(signal)

        return signals

    def _create_signal(self, article: Dict[str, Any], signal_config: Dict[str, Any], matches: List[str]) -> Optional[Dict[str, Any]]:
        """
        Create a signal dictionary from matches.

        Args:
            article: Article dictionary
            signal_config: Signal configuration
            matches: List of matched text

        Returns:
            Signal dictionary or None
        """
        signal_type = signal_config['signal_type']
        company_name = self._extract_company_name(article)

        if not company_name:
            return None

        # Calculate confidence score
        confidence_score = self._calculate_confidence_score(matches, signal_config, article)

        # Extract matched keywords
        matched_keywords = self._extract_matched_keywords(matches, signal_config.get('keywords', []))

        signal = {
            'signal_type': signal_type,
            'company_name': company_name,
            'confidence_score': confidence_score,
            'matched_keywords': matched_keywords,
            'source_url': article.get('url', ''),
            'date_detected': datetime.now().isoformat(),
            'article_date': article.get('date', ''),
            'article_title': article.get('title', ''),
            'priority': signal_config.get('priority', 'medium'),
            'is_negative': False
        }

        # Add specific signal type metadata
        if signal_type == 'funding_announcement':
            funding_amount = self._extract_funding_amount(article)
            if funding_amount:
                signal['funding_amount'] = funding_amount

        return signal

    # Words that are never valid standalone company names
    _COMPANY_NAME_STOPWORDS = {
        # Articles / determiners
        'the', 'a', 'an', 'this', 'that', 'these', 'those',
        # Ordinals / quantities
        'new', 'first', 'second', 'third', 'last', 'next', 'one', 'two',
        'q1', 'q2', 'q3', 'q4',
        # Common article-structure words mistaken for company names
        'exclusive', 'breaking', 'report', 'analysis', 'opinion', 'feature',
        'how', 'why', 'what', 'when', 'where', 'who',
        'just', 'only', 'closing', 'opening', 'starting', 'ending',
        'north', 'south', 'east', 'west', 'north america', 'south america',
        'research', 'study', 'survey', 'data', 'health', 'sector',
        'sector snapshot', 'generalist', 'starting',
        # Government / non-startup entities
        'congress', 'senate', 'white house', 'department', 'agency',
        'federal', 'state', 'city',
    }

    def _extract_company_name(self, article: Dict[str, Any]) -> Optional[str]:
        """
        Extract company name from article title or content.
        Uses specificity-ordered patterns; falls back to start-of-title only
        after stripping common content-site prefixes like "Exclusive:".
        """
        title = article.get('title', '') or ''
        content = article.get('content', '') or ''

        # ── Strip common content-site prefixes ───────────────────────────────
        # Crunchbase/TC pattern: "Exclusive: Miravoice raises $6M..."
        title = re.sub(r'^(?:Exclusive|Breaking|Report|Sponsored)\s*:\s*', '', title, flags=re.IGNORECASE)
        # Strip leading "The " only at start of title (it's usually "The X-era company")
        title_for_patterns = re.sub(r'^The\s+', '', title)

        # ── Ordered extraction patterns (highest → lowest specificity) ────────
        patterns = [
            # "funding for CompanyName" / "Series A for CompanyName"
            r'(?:Series\s+[A-Z]\s+(?:funding\s+)?for\s+|funding\s+for\s+)'
            r'([A-Z][A-Za-z0-9]+(?:[.\-][A-Za-z0-9]+)*(?:\s+[A-Z][A-Za-z0-9]+)*'
            r'(?:\s+(?:AI|Inc\.?|Corp\.?|LLC|Ltd\.?|Labs?|Health|Tech|IO))?)',

            # "CompanyName raises/launches/announced/hiring" (before action verb)
            r'\b([A-Z][A-Za-z0-9]+(?:[.\-][A-Za-z0-9]+)*(?:\s+[A-Z][A-Za-z0-9]+)*)'
            r'\s+(?:raises?|raised|announces?|announced|launches?|launched|is\s+hiring)',

            # "CompanyName, a startup/company/platform"
            r'\b([A-Z][A-Za-z0-9]+(?:[.\-][A-Za-z0-9]+)*(?:\s+[A-Z][A-Za-z0-9]+)*)'
            r',\s+(?:a\s+(?:startup|company|platform|fintech|firm|provider)|an\s+)',

            # Start of title (last resort — only used on title, not content)
            r'^([A-Z][A-Za-z0-9]+(?:[.\-][A-Za-z0-9]+)*(?:\s+[A-Z][A-Za-z0-9]+)*'
            r'(?:\s+(?:AI|Inc\.?|Corp\.?|LLC|Ltd\.?|Labs?|Health|Tech|IO))?)',
        ]

        def _is_valid(name: str) -> bool:
            """Return True if name looks like a real company, not a stopword/phrase."""
            if not name or len(name) < 2:
                return False
            # Too long to be a company name (probably a phrase)
            if len(name.split()) > 5:
                return False
            if name.lower() in self._COMPANY_NAME_STOPWORDS:
                return False
            # Reject pure lowercase (common words slipped through)
            if name == name.lower():
                return False
            return True

        # Try patterns against title (specificity 1–3 + start-of-title)
        for i, pattern in enumerate(patterns):
            src = title_for_patterns if i < 3 else title_for_patterns
            match = re.search(pattern, src)
            if match:
                candidate = match.group(1).strip()
                if _is_valid(candidate):
                    return candidate

        # Try patterns 1–3 (not start-of-title) against content
        for pattern in patterns[:3]:
            match = re.search(pattern, content)
            if match:
                candidate = match.group(1).strip()
                if _is_valid(candidate):
                    return candidate

        return None

    def _extract_funding_amount(self, article: Dict[str, Any]) -> Optional[int]:
        """
        Extract funding amount from article text.

        Args:
            article: Article dictionary

        Returns:
            Funding amount in dollars or None
        """
        text = f"{article.get('title', '')} {article.get('content', '')}"

        # Patterns for funding amounts
        patterns = [
            r'\$(\d+(?:\.\d+)?)\s*million',
            r'\$(\d+(?:\.\d+)?)\s*M',
            r'\$(\d+(?:,\d{3})*)',
            r'(\d+(?:\.\d+)?)\s*million\s*dollars?',
            r'(\d+)\s*thousand\s*dollars?',
            r'(\d+)k\s*dollars?'
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                amount = float(amount_str)

                # Convert to dollars
                if 'million' in pattern or 'M' in pattern:
                    return int(amount * 1_000_000)
                elif 'thousand' in pattern or 'k' in pattern:
                    return int(amount * 1_000)
                else:
                    return int(amount)

        return None

    def _calculate_confidence_score(self, matches: List[str], signal_config: Dict[str, Any], article: Dict[str, Any]) -> float:
        """
        Calculate confidence score based on matches and context.

        Args:
            matches: List of matched text
            signal_config: Signal configuration
            article: Article dictionary

        Returns:
            Confidence score between 0.0 and 1.0
        """
        base_score = 0.4

        # Count unique keyword matches for more nuanced scoring
        text = f"{article.get('title', '')} {article.get('content', '')}"
        unique_keyword_matches = 0
        matched_keywords = set()

        for keyword in signal_config.get('keywords', []):
            if keyword.lower() in text.lower():
                matched_keywords.add(keyword)
                unique_keyword_matches += 1

        # Boost for multiple unique keyword matches
        match_boost = min(unique_keyword_matches * 0.15, 0.4)

        # Boost for title matches
        title = article.get('title', '')
        title_boost = 0.0
        for keyword in signal_config.get('keywords', []):
            if keyword.lower() in title.lower():
                title_boost = 0.2
                break

        # Boost for specific high-value keywords
        high_value_keywords = ['funding', 'raised', 'investment', 'launched', 'announcing', 'million', 'series']
        high_value_count = 0
        for keyword in high_value_keywords:
            if keyword.lower() in text.lower():
                high_value_count += 1

        high_value_boost = min(high_value_count * 0.1, 0.3)

        # Additional context boost for specific patterns
        context_boost = 0.0
        if re.search(r'\$\d+(?:\.\d+)?\s*(?:million|M|thousand|k)', text, re.IGNORECASE):
            context_boost += 0.15  # Financial amounts
        if re.search(r'(?:series\s+[A-Z]|seed\s+round|venture\s+capital)', text, re.IGNORECASE):
            context_boost += 0.1  # Investment terms

        final_score = min(base_score + match_boost + title_boost + high_value_boost + context_boost, 1.0)
        return round(final_score, 2)

    def _extract_matched_keywords(self, matches: List[str], keywords: List[str]) -> List[str]:
        """
        Extract the actual keywords that were matched.

        Args:
            matches: List of matched text
            keywords: List of configured keywords

        Returns:
            List of matched keywords
        """
        matched_keywords = []
        text = ' '.join(matches).lower()

        for keyword in keywords:
            if keyword.lower() in text:
                matched_keywords.append(keyword.lower())

        return list(set(matched_keywords))  # Remove duplicates
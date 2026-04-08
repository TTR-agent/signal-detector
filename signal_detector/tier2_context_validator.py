"""
Tier 2 Context Validator

ICP (Ideal Customer Profile) validation component that filters Tier 1 signals
against TTR's target verticals and funding stages using context-aware analysis.
"""

import re
import logging
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime


class Tier2ContextValidator:
    """
    Tier 2 context validator for filtering signals against TTR's ICP.

    This class implements the second tier of the 3-tier signal detection system,
    focusing on validating Tier 1 signals against target verticals (AI/ML, blockchain, fintech)
    and funding stages (pre-seed to Series A) using proximity-based context analysis.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Tier2ContextValidator with configuration.

        Args:
            config: Configuration dictionary loaded from config.yaml
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Load tier 2 context configuration
        self.tier2_config = config.get('tier2_context', {})
        self.enabled = self.tier2_config.get('enabled', True)
        self.context_window_sentences = self.tier2_config.get('context_window_sentences', 3)

        # Load ICP verticals and funding stages
        self.icp_verticals = self.tier2_config.get('icp_verticals', {})
        self.funding_stages = self.tier2_config.get('funding_stages', {})
        self.context_analysis = self.tier2_config.get('context_analysis', {})
        self.confidence_tiers = self.tier2_config.get('confidence_tiers', {})
        self.validation_rules = self.tier2_config.get('validation_rules', {})

        # Compile regex patterns for better performance
        self._compile_patterns()

        # Sentence boundary patterns for context extraction
        boundary_patterns = self.context_analysis.get('sentence_boundary_patterns', ["\\.", "\\!", "\\?"])
        self.sentence_boundary_regex = re.compile('|'.join(boundary_patterns))

    def _compile_patterns(self):
        """Compile regex patterns for each vertical and stage for better performance."""
        self.compiled_vertical_patterns = {}
        self.compiled_stage_patterns = {}

        # Compile ICP vertical patterns
        for vertical_name, vertical_config in self.icp_verticals.items():
            keywords = vertical_config.get('keywords', [])
            if keywords:
                patterns = [f"\\b{re.escape(keyword)}\\b" for keyword in keywords]
                combined_pattern = '|'.join(patterns)
                self.compiled_vertical_patterns[vertical_name] = re.compile(combined_pattern, re.IGNORECASE)

        # Compile funding stage patterns
        for stage_name, stage_config in self.funding_stages.items():
            keywords = stage_config.get('keywords', [])
            if keywords:
                patterns = [f"\\b{re.escape(keyword)}\\b" for keyword in keywords]
                combined_pattern = '|'.join(patterns)
                self.compiled_stage_patterns[stage_name] = re.compile(combined_pattern, re.IGNORECASE)

    def validate_signals(self, signals: List[Dict[str, Any]], articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate Tier 1 signals against ICP criteria.

        Args:
            signals: List of Tier 1 signals to validate
            articles: List of original articles for context analysis

        Returns:
            List of validated signals with enhanced metadata
        """
        if not self.enabled:
            return signals

        validated_signals = []
        article_lookup = {article.get('url', ''): article for article in articles}

        for signal in signals:
            try:
                # Find corresponding article for context analysis
                article = article_lookup.get(signal.get('source_url', ''))
                if not article:
                    # No article found, return signal with warning
                    enhanced_signal = self._add_validation_warning(signal, "No corresponding article found")
                    validated_signals.append(enhanced_signal)
                    continue

                # Perform context validation
                validation_result = self._validate_signal_context(signal, article)

                # Enhance signal with validation metadata
                enhanced_signal = self._enhance_signal_with_context(signal, validation_result)
                validated_signals.append(enhanced_signal)

            except Exception as e:
                self.logger.warning(f"Context validation failed for signal {signal.get('company_name', 'Unknown')}: {e}")
                # Return original signal with error flag
                enhanced_signal = self._add_validation_warning(signal, f"Validation error: {str(e)}")
                validated_signals.append(enhanced_signal)

        return validated_signals

    def _validate_signal_context(self, signal: Dict[str, Any], article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate signal against ICP criteria using context analysis.

        Args:
            signal: Tier 1 signal to validate
            article: Corresponding article for context

        Returns:
            Validation result dictionary with ICP classification and confidence
        """
        # Extract context window around signal
        context = self._extract_context_window(signal, article)

        # Classify ICP vertical
        vertical_result = self._classify_icp_vertical(context)

        # Classify funding stage
        stage_result = self._classify_funding_stage(signal, context)

        # Calculate context quality
        context_quality = self._calculate_context_quality(context, vertical_result, stage_result)

        # Calculate enhanced confidence
        tier2_confidence = self._calculate_tier2_confidence(signal, vertical_result, stage_result, context_quality)

        # Determine validation status
        context_validated = self._determine_validation_status(vertical_result, stage_result, context_quality)

        return {
            'tier2_vertical': vertical_result['vertical'],
            'tier2_stage': stage_result['stage'],
            'tier2_confidence': tier2_confidence,
            'context_validated': context_validated,
            'vertical_confidence': vertical_result['confidence'],
            'stage_confidence': stage_result['confidence'],
            'context_quality_score': context_quality,
            'matched_vertical_keywords': vertical_result['matched_keywords'],
            'matched_stage_keywords': stage_result['matched_keywords'],
            'context_text': context[:200] + '...' if len(context) > 200 else context  # Truncated for storage
        }

    def _extract_context_window(self, signal: Dict[str, Any], article: Dict[str, Any]) -> str:
        """
        Extract context window around the signal in the article.

        Args:
            signal: Signal containing matched keywords
            article: Article to extract context from

        Returns:
            Context text around the signal
        """
        article_text = f"{article.get('title', '')} {article.get('content', '')}"
        matched_keywords = signal.get('matched_keywords', [])

        if not matched_keywords:
            # Fallback: use first 500 characters as context
            return article_text[:500]

        # Find positions of matched keywords
        keyword_positions = []
        for keyword in matched_keywords:
            for match in re.finditer(re.escape(keyword), article_text, re.IGNORECASE):
                keyword_positions.append(match.start())

        if not keyword_positions:
            return article_text[:500]

        # Use the first keyword position as anchor
        anchor_position = min(keyword_positions)

        # Extract sentences around the anchor
        sentences = self.sentence_boundary_regex.split(article_text)

        # Find sentence containing the anchor
        current_position = 0
        anchor_sentence_index = 0

        for i, sentence in enumerate(sentences):
            if current_position <= anchor_position < current_position + len(sentence):
                anchor_sentence_index = i
                break
            current_position += len(sentence) + 1  # +1 for the delimiter

        # Extract context window
        start_sentence = max(0, anchor_sentence_index - self.context_window_sentences // 2)
        end_sentence = min(len(sentences), anchor_sentence_index + self.context_window_sentences // 2 + 1)

        context_sentences = sentences[start_sentence:end_sentence]
        context = '. '.join(sentence.strip() for sentence in context_sentences if sentence.strip())

        return context

    def _classify_icp_vertical(self, context: str) -> Dict[str, Any]:
        """
        Classify ICP vertical from context text.

        Args:
            context: Context text to analyze

        Returns:
            Classification result with vertical, confidence, and matched keywords
        """
        best_vertical = None
        best_confidence = 0.0
        best_matches = []

        for vertical_name, pattern in self.compiled_vertical_patterns.items():
            matches = pattern.findall(context)
            if matches:
                # Calculate confidence based on keyword frequency and diversity
                unique_matches = list(set([match.lower() for match in matches]))
                match_count = len(matches)
                unique_count = len(unique_matches)

                # Base confidence from match frequency
                frequency_confidence = min(0.5, match_count * 0.1)

                # Diversity bonus
                diversity_bonus = min(0.3, unique_count * 0.1)

                # Vertical-specific confidence boost
                vertical_boost = self.icp_verticals[vertical_name].get('confidence_boost', 0.0)

                total_confidence = frequency_confidence + diversity_bonus + vertical_boost

                if total_confidence > best_confidence:
                    best_confidence = total_confidence
                    best_vertical = vertical_name
                    best_matches = unique_matches

        return {
            'vertical': best_vertical,
            'confidence': min(1.0, best_confidence),
            'matched_keywords': best_matches
        }

    def _classify_funding_stage(self, signal: Dict[str, Any], context: str) -> Dict[str, Any]:
        """
        Classify funding stage from signal data and context.

        Args:
            signal: Signal containing funding information
            context: Context text to analyze

        Returns:
            Classification result with stage, confidence, and matched keywords
        """
        best_stage = None
        best_confidence = 0.0
        best_matches = []

        # Check funding amount first if available
        funding_amount = signal.get('funding_amount', 0)
        if funding_amount > 0:
            for stage_name, stage_config in self.funding_stages.items():
                funding_range = stage_config.get('funding_range', [0, 0])
                if funding_range[0] <= funding_amount <= funding_range[1]:
                    best_stage = stage_name
                    best_confidence = 0.8  # High confidence from funding amount
                    break

        # Check for stage keywords in context
        for stage_name, pattern in self.compiled_stage_patterns.items():
            matches = pattern.findall(context)
            if matches:
                unique_matches = list(set([match.lower() for match in matches]))
                keyword_confidence = min(0.6, len(unique_matches) * 0.2)

                # Combine with funding amount confidence if same stage
                if stage_name == best_stage:
                    keyword_confidence += best_confidence

                if keyword_confidence > best_confidence:
                    best_confidence = keyword_confidence
                    best_stage = stage_name
                    best_matches = unique_matches

        return {
            'stage': best_stage,
            'confidence': min(1.0, best_confidence),
            'matched_keywords': best_matches
        }

    def _calculate_context_quality(self, context: str, vertical_result: Dict, stage_result: Dict) -> float:
        """
        Calculate the quality of the extracted context.

        Args:
            context: Extracted context text
            vertical_result: Vertical classification result
            stage_result: Stage classification result

        Returns:
            Context quality score (0.0 - 1.0)
        """
        if not context:
            return 0.0

        quality_score = 0.0

        # Length factor (optimal around 200-500 characters)
        length = len(context)
        if 100 <= length <= 600:
            quality_score += 0.3
        elif 50 <= length < 100 or 600 < length <= 1000:
            quality_score += 0.2
        elif length > 1000:
            quality_score += 0.1

        # Keyword density factor
        total_keywords = len(vertical_result.get('matched_keywords', [])) + len(stage_result.get('matched_keywords', []))
        if total_keywords > 0:
            density = total_keywords / max(1, length / 100)  # keywords per 100 chars
            quality_score += min(0.4, density * 0.1)

        # Signal relevance factor
        if vertical_result.get('vertical'):
            quality_score += 0.2
        if stage_result.get('stage'):
            quality_score += 0.1

        return min(1.0, quality_score)

    def _calculate_tier2_confidence(self, signal: Dict, vertical_result: Dict, stage_result: Dict, context_quality: float) -> float:
        """
        Calculate enhanced confidence score combining Tier 1 and Tier 2 factors.

        Args:
            signal: Original Tier 1 signal
            vertical_result: Vertical classification result
            stage_result: Stage classification result
            context_quality: Context quality score

        Returns:
            Enhanced confidence score
        """
        # Start with original Tier 1 confidence
        tier1_confidence = signal.get('confidence_score', 0.0)

        # Weight factors
        proximity_weight = self.context_analysis.get('proximity_weight', 0.20)
        density_weight = self.context_analysis.get('keyword_density_weight', 0.15)

        # Calculate Tier 2 factors
        vertical_factor = vertical_result.get('confidence', 0.0) * proximity_weight
        stage_factor = stage_result.get('confidence', 0.0) * proximity_weight
        quality_factor = context_quality * density_weight

        # Combine factors
        tier2_confidence = tier1_confidence + vertical_factor + stage_factor + quality_factor

        return min(1.0, tier2_confidence)

    def _determine_validation_status(self, vertical_result: Dict, stage_result: Dict, context_quality: float) -> bool:
        """
        Determine if signal passes validation criteria.

        Args:
            vertical_result: Vertical classification result
            stage_result: Stage classification result
            context_quality: Context quality score

        Returns:
            True if signal passes validation, False otherwise
        """
        # Check validation rules
        require_vertical = self.validation_rules.get('require_vertical_match', True)
        require_stage = self.validation_rules.get('require_stage_match', False)
        min_quality = self.validation_rules.get('min_context_quality', 0.40)

        # Vertical requirement
        if require_vertical and not vertical_result.get('vertical'):
            return False

        # Stage requirement (optional)
        if require_stage and not stage_result.get('stage'):
            return False

        # Context quality requirement
        if context_quality < min_quality:
            return False

        return True

    def _enhance_signal_with_context(self, signal: Dict[str, Any], validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhance signal with Tier 2 validation metadata.

        Args:
            signal: Original Tier 1 signal
            validation_result: Validation result from context analysis

        Returns:
            Enhanced signal with validation metadata
        """
        enhanced_signal = signal.copy()

        # Generate unique signal ID
        signal_id = self._generate_signal_id(signal)

        # Generate human-readable signal rationale
        signal_rationale = self._generate_signal_rationale(signal, validation_result)

        # Add Tier 2 validation fields
        enhanced_signal.update({
            'signal_id': signal_id,
            'tier2_vertical': validation_result['tier2_vertical'],
            'tier2_stage': validation_result['tier2_stage'],
            'tier2_confidence': validation_result['tier2_confidence'],
            'context_validated': validation_result['context_validated'],
            'signal_rationale': signal_rationale,
            'validation_metadata': {
                'vertical_confidence': validation_result['vertical_confidence'],
                'stage_confidence': validation_result['stage_confidence'],
                'context_quality_score': validation_result['context_quality_score'],
                'matched_vertical_keywords': validation_result['matched_vertical_keywords'],
                'matched_stage_keywords': validation_result['matched_stage_keywords'],
                'context_preview': validation_result['context_text']
            }
        })

        return enhanced_signal

    def _generate_signal_id(self, signal: Dict[str, Any]) -> str:
        """
        Generate unique signal ID to prevent duplicates.

        Args:
            signal: Signal data

        Returns:
            Unique signal ID hash
        """
        # Create unique identifier from key signal attributes
        company_name = signal.get('company_name', '')
        source_url = signal.get('source_url', '')
        signal_type = signal.get('signal_type', '')
        article_date = signal.get('article_date', '')

        # Create hash string
        hash_input = f"{company_name}|{source_url}|{signal_type}|{article_date}"

        # Generate SHA-256 hash and take first 12 characters for readability
        signal_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12]

        return f"sig_{signal_hash}"

    def _generate_signal_rationale(self, signal: Dict[str, Any], validation_result: Dict[str, Any]) -> str:
        """
        Generate human-readable signal rationale.

        Args:
            signal: Original signal
            validation_result: Validation result

        Returns:
            Human-readable rationale string
        """
        parts = []

        # ICP Vertical
        vertical = validation_result.get('tier2_vertical')
        if vertical:
            vertical_display = {
                'ai_ml': 'AI/ML',
                'blockchain_web3': 'Blockchain/Web3',
                'fintech': 'Fintech'
            }.get(vertical, vertical)
            parts.append(f"{vertical_display} company")

        # Funding Stage and Amount
        stage = validation_result.get('tier2_stage')
        funding_amount = signal.get('funding_amount')

        if stage and funding_amount:
            stage_display = {
                'pre_seed': 'Pre-seed',
                'seed': 'Seed',
                'series_a': 'Series A'
            }.get(stage, stage)
            parts.append(f"{stage_display} ${funding_amount:,} funding")
        elif stage:
            stage_display = {
                'pre_seed': 'Pre-seed stage',
                'seed': 'Seed stage',
                'series_a': 'Series A stage'
            }.get(stage, stage)
            parts.append(stage_display)
        elif funding_amount:
            parts.append(f"${funding_amount:,} funding")

        # Signal Type
        signal_type = signal.get('signal_type', '')
        type_display = {
            'funding_announcement': 'funding announcement',
            'product_launch': 'product launch',
            'hiring_spree': 'hiring activity'
        }.get(signal_type, signal_type.replace('_', ' '))
        parts.append(type_display)

        # Key matched keywords
        vertical_keywords = validation_result.get('matched_vertical_keywords', [])
        stage_keywords = validation_result.get('matched_stage_keywords', [])

        matched_terms = []
        if vertical_keywords:
            matched_terms.extend(vertical_keywords[:2])  # Top 2 vertical keywords
        if stage_keywords:
            matched_terms.extend(stage_keywords[:1])     # Top 1 stage keyword

        if matched_terms:
            keywords_str = "', '".join(matched_terms)
            parts.append(f"source matched '{keywords_str}'")

        # Join with commas and periods
        if len(parts) > 1:
            rationale = ", ".join(parts[:-1]) + f", {parts[-1]}"
        else:
            rationale = parts[0] if parts else "Signal detected"

        return rationale.capitalize() + "."

    def _add_validation_warning(self, signal: Dict[str, Any], warning_message: str) -> Dict[str, Any]:
        """
        Add validation warning to signal when processing fails.

        Args:
            signal: Original signal
            warning_message: Warning message to add

        Returns:
            Signal with validation warning
        """
        enhanced_signal = signal.copy()
        enhanced_signal.update({
            'tier2_vertical': None,
            'tier2_stage': None,
            'tier2_confidence': signal.get('confidence_score', 0.0),  # Preserve original confidence
            'context_validated': False,
            'validation_metadata': {
                'warning': warning_message,
                'validation_timestamp': datetime.now().isoformat()
            }
        })

        return enhanced_signal

    def get_validation_stats(self) -> Dict[str, Any]:
        """
        Get validation configuration and statistics.

        Returns:
            Dictionary with validation configuration and stats
        """
        return {
            'enabled': self.enabled,
            'context_window_sentences': self.context_window_sentences,
            'icp_verticals': list(self.icp_verticals.keys()),
            'funding_stages': list(self.funding_stages.keys()),
            'validation_rules': self.validation_rules
        }
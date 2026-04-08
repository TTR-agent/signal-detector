#!/usr/bin/env python3
"""
Demo script showing Tier 2 Context Validation in action

This script demonstrates the complete pipeline:
1. Sample articles → Tier 1 signal detection
2. Tier 1 signals → Tier 2 context validation
3. Enhanced signals with ICP classification and confidence scoring
"""

import yaml
import json
from signal_detector import Tier1Matcher, Tier2ContextValidator

def load_config():
    """Load configuration from config.yaml"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def create_sample_articles():
    """Create realistic sample articles for testing"""
    return [
        {
            "title": "Nexora Raises $2.3M Seed Round for AI-Powered Analytics Platform",
            "content": "Nexora, an artificial intelligence startup, announced today that it has raised $2.3 million in seed funding. The company is developing machine learning algorithms for predictive analytics in enterprise software. The funding round was led by prominent venture capital firm TechVentures, with participation from several angel investors. Nexora plans to use the funding to hire additional engineers and expand their AI capabilities.",
            "url": "https://techcrunch.com/nexora-funding",
            "date": "2026-04-05",
            "source": "TechCrunch"
        },
        {
            "title": "BlockFi Launches New DeFi Protocol for Institutional Lending",
            "content": "BlockFi today launched its new decentralized finance protocol aimed at institutional cryptocurrency lending. The platform uses smart contracts on the Ethereum blockchain to facilitate transparent lending between institutions. The protocol features automated risk management and supports multiple digital assets including Bitcoin, Ethereum, and other major cryptocurrencies. BlockFi is currently in Series A funding discussions with several blockchain-focused venture capital firms.",
            "url": "https://coindesk.com/blockfi-defi-launch",
            "date": "2026-04-04",
            "source": "CoinDesk"
        },
        {
            "title": "PayTech Announces $750K Pre-Seed to Revolutionize SMB Payments",
            "content": "PayTech, a fintech startup focused on small business payment solutions, has closed a $750,000 pre-seed funding round. The company is developing a comprehensive payment platform that integrates credit card processing, digital wallets, and banking services for small merchants. Angel investors from the financial services industry participated in the round. PayTech aims to simplify payment acceptance for businesses that currently struggle with complex payment systems.",
            "url": "https://venturebeat.com/paytech-funding",
            "date": "2026-04-03",
            "source": "VentureBeat"
        },
        {
            "title": "Local Restaurant Chain Expands to Three New Locations",
            "content": "Mario's Pizza announced the opening of three new restaurant locations across the metro area. The family-owned business has been serving the community for over 20 years and continues to grow through strong local support. The expansion includes new locations in downtown, the suburbs, and near the university campus. Mario's Pizza is known for its traditional recipes and community involvement.",
            "url": "https://local-news.com/marios-expansion",
            "date": "2026-04-02",
            "source": "Local News"
        }
    ]

def print_separator(title):
    """Print a formatted section separator"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def print_tier1_signals(signals):
    """Pretty print Tier 1 signals"""
    print(f"\n📊 Tier 1 Signals Detected: {len(signals)}")

    for i, signal in enumerate(signals, 1):
        print(f"\n--- Signal {i} ---")
        print(f"Company: {signal.get('company_name', 'Unknown')}")
        print(f"Signal Type: {signal.get('signal_type')}")
        print(f"Confidence: {signal.get('confidence_score', 0):.2f}")
        print(f"Keywords: {', '.join(signal.get('matched_keywords', []))}")
        print(f"Source: {signal.get('source_url', 'Unknown')}")
        if signal.get('funding_amount'):
            print(f"Funding: ${signal['funding_amount']:,}")

def print_tier2_signals(signals):
    """Pretty print enhanced Tier 2 signals"""
    print(f"\n🎯 Tier 2 Enhanced Signals: {len(signals)}")

    for i, signal in enumerate(signals, 1):
        print(f"\n--- Enhanced Signal {i} ---")
        print(f"Company: {signal.get('company_name', 'Unknown')}")
        print(f"Signal Type: {signal.get('signal_type')}")

        # Tier 1 info
        print(f"Tier 1 Confidence: {signal.get('confidence_score', 0):.2f}")

        # Tier 2 enhancements
        tier2_vertical = signal.get('tier2_vertical', 'None')
        tier2_stage = signal.get('tier2_stage', 'None')
        tier2_confidence = signal.get('tier2_confidence', 0)
        validated = signal.get('context_validated', False)

        print(f"🎯 ICP Vertical: {tier2_vertical}")
        print(f"💰 Funding Stage: {tier2_stage}")
        print(f"✨ Enhanced Confidence: {tier2_confidence:.2f}")
        print(f"✅ Validated: {'Yes' if validated else 'No'}")

        # Validation metadata
        if 'validation_metadata' in signal:
            metadata = signal['validation_metadata']
            if 'matched_vertical_keywords' in metadata and metadata['matched_vertical_keywords']:
                print(f"   📝 Vertical Keywords: {', '.join(metadata['matched_vertical_keywords'])}")
            if 'matched_stage_keywords' in metadata and metadata['matched_stage_keywords']:
                print(f"   💸 Stage Keywords: {', '.join(metadata['matched_stage_keywords'])}")
            if 'context_quality_score' in metadata:
                print(f"   🎯 Context Quality: {metadata['context_quality_score']:.2f}")

def main():
    """Run the demo"""
    print("🚀 TTR Signal Detection System - Tier 2 Context Validation Demo")
    print("This demo shows how Tier 1 signals get enhanced with ICP validation")

    # Load configuration
    print_separator("Step 1: Loading Configuration")
    config = load_config()
    print("✅ Configuration loaded successfully")
    print(f"   - ICP Verticals: {list(config['tier2_context']['icp_verticals'].keys())}")
    print(f"   - Funding Stages: {list(config['tier2_context']['funding_stages'].keys())}")

    # Initialize components
    print_separator("Step 2: Initializing Signal Detection Components")
    tier1_matcher = Tier1Matcher(config)
    tier2_validator = Tier2ContextValidator(config)
    print("✅ Tier 1 Matcher initialized")
    print("✅ Tier 2 Context Validator initialized")

    # Create sample articles
    print_separator("Step 3: Processing Sample Articles")
    articles = create_sample_articles()
    print(f"📰 Sample articles created: {len(articles)}")

    for i, article in enumerate(articles, 1):
        print(f"   {i}. {article['title'][:50]}...")

    # Tier 1 signal detection
    print_separator("Step 4: Tier 1 Signal Detection")
    tier1_signals = tier1_matcher.detect_signals(articles)
    print_tier1_signals(tier1_signals)

    # Tier 2 context validation
    print_separator("Step 5: Tier 2 Context Validation")
    tier2_signals = tier2_validator.validate_signals(tier1_signals, articles)
    print_tier2_signals(tier2_signals)

    # Summary comparison
    print_separator("Step 6: Enhancement Summary")

    validated_signals = [s for s in tier2_signals if s.get('context_validated', False)]
    icp_signals = [s for s in tier2_signals if s.get('tier2_vertical')]
    stage_classified = [s for s in tier2_signals if s.get('tier2_stage')]

    print(f"📊 Processing Results:")
    print(f"   • Original Articles: {len(articles)}")
    print(f"   • Tier 1 Signals: {len(tier1_signals)}")
    print(f"   • Tier 2 Enhanced: {len(tier2_signals)}")
    print(f"   • Context Validated: {len(validated_signals)}")
    print(f"   • ICP Classified: {len(icp_signals)}")
    print(f"   • Stage Classified: {len(stage_classified)}")

    # ICP breakdown
    if icp_signals:
        print(f"\n🎯 ICP Vertical Breakdown:")
        verticals = {}
        for signal in icp_signals:
            vertical = signal.get('tier2_vertical')
            verticals[vertical] = verticals.get(vertical, 0) + 1

        for vertical, count in verticals.items():
            print(f"   • {vertical}: {count} signal(s)")

    # Funding stage breakdown
    if stage_classified:
        print(f"\n💰 Funding Stage Breakdown:")
        stages = {}
        for signal in stage_classified:
            stage = signal.get('tier2_stage')
            stages[stage] = stages.get(stage, 0) + 1

        for stage, count in stages.items():
            print(f"   • {stage}: {count} signal(s)")

    print(f"\n✅ Demo completed! The system successfully:")
    print(f"   • Filtered signals against TTR's target ICP")
    print(f"   • Enhanced confidence scoring with context analysis")
    print(f"   • Classified companies by vertical and funding stage")
    print(f"   • Preserved all original signal data while adding validation metadata")

if __name__ == "__main__":
    main()
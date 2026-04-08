#!/usr/bin/env python3
"""
JSON output demo for Tier 2 Context Validation
Shows the enhanced signal data structure for integration testing
"""

import yaml
import json
from signal_detector import Tier1Matcher, Tier2ContextValidator

def main():
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # Initialize components
    tier1_matcher = Tier1Matcher(config)
    tier2_validator = Tier2ContextValidator(config)

    # Sample article
    articles = [{
        "title": "AI Startup Nexora Raises $2.3M Seed Round",
        "content": "Nexora, an artificial intelligence startup developing machine learning algorithms for predictive analytics, announced today that it has raised $2.3 million in seed funding. The funding round was led by TechVentures with participation from angel investors.",
        "url": "https://example.com/nexora-funding",
        "date": "2026-04-06"
    }]

    # Process through pipeline
    tier1_signals = tier1_matcher.detect_signals(articles)
    tier2_signals = tier2_validator.validate_signals(tier1_signals, articles)

    # Output JSON
    print("Enhanced Signal JSON Structure:")
    print(json.dumps(tier2_signals, indent=2))

if __name__ == "__main__":
    main()
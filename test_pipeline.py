#!/usr/bin/env python3
"""
Simple CLI for testing the TTR Signal Detection Pipeline

Usage:
  python3 test_pipeline.py --title "Your Article Title" --content "Your article content..."
  python3 test_pipeline.py --interactive
"""

import argparse
import yaml
import json
from signal_detector import Tier1Matcher, Tier2ContextValidator

def load_config():
    """Load configuration"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def process_article(title, content, url="https://example.com/test"):
    """Process a single article through the pipeline"""

    # Load configuration and initialize components
    config = load_config()
    tier1_matcher = Tier1Matcher(config)
    tier2_validator = Tier2ContextValidator(config)

    # Create article
    article = {
        "title": title,
        "content": content,
        "url": url,
        "date": "2026-04-06"
    }

    print(f"🔍 Processing Article: {title[:50]}...")
    print(f"📝 Content Length: {len(content)} characters")

    # Run through pipeline
    tier1_signals = tier1_matcher.detect_signals([article])
    tier2_signals = tier2_validator.validate_signals(tier1_signals, [article])

    return tier1_signals, tier2_signals

def print_results(tier1_signals, tier2_signals):
    """Print formatted results"""

    print(f"\n📊 Results:")
    print(f"   Tier 1 Signals: {len(tier1_signals)}")
    print(f"   Tier 2 Enhanced: {len(tier2_signals)}")

    if not tier2_signals:
        print("\n❌ No signals detected. Try articles about:")
        print("   • AI/ML companies raising funding")
        print("   • Blockchain/Web3 product launches")
        print("   • Fintech startup announcements")
        return

    for i, signal in enumerate(tier2_signals, 1):
        print(f"\n--- Signal {i} ---")
        print(f"Company: {signal.get('company_name', 'Unknown')}")
        print(f"Type: {signal.get('signal_type')}")
        print(f"Confidence: {signal.get('confidence_score', 0):.2f} → {signal.get('tier2_confidence', 0):.2f}")

        vertical = signal.get('tier2_vertical', 'None')
        stage = signal.get('tier2_stage', 'None')
        validated = signal.get('context_validated', False)

        print(f"ICP Vertical: {vertical}")
        print(f"Funding Stage: {stage}")
        print(f"Validated: {'✅' if validated else '❌'}")

        # Show detected keywords
        metadata = signal.get('validation_metadata', {})
        vertical_kw = metadata.get('matched_vertical_keywords', [])
        stage_kw = metadata.get('matched_stage_keywords', [])

        if vertical_kw:
            print(f"Vertical Keywords: {', '.join(vertical_kw[:3])}...")
        if stage_kw:
            print(f"Stage Keywords: {', '.join(stage_kw)}")

def interactive_mode():
    """Interactive mode for testing"""
    print("🚀 TTR Signal Detection - Interactive Mode")
    print("Enter article details to test the pipeline\n")

    while True:
        try:
            title = input("Article Title: ")
            if not title.strip():
                break

            print("\nArticle Content (press Enter twice to finish):")
            content_lines = []
            while True:
                line = input()
                if not line:
                    break
                content_lines.append(line)

            content = "\n".join(content_lines)

            if content.strip():
                tier1, tier2 = process_article(title, content)
                print_results(tier1, tier2)
            else:
                print("❌ Content cannot be empty")

            print("\n" + "="*50)
            print("Enter another article or press Enter to exit")

        except KeyboardInterrupt:
            break

    print("\n👋 Goodbye!")

def main():
    parser = argparse.ArgumentParser(description="Test TTR Signal Detection Pipeline")
    parser.add_argument('--title', help='Article title')
    parser.add_argument('--content', help='Article content')
    parser.add_argument('--url', default='https://example.com/test', help='Article URL')
    parser.add_argument('--interactive', action='store_true', help='Interactive mode')
    parser.add_argument('--json', action='store_true', help='Output JSON format')

    args = parser.parse_args()

    if args.interactive:
        interactive_mode()
    elif args.title and args.content:
        tier1, tier2 = process_article(args.title, args.content, args.url)

        if args.json:
            print(json.dumps(tier2, indent=2))
        else:
            print_results(tier1, tier2)
    else:
        # Show example usage
        print("🚀 TTR Signal Detection Pipeline Test Tool")
        print("\nExample Usage:")
        print('python3 test_pipeline.py --title "AI Startup Raises $5M" --content "..."')
        print('python3 test_pipeline.py --interactive')
        print("\nQuick Test:")

        # Run a quick test
        title = "TechCorp Announces $3M Series A for Blockchain Analytics Platform"
        content = "TechCorp today announced it has raised $3 million in Series A funding to expand its blockchain analytics platform. The company uses artificial intelligence to track cryptocurrency transactions and provide compliance tools for financial institutions. The funding round was led by Crypto Ventures."

        tier1, tier2 = process_article(title, content)
        print_results(tier1, tier2)

if __name__ == "__main__":
    main()
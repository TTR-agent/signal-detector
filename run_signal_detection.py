#!/usr/bin/env python3
"""
TTR Signal Detection System - Production Pipeline

Full morning run that fetches real articles from techsnif and processes them
through the complete Tier 1 → Tier 2 signal detection pipeline.

Usage:
    python3 run_signal_detection.py [--days=7] [--output=json|csv|console] [--save]
"""

import argparse
import json
import csv
import os
import re
import time
import yaml
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ── Load .env ────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    os.environ.setdefault(key.strip(), value.strip())

from integrations.techsnif_client import TechsnifClient
from integrations.rss_client import StartupRSSClient
from signal_detector import Tier1Matcher, Tier2ContextValidator

class TTRSignalRunner:
    """Main application class for running the TTR signal detection pipeline"""

    def __init__(self, config_path='config.yaml'):
        """Initialize with configuration"""
        self.load_config(config_path)
        self.initialize_components()

        # Generate unique run ID and timestamp
        self.run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        self.run_timestamp = datetime.now().isoformat()

        # Create runs directory for local backup
        self.runs_dir = Path('runs')
        self.runs_dir.mkdir(exist_ok=True)

    def load_config(self, config_path):
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            print(f"✅ Configuration loaded from {config_path}")
        except FileNotFoundError:
            print(f"❌ Config file not found: {config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"❌ Error parsing config: {e}")
            sys.exit(1)

    def initialize_components(self):
        """Initialize all pipeline components"""
        try:
            self.techsnif_client = TechsnifClient(self.config)
            self.rss_client      = StartupRSSClient(self.config)
            self.tier1_matcher   = Tier1Matcher(self.config)
            self.tier2_validator = Tier2ContextValidator(self.config)
            print("✅ All pipeline components initialized successfully")
        except Exception as e:
            print(f"❌ Error initializing components: {e}")
            sys.exit(1)

    def fetch_articles(self, days=7):
        """
        Fetch recent articles from startup RSS feeds (primary) and TechSnif (supplement).

        RSS feeds (TechCrunch Startups/Fundings, VentureBeat, Crunchbase News) are
        checked first because they are curated for early-stage startup news.
        TechSnif is added afterward for broader coverage; duplicates are dropped.
        """
        hours = days * 24
        print(f"\n🔍 Fetching articles from last {days} days ({hours}h window)...")

        seen_urls: set           = set()
        all_articles: list       = []

        # ── 1. Startup RSS feeds (primary source) ────────────────────────────
        print("\n   📰 Pulling startup RSS feeds (primary source)...")
        try:
            rss_articles = self.rss_client.fetch_recent_articles(hours=hours)
            for art in rss_articles:
                url = art.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(art)
        except Exception as e:
            print(f"   ⚠️  RSS feeds error: {e}")

        # ── 2. TechSnif (supplemental) ────────────────────────────────────────
        print("\n   📡 Pulling TechSnif (supplemental)...")
        try:
            techsnif_articles = self.techsnif_client.fetch_recent_articles(hours=hours)
            added = 0
            for art in techsnif_articles:
                url = art.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(art)
                    added += 1
            print(f"   ✅ TechSnif added {added} unique articles")
        except Exception as e:
            print(f"   ⚠️  TechSnif error: {e}")

        if not all_articles:
            print("⚠️  No articles found. Check your network connection or increase --days")
            return []

        # Normalize field names so downstream detectors find 'date'.
        # Always prefer normalized_date (guaranteed ISO) over raw published_date
        # so that RFC 2822 strings from RSS feeds never reach Airtable uncleaned.
        for article in all_articles:
            norm = article.get('normalized_date', '')
            if norm:
                article['date'] = norm
            elif 'published_date' in article:
                article['date'] = article['published_date']

        print(f"\n✅ Total unique articles: {len(all_articles)}")

        # Show article sources breakdown
        sources: dict = {}
        for article in all_articles:
            source = article.get('source', 'Unknown')
            sources[source] = sources.get(source, 0) + 1

        print(f"📊 Article sources:")
        for source, count in sorted(sources.items(), key=lambda x: -x[1]):
            print(f"   • {source}: {count} articles")

        return all_articles

    def process_signals(self, articles):
        """Process articles through the signal detection pipeline"""
        print(f"\n⚙️  Processing {len(articles)} articles through signal detection pipeline...")
        print(f"   🆔 Run ID: {self.run_id}")

        # Tier 1: Signal Detection
        print("   🔍 Running Tier 1 signal detection...")
        tier1_signals = self.tier1_matcher.detect_signals(articles)
        print(f"   ✅ Detected {len(tier1_signals)} Tier 1 signals")

        if not tier1_signals:
            print("   ℹ️  No signals detected in articles")
            return []

        # Show Tier 1 breakdown
        signal_types = {}
        for signal in tier1_signals:
            sig_type = signal.get('signal_type', 'unknown')
            signal_types[sig_type] = signal_types.get(sig_type, 0) + 1

        print(f"   📊 Tier 1 signal types:")
        for sig_type, count in signal_types.items():
            print(f"      • {sig_type}: {count}")

        # Tier 2: Context Validation
        print("   🎯 Running Tier 2 context validation...")
        tier2_signals = self.tier2_validator.validate_signals(tier1_signals, articles)
        print(f"   ✅ Enhanced {len(tier2_signals)} signals with ICP validation")

        # Add run metadata to each signal
        enhanced_signals = []
        for signal in tier2_signals:
            enhanced_signal = signal.copy()

            # Generate a stable Signal ID from company + signal_type + source URL
            # so re-runs don't create duplicates for the same story
            company  = (signal.get('company_name') or 'unknown').lower().replace(' ', '_')
            sig_type = signal.get('signal_type', 'unknown')
            src_url  = signal.get('source_url', '')
            raw_id   = f"{company}_{sig_type}_{src_url}"
            signal_id = 'sig_' + str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id)).replace('-', '')[:12]

            enhanced_signal.update({
                'signal_id': signal_id,
                'run_id': self.run_id,
                'run_timestamp': self.run_timestamp,
                # Airtable-ready fields
                'review_status': 'Pending',
                'review_notes': '',
                'send_to_attio': False,
                'attio_sync_status': 'Not Synced',
                'attio_sync_date': None,
                # Extract website if available
                'website': self._extract_website_from_signal(signal)
            })
            enhanced_signals.append(enhanced_signal)

        # Apply targeting filters (US-only, funding cap, company exclusions)
        enhanced_signals = self._apply_targeting_filters(enhanced_signals, articles)
        print(f"   🎯 After targeting filters: {len(enhanced_signals)} signals remain")

        # Save local backup before any external operations
        self._save_local_backup(enhanced_signals, articles)

        # Show Tier 2 enhancement stats
        validated_signals = [s for s in enhanced_signals if s.get('context_validated', False)]
        icp_classified = [s for s in enhanced_signals if s.get('tier2_vertical')]
        stage_classified = [s for s in enhanced_signals if s.get('tier2_stage')]

        print(f"   📈 Tier 2 enhancements:")
        print(f"      • Context validated: {len(validated_signals)}/{len(enhanced_signals)}")
        print(f"      • ICP classified: {len(icp_classified)}/{len(enhanced_signals)}")
        print(f"      • Stage classified: {len(stage_classified)}/{len(enhanced_signals)}")

        # ICP breakdown
        if icp_classified:
            verticals = {}
            for signal in icp_classified:
                vertical = signal.get('tier2_vertical')
                verticals[vertical] = verticals.get(vertical, 0) + 1

            print(f"   🎯 ICP vertical breakdown:")
            for vertical, count in verticals.items():
                print(f"      • {vertical}: {count}")

        return enhanced_signals

    def output_results(self, signals, format='console', save=False, enable_airtable=True):
        """Output results and integrate with Airtable"""
        print(f"\n📤 Processing results...")

        if not signals:
            print("   ℹ️  No signals to output")
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Step 1: Send to Airtable first (primary integration)
        if enable_airtable:
            print("   📊 Sending to Airtable...")
            airtable_success = self.send_to_airtable(signals)
        else:
            airtable_success = False
            print("   ⚠️ Airtable integration disabled")

        # Step 2: Output in requested format
        if format == 'console':
            self.output_console(signals)
        elif format == 'json':
            self.output_json(signals, save, timestamp)
        elif format == 'csv':
            self.output_csv(signals, save, timestamp)
        else:
            print(f"   ❌ Unknown output format: {format}")

        # Step 3: Email summary (placeholder for future)
        self.send_email_summary(signals, airtable_success)

    def send_email_summary(self, signals: List[Dict[str, Any]], airtable_success: bool):
        """
        Send email summary with run results.

        Args:
            signals: Enhanced signals
            airtable_success: Whether Airtable integration succeeded
        """
        try:
            validated_count = len([s for s in signals if s.get('context_validated', False)])
            icp_count = len([s for s in signals if s.get('tier2_vertical')])

            # Email summary text
            summary = f"""
TTR Signal Detection Run Complete

Run ID: {self.run_id}
Timestamp: {self.run_timestamp}
Total Signals: {len(signals)}
Validated Signals: {validated_count}
ICP Classified: {icp_count}
Airtable Status: {'✅ Success' if airtable_success else '❌ Failed'}

Review signals at: [Airtable URL will go here]
Local backup: runs/{self.run_id}_signals.json
            """.strip()

            # Send actual email if SMTP configured
            if self._send_email(summary):
                print(f"   📧 Email sent successfully to {os.getenv('EMAIL_RECIPIENTS')}")
            else:
                print(f"   📧 Email sending failed - summary saved locally only")

            # Save email content locally as backup
            email_file = self.runs_dir / f"{self.run_id}_email_summary.txt"
            with open(email_file, 'w') as f:
                f.write(summary)

        except Exception as e:
            print(f"   ⚠️ Email summary failed: {e}")

    def _send_email(self, summary_text: str) -> bool:
        """
        Send email summary using SMTP configuration from environment variables.

        Args:
            summary_text: The email content to send

        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        try:
            # Get SMTP configuration from environment
            smtp_server = os.getenv('SMTP_SERVER')
            smtp_port = int(os.getenv('SMTP_PORT', 587))
            smtp_username = os.getenv('SMTP_USERNAME')
            smtp_password = os.getenv('SMTP_PASSWORD')
            email_recipients = os.getenv('EMAIL_RECIPIENTS', '').split(',')

            # Validate required settings
            if not all([smtp_server, smtp_username, smtp_password, email_recipients[0]]):
                print(f"   ⚠️ Email configuration incomplete - check SMTP settings in .env")
                return False

            # Create message
            msg = MIMEMultipart()
            msg['From'] = smtp_username
            msg['To'] = ', '.join(email_recipients)
            msg['Subject'] = f"TTR Signal Detection Results - {self.run_id}"

            # Add body to email
            msg.attach(MIMEText(summary_text, 'plain'))

            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(smtp_username, email_recipients, msg.as_string())

            return True

        except Exception as e:
            print(f"   ⚠️ Email sending failed: {e}")
            return False

    def output_console(self, signals):
        """Output signals to console in readable format"""
        print(f"\n🎯 TTR Signal Detection Results")
        print(f"={'='*60}")

        validated_signals = [s for s in signals if s.get('context_validated', False)]

        for i, signal in enumerate(validated_signals, 1):
            print(f"\n--- Signal {i} ---")
            print(f"Company: {signal.get('company_name', 'Unknown')}")
            print(f"Signal Type: {signal.get('signal_type', 'Unknown')}")
            print(f"ICP Vertical: {signal.get('tier2_vertical', 'None')}")
            print(f"Funding Stage: {signal.get('tier2_stage', 'None')}")
            print(f"Confidence: {signal.get('tier2_confidence', 0):.2f}")
            print(f"Source: {signal.get('source_url', 'Unknown')}")

            if signal.get('funding_amount'):
                print(f"Funding Amount: ${signal['funding_amount']:,}")

            # Show matched keywords
            metadata = signal.get('validation_metadata', {})
            vertical_kw = metadata.get('matched_vertical_keywords', [])
            if vertical_kw:
                print(f"Keywords: {', '.join(vertical_kw[:5])}")

    def output_json(self, signals, save=False, timestamp=None):
        """Output signals as JSON"""
        json_output = json.dumps(signals, indent=2, default=str)

        if save and timestamp:
            filename = f"ttr_signals_{timestamp}.json"
            Path(filename).write_text(json_output)
            print(f"   ✅ Saved to {filename}")
        else:
            print(json_output)

    def output_csv(self, signals, save=False, timestamp=None):
        """Output signals as CSV"""
        if not signals:
            return

        # Define CSV columns
        columns = [
            'company_name', 'signal_type', 'tier2_vertical', 'tier2_stage',
            'tier2_confidence', 'context_validated', 'funding_amount',
            'source_url', 'article_title', 'date_detected'
        ]

        output_lines = []
        output_lines.append(','.join(columns))

        for signal in signals:
            row = []
            for col in columns:
                value = signal.get(col, '')
                if isinstance(value, bool):
                    value = 'Yes' if value else 'No'
                elif value is None:
                    value = ''
                else:
                    value = str(value)
                # Escape quotes and commas for CSV
                if '"' in value or ',' in value:
                    escaped_value = value.replace('"', '""')
                    value = f'"{escaped_value}"'
                row.append(value)
            output_lines.append(','.join(row))

        csv_output = '\n'.join(output_lines)

        if save and timestamp:
            filename = f"ttr_signals_{timestamp}.csv"
            Path(filename).write_text(csv_output)
            print(f"   ✅ Saved to {filename}")
        else:
            print(csv_output)

    def run(self, days=7, output_format='console', save=False):
        """Run the complete signal detection pipeline"""
        print("🚀 TTR Signal Detection System - Full Morning Run")
        print(f"⏰ Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Fetch articles
        articles = self.fetch_articles(days)
        if not articles:
            print("❌ No articles to process. Exiting.")
            return

        # Process signals
        signals = self.process_signals(articles)
        if not signals:
            print("❌ No signals detected. Check your articles or configuration.")
            return

        # Output results and integrate with Airtable
        self.output_results(signals, output_format, save, enable_airtable=True)

        print(f"\n✅ Pipeline completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Summary stats
        validated = len([s for s in signals if s.get('context_validated', False)])
        print(f"📊 Final Summary:")
        print(f"   • Run ID: {self.run_id}")
        print(f"   • Articles processed: {len(articles)}")
        print(f"   • Total signals: {len(signals)}")
        print(f"   • Validated signals: {validated}")
        print(f"   • Success rate: {(validated/len(signals)*100):.1f}%" if signals else "   • Success rate: 0%")
        print(f"   • Local backup: runs/{self.run_id}_signals.json")

    def _apply_targeting_filters(
        self,
        signals: List[Dict[str, Any]],
        articles: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply TTR targeting filters to remove signals outside our ICP:
          - Funding amount cap (no mega-rounds)
          - US-only geography
          - Excluded companies (Big Tech, government entities)
        """
        targeting = self.config.get('targeting', {})
        if not targeting:
            return signals

        max_funding       = targeting.get('max_funding_amount', 25_000_000)
        us_only           = targeting.get('us_only', True)
        us_indicators     = [s.lower() for s in targeting.get('us_city_indicators', [])]
        non_us_patterns   = [s.lower() for s in targeting.get('non_us_exclusion_patterns', [])]
        excluded_companies = [s.lower() for s in targeting.get('excluded_companies', [])]

        # Build URL → article text lookup for geography checks
        article_lookup = {a.get('url', ''): f"{a.get('title','')} {a.get('content','')}".lower()
                          for a in articles}

        # Startup indicator words — at least one must appear in the article
        startup_indicators = [
            'startup', 'start-up', 'founded', 'co-founded', 'co-founder', 'founder',
            'seed round', 'series a', 'series b', 'seed funding', 'venture backed',
            'venture-backed', 'vc-backed', 'early-stage', 'early stage',
            'raises', 'raised', 'secures funding', 'closes funding',
            'pre-seed', 'angel round', 'angel investment',
            # Company-stage context (TechCrunch/Crunchbase style)
            'y combinator', 'yc-backed', 'yc batch', 'techstars', 'a16z',
            'sequoia', 'lightspeed', 'accel', 'bessemer', 'greylock',
            'andreessen', 'general catalyst', 'battery ventures',
            'newly launched', 'newly founded', 'recently launched',
            'based startup', 'the company was founded', 'team of',
            'ceo and co-founder', 'cto and co-founder', 'launched in 20',
        ]

        # Words that indicate a person, not a company, is the subject
        person_title_words = [
            'ceo', 'cto', 'coo', 'cfo',
            'president', 'director', 'senator', 'congressman', 'secretary',
        ]

        # Suffixes that are near-proof the extracted name is a company, not a person
        company_name_suffixes = {
            'ai', 'inc', 'corp', 'llc', 'ltd', 'co', 'labs', 'lab',
            'health', 'tech', 'technologies', 'io', 'app', 'cloud',
            'data', 'systems', 'solutions', 'ventures', 'capital',
            'group', 'platform', 'networks', 'media', 'finance',
            'financial', 'analytics', 'robotics', 'bio', 'pharma',
            'security', 'intelligence', 'energy', 'software', 'services',
        }

        kept, dropped = [], []

        for signal in signals:
            company     = (signal.get('company_name') or '').strip()
            co_lower    = company.lower()
            source_url  = signal.get('source_url', '')
            article_text = article_lookup.get(source_url, '')
            headline    = (signal.get('article_title') or '').lower()
            full_text   = headline + ' ' + article_text
            sig_type    = signal.get('signal_type', '')

            # ── 0. Drop signals with no company name or garbage extraction ──────
            if not company or len(company) < 2:
                dropped.append((company or '(empty)', 'no company name extracted'))
                continue
            # Reject names that are clearly not companies (long phrases, pure
            # lowercase, or start with common article/question words)
            bad_name_patterns = [
                r'^(How|Why|What|When|Where|Who|Just|Q[1-4]|North|South|East|West)\b',
                r'^(Exclusive|Breaking|Closing|Opening|Starting|This Is|Sector)\b',
                r'^The\s+(US|UK|EU|UN)\b',
            ]
            if any(re.match(p, company, re.IGNORECASE) for p in bad_name_patterns):
                dropped.append((company, 'extracted name is not a company'))
                continue
            if len(company.split()) > 6:
                dropped.append((company, 'extracted name too long to be a company'))
                continue

            # ── 1. Detect person names masquerading as companies ──────────────
            words = company.split()
            if len(words) == 2 and all(w[0].isupper() for w in words if w):
                last_word = words[-1].lower().rstrip('.,;')
                # Skip person check if last word is a known company suffix
                is_company_suffix = last_word in company_name_suffixes
                if not is_company_suffix:
                    # Two-word title-case without company suffix → check person context
                    # Require BOTH a person-role word AND the name used in a quote/attribution
                    has_person_role = any(pt in full_text for pt in person_title_words)
                    name_attributed = re.search(
                        rf'\b{re.escape(company)}\s+(says|said|told|writes|wrote|tweeted|posted)',
                        full_text, re.IGNORECASE
                    )
                    if has_person_role and name_attributed:
                        dropped.append((company, 'appears to be a person, not a company'))
                        continue

            # ── 2. Excluded company list ──────────────────────────────────────
            if any(excl == co_lower or excl in co_lower for excl in excluded_companies):
                dropped.append((company, 'excluded company'))
                continue

            # ── 3. Funding amount cap ─────────────────────────────────────────
            funding = signal.get('funding_amount') or 0
            if funding and funding > max_funding:
                dropped.append((company, f'funding ${funding:,} exceeds ${max_funding:,} cap'))
                continue

            # ── 4. Must be about an actual startup ────────────────────────────
            has_startup_context = any(ind in full_text for ind in startup_indicators)
            if not has_startup_context:
                dropped.append((company, 'no startup context found in article'))
                continue

            # ── 5. For non-funding signals: require ICP vertical match ─────────
            if sig_type != 'funding_announcement':
                icp_vertical = signal.get('tier2_vertical')
                if not icp_vertical:
                    dropped.append((company, f'{sig_type} with no ICP vertical match'))
                    continue

            # ── 6. Require context_validated for all signals ──────────────────
            if not signal.get('context_validated', False):
                dropped.append((company, 'did not pass context validation'))
                continue

            # ── 7. US-only geography check ────────────────────────────────────
            if us_only:
                # Hard reject: non-US indicator in headline
                if any(pat in headline for pat in non_us_patterns):
                    dropped.append((company, 'non-US geography in headline'))
                    continue

                # Hard reject: non-US in article text too
                has_non_us = any(pat in full_text for pat in non_us_patterns)
                if has_non_us:
                    dropped.append((company, 'non-US geography in article'))
                    continue

            kept.append(signal)

        if dropped:
            print(f"   🚫 Filtered out {len(dropped)} signals:")
            for company, reason in dropped:
                print(f"      • {company}: {reason}")

        return kept

    def _extract_website_from_signal(self, signal: Dict[str, Any]) -> str:
        """
        Extract or infer website from signal data.

        Args:
            signal: Signal data

        Returns:
            Website URL or empty string
        """
        # Placeholder for future website extraction logic
        # Could extract from article content or use company name lookup
        return ""

    def _save_local_backup(self, signals: List[Dict[str, Any]], articles: List[Dict[str, Any]]):
        """
        Save local backup of run data for failure recovery.

        Args:
            signals: Enhanced signals
            articles: Original articles
        """
        try:
            backup_data = {
                'run_metadata': {
                    'run_id': self.run_id,
                    'run_timestamp': self.run_timestamp,
                    'total_articles': len(articles),
                    'total_signals': len(signals),
                    'validated_signals': len([s for s in signals if s.get('context_validated', False)])
                },
                'signals': signals,
                'articles_summary': [
                    {
                        'title': article.get('title', ''),
                        'url': article.get('url', ''),
                        'date': article.get('date', ''),
                        'source': article.get('source', '')
                    }
                    for article in articles
                ]
            }

            # Save as JSON
            backup_file = self.runs_dir / f"{self.run_id}_signals.json"
            with open(backup_file, 'w') as f:
                json.dump(backup_data, f, indent=2, default=str)

            # Also save as CSV for easy review
            self._save_signals_csv(signals, self.runs_dir / f"{self.run_id}_signals.csv")

            print(f"   💾 Local backup saved: {backup_file}")

        except Exception as e:
            print(f"   ⚠️ Warning: Local backup failed: {e}")

    def _save_signals_csv(self, signals: List[Dict[str, Any]], filepath: Path):
        """
        Save signals as CSV format.

        Args:
            signals: Enhanced signals
            filepath: Output CSV file path
        """
        if not signals:
            return

        # Define CSV columns for Airtable compatibility
        columns = [
            'signal_id', 'run_id', 'company_name', 'website', 'signal_type',
            'tier2_vertical', 'tier2_stage', 'funding_amount', 'confidence_score',
            'tier2_confidence', 'context_validated', 'signal_rationale',
            'article_title', 'source_url', 'article_date', 'date_detected',
            'review_status', 'send_to_attio', 'attio_sync_status'
        ]

        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)

            for signal in signals:
                row = []
                for col in columns:
                    value = signal.get(col, '')

                    # Handle special formatting
                    if col == 'context_validated':
                        value = 'Yes' if value else 'No'
                    elif col == 'send_to_attio':
                        value = 'Yes' if value else 'No'
                    elif col == 'funding_amount' and value:
                        value = f"${value:,}"
                    elif col in ['confidence_score', 'tier2_confidence'] and value:
                        value = f"{value:.2f}"
                    elif value is None:
                        value = ''
                    else:
                        value = str(value)

                    row.append(value)

                writer.writerow(row)

    def prepare_airtable_data(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare signals for Airtable with proper field mapping.

        Args:
            signals: Enhanced signals

        Returns:
            List of Airtable-ready records
        """
        airtable_records = []

        for signal in signals:
            # Extract validation metadata
            validation_metadata = signal.get('validation_metadata', {})

            # Prepare matched keywords as comma-separated string
            tier1_keywords = ', '.join(signal.get('matched_keywords', []))
            vertical_keywords = ', '.join(validation_metadata.get('matched_vertical_keywords', []))
            stage_keywords = ', '.join(validation_metadata.get('matched_stage_keywords', []))

            # Date fields: Airtable date fields require 'YYYY-MM-DD' only
            article_date = signal.get('article_date', '') or ''
            article_date = self._normalize_date_for_airtable(article_date)

            # DateTime fields: ensure UTC ISO format with Z suffix
            date_detected = signal.get('date_detected', '') or ''
            if date_detected and '+' not in date_detected and not date_detected.endswith('Z'):
                date_detected = date_detected.split('.')[0] + 'Z'

            # singleSelect fields: only include if value is non-empty
            signal_type   = signal.get('signal_type') or ''
            icp_vertical  = signal.get('tier2_vertical') or ''
            funding_stage = signal.get('tier2_stage') or ''

            # Skip negative signals — not a valid singleSelect option
            valid_signal_types = {'funding_announcement', 'product_launch', 'hiring_spree'}
            if signal_type not in valid_signal_types:
                continue

            airtable_record = {
                'Signal ID':         signal.get('signal_id', ''),
                'Run ID':            signal.get('run_id', ''),
                'Company Name':      signal.get('company_name', ''),
                'Website':           signal.get('website', ''),
                'Signal Type':       signal_type,
                'Tier 1 Confidence': signal.get('confidence_score') or 0,
                'Tier 2 Confidence': signal.get('tier2_confidence') or 0,
                'Context Validated': bool(signal.get('context_validated', False)),
                'Signal Rationale':  signal.get('signal_rationale', ''),
                'Article Title':     signal.get('article_title', ''),
                'Source URL':        signal.get('source_url', ''),
                'Date Detected':     date_detected,
                'Context Preview':   (validation_metadata.get('context_preview', '') or '')[:500],
                'Matched Keywords':  tier1_keywords,
                'Vertical Keywords': vertical_keywords,
                'Stage Keywords':    stage_keywords,
                'Review Status':     signal.get('review_status', 'Pending'),
                'Review Notes':      signal.get('review_notes', ''),
                'Send to Attio':     bool(signal.get('send_to_attio', False)),
                'Attio Sync Status': signal.get('attio_sync_status', 'Not Synced'),
            }

            # Only add optional fields if they have real values
            if icp_vertical:
                airtable_record['ICP Vertical'] = icp_vertical
            if funding_stage:
                airtable_record['Funding Stage'] = funding_stage
            if article_date:
                airtable_record['Article Date'] = article_date
            funding_amount = signal.get('funding_amount')
            if funding_amount:
                airtable_record['Funding Amount'] = int(funding_amount)

            airtable_records.append(airtable_record)

        return airtable_records

    def send_to_airtable(self, signals: List[Dict[str, Any]]) -> bool:
        """
        Send signals to the Airtable Marketing Partnership Signals table.

        Uses the Airtable Records API (POST /v0/{baseId}/{tableId}).
        Skips records whose Signal ID already exists to prevent duplicates.
        Creates records in batches of 10 (Airtable limit).

        Args:
            signals: Enhanced signals from the pipeline.

        Returns:
            True if all records were created successfully, False otherwise.
        """
        api_token  = os.environ.get('AIRTABLE_API_TOKEN', '').strip()
        base_id    = os.environ.get('AIRTABLE_BASE_ID', '').strip()
        table_name = os.environ.get('AIRTABLE_TABLE_NAME', 'Marketing Partnership Signals').strip()

        if not api_token or api_token in ('your_personal_access_token_here', ''):
            print("   ⚠️  AIRTABLE_API_TOKEN not set — skipping Airtable push")
            return False
        if not base_id or base_id in ('your_base_id_here', ''):
            print("   ⚠️  AIRTABLE_BASE_ID not set — skipping Airtable push")
            return False

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json',
        }
        base_url = f"https://api.airtable.com/v0/{base_id}/{requests.utils.quote(table_name, safe='')}"

        # ── 1. Fetch existing Signal IDs to avoid duplicates ─────────────────
        existing_ids: set = set()
        try:
            offset = None
            while True:
                params = {'fields[]': 'Signal ID', 'pageSize': 100}
                if offset:
                    params['offset'] = offset
                resp = requests.get(base_url, headers=headers, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                for rec in data.get('records', []):
                    sid = rec.get('fields', {}).get('Signal ID', '')
                    if sid:
                        existing_ids.add(sid)
                offset = data.get('offset')
                if not offset:
                    break
                time.sleep(0.25)
        except Exception as e:
            print(f"   ⚠️  Could not fetch existing records (will skip duplicate check): {e}")

        # ── 2. Prepare and filter records ─────────────────────────────────────
        airtable_records = self.prepare_airtable_data(signals)
        new_records = [
            r for r in airtable_records
            if r.get('Signal ID') not in existing_ids
        ]
        skipped = len(airtable_records) - len(new_records)

        print(f"   📊 {len(new_records)} new records to send "
              f"({skipped} already in Airtable)")

        if not new_records:
            print("   ✅ Airtable is already up to date")
            return True

        # ── 3. Batch create (max 10 per request) ──────────────────────────────
        success_count = 0
        fail_count    = 0
        batch_size    = 10

        for i in range(0, len(new_records), batch_size):
            batch = new_records[i:i + batch_size]
            payload = {
                'records': [
                    {'fields': self._clean_airtable_fields(rec)}
                    for rec in batch
                ]
            }

            try:
                resp = requests.post(base_url, headers=headers,
                                     json=payload, timeout=15)
                if resp.status_code in (200, 201):
                    created = len(resp.json().get('records', []))
                    success_count += created
                    print(f"   ✅ Batch {i // batch_size + 1}: pushed {created} records")
                else:
                    fail_count += len(batch)
                    print(f"   ❌ Batch {i // batch_size + 1} HTTP {resp.status_code}: {resp.text[:200]}")
            except requests.RequestException as e:
                fail_count += len(batch)
                print(f"   ❌ Batch {i // batch_size + 1} failed: {e}")

            time.sleep(0.25)   # respect rate limits

        # ── 4. Summary ────────────────────────────────────────────────────────
        if fail_count == 0:
            print(f"   🎉 Airtable: {success_count} records pushed successfully")
            return True
        else:
            print(f"   ⚠️  Airtable: {success_count} pushed, {fail_count} failed")
            return fail_count == 0

    def _normalize_date_for_airtable(self, date_str: str) -> str:
        """
        Convert any date string to YYYY-MM-DD for Airtable date fields.
        Handles ISO 8601, RFC 2822 (RSS standard), and bare dates.
        Returns empty string if parsing fails so the field is simply omitted.
        """
        if not date_str:
            return ''
        date_str = date_str.strip()
        # Already bare date
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        # ISO 8601 with time component (2026-04-07T12:00:00...)
        if 'T' in date_str:
            return date_str.split('T')[0]
        # RFC 2822 (Mon, 07 Apr 2026 12:00:00 +0000)
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(date_str).strftime('%Y-%m-%d')
        except Exception:
            pass
        # Other common formats
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S%z',
                    '%d %b %Y', '%B %d, %Y'):
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return ''

    @staticmethod
    def _clean_airtable_fields(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Strip None / empty values and coerce types so Airtable accepts the payload.
        - Removes keys with None or empty-string values
        - Keeps booleans (checkbox fields need explicit False)
        - Keeps numeric 0 for currency/number fields
        """
        clean = {}
        for key, value in record.items():
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == '':
                continue
            # Airtable currency/number fields must be actual numbers
            if key in ('Funding Amount', 'Tier 1 Confidence', 'Tier 2 Confidence'):
                try:
                    value = float(value) if value != 0 else 0
                except (TypeError, ValueError):
                    continue
            clean[key] = value
        return clean

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='TTR Signal Detection - Full Pipeline')
    parser.add_argument('--days', type=int, default=3,
                       help='Number of days of articles to fetch (default: 3)')
    parser.add_argument('--output', choices=['console', 'json', 'csv'], default='console',
                       help='Output format (default: console)')
    parser.add_argument('--save', action='store_true',
                       help='Save output to file with timestamp')
    parser.add_argument('--config', default='config.yaml',
                       help='Configuration file path (default: config.yaml)')

    args = parser.parse_args()

    try:
        runner = TTRSignalRunner(args.config)
        runner.run(days=args.days, output_format=args.output, save=args.save)
    except KeyboardInterrupt:
        print("\n❌ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
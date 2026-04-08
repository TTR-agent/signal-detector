"""
TTR Signal Detection System

A 3-tier signal detection system for identifying high-confidence early-stage startup signals.
"""

__version__ = "1.0.0"
__author__ = "Tyler Roessel"

from .tier_matcher import Tier1Matcher
from .tier2_context_validator import Tier2ContextValidator

__all__ = ['Tier1Matcher', 'Tier2ContextValidator']
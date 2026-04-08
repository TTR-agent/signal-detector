"""
TTR Signal Detection System

A 3-tier signal detection system for identifying early-stage startup opportunities
targeting AI/ML, blockchain/Web3, and fintech companies in pre-seed to Series A stages.
"""

__version__ = "1.0.0"
__author__ = "Tyler Roessel"

# Import main components
from signal_detector import Tier1Matcher, Tier2ContextValidator
from integrations import TechsnifClient, TechsnifError

__all__ = [
    'Tier1Matcher', 
    'Tier2ContextValidator', 
    'TechsnifClient', 
    'TechsnifError'
]

"""
Integrations package for TTR Signal Detection System.

This package contains integrations with external data sources and services
for fetching and processing startup signals from various news sources.

Modules:
    techsnif_client: Client for techsnif CLI integration
"""

from .techsnif_client import TechsnifClient, TechsnifError

__all__ = ['TechsnifClient', 'TechsnifError']
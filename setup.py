#!/usr/bin/env python3
"""
Setup configuration for TTR Signal Detection System
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="ttr-signal-detector",
    version="1.0.0",
    author="Tyler Roessel",
    author_email="hello@tylerroessel.com",
    description="A 3-tier signal detection system for identifying early-stage startup opportunities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tylerroessel/ttr-signal-detector",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Office/Business",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ttr-signals=ttr_signal_detector.cli:main",
            "ttr-signal-detection=run_signal_detection:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.md"],
    },
)
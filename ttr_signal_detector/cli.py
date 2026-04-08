#!/usr/bin/env python3
"""
Command-line interface for TTR Signal Detection System
"""

import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import and run the main script
from run_signal_detection import main

if __name__ == "__main__":
    main()

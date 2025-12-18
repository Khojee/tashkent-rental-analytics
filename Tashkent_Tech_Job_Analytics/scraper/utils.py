"""
Utility functions for the HH scraper
"""

import time
import random


def get_headers():
    """
    Returns headers to mimic a real browser request
    """
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://tashkent.hh.uz/search/vacancy/map',
        'Origin': 'https://tashkent.hh.uz',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }


def respectful_delay(min_seconds=1, max_seconds=3):
    """
    Add a random delay between requests to be respectful to the server
    
    Args:
        min_seconds: Minimum delay in seconds
        max_seconds: Maximum delay in seconds
    """
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)


def safe_get(data, *keys, default=None):
    """
    Safely get nested dictionary values
    
    Args:
        data: Dictionary to extract from
        *keys: Sequence of keys to traverse
        default: Default value if key path doesn't exist
    
    Returns:
        Value at the key path or default
    """
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
    return data if data is not None else default

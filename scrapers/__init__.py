# Scrapers package - REAL DATA ONLY
from .base_scraper import BaseScraper, BusinessRecord, ScraperException

# Real scrapers
try:
    from .real_scrapers import (
        FloridaScraper, 
        OpenCorporatesScraper, 
        SECEdgarScraper,
        StateSpecificEdgarScraper,
        get_real_scraper,
        get_available_states
    )
except ImportError:
    pass

try:
    from .florida_playwright_scraper import FloridaPlaywrightScraper
except ImportError:
    pass

__all__ = [
    'BaseScraper', 
    'BusinessRecord', 
    'ScraperException',
    'FloridaScraper',
    'OpenCorporatesScraper', 
    'SECEdgarScraper',
    'StateSpecificEdgarScraper',
    'FloridaPlaywrightScraper',
    'get_real_scraper',
    'get_available_states'
]

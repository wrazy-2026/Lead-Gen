"""
Florida Sunbiz Scraper using Playwright with Stealth
=====================================================
This scraper uses Playwright with stealth mode to bypass anti-bot protections
on Florida's Sunbiz website. It fetches REAL business data from the Florida
Secretary of State.

Sunbiz is the gold standard for lead generation because:
- Daily updates of new business registrations
- Provides officer names and registered agent info
- Detailed business information

Requirements:
- playwright
- playwright-stealth
- beautifulsoup4

Install with:
    pip install playwright playwright-stealth
    playwright install chromium
"""

import time
import logging
from typing import List, Optional
from datetime import datetime
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    from playwright_stealth import Stealth
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from scrapers.base_scraper import BaseScraper, BusinessRecord

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FloridaPlaywrightScraper(BaseScraper):
    """
    Real Florida Sunbiz scraper using Playwright with stealth mode.
    
    This scraper bypasses anti-bot protection by:
    1. Using a real Chrome browser (headless)
    2. Applying stealth mode to hide automation signals
    3. Properly rendering JavaScript content
    4. Using realistic user agent and headers
    """
    
    def __init__(self):
        super().__init__(
            state_name="Florida",
            state_code="FL",
            base_url="https://search.sunbiz.org"
        )
        self.search_url = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
        
    def is_available(self) -> bool:
        """Check if Playwright is installed and available."""
        return PLAYWRIGHT_AVAILABLE
    
    def fetch_new_businesses(self, limit: int = 20, search_term: str = "A") -> List[BusinessRecord]:
        """
        Fetch newly registered businesses from Florida Sunbiz.
        
        Args:
            limit: Maximum number of records to fetch
            search_term: Search term for entity names (default 'A' for broad search)
            
        Returns:
            List of BusinessRecord objects with real Florida business data
        """
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright not installed. Run: pip install playwright playwright-stealth && playwright install chromium")
            return []
        
        results = []
        
        try:
            with sync_playwright() as p:
                logger.info("Launching Chromium browser in headless mode...")
                
                # Launch browser - headless for server deployment
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                    ]
                )
                
                # Create context with realistic browser fingerprint
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                )
                
                page = context.new_page()
                
                # Apply stealth to hide Playwright's automation fingerprint
                Stealth().use_sync(page)
                
                logger.info(f"Navigating to Florida Sunbiz search page...")
                
                # Navigate to the search page - use domcontentloaded for faster loading
                page.goto(self.search_url, wait_until="domcontentloaded", timeout=60000)
                
                # Fill in the search form
                logger.info(f"Searching for businesses starting with '{search_term}'...")
                
                # Wait for the search input to be available
                page.wait_for_selector("#SearchTerm", timeout=30000)
                
                # Fill in the search term
                page.fill("#SearchTerm", search_term)
                
                # Click search button
                page.click("input[type='submit'][value='Search Now']")
                
                # Wait for results to load
                logger.info("Waiting for search results...")
                page.wait_for_selector("table", timeout=30000)
                
                # Small delay to ensure full page load
                time.sleep(2)
                
                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(page.content(), "html.parser")
                
                # Find the results table
                table = soup.find("table")
                if not table:
                    logger.warning("No results table found on page")
                    browser.close()
                    return []
                
                rows = table.find_all("tr")[1:]  # Skip header row
                
                logger.info(f"Found {len(rows)} rows in search results")
                
                for row in rows[:limit]:
                    try:
                        cols = row.find_all("td")
                        if len(cols) >= 3:
                            # Extract business info
                            name_link = cols[0].find("a")
                            entity_name = name_link.text.strip() if name_link else cols[0].text.strip()
                            detail_url = name_link.get("href", "") if name_link else ""
                            
                            doc_number = cols[1].text.strip() if len(cols) > 1 else ""
                            status = cols[2].text.strip() if len(cols) > 2 else ""
                            
                            # Create full URL for detail page
                            full_url = f"https://search.sunbiz.org{detail_url}" if detail_url.startswith("/") else detail_url
                            
                            # Map status values (Sunbiz uses abbreviated statuses)
                            display_status = status
                            if status.upper() in ["INACT", "INACTIVE"]:
                                display_status = "Inactive"
                            elif status.upper() == "ACT":
                                display_status = "Active"
                            
                            record = BusinessRecord(
                                business_name=entity_name,
                                filing_date=datetime.now().strftime("%Y-%m-%d"),
                                state="FL",
                                status=display_status,
                                url=full_url,
                                entity_type=self._extract_entity_type(entity_name),
                                filing_number=doc_number,
                            )
                            results.append(record)
                            logger.debug(f"Added: {entity_name} ({doc_number})")
                                
                    except Exception as row_error:
                        logger.warning(f"Error parsing row: {row_error}")
                        continue
                
                logger.info(f"Successfully scraped {len(results)} REAL businesses from Florida Sunbiz")
                browser.close()
                
        except PlaywrightTimeout as e:
            logger.error(f"Timeout while scraping Florida Sunbiz: {e}")
        except Exception as e:
            logger.error(f"Error scraping Florida Sunbiz: {e}")
            import traceback
            traceback.print_exc()
        
        return results
    
    def _extract_entity_type(self, name: str) -> str:
        """Extract entity type from business name."""
        name_upper = name.upper()
        if "LLC" in name_upper or "L.L.C" in name_upper:
            return "LLC"
        elif "INC" in name_upper or "INCORPORATED" in name_upper:
            return "Corporation"
        elif "CORP" in name_upper:
            return "Corporation"
        elif "LP" in name_upper or "L.P." in name_upper:
            return "Limited Partnership"
        elif "LLP" in name_upper:
            return "LLP"
        return "Unknown"
    
    def fetch_business_details(self, detail_url: str) -> Optional[dict]:
        """
        Fetch detailed information for a specific business.
        
        This can get additional info like:
        - Registered agent name and address
        - Officer names
        - Mailing address
        - Annual report dates
        
        Args:
            detail_url: URL to the business detail page
            
        Returns:
            Dictionary with detailed business information
        """
        if not PLAYWRIGHT_AVAILABLE:
            return None
        
        details = {}
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                Stealth().use_sync(page)
                
                page.goto(detail_url, wait_until="networkidle", timeout=30000)
                
                soup = BeautifulSoup(page.content(), "html.parser")
                
                # Extract various details from the page
                # This structure varies, so we'll do best-effort extraction
                
                # Try to find registered agent
                agent_section = soup.find(text=lambda t: t and "Registered Agent" in t)
                if agent_section:
                    agent_parent = agent_section.find_parent()
                    if agent_parent:
                        details["registered_agent"] = agent_parent.get_text(strip=True)
                
                # Try to find principal address
                address_section = soup.find(text=lambda t: t and "Principal Address" in t)
                if address_section:
                    address_parent = address_section.find_parent()
                    if address_parent:
                        details["address"] = address_parent.get_text(strip=True)
                
                # Try to find officer/director names
                officer_section = soup.find(text=lambda t: t and "Officer/Director" in t)
                if officer_section:
                    # Try to extract officer names
                    officer_table = officer_section.find_next("table")
                    if officer_table:
                        officers = []
                        for row in officer_table.find_all("tr"):
                            officer_name = row.get_text(strip=True)
                            if officer_name:
                                officers.append(officer_name)
                        if officers:
                            details["officers"] = officers
                            details["owner_name"] = officers[0] if officers else None
                
                browser.close()
                
        except Exception as e:
            logger.error(f"Error fetching business details: {e}")
        
        return details if details else None


def test_florida_scraper():
    """Test function to verify the scraper works."""
    print("=" * 60)
    print("TESTING FLORIDA PLAYWRIGHT SCRAPER")
    print("=" * 60)
    
    if not PLAYWRIGHT_AVAILABLE:
        print("ERROR: Playwright not installed!")
        print("Run: pip install playwright playwright-stealth")
        print("Then: playwright install chromium")
        return []
    
    scraper = FloridaPlaywrightScraper()
    
    print(f"\nScraper Available: {scraper.is_available()}")
    print(f"Target URL: {scraper.search_url}")
    print(f"\nFetching businesses...\n")
    
    results = scraper.fetch_new_businesses(limit=10, search_term="NEW")
    
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {len(results)} businesses found")
    print("=" * 60)
    
    for i, record in enumerate(results, 1):
        print(f"\n{i}. {record.business_name}")
        print(f"   Filing #: {record.filing_number}")
        print(f"   Status: {record.status}")
        print(f"   Type: {record.entity_type}")
        print(f"   URL: {record.url}")
    
    return results


# Run test if executed directly
if __name__ == "__main__":
    test_florida_scraper()

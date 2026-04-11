#!/usr/bin/env python3
"""
Illinois Secretary of State Business Entity Scraper

Scrapes business data from Illinois SOS registry:
https://apps.ilsos.gov/businessentitysearch/

Features:
- Handles reCAPTCHA protection with 2Captcha integration
- Extracts business name, file number, status, entity type
- Supports multiple search methods (Business Name, Keyword, etc.)
- Parses individual business detail pages
- Saves results to JSON and CSV
- Comprehensive logging and error handling
"""

import asyncio
import json
import logging
import csv
import os
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import re

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class IllinoisBusiness:
    """Illinois SOS business record."""
    name: str
    file_number: str
    entity_type: str
    status: str
    filing_date: Optional[str] = None
    registered_agent: Optional[str] = None
    principal_officer: Optional[str] = None
    address: Optional[str] = None
    county: Optional[str] = None
    jurisdiction: Optional[str] = None
    category: str = "HVAC"
    scraped_at: str = None

    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.now().isoformat()


class TwoCaptchaClient:
    """Client for 2Captcha service to solve reCAPTCHA."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize 2Captcha client."""
        self.api_key = api_key or os.getenv("TWOCAPTCHA_API_KEY")
        if not self.api_key:
            logger.warning("TWOCAPTCHA_API_KEY not set - CAPTCHA solving will not work")
        self.base_url = "http://2captcha.com"

    async def solve_recaptcha(self, page_url: str, site_key: str, timeout: int = 300) -> Optional[str]:
        """Solve reCAPTCHA v2 and return the token."""
        if not self.api_key:
            logger.warning("Skipping CAPTCHA - no API key")
            return None

        try:
            logger.info(f"Solving reCAPTCHA for: {page_url}")

            # Submit CAPTCHA to 2Captcha
            async with aiohttp.ClientSession() as session:
                # Step 1: Submit CAPTCHA
                submit_url = f"{self.base_url}/api/upload"
                data = {
                    "key": self.api_key,
                    "method": "userrecaptcha",
                    "googlekey": site_key,
                    "pageurl": page_url,
                }

                async with session.post(submit_url, data=data) as response:
                    result = await response.text()
                    if not result.startswith("OK|"):
                        logger.error(f"CAPTCHA submission failed: {result}")
                        return None

                    captcha_id = result.split("|")[1]
                    logger.info(f"CAPTCHA submitted with ID: {captcha_id}")

                # Step 2: Poll for result
                result_url = f"{self.base_url}/api/res"
                start_time = time.time()

                while time.time() - start_time < timeout:
                    await asyncio.sleep(5)  # Wait 5 seconds before polling

                    params = {
                        "key": self.api_key,
                        "action": "get",
                        "id": captcha_id,
                        "json": 1,
                    }

                    async with session.get(result_url, params=params) as response:
                        result_data = await response.json()

                        if result_data.get("status") == 0:
                            logger.info("CAPTCHA still processing...")
                            continue
                        elif result_data.get("status") == 1:
                            token = result_data.get("request")
                            logger.success(f"CAPTCHA solved!")
                            return token
                        else:
                            logger.error(f"CAPTCHA error: {result_data}")
                            return None

                logger.error("CAPTCHA solving timeout")
                return None

        except Exception as e:
            logger.error(f"CAPTCHA solving error: {str(e)}")
            return None


class IllinoisSOSScraper:
    """Scraper for Illinois Secretary of State business registry."""

    BASE_URL = "https://apps.ilsos.gov/businessentitysearch/"

    def __init__(self, twocaptcha_key: Optional[str] = None, headless: bool = True):
        """Initialize scraper."""
        self.twocaptcha = TwoCaptchaClient(twocaptcha_key)
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.stats = {
            "searches_performed": 0,
            "results_found": 0,
            "businesses_scraped": 0,
            "errors": 0,
        }

    async def initialize(self):
        """Initialize browser."""
        logger.info("Initializing Playwright browser...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        logger.success("Browser initialized")

    async def close(self):
        """Close browser."""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed")

    async def solve_captcha_on_page(self):
        """Detect and solve reCAPTCHA on current page."""
        try:
            # Check if reCAPTCHA is present
            recaptcha_present = await self.page.query_selector('[data-sitekey]')

            if not recaptcha_present:
                logger.info("No reCAPTCHA detected on page")
                return True

            logger.info("reCAPTCHA detected, attempting to solve...")

            # Get site key
            site_key = await self.page.get_attribute('[data-sitekey]', 'data-sitekey')
            logger.info(f"Site key: {site_key}")

            # Solve CAPTCHA
            token = await self.twocaptcha.solve_recaptcha(
                self.page.url,
                site_key,
                timeout=300
            )

            if not token:
                logger.error("Failed to solve CAPTCHA")
                return False

            # Inject token into page
            await self.page.evaluate(f"""
                document.getElementById('g-recaptcha-response').innerHTML = '{token}';
                if (typeof ___grecaptcha_cfg !== 'undefined') {{
                    Object.entries(___grecaptcha_cfg.clients).forEach(([key, client]) => {{
                        if (client.callback) {{
                            client.callback('{token}');
                        }}
                    }});
                }}
            """)

            logger.success("CAPTCHA token injected")

            # Submit form
            await self.page.click('#btnSearch')
            await self.page.wait_for_load_state('networkidle', timeout=10000)

            logger.success("Form submitted after CAPTCHA")
            return True

        except Exception as e:
            logger.error(f"CAPTCHA solving error: {str(e)}")
            return False

    async def search_businesses(self, keyword: str, search_method: str = "Business Name") -> List[Dict]:
        """Search for businesses by keyword."""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Searching for: {keyword} ({search_method})")
            logger.info(f"{'='*60}")

            # Navigate to search page
            logger.info("Navigating to Illinois SOS search page...")
            await self.page.goto(self.BASE_URL, wait_until='networkidle')

            # Select search method
            search_method_map = {
                "Business Name": "#name",
                "Keyword": "#keyWord",
                "Partial Word": "#partialWord",
                "Registered Agent": "#agentsearch",
                "President": "#presidentsearch",
                "Secretary": "#secretarysearch",
                "Manager": "#managersearch",
                "File Number": "#fileNumber",
            }

            selector = search_method_map.get(search_method, "#name")
            logger.info(f"Selecting search method: {search_method}")
            await self.page.click(selector)

            # Enter search term
            logger.info(f"Entering search term: {keyword}")
            await self.page.fill('#searchValue', keyword)

            # Click submit
            logger.info("Submitting search...")
            await self.page.click('#btnSearch')

            # Wait for results or CAPTCHA
            try:
                await self.page.wait_for_selector('table', timeout=5000)
                logger.success("Results table loaded")
            except:
                logger.warning("Results table not found, checking for CAPTCHA...")

                # Try to solve CAPTCHA
                captcha_solved = await self.solve_captcha_on_page()
                if not captcha_solved:
                    logger.error("Failed to solve CAPTCHA, skipping search")
                    self.stats["errors"] += 1
                    return []

                # Wait for results after CAPTCHA
                try:
                    await self.page.wait_for_selector('table', timeout=10000)
                    logger.success("Results table loaded after CAPTCHA")
                except:
                    logger.warning("No results table found after CAPTCHA")
                    return []

            # Extract results
            results = await self.extract_search_results(keyword)
            self.stats["searches_performed"] += 1
            self.stats["results_found"] += len(results)

            return results

        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            self.stats["errors"] += 1
            return []

    async def extract_search_results(self, keyword: str) -> List[Dict]:
        """Extract business records from search results table."""
        try:
            logger.info("Extracting search results...")

            # Get all rows from results table
            rows = await self.page.query_selector_all('table tbody tr')
            logger.info(f"Found {len(rows)} result rows")

            businesses = []

            for idx, row in enumerate(rows):
                try:
                    # Extract cells
                    cells = await row.query_selector_all('td')

                    if len(cells) < 3:
                        continue

                    # Extract data from cells
                    name_elem = await cells[0].text_content()
                    file_num_elem = await cells[1].text_content() if len(cells) > 1 else ""
                    entity_type_elem = await cells[2].text_content() if len(cells) > 2 else ""
                    status_elem = await cells[3].text_content() if len(cells) > 3 else ""

                    name = name_elem.strip() if name_elem else ""
                    file_number = file_num_elem.strip() if file_num_elem else ""
                    entity_type = entity_type_elem.strip() if entity_type_elem else ""
                    status = status_elem.strip() if status_elem else ""

                    if not name or not file_number:
                        continue

                    logger.info(f"[{idx+1}] {name} | {file_number} | {entity_type} | {status}")

                    business = IllinoisBusiness(
                        name=name,
                        file_number=file_number,
                        entity_type=entity_type,
                        status=status,
                        category=keyword,
                    )

                    businesses.append(asdict(business))
                    self.stats["businesses_scraped"] += 1

                except Exception as e:
                    logger.warning(f"Error extracting row {idx}: {str(e)}")
                    continue

            logger.success(f"Extracted {len(businesses)} businesses")
            return businesses

        except Exception as e:
            logger.error(f"Result extraction error: {str(e)}")
            return []

    async def scrape_business_detail(self, file_number: str) -> Optional[Dict]:
        """Scrape detailed information for a specific business."""
        try:
            logger.info(f"Scraping details for file number: {file_number}")

            # Construct detail URL
            detail_url = f"https://apps.ilsos.gov/businessentitysearch/businessentitysearch?search=true&fileNumber={file_number}"

            # Navigate to detail page
            await self.page.goto(detail_url, wait_until='networkidle')

            # Extract details from page
            details = {}

            # Try to find common detail fields
            try:
                details["filing_date"] = await self.page.text_content('text=Filing Date')
                details["registered_agent"] = await self.page.text_content('text=Registered Agent')
                details["principal_officer"] = await self.page.text_content('text=Principal Officer')
                details["address"] = await self.page.text_content('text=Address')
            except:
                pass

            logger.info(f"Extracted details: {details}")
            return details

        except Exception as e:
            logger.error(f"Detail scraping error: {str(e)}")
            return None

    async def scrape_keywords(self, keywords: List[str]) -> List[Dict]:
        """Scrape businesses for multiple keywords."""
        logger.info(f"\n{'#'*60}")
        logger.info(f"# BATCH SCRAPING")
        logger.info(f"# Keywords: {len(keywords)}")
        logger.info(f"{'#'*60}\n")

        all_businesses = []

        for idx, keyword in enumerate(keywords):
            logger.info(f"\n[{idx+1}/{len(keywords)}] Processing keyword: {keyword}")

            results = await self.search_businesses(keyword, search_method="Business Name")
            all_businesses.extend(results)

            # Rate limiting
            await asyncio.sleep(2)

        logger.info(f"\n{'#'*60}")
        logger.info(f"# SCRAPING COMPLETE")
        logger.info(f"# Total searches: {self.stats['searches_performed']}")
        logger.info(f"# Total results: {self.stats['results_found']}")
        logger.info(f"# Businesses scraped: {self.stats['businesses_scraped']}")
        logger.info(f"# Errors: {self.stats['errors']}")
        logger.info(f"{'#'*60}\n")

        return all_businesses

    def save_results(self, businesses: List[Dict], output_dir: str = "illinois_sos_data"):
        """Save results to JSON and CSV."""
        os.makedirs(output_dir, exist_ok=True)

        # Save JSON
        json_path = os.path.join(output_dir, "businesses.json")
        with open(json_path, 'w') as f:
            json.dump(businesses, f, indent=2)
        logger.success(f"Saved {len(businesses)} businesses to {json_path}")

        # Save CSV
        csv_path = os.path.join(output_dir, "businesses.csv")
        if businesses:
            with open(csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=businesses[0].keys())
                writer.writeheader()
                writer.writerows(businesses)
            logger.success(f"Saved {len(businesses)} businesses to {csv_path}")


async def main():
    """Main execution."""
    # Keywords to search
    keywords = [
        "HVAC",
        "Plumbing",
        "Roofing",
        "Electrical",
        "Cleaning",
    ]

    # Initialize scraper
    scraper = IllinoisSOSScraper(headless=False)  # Set to True for headless mode

    try:
        await scraper.initialize()

        # Scrape businesses
        businesses = await scraper.scrape_keywords(keywords)

        # Save results
        scraper.save_results(businesses)

        logger.success("Scraping complete!")

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())

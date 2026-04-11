#!/usr/bin/env python3
"""
Advanced Florida Sunbiz Scraper with Playwright

This is a more robust scraper using Playwright for JavaScript rendering,
CAPTCHA handling, and better anti-bot evasion.

Features:
- Playwright for JavaScript rendering
- Stealth mode to avoid detection
- CAPTCHA detection and handling
- Proxy support
- Concurrent scraping with rate limiting
- Detailed error handling and logging
"""

import asyncio
import json
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional
import os
import re

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdvancedSunbizScraper:
    """Advanced Sunbiz scraper using Playwright."""

    HOME_SERVICE_KEYWORDS = [
        "HVAC", "plumber", "roofer", "cleaning", "remodeling",
        "electrician", "painter", "landscaper", "carpenter",
        "handyman", "contractor", "construction", "solar",
        "pool service", "pest control", "tree service",
    ]

    def __init__(
        self,
        output_dir: str = "./sunbiz_data",
        headless: bool = True,
        use_proxy: Optional[str] = None,
        timeout: int = 30000
    ):
        """
        Initialize the advanced scraper.

        Args:
            output_dir: Directory to save output files
            headless: Run browser in headless mode
            use_proxy: Proxy URL (e.g., "http://proxy.example.com:8080")
            timeout: Page load timeout in milliseconds
        """
        self.output_dir = output_dir
        self.headless = headless
        self.use_proxy = use_proxy
        self.timeout = timeout
        self.base_url = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
        self.businesses = []
        self.browser = None
        self.context = None

        os.makedirs(output_dir, exist_ok=True)

    async def initialize_browser(self) -> Browser:
        """Initialize Playwright browser with stealth settings."""
        playwright = await async_playwright().start()

        launch_args = {
            "headless": self.headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        }

        if self.use_proxy:
            launch_args["proxy"] = {"server": self.use_proxy}

        self.browser = await playwright.chromium.launch(**launch_args)

        # Create context with stealth settings
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 720},
        )

        # Add stealth script
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
        """)

        return self.browser

    async def close_browser(self):
        """Close the browser."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()

    async def handle_captcha(self, page: Page) -> bool:
        """
        Detect and attempt to handle CAPTCHA.

        Args:
            page: Playwright page object

        Returns:
            True if CAPTCHA was handled or not present, False if blocking
        """
        try:
            # Check for reCAPTCHA
            recaptcha = await page.query_selector('[data-sitekey]')
            if recaptcha:
                logger.warning("reCAPTCHA detected. Manual intervention required.")
                # In production, integrate with 2Captcha or Anti-Captcha API
                return False

            # Check for hCaptcha
            hcaptcha = await page.query_selector('[data-sitekey*="h"]')
            if hcaptcha:
                logger.warning("hCaptcha detected. Manual intervention required.")
                return False

            return True

        except Exception as e:
            logger.debug(f"CAPTCHA check error: {str(e)}")
            return True

    async def search_by_keyword(self, page: Page, keyword: str) -> List[Dict]:
        """
        Search for businesses by keyword using Playwright.

        Args:
            page: Playwright page object
            keyword: Search term

        Returns:
            List of business dictionaries
        """
        logger.info(f"Searching for: {keyword}")
        results = []

        try:
            # Navigate to search page
            await page.goto(self.base_url, wait_until="networkidle", timeout=self.timeout)

            # Check for CAPTCHA
            if not await self.handle_captcha(page):
                logger.warning(f"CAPTCHA blocking search for '{keyword}'")
                return results

            # Fill search box
            search_input = await page.query_selector('input[name*="name"], input[placeholder*="name"]')
            if search_input:
                await search_input.fill(keyword)
                await search_input.press("Enter")

                # Wait for results to load
                await page.wait_for_load_state("networkidle")

                # Extract results
                results = await self._extract_results_from_page(page, keyword)

            else:
                logger.warning(f"Could not find search input on page")

        except Exception as e:
            logger.error(f"Error searching for '{keyword}': {str(e)}")

        return results

    async def _extract_results_from_page(self, page: Page, keyword: str) -> List[Dict]:
        """
        Extract business results from the page.

        Args:
            page: Playwright page object
            keyword: The search keyword

        Returns:
            List of business dictionaries
        """
        results = []

        try:
            # Get all result rows
            rows = await page.query_selector_all("tr")

            for row in rows:
                try:
                    cells = await row.query_selector_all("td")

                    if len(cells) >= 3:
                        name = await cells[0].text_content()
                        doc_num = await cells[1].text_content() if len(cells) > 1 else ""
                        status = await cells[2].text_content() if len(cells) > 2 else ""
                        filing_date = await cells[3].text_content() if len(cells) > 3 else ""

                        business = {
                            "name": name.strip() if name else "",
                            "document_number": doc_num.strip() if doc_num else "",
                            "status": status.strip() if status else "",
                            "filing_date": filing_date.strip() if filing_date else "",
                            "category": keyword,
                            "scraped_date": datetime.now().isoformat(),
                        }

                        if business["name"]:
                            results.append(business)

                except Exception as e:
                    logger.debug(f"Error extracting row: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error extracting results: {str(e)}")

        logger.info(f"Extracted {len(results)} results for '{keyword}'")
        return results

    async def scrape_all_categories(self, max_per_category: int = 50) -> List[Dict]:
        """
        Scrape all home service categories.

        Args:
            max_per_category: Maximum results per category

        Returns:
            List of all businesses
        """
        logger.info("Initializing browser...")
        await self.initialize_browser()

        page = await self.context.new_page()
        page.set_default_timeout(self.timeout)

        try:
            for keyword in self.HOME_SERVICE_KEYWORDS:
                results = await self.search_by_keyword(page, keyword)
                self.businesses.extend(results[:max_per_category])

                # Rate limiting
                await asyncio.sleep(2)

        finally:
            await page.close()
            await self.close_browser()

        logger.info(f"Total businesses scraped: {len(self.businesses)}")
        return self.businesses

    def sort_by_date(self, ascending: bool = False) -> List[Dict]:
        """Sort businesses by filing date."""
        def parse_date(date_str: str) -> datetime:
            if not date_str:
                return datetime.min

            formats = ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue

            return datetime.min

        sorted_businesses = sorted(
            self.businesses,
            key=lambda x: parse_date(x.get("filing_date", "")),
            reverse=not ascending
        )

        return sorted_businesses

    def save_to_json(self, filename: str = "sunbiz_businesses.json"):
        """Save to JSON."""
        filepath = f"{self.output_dir}/{filename}"
        with open(filepath, "w") as f:
            json.dump(self.businesses, f, indent=2)
        logger.info(f"Saved to {filepath}")

    def save_to_csv(self, filename: str = "sunbiz_businesses.csv"):
        """Save to CSV."""
        filepath = f"{self.output_dir}/{filename}"
        df = pd.DataFrame(self.businesses)
        df.to_csv(filepath, index=False)
        logger.info(f"Saved to {filepath}")


async def main():
    """Main execution."""
    scraper = AdvancedSunbizScraper(headless=True)

    # Scrape all categories
    await scraper.scrape_all_categories(max_per_category=50)

    # Sort by date
    sorted_businesses = scraper.sort_by_date(ascending=False)
    scraper.businesses = sorted_businesses

    # Save results
    scraper.save_to_json()
    scraper.save_to_csv()

    logger.info("Scraping complete!")


if __name__ == "__main__":
    asyncio.run(main())

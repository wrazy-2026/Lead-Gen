#!/usr/bin/env python3
"""
Florida Sunbiz Home Services Business Scraper

This scraper extracts business information from the Florida Sunbiz registry
(https://search.sunbiz.org/Inquiry/CorporationSearch/ByName) for home service
categories including HVAC, plumbing, roofing, cleaning, remodeling, and more.

Features:
- Searches by common home service keywords
- Extracts detailed business information
- Sorts results by filing date (newest to oldest)
- Handles pagination and rate limiting
- Saves data to JSON and CSV formats
"""

import asyncio
import json
import csv
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlencode
import re

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SunbizScraper:
    """Scraper for Florida Sunbiz business registry."""

    # Home service business keywords and categories
    HOME_SERVICE_KEYWORDS = [
        "HVAC", "heating", "cooling", "air conditioning",
        "plumber", "plumbing", "pipe",
        "roofer", "roofing", "roof",
        "cleaning", "cleaner", "janitorial",
        "remodeling", "remodeler", "renovation", "contractor",
        "electrician", "electrical", "electric",
        "painting", "painter", "paint",
        "landscaping", "landscaper", "lawn", "garden",
        "carpentry", "carpenter", "wood",
        "masonry", "mason", "concrete",
        "flooring", "floor",
        "drywall", "insulation",
        "window", "door",
        "gutter", "gutter cleaning",
        "pressure washing", "power wash",
        "tree service", "tree removal",
        "pest control", "termite",
        "pool service", "pool cleaning",
        "appliance repair", "appliance",
        "handyman", "home repair", "home maintenance",
        "construction", "builder",
        "solar", "solar panel",
        "water damage", "restoration",
        "mold", "mold removal",
    ]

    def __init__(self, output_dir: str = "./sunbiz_data"):
        """
        Initialize the scraper.

        Args:
            output_dir: Directory to save output files
        """
        self.base_url = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"
        self.output_dir = output_dir
        self.session = None
        self.businesses = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        # Create output directory if it doesn't exist
        import os
        os.makedirs(output_dir, exist_ok=True)

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def search_by_keyword(self, keyword: str, max_results: int = 100) -> List[Dict]:
        """
        Search for businesses by keyword.

        Args:
            keyword: Search term (e.g., "HVAC", "plumber")
            max_results: Maximum number of results to fetch

        Returns:
            List of business dictionaries
        """
        logger.info(f"Searching for: {keyword}")
        results = []

        try:
            # Perform search
            search_url = f"{self.base_url}?search={urlencode({'name': keyword})}"
            async with self.session.get(search_url, headers=self.headers) as response:
                if response.status != 200:
                    logger.warning(f"Search failed for '{keyword}': HTTP {response.status}")
                    return results

                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                # Extract business results from the search page
                # Note: The actual HTML structure depends on Sunbiz's current layout
                # This is a generic approach that may need adjustment
                results = self._parse_search_results(soup, keyword)

        except Exception as e:
            logger.error(f"Error searching for '{keyword}': {str(e)}")

        return results[:max_results]

    def _parse_search_results(self, soup: BeautifulSoup, keyword: str) -> List[Dict]:
        """
        Parse search results from HTML.

        Args:
            soup: BeautifulSoup object of the search results page
            keyword: The search keyword used

        Returns:
            List of parsed business dictionaries
        """
        results = []

        try:
            # Look for result rows (this is a generic pattern)
            # The actual selectors depend on Sunbiz's HTML structure
            result_rows = soup.find_all("tr", class_=re.compile("result|item|row"))

            for row in result_rows:
                try:
                    business = self._extract_business_info(row, keyword)
                    if business:
                        results.append(business)
                except Exception as e:
                    logger.debug(f"Error parsing row: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error parsing search results: {str(e)}")

        return results

    def _extract_business_info(self, row: BeautifulSoup, keyword: str) -> Optional[Dict]:
        """
        Extract business information from a result row.

        Args:
            row: BeautifulSoup object of a single result row
            keyword: The search keyword used

        Returns:
            Dictionary with business information or None if extraction fails
        """
        try:
            # Extract common fields (adjust selectors based on actual HTML)
            cells = row.find_all("td")

            if len(cells) < 3:
                return None

            business = {
                "name": cells[0].get_text(strip=True) if len(cells) > 0 else "",
                "document_number": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                "status": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                "filing_date": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "category": keyword,
                "scraped_date": datetime.now().isoformat(),
            }

            # Validate that we have at least a name
            if not business["name"]:
                return None

            return business

        except Exception as e:
            logger.debug(f"Error extracting business info: {str(e)}")
            return None

    async def scrape_all_categories(self, max_per_category: int = 50) -> List[Dict]:
        """
        Scrape businesses from all home service categories.

        Args:
            max_per_category: Maximum results per category

        Returns:
            List of all scraped businesses
        """
        logger.info(f"Starting scrape of {len(self.HOME_SERVICE_KEYWORDS)} categories")

        all_businesses = []

        for keyword in self.HOME_SERVICE_KEYWORDS:
            results = await self.search_by_keyword(keyword, max_per_category)
            all_businesses.extend(results)

            # Rate limiting: wait between requests
            await asyncio.sleep(1)

        logger.info(f"Total businesses scraped: {len(all_businesses)}")
        self.businesses = all_businesses
        return all_businesses

    def sort_by_date(self, ascending: bool = False) -> List[Dict]:
        """
        Sort businesses by filing date (newest to oldest by default).

        Args:
            ascending: If True, sort oldest to newest

        Returns:
            Sorted list of businesses
        """
        def parse_date(date_str: str) -> datetime:
            """Parse various date formats."""
            if not date_str:
                return datetime.min

            # Try common date formats
            formats = ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue

            logger.warning(f"Could not parse date: {date_str}")
            return datetime.min

        sorted_businesses = sorted(
            self.businesses,
            key=lambda x: parse_date(x.get("filing_date", "")),
            reverse=not ascending
        )

        return sorted_businesses

    def save_to_json(self, filename: str = "sunbiz_businesses.json"):
        """
        Save scraped businesses to JSON file.

        Args:
            filename: Output filename
        """
        filepath = f"{self.output_dir}/{filename}"
        try:
            with open(filepath, "w") as f:
                json.dump(self.businesses, f, indent=2)
            logger.info(f"Saved {len(self.businesses)} businesses to {filepath}")
        except Exception as e:
            logger.error(f"Error saving JSON: {str(e)}")

    def save_to_csv(self, filename: str = "sunbiz_businesses.csv"):
        """
        Save scraped businesses to CSV file.

        Args:
            filename: Output filename
        """
        filepath = f"{self.output_dir}/{filename}"
        try:
            df = pd.DataFrame(self.businesses)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(self.businesses)} businesses to {filepath}")
        except Exception as e:
            logger.error(f"Error saving CSV: {str(e)}")

    def get_summary(self) -> Dict:
        """
        Get summary statistics of scraped data.

        Returns:
            Dictionary with summary information
        """
        if not self.businesses:
            return {"total": 0, "categories": {}}

        df = pd.DataFrame(self.businesses)

        summary = {
            "total_businesses": len(self.businesses),
            "categories": df["category"].value_counts().to_dict(),
            "statuses": df["status"].value_counts().to_dict() if "status" in df else {},
            "date_range": {
                "earliest": df["filing_date"].min() if "filing_date" in df else None,
                "latest": df["filing_date"].max() if "filing_date" in df else None,
            }
        }

        return summary


async def main():
    """Main execution function."""
    logger.info("Starting Florida Sunbiz Scraper")

    async with SunbizScraper() as scraper:
        # Scrape all home service categories
        businesses = await scraper.scrape_all_categories(max_per_category=50)

        # Sort by date (newest to oldest)
        sorted_businesses = scraper.sort_by_date(ascending=False)
        scraper.businesses = sorted_businesses

        # Save results
        scraper.save_to_json()
        scraper.save_to_csv()

        # Display summary
        summary = scraper.get_summary()
        logger.info(f"Summary: {json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    asyncio.run(main())

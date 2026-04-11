#!/usr/bin/env python3
"""
Fixed Florida Sunbiz Scraper – Playwright-based

Flow:
1. Go to https://search.sunbiz.org/Inquiry/CorporationSearch/ByName
2. Type a keyword, submit the form
3. Parse the search-results table (each row = entity name link, doc#, status)
4. Click each entity link → opens detail page with full business data
5. Extract all fields from the detail page
6. Go back, continue with next row
7. Handle "Next List" pagination
8. Repeat for every keyword

A callback function (on_log) is called at every step so the web UI
can show a real-time log panel.
"""

import asyncio
import json
import csv
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Optional, Callable

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FixedSunbizScraper:
    """Playwright-driven Sunbiz scraper with step-by-step logging."""

    HOME_SERVICE_KEYWORDS = [
        "HVAC", "plumber", "roofer", "cleaning", "remodeling",
        "electrician", "painter", "landscaper", "carpenter",
        "handyman", "contractor", "construction", "solar",
        "pool service", "pest control", "tree service",
        "pressure washing", "drywall", "flooring", "masonry",
        "gutter", "appliance repair", "water damage", "mold",
    ]

    SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName"

    def __init__(
        self,
        output_dir: str = "./sunbiz_data",
        headless: bool = True,
        timeout: int = 60000,
        on_log: Optional[Callable[[str], None]] = None,
    ):
        self.output_dir = output_dir
        self.headless = headless
        self.timeout = timeout
        self.businesses: List[Dict] = []
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self._playwright = None
        self._log_cb = on_log or (lambda msg: None)
        os.makedirs(output_dir, exist_ok=True)

    # ── helpers ──────────────────────────────────────────────────────────

    def _log(self, msg: str):
        logger.info(msg)
        self._log_cb(msg)

    # ── browser lifecycle ────────────────────────────────────────────────

    async def start_browser(self):
        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage"],
        )
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        await self.context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>false});"
        )
        self._log("Browser started.")

    async def stop_browser(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._log("Browser closed.")

    # ── search ───────────────────────────────────────────────────────────

    async def _do_search(self, page: Page, keyword: str) -> bool:
        """Navigate to the search page, type keyword, submit. Returns True on success."""
        self._log(f"Navigating to {self.SEARCH_URL}")
        await page.goto(self.SEARCH_URL, wait_until="domcontentloaded", timeout=self.timeout)

        # The form has: <input id="SearchTerm" name="SearchTerm" ...>
        input_sel = 'input#SearchTerm, input[name="SearchTerm"]'
        try:
            await page.wait_for_selector(input_sel, timeout=10_000)
        except Exception:
            self._log("ERROR: Could not find search input on page.")
            return False

        await page.fill(input_sel, keyword)
        self._log(f"Typed keyword '{keyword}' into search box.")

        # Click the Search button or press Enter
        btn = await page.query_selector('input[type="submit"][value*="Search"], input.button')
        if btn:
            await btn.click()
            self._log("Clicked Search button.")
        else:
            await page.press(input_sel, "Enter")
            self._log("Pressed Enter to search.")

        # Wait for the results page
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=self.timeout)
            self._log(f"Results page loaded: {page.url}")
        except Exception as e:
            self._log(f"ERROR waiting for results: {e}")
            return False

        return True

    # ── parse search-results table ───────────────────────────────────────

    async def _parse_results_table(self, page: Page) -> List[Dict]:
        """Return list of {name, doc_number, status, href} from the current results page."""
        rows: List[Dict] = []

        # Valid Florida document numbers match pattern: Letter + numbers (e.g., P15000005427, L09000043622)
        import re
        DOC_NUMBER_PATTERN = re.compile(r'^[A-Z]\d{8,}$', re.I)

        # Results are in a table. Each data row has 3 <td> cells:
        #   <td><a href="/Inquiry/CorporationSearch/SearchResultDetail?...">NAME</a></td>
        #   <td>DOC_NUMBER</td>
        #   <td>STATUS</td>
        all_trs = await page.query_selector_all("table tr")
        self._log(f"Found {len(all_trs)} <tr> elements on results page.")

        for tr in all_trs:
            tds = await tr.query_selector_all("td")
            if len(tds) < 3:
                continue

            # Get all text from first cell
            first_cell_text = (await tds[0].text_content() or "").strip()
            
            # Find link in first cell - look for any link with SearchResultDetail
            link = await tds[0].query_selector('a[href*="SearchResultDetail"]')
            if not link:
                continue

            # Get business name from link text
            name = (await link.text_content() or "").strip()
            href = await link.get_attribute("href") or ""
            
            # Get document number from second cell
            doc_number = (await tds[1].text_content() or "").strip()
            
            # Get status from third cell
            status = (await tds[2].text_content() or "").strip()

            # Skip if no name
            if not name:
                self._log(f"  Skipping row: empty name")
                continue

            # Validate document number format (Letter + 8+ digits)
            if not DOC_NUMBER_PATTERN.match(doc_number):
                self._log(f"  Skipping row: invalid doc number '{doc_number}' for '{name[:30]}'")
                continue

            self._log(f"  Found business: {name[:50]} | {doc_number} | {status}")
            
            rows.append({
                "name": name,
                "document_number": doc_number,
                "status": status,
                "href": href,
            })

        self._log(f"Parsed {len(rows)} valid business rows from results table.")
        return rows

    # ── get "Next List" link ─────────────────────────────────────────────

    async def _get_next_page_url(self, page: Page) -> Optional[str]:
        """Return the href of the 'Next List' link, or None."""
        link = await page.query_selector('a:has-text("Next List")')
        if link:
            href = await link.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = "https://search.sunbiz.org" + href
                return href
        return None

    # ── scrape a single detail page ──────────────────────────────────────

    async def _scrape_detail(self, page: Page, href: str, keyword: str) -> Optional[Dict]:
        """Navigate to a detail page, extract fields, return dict."""
        full_url = href if href.startswith("http") else "https://search.sunbiz.org" + href

        try:
            await page.goto(full_url, wait_until="domcontentloaded", timeout=self.timeout)
        except Exception as e:
            self._log(f"  ERROR loading detail page: {e}")
            return None

        # Check for "Document Not Found"
        body_text = await page.inner_text("body")
        if "Document Not Found" in body_text:
            self._log("  Detail page says 'Document Not Found' - skipping.")
            return None

        biz: Dict[str, str] = {"category": keyword, "scraped_date": datetime.now().isoformat()}

        # --- Extract labelled fields via regex on visible text ---

        # Entity / Corporation name
        m = re.search(r'(?:Corporation Name|Entity Name|LLC Name|LP Name)[:\s]*\n?\s*([^\n]+)', body_text, re.I)
        if m:
            biz["name"] = m.group(1).strip()

        # Document Number
        m = re.search(r'Document Number[:\s]*\n?\s*([^\n]+)', body_text, re.I)
        if m:
            biz["document_number"] = m.group(1).strip()

        # FEI/EIN Number
        m = re.search(r'FEI/EIN Number[:\s]*\n?\s*([^\n]+)', body_text, re.I)
        if m:
            biz["fei_ein"] = m.group(1).strip()

        # Date Filed
        m = re.search(r'Date Filed[:\s]*\n?\s*(\d{1,2}/\d{1,2}/\d{4})', body_text, re.I)
        if m:
            biz["filing_date"] = m.group(1).strip()

        # Effective Date
        m = re.search(r'Effective Date[:\s]*\n?\s*(\d{1,2}/\d{1,2}/\d{4})', body_text, re.I)
        if m:
            biz["effective_date"] = m.group(1).strip()

        # State
        m = re.search(r'State[:\s]*\n?\s*([A-Z]{2})\b', body_text)
        if m:
            biz["state"] = m.group(1).strip()

        # Status
        m = re.search(r'Status[:\s]*\n?\s*([^\n]+)', body_text, re.I)
        if m:
            biz["status"] = m.group(1).strip()

        # Last Event
        m = re.search(r'Last Event[:\s]*\n?\s*([^\n]+)', body_text, re.I)
        if m:
            biz["last_event"] = m.group(1).strip()

        # Event Date Filed
        m = re.search(r'Event Date Filed[:\s]*\n?\s*([^\n]+)', body_text, re.I)
        if m:
            biz["event_date_filed"] = m.group(1).strip()

        # Principal Address (multi-line)
        m = re.search(r'Principal Address\s*\n((?:.+\n?){1,4})', body_text, re.I)
        if m:
            biz["principal_address"] = " | ".join(
                line.strip() for line in m.group(1).strip().splitlines() if line.strip()
            )

        # Mailing Address
        m = re.search(r'Mailing Address\s*\n((?:.+\n?){1,4})', body_text, re.I)
        if m:
            biz["mailing_address"] = " | ".join(
                line.strip() for line in m.group(1).strip().splitlines() if line.strip()
            )

        # Registered Agent
        m = re.search(r'Registered Agent Name & Address\s*\n((?:.+\n?){1,5})', body_text, re.I)
        if m:
            biz["registered_agent"] = " | ".join(
                line.strip() for line in m.group(1).strip().splitlines() if line.strip()
            )

        # Officer/Director – grab first officer block
        m = re.search(r'(?:Officer/Director Detail|Name & Address)\s*\n\s*Title\s+(\S+)\s*\n\s*(.+)', body_text, re.I)
        if m:
            biz["officer_title"] = m.group(1).strip()
            biz["officer_name"] = m.group(2).strip()

        biz["detail_url"] = full_url
        return biz

    # ── scrape one keyword ───────────────────────────────────────────────

    async def scrape_keyword(self, page: Page, keyword: str, max_results: int = 50) -> List[Dict]:
        """Full scrape for one keyword: search -> paginate -> detail pages."""
        self._log(f"\n{'='*60}")
        self._log(f"KEYWORD: {keyword}  (max {max_results} results)")
        self._log(f"{'='*60}")

        ok = await self._do_search(page, keyword)
        if not ok:
            return []

        all_rows: List[Dict] = []
        page_num = 1

        while len(all_rows) < max_results:
            self._log(f"--- Results page {page_num} ---")
            rows = await self._parse_results_table(page)

            if not rows:
                self._log("No more rows found - stopping pagination.")
                break

            all_rows.extend(rows)
            self._log(f"Total rows collected so far: {len(all_rows)}")

            if len(all_rows) >= max_results:
                break

            next_url = await self._get_next_page_url(page)
            if not next_url:
                self._log("No 'Next List' link - last page.")
                break

            self._log(f"Navigating to next page: {next_url}")
            await page.goto(next_url, wait_until="domcontentloaded", timeout=self.timeout)
            page_num += 1
            await asyncio.sleep(1)

        # Trim to max
        all_rows = all_rows[:max_results]

        # Now visit each detail page
        businesses: List[Dict] = []
        for idx, row in enumerate(all_rows, 1):
            self._log(f"[{idx}/{len(all_rows)}] Opening detail: {row['name']}  (doc# {row['document_number']})")

            if not row.get("href"):
                self._log("  No link - using table data only.")
                detail_url = ""
                businesses.append({
                    "name": row["name"],
                    "document_number": row["document_number"],
                    "status": row["status"],
                    "category": keyword,
                    "detail_url": detail_url,
                    "scraped_date": datetime.now().isoformat(),
                })
                continue

            # Build full URL for the detail page
            href = row["href"]
            detail_url = href if href.startswith("http") else "https://search.sunbiz.org" + href

            biz = await self._scrape_detail(page, row["href"], keyword)
            if biz:
                # ALWAYS use name from table row (it's the most reliable)
                biz["name"] = row["name"]
                biz["document_number"] = row["document_number"]
                # Use status from detail page if available, otherwise from table
                if not biz.get("status"):
                    biz["status"] = row["status"]
                biz.setdefault("detail_url", detail_url)
                businesses.append(biz)
                self._log(f"  OK - filing_date={biz.get('filing_date','N/A')}, status={biz.get('status','N/A')}")
            else:
                # Fallback - keep table-level data
                businesses.append({
                    "name": row["name"],
                    "document_number": row["document_number"],
                    "status": row["status"],
                    "category": keyword,
                    "detail_url": detail_url,
                    "scraped_date": datetime.now().isoformat(),
                })
                self._log("  Used fallback (table data only).")

            await asyncio.sleep(0.5)

        self._log(f"Keyword '{keyword}' complete: {len(businesses)} businesses scraped.\n")
        return businesses

    # ── scrape all keywords ──────────────────────────────────────────────

    async def scrape_all(self, keywords: List[str], max_per_keyword: int = 50) -> List[Dict]:
        await self.start_browser()
        page = await self.context.new_page()
        page.set_default_timeout(self.timeout)

        try:
            for kw in keywords:
                results = await self.scrape_keyword(page, kw, max_per_keyword)
                self.businesses.extend(results)
                await asyncio.sleep(2)
        finally:
            await page.close()
            await self.stop_browser()

        self._log(f"\nAll keywords done. Total businesses: {len(self.businesses)}")
        return self.businesses

    # ── sorting & export ─────────────────────────────────────────────────

    def sort_by_date(self, ascending: bool = False) -> List[Dict]:
        def _parse(d):
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(d, fmt)
                except Exception:
                    pass
            return datetime.min
        return sorted(
            self.businesses,
            key=lambda b: _parse(b.get("filing_date", "")),
            reverse=not ascending,
        )

    def save_to_json(self, filename="sunbiz_businesses.json"):
        path = os.path.join(self.output_dir, filename)
        with open(path, "w") as f:
            json.dump(self.businesses, f, indent=2)
        self._log(f"Saved JSON -> {path}")

    def save_to_csv(self, filename="sunbiz_businesses.csv"):
        path = os.path.join(self.output_dir, filename)
        pd.DataFrame(self.businesses).to_csv(path, index=False)
        self._log(f"Saved CSV -> {path}")

    def get_summary(self) -> Dict:
        if not self.businesses:
            return {"total": 0}
        df = pd.DataFrame(self.businesses)
        return {
            "total": len(self.businesses),
            "categories": df["category"].value_counts().to_dict() if "category" in df else {},
            "statuses": df["status"].value_counts().to_dict() if "status" in df else {},
        }


async def main():
    scraper = FixedSunbizScraper(headless=True, on_log=lambda m: print(m))
    await scraper.scrape_all(FixedSunbizScraper.HOME_SERVICE_KEYWORDS[:2], max_per_keyword=5)
    scraper.businesses = scraper.sort_by_date()
    scraper.save_to_json()
    scraper.save_to_csv()
    print(json.dumps(scraper.get_summary(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())

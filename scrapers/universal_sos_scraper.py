"""
Universal SOS Scraper Engine
=============================
A single, config-driven scraper that can search ANY state's Secretary of State
business database using the configuration from state_configs.py.

Strategies implemented:
  suffix_search  – searches the SOS with common trade suffixes (LLC, Services, etc.)
  api_json       – calls a JSON API endpoint (Tyler Technologies pattern)
  asp_form       – handles ASP.NET ViewState form posts
  date_search    – searches by recent filing date range (where supported)

Falls back to OpenCorporates, then SEC EDGAR for states that block scraping.
"""

import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper, BusinessRecord, ScraperException, CaptchaException
from scrapers.anti_bot import (
    CaptchaDetectedError,
    create_scraper_session,
    detect_captcha,
    get_browser_headers,
    get_random_ua,
    make_request_with_retry,
)
from scrapers.state_configs import (
    StateSOSConfig,
    get_state_config,
    TRADE_SUFFIXES,
)

logger = logging.getLogger(__name__)


class UniversalSOSScraper(BaseScraper):
    """
    Config-driven scraper that adapts to any US state's SOS portal.

    Usage:
        scraper = UniversalSOSScraper("GA")  # Georgia
        records = scraper.fetch_new_businesses(limit=50)
    """

    def __init__(self, state_code: str, config: Optional[StateSOSConfig] = None):
        self.config = config or get_state_config(state_code)
        super().__init__(
            self.config.state_name,
            self.config.state_code,
            self.config.sos_url,
        )
        self.session = create_scraper_session()
        self._last_request_time = 0.0

    # ------------------------------------------------------------------
    # Rate-limiting helper
    # ------------------------------------------------------------------

    def _throttle(self):
        """Enforce per-state rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.config.rate_limit_delay:
            time.sleep(self.config.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited GET with retry, CAPTCHA detection, and UA rotation."""
        self._throttle()
        return make_request_with_retry(self.session, url, method="GET", **kwargs)

    def _post(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited POST with retry."""
        self._throttle()
        return make_request_with_retry(self.session, url, method="POST", **kwargs)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """
        Fetch recently filed businesses from this state's SOS.

        Dispatches to the appropriate strategy based on config.search_strategy.
        """
        strategy = self.config.search_strategy
        self.logger.info(
            f"[{self.config.state_code}] Fetching via strategy '{strategy}' "
            f"from {self.config.sos_url}"
        )

        try:
            if strategy == "api_json":
                return self._strategy_api_json(limit)
            elif strategy == "asp_form":
                return self._strategy_asp_form(limit)
            elif strategy == "date_search":
                return self._strategy_date_search(limit)
            elif strategy == "sequential_id":
                return self._strategy_sequential_id(limit)
            else:
                # Default: suffix_search
                return self._strategy_suffix_search(limit)
        except CaptchaDetectedError as e:
            self.logger.warning(f"[{self.config.state_code}] CAPTCHA blocked: {e}")
            raise CaptchaException(
                self.config.state_code,
                f"CAPTCHA detected on {self.config.state_name} SOS",
            )
        except requests.RequestException as e:
            self.logger.error(f"[{self.config.state_code}] Request failed: {e}")
            raise ScraperException(
                self.config.state_code,
                f"Request error: {e}",
                original_exception=e,
            )

    def is_available(self) -> bool:
        """Quick HEAD check on the SOS URL."""
        try:
            resp = self.session.head(self.config.sos_url, timeout=10)
            return resp.status_code < 500
        except Exception:
            return False

    # ==================================================================
    # STRATEGY: suffix_search
    # ==================================================================

    def _strategy_suffix_search(self, limit: int) -> List[BusinessRecord]:
        """
        Search the SOS portal with trade-related suffixes.
        e.g. search for "Plumbing", "HVAC LLC", "Roofing Services" etc.
        """
        records: List[BusinessRecord] = []
        seen_names = set()

        # Shuffle suffixes so different runs hit different terms first
        suffixes = list(TRADE_SUFFIXES)
        random.shuffle(suffixes)

        for suffix in suffixes:
            if len(records) >= limit:
                break

            try:
                batch = self._search_sos_html(suffix, max_results=min(limit - len(records), 30))
                for rec in batch:
                    key = (rec.business_name or "").strip().upper()
                    if key and key not in seen_names:
                        seen_names.add(key)
                        records.append(rec)
            except Exception as e:
                self.logger.debug(
                    f"[{self.config.state_code}] suffix '{suffix}' failed: {e}"
                )
                continue

        self.logger.info(
            f"[{self.config.state_code}] suffix_search found {len(records)} records"
        )
        return records[:limit]

    def _search_sos_html(self, query: str, max_results: int = 30) -> List[BusinessRecord]:
        """
        Generic HTML form/GET search on the SOS endpoint.
        Parses the response HTML for business name rows.
        """
        endpoint = self.config.search_endpoint or self.config.sos_url

        # Try GET with query parameter (most common pattern)
        params = {"SearchName": query, "q": query, "searchterm": query, "SearchValue": query}
        resp = self._get(endpoint, params=params)

        if resp.status_code != 200:
            return []

        return self._parse_html_results(resp.text, max_results)

    def _parse_html_results(self, html: str, max_results: int) -> List[BusinessRecord]:
        """
        Generic HTML result parser. Looks for table rows or common patterns.
        Uses config selectors if defined, otherwise heuristic fallback.
        """
        soup = BeautifulSoup(html, "lxml")
        records = []

        # --- Config-driven extraction ---
        if self.config.result_selector:
            rows = soup.select(self.config.result_selector)
            for row in rows[:max_results]:
                rec = self._extract_from_row(row)
                if rec:
                    records.append(rec)
            if records:
                return records

        # --- Heuristic: look for tables with business data ---
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows[1:]:  # skip header
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    name_text = cells[0].get_text(strip=True)
                    if not name_text or len(name_text) < 3:
                        continue

                    # Try to find a link
                    link_el = cells[0].find("a")
                    url = ""
                    if link_el and link_el.get("href"):
                        url = urljoin(self.config.sos_url, link_el["href"])

                    # Try to find filing date
                    date_str = ""
                    for cell in cells[1:]:
                        text = cell.get_text(strip=True)
                        date_match = re.search(
                            r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})", text
                        )
                        if date_match:
                            date_str = self._normalize_date(date_match.group(1))
                            break

                    # Entity type heuristic
                    entity_type = None
                    for cell in cells:
                        text = cell.get_text(strip=True).upper()
                        if any(t in text for t in ["LLC", "CORP", "INC", "LP", "LLP"]):
                            entity_type = text
                            break

                    records.append(
                        BusinessRecord(
                            business_name=name_text,
                            filing_date=date_str or datetime.now().strftime("%Y-%m-%d"),
                            state=self.config.state_code,
                            status="Active",
                            url=url or self.config.sos_url,
                            entity_type=entity_type,
                        )
                    )
                    if len(records) >= max_results:
                        break
            if records:
                break  # Use first table that yields results

        # --- Heuristic: look for list items / divs with links ---
        if not records:
            for item in soup.select("div.search-result, li.result, div.entity-row, div.result-item"):
                name_el = item.find(["a", "span", "strong"])
                if name_el:
                    name = name_el.get_text(strip=True)
                    link = name_el.get("href", "") if name_el.name == "a" else ""
                    if link:
                        link = urljoin(self.config.sos_url, link)
                    records.append(
                        BusinessRecord(
                            business_name=name,
                            filing_date=datetime.now().strftime("%Y-%m-%d"),
                            state=self.config.state_code,
                            status="Active",
                            url=link or self.config.sos_url,
                        )
                    )
                    if len(records) >= max_results:
                        break

        return records

    def _extract_from_row(self, row) -> Optional[BusinessRecord]:
        """Extract a BusinessRecord from a table row using config selectors."""
        try:
            # Business name
            name_el = row.select_one(self.config.name_selector) if self.config.name_selector else None
            if not name_el:
                cells = row.find_all("td")
                name_el = cells[0] if cells else None
            if not name_el:
                return None

            name = name_el.get_text(strip=True)
            if not name or len(name) < 3:
                return None

            # URL
            url = ""
            link_el = None
            if self.config.detail_link_selector:
                link_el = row.select_one(self.config.detail_link_selector)
            if not link_el:
                link_el = name_el.find("a") if name_el else None
            if not link_el:
                link_el = row.find("a")
            if link_el and link_el.get("href"):
                url = urljoin(self.config.sos_url, link_el["href"])

            # Filing date
            date_str = ""
            if self.config.date_selector:
                date_el = row.select_one(self.config.date_selector)
                if date_el:
                    date_str = self._normalize_date(date_el.get_text(strip=True))
            if not date_str:
                date_str = datetime.now().strftime("%Y-%m-%d")

            return BusinessRecord(
                business_name=name,
                filing_date=date_str,
                state=self.config.state_code,
                status="Active",
                url=url or self.config.sos_url,
            )
        except Exception:
            return None

    # ==================================================================
    # STRATEGY: api_json (Tyler Technologies pattern)
    # ==================================================================

    def _strategy_api_json(self, limit: int) -> List[BusinessRecord]:
        """
        Search via JSON API (common for Tyler Technologies portals used by
        ID, MT, ND, WA, MI, CA, etc.).
        """
        records: List[BusinessRecord] = []
        seen_names = set()

        suffixes = list(TRADE_SUFFIXES)
        random.shuffle(suffixes)

        for suffix in suffixes:
            if len(records) >= limit:
                break

            try:
                # Tyler Technologies standard JSON payload
                payload = {
                    "SEARCH_VALUE": suffix,
                    "STARTS_WITH_YN": "N",  # Contains search
                    "ACTIVE_ONLY_YN": "Y",
                }
                headers = get_browser_headers()
                headers["Content-Type"] = "application/json"
                headers["Accept"] = "application/json"

                resp = self._post(
                    self.config.search_endpoint,
                    json=payload,
                    headers=headers,
                )

                if resp.status_code != 200:
                    continue

                data = resp.json()

                # Handle different JSON shapes
                results_list = []
                if isinstance(data, list):
                    results_list = data
                elif isinstance(data, dict):
                    results_list = (
                        data.get("rows")
                        or data.get("results")
                        or data.get("ROWS")
                        or data.get("data")
                        or []
                    )

                for item in results_list:
                    if len(records) >= limit:
                        break
                    rec = self._parse_json_record(item)
                    if rec:
                        key = rec.business_name.strip().upper()
                        if key not in seen_names:
                            seen_names.add(key)
                            records.append(rec)

            except Exception as e:
                self.logger.debug(
                    f"[{self.config.state_code}] JSON API suffix '{suffix}' error: {e}"
                )
                continue

        self.logger.info(
            f"[{self.config.state_code}] api_json found {len(records)} records"
        )
        return records[:limit]

    def _parse_json_record(self, item: dict) -> Optional[BusinessRecord]:
        """Parse a single business record from JSON API response."""
        if not isinstance(item, dict):
            return None

        # Try common field names
        name = (
            item.get("TITLE", [None])[0]
            if isinstance(item.get("TITLE"), list)
            else item.get("TITLE")
            or item.get("title")
            or item.get("FILING_NAME")
            or item.get("EntityName")
            or item.get("entityName")
            or item.get("BusinessName")
            or item.get("businessName")
            or item.get("name")
            or item.get("NAME")
        )
        if not name or len(str(name).strip()) < 3:
            return None

        status = (
            item.get("STATUS")
            or item.get("status")
            or item.get("EntityStatus")
            or item.get("entityStatus")
            or "Active"
        )

        filing_date = (
            item.get("FILING_DATE")
            or item.get("filingDate")
            or item.get("FormationDate")
            or item.get("formationDate")
            or item.get("date")
            or ""
        )
        if filing_date:
            filing_date = self._normalize_date(str(filing_date))
        else:
            filing_date = datetime.now().strftime("%Y-%m-%d")

        filing_number = (
            item.get("FILING_NUMBER")
            or item.get("filingNumber")
            or item.get("EntityNumber")
            or item.get("entityNumber")
            or item.get("Id")
            or item.get("id")
        )

        entity_type = (
            item.get("ENTITY_TYPE")
            or item.get("entityType")
            or item.get("EntityType")
            or item.get("type")
        )

        url = item.get("URL") or item.get("url") or item.get("detailUrl") or ""
        if url and not url.startswith("http"):
            url = urljoin(self.config.sos_url, url)

        return BusinessRecord(
            business_name=str(name).strip(),
            filing_date=filing_date,
            state=self.config.state_code,
            status=str(status),
            url=url or self.config.sos_url,
            entity_type=str(entity_type) if entity_type else None,
            filing_number=str(filing_number) if filing_number else None,
        )

    # ==================================================================
    # STRATEGY: asp_form (ASP.NET ViewState)
    # ==================================================================

    def _strategy_asp_form(self, limit: int) -> List[BusinessRecord]:
        """
        Handle ASP.NET WebForms with __VIEWSTATE, __EVENTVALIDATION, etc.
        Used by DE, IA, KY, LA, MA, MS, MO, OK, RI, SC, SD, TN, WI, WY, PR.
        """
        records: List[BusinessRecord] = []
        seen_names = set()

        # Step 1: GET the form page to extract ViewState tokens
        try:
            resp = self._get(self.config.search_endpoint)
            if resp.status_code != 200:
                self.logger.error(
                    f"[{self.config.state_code}] Failed to load ASP form: {resp.status_code}"
                )
                return []
        except Exception as e:
            self.logger.error(f"[{self.config.state_code}] ASP form GET error: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        form_data = self._extract_asp_viewstate(soup)

        suffixes = list(TRADE_SUFFIXES)
        random.shuffle(suffixes)

        for suffix in suffixes:
            if len(records) >= limit:
                break

            try:
                # Build POST data with ViewState
                post_data = dict(form_data)  # copy

                # Common ASP.NET search field names
                for field_name in [
                    "txtSearchName", "txtEntityName", "txtName",
                    "SearchName", "EntityName", "BusinessName",
                    "ctl00$MainContent$txtEntityName",
                    "ctl00$ContentPlaceHolder1$txtEntityName",
                    "ctl00$cphMainContent$txtEntityName",
                ]:
                    post_data[field_name] = suffix

                # Common submit button names
                for btn_name in [
                    "btnSearch", "btnSubmit", "Search",
                    "ctl00$MainContent$btnSearch",
                    "ctl00$ContentPlaceHolder1$btnSearch",
                    "ctl00$cphMainContent$btnSearch",
                ]:
                    post_data[btn_name] = "Search"

                headers = get_browser_headers(referer=self.config.search_endpoint)
                headers["Content-Type"] = "application/x-www-form-urlencoded"

                resp = self._post(
                    self.config.search_endpoint,
                    data=post_data,
                    headers=headers,
                )

                if resp.status_code != 200:
                    continue

                batch = self._parse_html_results(resp.text, max_results=min(limit - len(records), 30))
                for rec in batch:
                    key = (rec.business_name or "").strip().upper()
                    if key and key not in seen_names:
                        seen_names.add(key)
                        records.append(rec)

                # Update ViewState from response for next POST
                new_soup = BeautifulSoup(resp.text, "lxml")
                new_vs = self._extract_asp_viewstate(new_soup)
                if new_vs.get("__VIEWSTATE"):
                    form_data = new_vs

            except Exception as e:
                self.logger.debug(
                    f"[{self.config.state_code}] ASP suffix '{suffix}' error: {e}"
                )
                continue

        self.logger.info(
            f"[{self.config.state_code}] asp_form found {len(records)} records"
        )
        return records[:limit]

    def _extract_asp_viewstate(self, soup: BeautifulSoup) -> dict:
        """Extract __VIEWSTATE and related hidden fields from ASP.NET page."""
        data = {}
        for field_name in [
            "__VIEWSTATE",
            "__VIEWSTATEGENERATOR",
            "__EVENTVALIDATION",
            "__EVENTTARGET",
            "__EVENTARGUMENT",
            "__VIEWSTATEENCRYPTED",
            "__PREVIOUSPAGE",
            "__SCROLLPOSITIONX",
            "__SCROLLPOSITIONY",
        ]:
            el = soup.find("input", {"name": field_name})
            if el:
                data[field_name] = el.get("value", "")
        return data

    # ==================================================================
    # STRATEGY: date_search (by filing date range)
    # ==================================================================

    def _strategy_date_search(self, limit: int) -> List[BusinessRecord]:
        """
        Search by date range (last 30 days). Some SOS portals support this.
        Falls back to suffix_search if no results.
        """
        records = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        params = {
            "StartDate": start_date.strftime("%m/%d/%Y"),
            "EndDate": end_date.strftime("%m/%d/%Y"),
            "dateFrom": start_date.strftime("%Y-%m-%d"),
            "dateTo": end_date.strftime("%Y-%m-%d"),
        }

        try:
            resp = self._get(self.config.search_endpoint, params=params)
            if resp.status_code == 200:
                records = self._parse_html_results(resp.text, limit)
        except Exception as e:
            self.logger.debug(f"[{self.config.state_code}] date_search failed: {e}")

        # Fallback to suffix search if date search yields nothing
        if not records:
            self.logger.info(
                f"[{self.config.state_code}] date_search yielded 0 – falling back to suffix_search"
            )
            records = self._strategy_suffix_search(limit)

        return records

    # ==================================================================
    # STRATEGY: sequential_id (iterate entity/document IDs)
    # ==================================================================

    def _strategy_sequential_id(self, limit: int) -> List[BusinessRecord]:
        """
        Iterate through entity IDs sequentially (current year + incrementing number).
        Falls back to suffix_search if sequential yields nothing.

        Used by: FL, TX, DE, NY, AL, AK, AZ, AR, CT, DC, HI, MD, ME, PR.
        """
        records: List[BusinessRecord] = []
        seen_names = set()

        year = datetime.now().year
        # Try entity IDs in the format: YEAR + 6-digit incrementing number
        # Start from a high number and work backwards to find recent filings
        base_id = year * 1000000
        # Try multiple ranges to increase hit rate
        id_ranges = [
            range(base_id + 999999, base_id + 999899, -1),  # Most recent
            range(base_id + 500000, base_id + 500100),       # Mid-year
            range(base_id + 100000, base_id + 100100),       # Early year
        ]

        for id_range in id_ranges:
            if len(records) >= limit:
                break
            for entity_id in id_range:
                if len(records) >= limit:
                    break
                try:
                    entity_str = str(entity_id)
                    # Try to look up this entity ID on the SOS portal
                    params = {
                        "EntityId": entity_str,
                        "Id": entity_str,
                        "filingNumber": entity_str,
                        "DocumentId": entity_str,
                    }
                    resp = self._get(self.config.search_endpoint, params=params)
                    if resp.status_code != 200:
                        continue

                    batch = self._parse_html_results(resp.text, max_results=5)
                    for rec in batch:
                        key = (rec.business_name or "").strip().upper()
                        if key and key not in seen_names:
                            seen_names.add(key)
                            rec.filing_number = rec.filing_number or entity_str
                            records.append(rec)
                except Exception as e:
                    self.logger.debug(
                        f"[{self.config.state_code}] sequential_id {entity_id} error: {e}"
                    )
                    continue

        # Fallback to suffix search if sequential yields nothing
        if not records:
            self.logger.info(
                f"[{self.config.state_code}] sequential_id yielded 0 – falling back to suffix_search"
            )
            records = self._strategy_suffix_search(limit)

        self.logger.info(
            f"[{self.config.state_code}] sequential_id found {len(records)} records"
        )
        return records[:limit]

    # ==================================================================
    # HELPERS
    # ==================================================================

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize a date string to YYYY-MM-DD."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")

        date_str = date_str.strip()

        # Already in ISO format
        if re.match(r"^\d{4}-\d{2}-\d{2}", date_str):
            return date_str[:10]

        # Common US formats
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y", "%d-%b-%Y", "%b %d, %Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        return datetime.now().strftime("%Y-%m-%d")

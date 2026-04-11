"""
SEC EDGAR Full-Text Search + Submissions API Scraper
=====================================================
Uses the modern SEC EDGAR APIs to discover recently-filed companies
filtered by US state, focusing on Form D (Notice of Exempt Offering)
and Form 1 (Application for Registration) — the two filing types
most commonly associated with new local businesses.

Architecture:
  1. EFTS Full-Text Search API → discover filings by state + form type
  2. data.sec.gov/submissions/CIK##########.json → hydrate company details
  3. Fallback: suffix-broadened search queries

Rate Limits (SEC EDGAR):
  - 10 requests/second max
  - Must include descriptive User-Agent with contact info
"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

from scrapers.base_scraper import BaseScraper, BusinessRecord

logger = logging.getLogger(__name__)

# SEC requires a descriptive User-Agent with a contact email.
_SEC_UA = "LeadGenDashboard/3.0 (lead-gen-app; contact@leadgendashboard.com)"

# Filing types most commonly filed by NEW local businesses.
TARGET_FORM_TYPES = ["D", "D/A", "1", "1-A", "S-1", "10-K"]

# All 52 US jurisdictions
STATES: Dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia", "PR": "Puerto Rico",
}

# Common business search suffixes for broadening EFTS queries
BUSINESS_SUFFIXES = ["LLC", "Inc", "Corp", "Services", "Consulting", "Management", "Holdings"]


class GlobalEdgarScraper(BaseScraper):
    """
    SEC EDGAR scraper for all 50 US states + DC + PR.

    Primary flow:
      1. Hit EFTS full-text search (efts.sec.gov/LATEST/search-index)
         filtered by state + Form D / Form 1 to get accession numbers & CIKs.
      2. For each CIK, hit data.sec.gov/submissions/CIK{cik}.json to
         hydrate company details (address, phone, SIC, EIN, etc.).
      3. Filter results where stateOfIncorporation OR
         addresses.business.stateOrCountry matches target state.

    Fallback: If EFTS returns 0 hits, iterate BUSINESS_SUFFIXES as
    search terms to broaden coverage.
    """

    HEADERS = {
        "User-Agent": _SEC_UA,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }

    def __init__(self):
        super().__init__("SEC EDGAR", "US_ALL", "https://efts.sec.gov")
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        # Track last-processed CIK per state for incremental scraping
        self._last_processed: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_new_businesses(self, limit: int = 5) -> List[BusinessRecord]:
        """Scrape across ALL states. `limit` is per-state."""
        all_records: List[BusinessRecord] = []
        for state_code in STATES:
            try:
                records = self.fetch_for_state(state_code, limit=limit)
                all_records.extend(records)
            except Exception as e:
                logger.error(f"[EDGAR] Error for {state_code}: {e}")
        return all_records

    def fetch_for_state(self, state_code: str, limit: int = 10) -> List[BusinessRecord]:
        """
        Fetch recent SEC filings for a single state.

        Strategy cascade:
          1. EFTS search with Form D + Form 1 for the state
          2. If 0 results, broaden to common suffixes (LLC, Inc, etc.)
          3. For each hit, hydrate via /submissions/ JSON API
        """
        state_code = state_code.upper()
        records: List[BusinessRecord] = []

        # --- Phase 1: EFTS search with target forms ---
        cik_hits = self._efts_search(state_code, forms=TARGET_FORM_TYPES, limit=limit)

        # --- Phase 2: Suffix-broadening fallback ---
        if not cik_hits:
            logger.info(f"[EDGAR][{state_code}] EFTS returned 0 — trying suffix broadening")
            for suffix in BUSINESS_SUFFIXES:
                if len(cik_hits) >= limit:
                    break
                extra = self._efts_search(
                    state_code, query=suffix, forms=None, limit=limit - len(cik_hits)
                )
                for hit in extra:
                    if hit["cik"] not in {h["cik"] for h in cik_hits}:
                        cik_hits.append(hit)

        if not cik_hits:
            logger.warning(f"[EDGAR][{state_code}] No EFTS results even after suffix broadening")
            return []

        # --- Phase 3: Hydrate each CIK via /submissions/ API ---
        seen_ciks = set()
        for hit in cik_hits:
            if len(records) >= limit:
                break
            cik = hit["cik"]
            if cik in seen_ciks:
                continue
            seen_ciks.add(cik)

            company = self._fetch_submission(cik)
            if not company:
                continue

            # Filter: stateOfIncorporation OR businessAddress.state must match
            inc_state = (company.get("stateOfIncorporation") or "").upper()
            biz_state = ""
            biz_addr = company.get("addresses", {}).get("business", {})
            if biz_addr:
                biz_state = (biz_addr.get("stateOrCountry") or "").upper()

            if state_code not in (inc_state, biz_state):
                continue

            record = self._company_to_record(company, hit, state_code)
            if record:
                records.append(record)

            # Be polite — SEC asks for <= 10 req/s
            time.sleep(0.12)

        # Update incremental tracker
        if records:
            self._last_processed[state_code] = records[0].cik or ""

        logger.info(f"[EDGAR][{state_code}] Returning {len(records)} records")
        return records[:limit]

    # ------------------------------------------------------------------
    # EFTS Full-Text Search
    # ------------------------------------------------------------------

    def _efts_search(
        self,
        state_code: str,
        query: str = "*",
        forms: Optional[List[str]] = None,
        limit: int = 20,
    ) -> List[dict]:
        """
        Query efts.sec.gov/LATEST/search-index for filings from a state.

        Returns list of dicts with cik, filing_date, form, and name.
        """
        url = "https://efts.sec.gov/LATEST/search-index"
        params: Dict[str, str] = {
            "q": query,
            "dateRange": "custom",
            "startdt": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "enddt": datetime.now().strftime("%Y-%m-%d"),
            "locationCodes": state_code,
            "from": "0",
            "size": str(min(limit * 2, 100)),
        }
        if forms:
            params["forms"] = ",".join(forms)

        try:
            resp = self.session.get(url, params=params, timeout=20)

            if resp.status_code != 200:
                logger.warning(f"[EFTS][{state_code}] HTTP {resp.status_code}")
                return []

            data = resp.json()
            hits_list = data.get("hits", {}).get("hits", [])
            results = []
            for hit in hits_list:
                src = hit.get("_source", {})
                ciks = src.get("ciks", [])
                if not ciks:
                    continue
                results.append({
                    "cik": str(ciks[0]).zfill(10),
                    "filing_date": src.get("file_date", datetime.now().strftime("%Y-%m-%d")),
                    "form": src.get("form", ""),
                    "name": (src.get("display_names") or ["Unknown"])[0],
                })
            return results

        except Exception as e:
            logger.error(f"[EFTS][{state_code}] Request error: {e}")
            return []

    # ------------------------------------------------------------------
    # Submissions API (data.sec.gov)
    # ------------------------------------------------------------------

    def _fetch_submission(self, cik: str) -> Optional[dict]:
        """
        Fetch company metadata from data.sec.gov/submissions/CIK{cik}.json
        """
        cik_padded = str(cik).zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            logger.debug(f"[SUBMISSIONS] {url} -> HTTP {resp.status_code}")
        except Exception as e:
            logger.debug(f"[SUBMISSIONS] Error fetching {cik}: {e}")

        return None

    # ------------------------------------------------------------------
    # Record conversion
    # ------------------------------------------------------------------

    def _company_to_record(
        self, company: dict, efts_hit: dict, target_state: str
    ) -> Optional[BusinessRecord]:
        """Convert a /submissions/ JSON blob + EFTS hit into a BusinessRecord."""
        try:
            name = company.get("name") or efts_hit.get("name") or "Unknown"
            name = re.sub(r"\s*\(CIK.*?\)", "", name).strip()

            cik = str(company.get("cik", efts_hit.get("cik", ""))).zfill(10)

            # Addresses
            biz_addr = company.get("addresses", {}).get("business", {})
            mail_addr = company.get("addresses", {}).get("mailing", {})

            address_parts = [
                biz_addr.get("street1", ""),
                biz_addr.get("street2", ""),
                biz_addr.get("city", ""),
                biz_addr.get("stateOrCountry", ""),
                biz_addr.get("zipCode", ""),
            ]
            address = ", ".join(p for p in address_parts if p).strip(", ")

            mail_parts = [
                mail_addr.get("street1", ""),
                mail_addr.get("street2", ""),
                mail_addr.get("city", ""),
                mail_addr.get("stateOrCountry", ""),
                mail_addr.get("zipCode", ""),
            ]
            mailing_address = ", ".join(p for p in mail_parts if p).strip(", ")

            phone = biz_addr.get("phone", "")

            sic_code = company.get("sic", "")
            sic_desc = company.get("sicDescription", "")
            ein = company.get("ein", "")
            fiscal_year = company.get("fiscalYearEnd", "")

            filing_date = efts_hit.get("filing_date", datetime.now().strftime("%Y-%m-%d"))
            form_type = efts_hit.get("form", "")

            state_of_inc = (company.get("stateOfIncorporation") or "").upper()

            return BusinessRecord(
                business_name=name,
                filing_date=filing_date,
                state=target_state,
                status="SEC Filing",
                url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=10",
                entity_type=form_type,
                cik=cik,
                ein=ein if ein else None,
                sic_code=str(sic_code) if sic_code else None,
                industry_category=sic_desc or None,
                fiscal_year_end=fiscal_year or None,
                state_of_incorporation=state_of_inc or target_state,
                address=address or None,
                business_address=address or None,
                business_phone=phone or None,
                phone=phone or None,
                mailing_address=mailing_address or None,
            )
        except Exception as e:
            logger.debug(f"[EDGAR] Record conversion error: {e}")
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def get_last_processed(self, state_code: str) -> Optional[str]:
        """Get last processed CIK for incremental scraping."""
        return self._last_processed.get(state_code.upper())

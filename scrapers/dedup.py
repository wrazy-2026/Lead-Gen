"""
Deduplication & Validation Engine
==================================
Ensures data quality across all scrapers by:
 - Deduplicating records by Entity ID (filing_number) or Business Name + State
 - Validating required fields and address formats
 - Normalizing business names (strip suffixes, case, whitespace)
 - Tracking seen entities across scraper runs (in-memory + optional Firestore)
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple

from scrapers.base_scraper import BusinessRecord

logger = logging.getLogger(__name__)


# ============================================================================
# NAME NORMALIZATION
# ============================================================================

# Suffixes to strip for dedup comparison (preserves original name in record)
_ENTITY_SUFFIXES = re.compile(
    r",?\s*\b(LLC|L\.L\.C\.|INC|INCORPORATED|CORP|CORPORATION|"
    r"LTD|LIMITED|LP|L\.P\.|LLP|L\.L\.P\.|CO|COMPANY|"
    r"PC|P\.C\.|PA|P\.A\.|PLLC|P\.L\.L\.C\.)\b\.?$",
    re.IGNORECASE,
)

# Extra whitespace and punctuation
_MULTI_SPACE = re.compile(r"\s+")
_PUNCTUATION = re.compile(r"[.,;:'\"!@#$%^&*()=+\[\]{}<>?/\\|`~]")


def normalize_name(name: str) -> str:
    """
    Normalize a business name for deduplication comparison.
    
    Steps:
      1. Upper-case
      2. Strip entity suffixes (LLC, Inc, etc.)
      3. Remove punctuation
      4. Collapse multiple spaces
      5. Strip whitespace
    """
    if not name:
        return ""
    n = name.upper()
    n = _ENTITY_SUFFIXES.sub("", n)
    n = _PUNCTUATION.sub(" ", n)
    n = _MULTI_SPACE.sub(" ", n)
    return n.strip()


# ============================================================================
# DEDUPLICATION ENGINE
# ============================================================================

class DedupEngine:
    """
    In-memory deduplication engine.
    
    Tracks two keys:
      1. filing_number + state  (primary – most reliable)
      2. normalized_name + state (secondary – catches name variants)
    """

    def __init__(self):
        self._seen_filing_ids: Set[str] = set()   # "FL:L24000123456"
        self._seen_name_keys: Set[str] = set()    # "FL:JOES PLUMBING"
        self._stats = {"total_input": 0, "duplicates_removed": 0, "output": 0}

    def deduplicate(self, records: List[BusinessRecord]) -> List[BusinessRecord]:
        """
        Remove duplicate records.
        
        Priority:
        1. If filing_number is present, use filing_number+state as key.
        2. Otherwise, use normalized_name+state.
        
        Returns only the first occurrence.
        """
        unique: List[BusinessRecord] = []
        self._stats["total_input"] += len(records)

        for rec in records:
            # Primary key: filing_number + state
            if rec.filing_number:
                fid_key = f"{rec.state}:{rec.filing_number}".upper()
                if fid_key in self._seen_filing_ids:
                    self._stats["duplicates_removed"] += 1
                    continue
                self._seen_filing_ids.add(fid_key)

            # Secondary key: normalized name + state
            name_key = f"{rec.state}:{normalize_name(rec.business_name)}"
            if name_key in self._seen_name_keys:
                self._stats["duplicates_removed"] += 1
                continue
            self._seen_name_keys.add(name_key)

            unique.append(rec)

        self._stats["output"] += len(unique)
        return unique

    def is_duplicate(self, rec: BusinessRecord) -> bool:
        """Check if a single record is a duplicate without adding it."""
        if rec.filing_number:
            fid_key = f"{rec.state}:{rec.filing_number}".upper()
            if fid_key in self._seen_filing_ids:
                return True

        name_key = f"{rec.state}:{normalize_name(rec.business_name)}"
        return name_key in self._seen_name_keys

    def add(self, rec: BusinessRecord):
        """Register a record as seen (for incremental scraping)."""
        if rec.filing_number:
            self._seen_filing_ids.add(f"{rec.state}:{rec.filing_number}".upper())
        self._seen_name_keys.add(f"{rec.state}:{normalize_name(rec.business_name)}")

    def reset(self):
        """Clear all tracked entities."""
        self._seen_filing_ids.clear()
        self._seen_name_keys.clear()
        self._stats = {"total_input": 0, "duplicates_removed": 0, "output": 0}

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    @property
    def size(self) -> int:
        return len(self._seen_filing_ids) + len(self._seen_name_keys)


# ============================================================================
# VALIDATION
# ============================================================================

# US state codes (including DC, PR)
_VALID_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR",
}

# Simple US address pattern (number + street name)
_ADDRESS_PATTERN = re.compile(
    r"\d+\s+[A-Za-z]", re.IGNORECASE
)

# US phone pattern
_PHONE_PATTERN = re.compile(
    r"^[\d\s.()+\-]{7,20}$"
)


def validate_record(rec: BusinessRecord) -> Tuple[bool, List[str]]:
    """
    Validate a BusinessRecord's data quality.
    
    Returns (is_valid, list_of_issues).
    A record is 'valid' if it passes all critical checks (name, state).
    Non-critical issues are still listed for logging.
    """
    issues: List[str] = []
    is_valid = True

    # --- Critical checks ---
    if not rec.business_name or len(rec.business_name.strip()) < 2:
        issues.append("Missing or too-short business name")
        is_valid = False

    if not rec.state or rec.state.upper() not in _VALID_STATES:
        issues.append(f"Invalid state code: {rec.state}")
        is_valid = False

    # --- Non-critical checks ---
    if not rec.filing_date:
        issues.append("Missing filing date")

    if rec.address and not _ADDRESS_PATTERN.search(rec.address):
        issues.append(f"Address may be invalid: {rec.address[:50]}")

    if rec.phone and not _PHONE_PATTERN.match(rec.phone):
        issues.append(f"Phone format suspect: {rec.phone}")

    if rec.url and not rec.url.startswith("http"):
        issues.append(f"URL not HTTP: {rec.url[:50]}")

    return is_valid, issues


def validate_and_filter(records: List[BusinessRecord]) -> List[BusinessRecord]:
    """
    Validate a batch of records. Drop invalid ones, log warnings for issues.
    """
    valid_records = []
    dropped = 0

    for rec in records:
        is_valid, issues = validate_record(rec)
        if not is_valid:
            dropped += 1
            logger.debug(f"[VALIDATION] Dropped '{rec.business_name}': {issues}")
            continue
        if issues:
            logger.debug(f"[VALIDATION] Warnings for '{rec.business_name}': {issues}")
        valid_records.append(rec)

    if dropped:
        logger.info(f"[VALIDATION] Dropped {dropped}/{len(records)} invalid records")

    return valid_records

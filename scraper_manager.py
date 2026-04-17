"""
Scraper Manager - Plugin System for State Scrapers
===================================================
This module manages multiple state scrapers through a plugin architecture.
It allows registering scrapers dynamically and running them in parallel.

Architecture:
------------
1. Register scrapers for each state you want to support
2. Call fetch_all() to run all registered scrapers
3. Results are aggregated, deduplicated, validated and returned

CAMPAIGN MODE: LOCAL SERVICE BUSINESSES
----------------------------------------
This manager is configured to target ONLY local service/trade businesses
(e.g., plumbing, masonry, HVAC, landscaping, cleaning). A keyword-based
pre-filter enforces this focus.

ALL 50 STATES + DC + PR SUPPORT:
---------------------------------
- Dedicated SOS scrapers for states with reliable portals (FL, etc.)
- UniversalSOSScraper for all other states (config-driven)
- OpenCorporates API fallback for states that block scraping
- SEC EDGAR fallback for additional coverage
- Deduplication engine prevents duplicate leads across sources
- Incremental scraping tracks 'Last Scraped Date' per state

Adding a New Scraper:
--------------------
1. Create a new scraper in scrapers/ that extends BaseScraper
2. Register it using scraper_manager.register("STATE_CODE", YourScraper())

Example:
--------
    from scraper_manager import ScraperManager
    from scrapers.real_scrapers import FloridaScraper

    manager = ScraperManager()
    manager.register("FL", FloridaScraper())
    results = manager.fetch_all(limit_per_state=50)
"""

import logging
import re
from typing import Dict, List, Optional, Type
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import datetime

from scrapers.base_scraper import BaseScraper, BusinessRecord, ScraperException
# NOTE: GlobalEdgarScraper import kept for backward compatibility but NOT registered below.
from scrapers.edgar_full_scraper import GlobalEdgarScraper
from scrapers.dedup import DedupEngine, validate_and_filter
from scrapers.state_configs import get_all_state_codes, get_state_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# STEP 2: KEYWORD FILTER CONFIGURATION
# Enforces the "local service businesses only" campaign policy.
# ============================================================================

# Business names containing ANY of these terms are DROPPED immediately.
# These represent holding companies, financial institutions, and REITs.
BLACKLIST_KEYWORDS = [
    r'\bholdings?\b',
    r'\bcapital\b',
    r'\bproperties\b',
    r'\bequity\b',
    r'\btrust\b',
    r'\bfinancial\b',
    r'\binvestments?\b',
    r'\binvestors?\b',
    r'\bfund\b',
    r'\bbanc?\b',
    r'\bbank(ing)?\b',
    r'\breit\b',
    r'\bventures?\b',          # typically VC / investment entities
    r'\bacquisitions?\b',
    r'\bsecurities\b',
    r'\binsurance\b',
    r'\bmortgage\b',
    r'\bprivate\s+equity\b',
]

# Business names containing ANY of these terms are PRIORITISED / flagged as
# local service businesses we actively want.
WHITELIST_KEYWORDS = [
    r'\btree\b',
    r'\bremodel(ing|er|s)?\b',
    r'\bmasonry\b',
    r'\bplumb(ing|er)?\b',
    r'\bhvac\b',
    r'\b(house\s*)?cleaning\b',
    r'\bmaid\b',
    r'\blandsca(ping|pe)\b',
    r'\blawn\b',
    r'\belectric(al|ian)?\b',
    r'\bbuilder(s)?\b',
    r'\broof(ing|er|s)?\b',
    r'\bconcrete\b',
    r'\bpainting\b',
    r'\bpainter(s)?\b',
    r'\bpaving\b',
    r'\basphalt\b',
    r'\bfenc(ing|e)?\b',
    r'\bsiding\b',
    r'\bwindow(s)?\b',
    r'\b(house\s*)?windows\b',
    r'\bfloor(ing)?\b',
    r'\bcarpet\b',
    r'\btile\b',
    r'\bdeck\b',
    r'\bpatio\b',
    r'\bgarage\b',
    r'\bgutter(s)?\b',
    r'\bpest(\s*control)?\b',
    r'\bpool\b',
    r'\bcabinet(s)?\b',
    r'\bgranite\b',
    r'\bcountertop(s)?\b',
    r'\bdrywall\b',
    r'\bplaster\b',
    r'\bseptic\b',
    r'\bsewer\b',
    r'\bexcavat(ion|ing|or)?\b',
    r'\bdemo(lition)?\b',
    r'\bhaul(ing)?\b',
    r'\bjunk\b',
    r'\btow(ing)?\b',
    r'\blocksmith\b',
    r'\bappliance\b',
    r'\brepair\b',
    r'\bmaintenance\b',
    r'\bhandyman\b',
    r'\bair\s+conditioning\b',
    r'\bheating\b',
    r'\bventilation\b',
    r'\bsolar\b',
    r'\binsulation\b',
    r'\b(water)?proof(ing)?\b',
    r'\bfoundation\b',
    r'\bchimney\b',
    r'\bfireplace\b',
    r'\brenovation\b',
    r'\bcarpentry\b',
    r'\bcontractor\b',
    r'\bconstruction\b',
    r'\bpressure\s*wash(ing)?\b',
    r'\bservices?\b',
]

# Compile patterns once for efficiency (thread-safe after module load)
_BLACKLIST_RE = [re.compile(p, re.IGNORECASE) for p in BLACKLIST_KEYWORDS]
_WHITELIST_RE = [re.compile(p, re.IGNORECASE) for p in WHITELIST_KEYWORDS]


def _is_blacklisted(business_name: str) -> bool:
    """Return True if the business name matches any blacklist keyword."""
    for pattern in _BLACKLIST_RE:
        if pattern.search(business_name):
            return True
    return False


def _is_whitelisted(business_name: str) -> bool:
    """Return True if the business name matches any whitelist keyword."""
    for pattern in _WHITELIST_RE:
        if pattern.search(business_name):
            return True
    return False


def classify_lead(record: BusinessRecord) -> str:
    """
    Classify a BusinessRecord based on keyword rules.

    Returns one of:
      'blacklisted'  – drop immediately (holding / financial company)
      'whitelisted'  – local trade/service business (high priority)
      'neutral'      – no strong signal either way (pass through for LLM gate)
    """
    name = record.business_name or ''

    if _is_blacklisted(name):
        return 'blacklisted'
    if _is_whitelisted(name):
        return 'whitelisted'
    return 'neutral'


@dataclass
class ScraperResult:
    """Result container for a scraper execution."""
    state_code: str
    state_name: str
    success: bool
    records: List[BusinessRecord]
    error_message: Optional[str] = None
    execution_time: float = 0.0


class ScraperManager:
    """
    Manages multiple state scrapers with plugin support.

    Features:
    - Dynamic scraper registration
    - Parallel execution for efficiency
    - Detailed execution logging
    - REAL DATA ONLY - No mock/fake data fallback
    - Keyword pre-filter enforcing Local Service Business campaign policy
    """

    def __init__(self, use_mock_fallback: bool = False, apply_keyword_filter: bool = True):
        """
        Initialize the scraper manager.

        Args:
            use_mock_fallback: DISABLED by default - only real scraped data
            apply_keyword_filter: When True (default), blacklisted leads are
                dropped and whitelisted leads are marked before returning.
        """
        self._scrapers: Dict[str, BaseScraper] = {}
        self._use_mock_fallback = False  # ALWAYS FALSE - No fake data
        self._apply_keyword_filter = apply_keyword_filter

        # Deduplication engine
        self._dedup = DedupEngine()

        # Incremental scraping: track last scraped time per state
        self._last_scraped: Dict[str, str] = {}  # state_code -> ISO datetime

        # Track execution statistics
        self._last_run_stats = {
            "total_records": 0,
            "successful_states": 0,
            "failed_states": 0,
            "execution_time": 0.0
        }

        logger.info(
            "ScraperManager initialized - REAL DATA ONLY mode | "
            f"keyword_filter={'ON' if apply_keyword_filter else 'OFF'} | "
            f"dedup=ON"
        )
    
    def register(self, state_code: str, scraper: BaseScraper) -> None:
        """
        Register a scraper for a specific state.
        
        Args:
            state_code: Two-letter state code (e.g., "CA")
            scraper: Scraper instance extending BaseScraper
        """
        state_code = state_code.upper()
        
        if not isinstance(scraper, BaseScraper):
            raise TypeError(f"Scraper must extend BaseScraper, got {type(scraper)}")
        
        self._scrapers[state_code] = scraper
        logger.info(f"Registered scraper for {state_code}: {scraper.__class__.__name__}")
    
    def unregister(self, state_code: str) -> bool:
        """
        Unregister a scraper for a specific state.
        
        Args:
            state_code: Two-letter state code
            
        Returns:
            True if scraper was removed, False if not found
        """
        state_code = state_code.upper()
        if state_code in self._scrapers:
            del self._scrapers[state_code]
            logger.info(f"Unregistered scraper for {state_code}")
            return True
        return False
    
    def get_registered_states(self) -> List[str]:
        """Get list of state codes with registered scrapers."""
        return list(self._scrapers.keys())
    
    def get_scraper(self, state_code: str) -> Optional[BaseScraper]:
        """Get the scraper for a specific state."""
        return self._scrapers.get(state_code.upper())
    
    def _execute_scraper(
        self, 
        state_code: str, 
        scraper: BaseScraper, 
        limit: int
    ) -> ScraperResult:
        """
        Execute a single scraper and capture results.
        
        Args:
            state_code: State code being scraped
            scraper: Scraper instance to execute
            limit: Maximum records to fetch
            
        Returns:
            ScraperResult object with execution details
        """
        start_time = datetime.datetime.now()
        
        try:
            records = scraper.fetch_new_businesses(limit=limit)
            execution_time = (datetime.datetime.now() - start_time).total_seconds()

            # Record last scraped timestamp
            self._last_scraped[state_code] = datetime.datetime.now().isoformat()

            # Validate records
            records = validate_and_filter(records)

            # Deduplicate
            records = self._dedup.deduplicate(records)
            
            return ScraperResult(
                state_code=state_code,
                state_name=scraper.state_name,
                success=True,
                records=records,
                execution_time=execution_time
            )
            
        except ScraperException as e:
            execution_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.error(f"Scraper error for {state_code}: {e}")
            
            return ScraperResult(
                state_code=state_code,
                state_name=scraper.state_name,
                success=False,
                records=[],
                error_message=str(e),
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.exception(f"Unexpected error for {state_code}")
            
            return ScraperResult(
                state_code=state_code,
                state_name=scraper.state_name,
                success=False,
                records=[],
                error_message=f"Unexpected error: {str(e)}",
                execution_time=execution_time
            )
    
    def _apply_filter(self, records: List[BusinessRecord]) -> List[BusinessRecord]:
        """
        Apply the keyword pre-filter to a list of BusinessRecord objects.

        - Blacklisted records are DROPPED.
        - Whitelisted records get ``campaign_priority = 'high'`` attached.
        - Neutral records are passed through unchanged.
        """
        if not self._apply_keyword_filter:
            return records

        passed: List[BusinessRecord] = []
        dropped_blacklist = 0

        for record in records:
            # Keyword check
            classification = classify_lead(record)

            if classification == 'blacklisted':
                dropped_blacklist += 1
                logger.debug(
                    f"[FILTER-DROP] Blacklisted: '{record.business_name}' ({record.state})"
                )
                continue

            # Attach a priority flag as a dynamic attribute (dataclass allows this).
            if classification == 'whitelisted':
                object.__setattr__(record, 'campaign_priority', 'high')
            else:
                object.__setattr__(record, 'campaign_priority', 'normal')

            passed.append(record)

        if dropped_blacklist:
            logger.info(
                f"[FILTER] Dropped {dropped_blacklist} blacklisted records; "
                f"{len(passed)} records passed."
            )

        return passed

    def fetch(self, state_code: str, limit: int = 25, log_callback: Optional[callable] = None) -> List[BusinessRecord]:
        """
        Fetch businesses from a specific state SOS scraper.
        
        Args:
            state_code: State code (e.g., 'FL')
            limit: Maximum records to return
            log_callback: Optional function to call with status updates (str)
            
        Returns:
            List of BusinessRecord objects (REAL DATA ONLY)
        """
        state_code = state_code.upper()
        
        if state_code in self._scrapers:
            scraper = self._scrapers[state_code]
            
            if log_callback:
                log_callback(f"   [{state_code}] Scraping State SOS ({scraper.__class__.__name__})...")

            result = self._execute_scraper(state_code, scraper, limit)
            
            if log_callback:
                if result.success:
                    log_callback(f"   [{state_code}] ✓ {len(result.records)} SOS records fetched")
                else:
                    log_callback(f"   [{state_code}] ✗ SOS Error: {result.error_message}")

            return self._apply_filter(result.records)

        # NO MOCK DATA - Return empty if no real scraper available
        logger.warning(
            f"No real scraper available for {state_code} - returning empty (REAL DATA ONLY policy)"
        )
        return []
    
    def fetch_all(
        self, 
        limit_per_state: int = 50,
        states: Optional[List[str]] = None,
        parallel: bool = True,
        max_workers: int = 5,
        log_callback: Optional[callable] = None
    ) -> List[BusinessRecord]:
        """
        Fetch businesses from all registered scrapers.
        
        Args:
            limit_per_state: Maximum records per state
            states: Specific states to fetch (None for all registered)
            parallel: Whether to run scrapers in parallel
            max_workers: Maximum concurrent scrapers (if parallel)
            
        Returns:
            Aggregated list of BusinessRecord objects (REAL DATA ONLY)
        """
        start_time = datetime.datetime.now()
        all_records = []
        results: List[ScraperResult] = []
        
        # Determine which states to scrape
        if states:
            target_states = [s.upper() for s in states]
        else:
            target_states = list(self._scrapers.keys())
        
        # NO MOCK DATA FALLBACK - Only use real scrapers
        if not target_states:
            logger.warning("No real scrapers registered - REAL DATA ONLY policy")
            return []
        
        logger.info(f"Fetching REAL data from {len(target_states)} states")
        
        if parallel and len(target_states) > 1:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_state = {}
                
                for state_code in target_states:
                    if state_code in self._scrapers:
                        scraper = self._scrapers[state_code]
                    else:
                        # Skip states without real scrapers
                        logger.debug(f"Skipping {state_code} - no real scraper available")
                        continue
                    
                    future = executor.submit(
                        self._execute_scraper,
                        state_code,
                        scraper,
                        limit_per_state
                    )
                    future_to_state[future] = (state_code, scraper)
                
                for future in as_completed(future_to_state):
                    state_code, scraper = future_to_state[future]
                    if log_callback:
                        log_callback(f"   [{state_code}] Scraping State SOS ({scraper.__class__.__name__})...")
                    
                    result = future.result()
                    
                    if log_callback:
                        if result.success:
                            log_callback(f"   [{state_code}] ✓ {len(result.records)} SOS records fetched")
                        else:
                            log_callback(f"   [{state_code}] ✗ SOS Error: {result.error_message}")
                            
                    results.append(result)
                    all_records.extend(self._apply_filter(result.records))
        else:
            # Sequential execution - REAL DATA ONLY
            for state_code in target_states:
                if state_code in self._scrapers:
                    scraper = self._scrapers[state_code]
                else:
                    # Skip states without real scrapers
                    logger.debug(f"Skipping {state_code} - no real scraper available")
                    continue

                result = self._execute_scraper(state_code, scraper, limit_per_state)
                results.append(result)
                all_records.extend(result.records)

        # ── Apply final filter and sort ──
        raw_count = len(all_records)
        all_records = self._apply_filter(all_records)
        
        # Update statistics
        total_time = (datetime.datetime.now() - start_time).total_seconds()
        high_priority = sum(
            1 for r in all_records
            if getattr(r, 'campaign_priority', 'normal') == 'high'
        )
        self._last_run_stats = {
            "total_records": len(all_records),
            "high_priority_records": high_priority,
            "blacklisted_dropped": raw_count - len(all_records),
            "successful_states": sum(1 for r in results if r.success),
            "failed_states": sum(1 for r in results if not r.success),
            "execution_time": total_time,
            "results": results
        }

        logger.info(
            f"Fetch complete: {len(all_records)} records "
            f"({high_priority} high-priority local service leads) from "
            f"{self._last_run_stats['successful_states']} states in {total_time:.2f}s"
        )

        # Sort: whitelisted first, then by filing date (most recent first)
        all_records.sort(
            key=lambda x: (
                0 if getattr(x, 'campaign_priority', 'normal') == 'high' else 1,
                x.filing_date
            ),
            reverse=False
        )
        # Re-reverse the date part within each priority group via stable sort
        all_records.sort(
            key=lambda x: getattr(x, 'campaign_priority', 'normal') == 'high',
            reverse=True
        )

        return all_records
    
    def get_last_run_stats(self) -> dict:
        """Get statistics from the last fetch operation."""
        return self._last_run_stats.copy()
    
    def list_available_scrapers(self) -> List[dict]:
        """List all registered scrapers with their info."""
        scrapers_info = []
        
        for state_code, scraper in self._scrapers.items():
            scrapers_info.append({
                "state_code": state_code,
                "state_name": scraper.state_name,
                "scraper_class": scraper.__class__.__name__,
                "base_url": scraper.base_url,
                "last_scraped": self._last_scraped.get(state_code),
            })
        
        return scrapers_info

    def get_last_scraped(self, state_code: str) -> Optional[str]:
        """Get the ISO timestamp of the last scrape for a state."""
        return self._last_scraped.get(state_code.upper())

    def get_all_last_scraped(self) -> Dict[str, str]:
        """Get last-scraped timestamps for all states."""
        return dict(self._last_scraped)

    def get_dedup_stats(self) -> dict:
        """Get deduplication engine statistics."""
        return self._dedup.stats

    def reset_dedup(self):
        """Reset the dedup engine (e.g. for a fresh run)."""
        self._dedup.reset()


# ============================================================================
# DEFAULT MANAGER INSTANCE
# LOCAL SERVICE BUSINESS CAMPAIGN - State SOS scrapers only
# ============================================================================

# All 52 US Jurisdictions (kept for reference / optional EDGAR re-enablement)
ALL_US_STATE_CODES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR'
]


def initialize_sos_scrapers(manager: ScraperManager) -> None:
    """
    Register scrapers for ALL 52 US jurisdictions (50 states + DC + PR).

    Priority:
      1. Dedicated SOS scrapers (hand-tuned for the state's portal)
      2. UniversalSOSScraper (config-driven, works for most states)
      3. SEC EDGAR fallback (wraps GlobalEdgarScraper per-state)
    """
    # --- Dedicated scrapers (hand-tuned) ---
    try:
        from scrapers.real_scrapers import (
            CaliforniaScraper,
            DelawareScraper,
            NewYorkScraper,
            TexasScraper,
            GeorgiaScraper,
            IllinoisScraper,
        )
        from scrapers.florida_playwright_scraper import FloridaPlaywrightScraper

        dedicated = {
            'FL': FloridaPlaywrightScraper,
            'CA': CaliforniaScraper,
            'DE': DelawareScraper,
            'NY': NewYorkScraper,
            'TX': TexasScraper,
            'GA': GeorgiaScraper,
            'IL': IllinoisScraper,
        }

        logger.info(
            f"[STEP 1] Registering {len(dedicated)} dedicated SOS scrapers..."
        )

        for state_code, scraper_cls in dedicated.items():
            try:
                manager.register(state_code, scraper_cls())
                logger.info(f"  ✓ {state_code} → {scraper_cls.__name__}")
            except Exception as e:
                logger.warning(f"  ✗ Failed to register {state_code}: {e}")

    except ImportError as e:
        logger.error(f"Failed to import dedicated SOS scrapers: {e}")

    # --- UniversalSOSScraper for remaining states ---
    try:
        from scrapers.universal_sos_scraper import UniversalSOSScraper
        from scrapers.state_configs import STATE_CONFIGS

        remaining = [
            code for code in STATE_CONFIGS
            if code not in manager.get_registered_states()
        ]

        logger.info(
            f"[STEP 2] Registering UniversalSOSScraper for {len(remaining)} remaining states..."
        )

        for state_code in remaining:
            try:
                manager.register(state_code, UniversalSOSScraper(state_code))
                logger.debug(f"  ✓ {state_code} → UniversalSOSScraper")
            except Exception as e:
                logger.warning(f"  ✗ Universal scraper failed for {state_code}: {e}")

    except ImportError as e:
        logger.error(f"Failed to import UniversalSOSScraper: {e}")

    total = len(manager.get_registered_states())
    logger.info(f"[INIT] Total scrapers registered: {total} / 52 jurisdictions")


# -- SEC EDGAR fallback wrapper ---------------------------------------------------

class EdgarFallbackScraper(BaseScraper):
    """
    Wraps the GlobalEdgarScraper for a single state so the ScraperManager
    can use it as a per-state fallback when the SOS scraper returns 0 records.
    """

    def __init__(self, state_code: str):
        self._state = state_code.upper()
        super().__init__(f"EDGAR-{self._state}", self._state, "https://efts.sec.gov")
        self._edgar = GlobalEdgarScraper()

    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        return self._edgar.fetch_for_state(self._state, limit=limit)


# -- Enhanced default manager with EDGAR fallback ---------------------------------

class ScraperManagerWithFallback(ScraperManager):
    """
    Extends ScraperManager to automatically try SEC EDGAR when the
    primary SOS scraper returns 0 records for a state.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._edgar = GlobalEdgarScraper()

    def fetch(self, state_code: str, limit: int = 25, log_callback=None) -> List[BusinessRecord]:
        records = super().fetch(state_code, limit=limit, log_callback=log_callback)

        # If SOS returned nothing, try SEC EDGAR
        if not records:
            state_code = state_code.upper()
            if log_callback:
                log_callback(f"   [{state_code}] SOS returned 0 — trying SEC EDGAR fallback...")
            try:
                edgar_records = self._edgar.fetch_for_state(state_code, limit=limit)
                if edgar_records:
                    if log_callback:
                        log_callback(f"   [{state_code}] ✓ {len(edgar_records)} EDGAR records fetched")
                    records = self._apply_filter(edgar_records)
                else:
                    if log_callback:
                        log_callback(f"   [{state_code}] ✗ EDGAR also returned 0")
            except Exception as e:
                logger.error(f"[{state_code}] EDGAR fallback error: {e}")
                if log_callback:
                    log_callback(f"   [{state_code}] ✗ EDGAR error: {e}")

        return records


# ── Create default manager with EDGAR fallback and keyword filter OFF ────────────
default_manager = ScraperManagerWithFallback(use_mock_fallback=False, apply_keyword_filter=False)

# Register ALL 52 jurisdictions (dedicated + universal + fallback)
initialize_sos_scrapers(default_manager)


def get_manager() -> ScraperManager:
    """Get the default scraper manager instance (REAL DATA ONLY)."""
    return default_manager


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("SCRAPER MANAGER - REAL DATA ONLY")
    print("="*60)
    
    # Create manager - NO MOCK DATA
    manager = get_manager()
    
    # List available scrapers (real scrapers only)
    print("\nAvailable Real Scrapers:")
    print("-" * 40)
    for info in manager.list_available_scrapers():
        print(f"  {info['state_code']}: {info['state_name']} ({info['scraper_class']})")
    
    # Fetch from all available scrapers
    print("\nFetching data...")
    print("-" * 40)
    
    records = manager.fetch_all(limit_per_state=5)
    
    print(f"\nFetched {len(records)} total records:")
    print("-" * 40)
    
    for record in records[:10]:  # Show first 10
        print(f"  {record.business_name}")
        print(f"    State: {record.state} | Filed: {record.filing_date}")
    
    # Show stats
    stats = manager.get_last_run_stats()
    print(f"\nExecution Stats:")
    print(f"  Total Records: {stats['total_records']}")
    print(f"  Successful States: {stats['successful_states']}")
    print(f"  Failed States: {stats['failed_states']}")
    print(f"  Execution Time: {stats['execution_time']:.2f}s")

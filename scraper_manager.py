"""
Scraper Manager - Plugin System for State Scrapers
===================================================
This module manages multiple state scrapers through a plugin architecture.
It allows registering scrapers dynamically and running them in parallel.

Architecture:
------------
1. Register scrapers for each state you want to support
2. Call fetch_all() to run all registered scrapers
3. Results are aggregated and returned as a list of BusinessRecord objects

Adding a New Scraper:
--------------------
1. Create a new scraper in scrapers/ that extends BaseScraper
2. Register it using scraper_manager.register("STATE_CODE", YourScraper())

Example:
--------
    from scraper_manager import ScraperManager
    from scrapers.california_scraper import CaliforniaScraper
    
    manager = ScraperManager()
    manager.register("CA", CaliforniaScraper())
    results = manager.fetch_all(limit_per_state=50)
"""

import logging
from typing import Dict, List, Optional, Type
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime

from scrapers.base_scraper import BaseScraper, BusinessRecord, ScraperException
from scrapers.edgar_full_scraper import GlobalEdgarScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
    """
    
    def __init__(self, use_mock_fallback: bool = False):
        """
        Initialize the scraper manager.
        
        Args:
            use_mock_fallback: DISABLED by default - only real scraped data
        """
        self._scrapers: Dict[str, BaseScraper] = {}
        self._use_mock_fallback = False  # ALWAYS FALSE - No fake data
        # NOTE: No mock scraper - REAL DATA ONLY
        
        # Track execution statistics
        self._last_run_stats = {
            "total_records": 0,
            "successful_states": 0,
            "failed_states": 0,
            "execution_time": 0.0
        }
        
        logger.info("ScraperManager initialized - REAL DATA ONLY mode")
    
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
        start_time = datetime.now()
        
        try:
            records = scraper.fetch_new_businesses(limit=limit)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return ScraperResult(
                state_code=state_code,
                state_name=scraper.state_name,
                success=True,
                records=records,
                execution_time=execution_time
            )
            
        except ScraperException as e:
            execution_time = (datetime.now() - start_time).total_seconds()
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
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.exception(f"Unexpected error for {state_code}")
            
            return ScraperResult(
                state_code=state_code,
                state_name=scraper.state_name,
                success=False,
                records=[],
                error_message=f"Unexpected error: {str(e)}",
                execution_time=execution_time
            )
    
    def fetch_state(
        self, 
        state_code: str, 
        limit: int = 50
    ) -> List[BusinessRecord]:
        """
        Fetch businesses from a specific state.
        
        Args:
            state_code: Two-letter state code
            limit: Maximum records to fetch
            
        Returns:
            List of BusinessRecord objects
        """
        state_code = state_code.upper()
        
        # Check for registered scraper
        if state_code in self._scrapers:
            scraper = self._scrapers[state_code]
            result = self._execute_scraper(state_code, scraper, limit)
            return result.records
        
        # NO MOCK DATA - Return empty if no real scraper available
        logger.warning(f"No real scraper available for {state_code} - returning empty (REAL DATA ONLY policy)")
        return []
    
    def fetch_all(
        self, 
        limit_per_state: int = 50,
        states: Optional[List[str]] = None,
        parallel: bool = True,
        max_workers: int = 5
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
        start_time = datetime.now()
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
                    future_to_state[future] = state_code
                
                for future in as_completed(future_to_state):
                    result = future.result()
                    results.append(result)
                    all_records.extend(result.records)
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
        
        # Update statistics
        total_time = (datetime.now() - start_time).total_seconds()
        self._last_run_stats = {
            "total_records": len(all_records),
            "successful_states": sum(1 for r in results if r.success),
            "failed_states": sum(1 for r in results if not r.success),
            "execution_time": total_time,
            "results": results
        }
        
        logger.info(
            f"Fetch complete: {len(all_records)} records from "
            f"{self._last_run_stats['successful_states']} states in {total_time:.2f}s"
        )
        
        # Sort by filing date (most recent first)
        all_records.sort(key=lambda x: x.filing_date, reverse=True)
        
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
                "base_url": scraper.base_url
            })
        
        # REAL DATA ONLY - No mock scrapers added
        
        return scrapers_info


# ============================================================================
# DEFAULT MANAGER INSTANCE
# ============================================================================

# All 52 US Jurisdictions (50 States + DC + PR) - for edgar scraper initialization
ALL_US_STATE_CODES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR'
]


def initialize_edgar_scrapers(manager: ScraperManager) -> None:
    """
    Initialize and register EDGAR scrapers for all 50 US states.
    
    Args:
        manager: ScraperManager instance to register scrapers with
        
    Note:
        This function registers StateSpecificEdgarScraper instances for each state.
        Each scraper filters SEC EDGAR results by state of incorporation.
    """
    try:
        from scrapers.real_scrapers import StateSpecificEdgarScraper
        
        logger.info("Initializing EDGAR scrapers for all 50 US states...")
        
        for state_code in ALL_US_STATE_CODES:
            try:
                scraper = StateSpecificEdgarScraper(state_code)
                manager.register(state_code, scraper)
                logger.debug(f"Registered EDGAR scraper for {state_code}")
            except Exception as e:
                logger.warning(f"Failed to register EDGAR scraper for {state_code}: {e}")
        
        registered_states = manager.get_registered_states()
        logger.info(f"Successfully registered {len(registered_states)} state EDGAR scrapers: {', '.join(registered_states)}")
        
    except ImportError:
        logger.error("Failed to import StateSpecificEdgarScraper - EDGAR support unavailable")


# Create a default manager instance - REAL DATA ONLY
default_manager = ScraperManager(use_mock_fallback=False)

# Initialize EDGAR scrapers for all states
initialize_edgar_scrapers(default_manager)

# Register the new Global Scraper
default_manager.register("SEC_GLOBAL", GlobalEdgarScraper())


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
    manager = ScraperManager(use_mock_fallback=False)
    
    # List available scrapers (real scrapers only)
    print("\nAvailable Real Scrapers:")
    print("-" * 40)
    for info in manager.list_available_scrapers():
        print(f"  {info['state_code']}: {info['state_name']} ({info['scraper_class']})")
    
    # Fetch from all available (mock) scrapers
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

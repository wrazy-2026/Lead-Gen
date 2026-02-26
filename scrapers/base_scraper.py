"""
Base Scraper Interface
======================
All state scrapers must implement this abstract base class.
This provides a consistent interface for the scraper manager.

Usage:
------
1. Create a new file in scrapers/ for your state (e.g., california_scraper.py)
2. Inherit from BaseScraper
3. Implement the required methods
4. Register the scraper in scraper_manager.py

Example:
--------
    from scrapers.base_scraper import BaseScraper
    
    class CaliforniaScraper(BaseScraper):
        def __init__(self):
            super().__init__("California", "CA")
        
        def fetch_new_businesses(self, limit=50):
            # Your scraping logic here
            return [...]
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BusinessRecord:
    """
    Standard data structure for a business record.
    All scrapers must return data in this format.
    """
    business_name: str
    filing_date: str  # Format: YYYY-MM-DD
    state: str
    status: str  # e.g., "Active", "Pending", "New Filing"
    url: str  # Direct link to the business filing or search result
    
    # Optional fields for future expansion
    entity_type: Optional[str] = None  # LLC, Corporation, etc.
    filing_number: Optional[str] = None
    registered_agent: Optional[str] = None
    address: Optional[str] = None
    
    # Contact info fields
    phone: Optional[str] = None
    email: Optional[str] = None
    owner_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    domain: Optional[str] = None  # Company website domain
    website: Optional[str] = None  # Full website URL
    ein: Optional[str] = None  # Employer Identification Number
    cik: Optional[str] = None  # SEC CIK number
    
    # SEC EDGAR detailed fields
    sic_code: Optional[str] = None  # Standard Industrial Classification code
    industry_category: Optional[str] = None  # Industry description (e.g., "Petroleum Refining")
    fiscal_year_end: Optional[str] = None  # e.g., "1231" for Dec 31
    state_of_incorporation: Optional[str] = None  # State where incorporated
    sec_file_number: Optional[str] = None  # SEC file number (e.g., "001-35764")
    film_number: Optional[str] = None  # SEC film number
    sec_act: Optional[str] = None  # Securities Act (e.g., "34")
    cf_office: Optional[str] = None  # SEC division (e.g., "01 Energy & Transportation")
    business_address: Optional[str] = None  # Full business address
    business_phone: Optional[str] = None  # Business phone from SEC filing
    mailing_address: Optional[str] = None  # Mailing address if different
    
    def to_dict(self) -> dict:
        """Convert record to dictionary for DataFrame/database storage."""
        return {
            "business_name": self.business_name,
            "filing_date": self.filing_date,
            "state": self.state,
            "status": self.status,
            "url": self.url,
            "entity_type": self.entity_type,
            "filing_number": self.filing_number,
            "registered_agent": self.registered_agent,
            "address": self.address,
            "phone": self.phone,
            "email": self.email,
            "owner_name": self.owner_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "domain": self.domain,
            "website": self.website,
            "ein": self.ein,
            "cik": self.cik,
            "sic_code": self.sic_code,
            "industry_category": self.industry_category,
            "fiscal_year_end": self.fiscal_year_end,
            "state_of_incorporation": self.state_of_incorporation,
            "sec_file_number": self.sec_file_number,
            "film_number": self.film_number,
            "sec_act": self.sec_act,
            "cf_office": self.cf_office,
            "business_address": self.business_address,
            "business_phone": self.business_phone,
            "mailing_address": self.mailing_address,
            "fetched_at": datetime.now().isoformat()
        }


class BaseScraper(ABC):
    """
    Abstract base class for all state scrapers.
    
    Attributes:
        state_name (str): Full name of the state (e.g., "California")
        state_code (str): Two-letter state code (e.g., "CA")
        base_url (str): The main URL for the state's business search
    """
    
    def __init__(self, state_name: str, state_code: str, base_url: str = ""):
        self.state_name = state_name
        self.state_code = state_code
        self.base_url = base_url
        self.logger = logging.getLogger(f"{__name__}.{state_code}")
    
    @abstractmethod
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """
        Fetch newly registered businesses from the state's SOS website.
        
        Args:
            limit: Maximum number of records to fetch
            
        Returns:
            List of BusinessRecord objects
            
        Raises:
            ScraperException: If scraping fails
        """
        pass
    
    def validate_record(self, record: BusinessRecord) -> bool:
        """
        Validate that a business record has required fields.
        
        Args:
            record: BusinessRecord to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = [
            record.business_name,
            record.filing_date,
            record.state,
            record.status,
            record.url
        ]
        return all(field is not None and str(field).strip() for field in required_fields)
    
    def get_info(self) -> dict:
        """Return scraper metadata."""
        return {
            "state_name": self.state_name,
            "state_code": self.state_code,
            "base_url": self.base_url,
            "scraper_class": self.__class__.__name__
        }
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(state={self.state_code})>"


class ScraperException(Exception):
    """Custom exception for scraper-related errors."""
    
    def __init__(self, state_code: str, message: str, original_exception: Exception = None):
        self.state_code = state_code
        self.message = message
        self.original_exception = original_exception
        super().__init__(f"[{state_code}] {message}")


class RateLimitException(ScraperException):
    """Exception raised when rate-limited by a website."""
    pass


class CaptchaException(ScraperException):
    """Exception raised when CAPTCHA is encountered."""
    pass


class DataParsingException(ScraperException):
    """Exception raised when data parsing fails."""
    pass

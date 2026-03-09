"""
Real State Business Scrapers
=============================
Actual web scrapers for state Secretary of State business filing databases.

IMPORTANT NOTES:
----------------
1. These scrapers target public government websites
2. Many state sites use anti-bot measures (CAPTCHAs, JavaScript rendering)
3. Always respect robots.txt and rate limiting
4. Data available varies by state - typically just basic filing info
5. For production use, consider official data subscriptions

Supported States:
- Florida (FL) - Sunbiz.org - Most scraper-friendly
- California (CA) - Limited access
- Delaware (DE) - Limited access  
- New York (NY) - Limited access
- Texas (TX) - Limited access
"""

import re
import os
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Optional
import time
import random
from serper_service import detect_business_category

from scrapers.base_scraper import BaseScraper, BusinessRecord, ScraperException

logger = logging.getLogger(__name__)

# User agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]


class RealScraperBase(BaseScraper):
    """Base class for real state scrapers with common functionality."""
    
    def __init__(self, state_name: str, state_code: str, base_url: str):
        super().__init__(state_name, state_code, base_url)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        self.delay = 2.0  # Seconds between requests
    
    def _make_request(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """Make a rate-limited request."""
        time.sleep(self.delay * random.uniform(0.8, 1.2))
        
        try:
            if method.upper() == 'POST':
                response = self.session.post(url, timeout=30, **kwargs)
            else:
                response = self.session.get(url, timeout=30, **kwargs)
            
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return None
    
    def _parse_date(self, date_str: str) -> str:
        """Parse various date formats to YYYY-MM-DD."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")
        
        date_str = date_str.strip()
        
        formats = [
            "%m/%d/%Y",
            "%Y-%m-%d",
            "%m-%d-%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d-%b-%Y",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        return datetime.now().strftime("%Y-%m-%d")


class FloridaScraper(RealScraperBase):
    """
    Florida Sunbiz.org Scraper
    
    Florida's Sunbiz is one of the more accessible state business databases.
    This scraper attempts to get recent filings from their search system.
    """
    
    SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults"
    DETAIL_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResultDetail"
    
    def __init__(self):
        super().__init__(
            "Florida",
            "FL", 
            "https://search.sunbiz.org"
        )
    
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """
        Fetch recently filed businesses from Florida Sunbiz.
        
        Note: Sunbiz doesn't have a "recent filings" page, so we search
        for common business name patterns and filter by recent dates.
        """
        self.logger.info(f"Fetching up to {limit} businesses from Florida Sunbiz")
        records = []
        
        # Search for common business name prefixes to find new filings
        search_terms = ['A', 'B', 'C', 'D', 'E', 'THE', 'NEW', 'FIRST', 'AMERICAN']
        
        for term in search_terms:
            if len(records) >= limit:
                break
            
            try:
                # Search by entity name
                search_url = f"{self.SEARCH_URL}?searchNameOrder={term}&searchTypeId=ALLENT"
                response = self._make_request(search_url)
                
                if not response:
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Find result rows
                rows = soup.select('table.searchResultTable tbody tr')
                
                for row in rows[:min(10, limit - len(records))]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) >= 4:
                            name_link = cells[0].find('a')
                            if name_link:
                                record = BusinessRecord(
                                    business_name=name_link.text.strip(),
                                    filing_date=self._parse_date(cells[1].text.strip()),
                                    state="FL",
                                    status=cells[2].text.strip() if len(cells) > 2 else "Active",
                                    url=f"{self.base_url}{name_link.get('href', '')}",
                                    entity_type=cells[3].text.strip() if len(cells) > 3 else None,
                                    filing_number=name_link.get('href', '').split('=')[-1] if name_link.get('href') else None
                                )
                                records.append(record)
                    except Exception as e:
                        self.logger.debug(f"Error parsing row: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error searching with term '{term}': {e}")
                continue
        
        self.logger.info(f"Found {len(records)} businesses from Florida")
    
        # Fetch details for each record (address, etc.) if not too many
        # For Florida, we have to visit each detail page
        for i, record in enumerate(records):
            try:
                self.logger.info(f"[{i+1}/{len(records)}] Fetching details for {record.business_name}")
                time.sleep(0.5)  # Be polite to Sunbiz
                details = self._fetch_details(record.url)
                record.address = details.get('address')
                record.business_address = details.get('address')
                record.status = details.get('status', record.status)
                record.entity_type = details.get('entity_type', record.entity_type)
                # Florida Sunbiz doesn't usually have phone numbers, but we'll check
                record.phone = details.get('phone')
                record.business_phone = details.get('phone')
                # Ensure state is set
                record.state = "FL"
            except Exception as e:
                self.logger.debug(f"Error fetching details for {record.business_name}: {e}")
                
        return records[:limit]

    def _fetch_details(self, detail_url: str) -> dict:
        """Fetch details from Sunbiz detail page."""
        details = {}
        response = self._make_request(detail_url)
        if not response:
            return details
            
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Status
        status_label = soup.find('label', string=re.compile('Status', re.I))
        if status_label:
            status_span = status_label.find_next('span')
            if status_span:
                details['status'] = status_span.text.strip()
                
        # Principal Address
        addr_label = soup.find('label', string=re.compile('Principal Address', re.I))
        if addr_label:
            addr_span = addr_label.find_next('span')
            if addr_span:
                # Address parts are often in divs
                addr_text = addr_span.get_text(separator=', ').strip()
                # Clean up extra commas
                addr_text = re.sub(r',\s*,', ',', addr_text).strip(', ')
                details['address'] = addr_text
                
        # Entity Type
        type_div = soup.select_one('.searchResultDetail p:nth-of-type(1)')
        if type_div:
            details['entity_type'] = type_div.text.strip()
            
        # Note: Phone is almost never on Sunbiz
        
        return details
    def is_available(self) -> bool:
        """Check if Sunbiz is accessible."""
        try:
            response = self._make_request(self.base_url)
            return response is not None and response.status_code == 200
        except:
            return False


class CaliforniaScraper(RealScraperBase):
    """
    California bizfile Scraper
    
    Note: California's business search requires JavaScript and is harder to scrape.
    This provides limited functionality.
    """
    
    def __init__(self):
        super().__init__(
            "California",
            "CA",
            "https://bizfileonline.sos.ca.gov"
        )
    
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """
        Attempt to fetch businesses from California.
        
        Note: CA's site uses heavy JavaScript - limited results expected.
        """
        self.logger.info(f"Attempting to fetch from California (limited - JS required)")
        records = []
        
        # California's main search is JavaScript-based
        # We can try to access their API endpoints directly
        try:
            # Try the search API
            api_url = f"{self.base_url}/api/Records/businesssearch"
            
            # This typically requires session cookies from the JS frontend
            response = self._make_request(
                api_url,
                method='POST',
                json={
                    "SEARCH_VALUE": "A",
                    "FILING_TYPE": {"value": "ALL"},
                    "STATUS": {"value": "ALL"}
                }
            )
            
            if response and response.status_code == 200:
                try:
                    data = response.json()
                    for item in data.get('rows', [])[:limit]:
                        record = BusinessRecord(
                            business_name=item.get('ENTITY_NAME', 'Unknown'),
                            filing_date=self._parse_date(item.get('FILING_DATE', '')),
                            state="CA",
                            status=item.get('STATUS', 'Active'),
                            url=f"{self.base_url}/search/business/{item.get('ENTITY_NUM', '')}",
                            entity_type=item.get('ENTITY_TYPE'),
                            filing_number=item.get('ENTITY_NUM')
                        )
                        records.append(record)
                except:
                    pass
        except Exception as e:
            self.logger.warning(f"California scraping limited: {e}")
        
        if not records:
            self.logger.warning("California requires JavaScript - consider using Selenium for full access")
        
        return records[:limit]
    
    def is_available(self) -> bool:
        try:
            response = self._make_request(self.base_url)
            return response is not None
        except:
            return False


class DelawareScraper(RealScraperBase):
    """
    Delaware ICIS Scraper
    
    Delaware is popular for business incorporation but has limited public search.
    """
    
    def __init__(self):
        super().__init__(
            "Delaware",
            "DE",
            "https://icis.corp.delaware.gov"
        )
    
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """
        Attempt to fetch from Delaware ICIS.
        
        Note: Delaware's system is session-based and harder to scrape.
        """
        self.logger.info(f"Attempting to fetch from Delaware ICIS")
        records = []
        
        try:
            # Try to access the entity search
            search_url = f"{self.base_url}/ecorp/entitysearch/namesearch.aspx"
            response = self._make_request(search_url)
            
            if response:
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Delaware uses ASP.NET ViewState - need to extract for POST
                viewstate = soup.find('input', {'name': '__VIEWSTATE'})
                if viewstate:
                    # Attempt a search
                    form_data = {
                        '__VIEWSTATE': viewstate.get('value', ''),
                        'txtName': 'A',
                        'btnSearch': 'Search'
                    }
                    
                    search_response = self._make_request(search_url, method='POST', data=form_data)
                    if search_response:
                        result_soup = BeautifulSoup(search_response.text, 'lxml')
                        # Parse results...
                        rows = result_soup.select('table#MainContent_gvSearchResults tr')
                        
                        for row in rows[1:limit+1]:  # Skip header
                            cells = row.find_all('td')
                            if len(cells) >= 3:
                                link = cells[0].find('a')
                                if link:
                                    record = BusinessRecord(
                                        business_name=link.text.strip(),
                                        filing_date=datetime.now().strftime("%Y-%m-%d"),
                                        state="DE",
                                        status="Active",
                                        url=f"{self.base_url}{link.get('href', '')}",
                                        filing_number=cells[1].text.strip() if len(cells) > 1 else None
                                    )
                                    records.append(record)
        except Exception as e:
            self.logger.warning(f"Delaware scraping error: {e}")
        
        return records[:limit]
    
    def is_available(self) -> bool:
        try:
            response = self._make_request(self.base_url)
            return response is not None
        except:
            return False


class NewYorkScraper(RealScraperBase):
    """
    New York DOS Scraper
    """
    
    def __init__(self):
        super().__init__(
            "New York",
            "NY",
            "https://apps.dos.ny.gov"
        )
    
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """Fetch from New York Department of State."""
        self.logger.info(f"Attempting to fetch from New York DOS")
        records = []
        
        try:
            search_url = f"{self.base_url}/publicInquiry/"
            response = self._make_request(search_url)
            
            if response:
                # NY also uses JavaScript-heavy interface
                self.logger.warning("NY DOS requires JavaScript - limited results")
        except Exception as e:
            self.logger.warning(f"NY scraping error: {e}")
        
        return records[:limit]
    
    def is_available(self) -> bool:
        try:
            response = self._make_request(self.base_url)
            return response is not None
        except:
            return False


class TexasScraper(RealScraperBase):
    """
    Texas Comptroller Scraper
    """
    
    def __init__(self):
        super().__init__(
            "Texas",
            "TX",
            "https://mycpa.cpa.state.tx.us"
        )
    
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """Fetch from Texas Comptroller."""
        self.logger.info(f"Attempting to fetch from Texas")
        records = []
        
        try:
            # Texas Secretary of State is a different site
            sos_url = "https://direct.sos.state.tx.us/acct/acct-login.asp"
            response = self._make_request(sos_url)
            
            if response:
                # Texas requires account login for detailed searches
                self.logger.warning("Texas SOS requires authentication for detailed access")
        except Exception as e:
            self.logger.warning(f"TX scraping error: {e}")
        
        return records[:limit]
    
    def is_available(self) -> bool:
        return False  # Texas requires login


# ============================================================================
# OpenCorporates API (Free tier available - NO API KEY REQUIRED)
# ============================================================================

class OpenCorporatesScraper(BaseScraper):
    """
    OpenCorporates API Scraper
    
    OpenCorporates aggregates business data from around the world.
    Requires API key for most queries.
    
    API: https://api.opencorporates.com/
    Get API key: https://opencorporates.com/api_accounts/new
    """
    
    API_BASE = "https://api.opencorporates.com/v0.4"
    
    def __init__(self, api_key: str = None):
        super().__init__("OpenCorporates", "ALL", self.API_BASE)
        # Check for API key from parameter or environment
        self.api_key = api_key or os.environ.get('OPENCORPORATES_API_KEY')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
    
    def fetch_new_businesses(self, limit: int = 50, jurisdiction: str = "us_de") -> List[BusinessRecord]:
        """
        Fetch recently incorporated companies from OpenCorporates.
        
        Args:
            limit: Maximum number of results
            jurisdiction: Country/state code (e.g., 'us_de' for Delaware, 'us_fl' for Florida)
        """
        self.logger.info(f"Fetching from OpenCorporates (jurisdiction: {jurisdiction})")
        records = []
        
        try:
            # Search for recently created companies
            params = {
                'jurisdiction_code': jurisdiction,
                'order': 'incorporation_date desc',
                'per_page': min(limit, 30),  # Free tier limit
                'inactive': 'false',
            }
            
            if self.api_key:
                params['api_token'] = self.api_key
            
            url = f"{self.API_BASE}/companies/search"
            self.logger.info(f"Requesting: {url} with params: {params}")
            
            response = self.session.get(url, params=params, timeout=30)
            self.logger.info(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                companies = data.get('results', {}).get('companies', [])
                self.logger.info(f"API returned {len(companies)} companies")
                
                for company in companies:
                    comp = company.get('company', {})
                    
                    # Map jurisdiction to state code
                    jur = comp.get('jurisdiction_code', '')
                    if jur.startswith('us_'):
                        state = jur.replace('us_', '').upper()
                    else:
                        state = jur.upper()[:2]
                    
                    inc_date = comp.get('incorporation_date')
                    if not inc_date:
                        inc_date = datetime.now().strftime("%Y-%m-%d")
                    
                    record = BusinessRecord(
                        business_name=comp.get('name', 'Unknown'),
                        filing_date=inc_date,
                        state=state,
                        status=comp.get('current_status', 'Active'),
                        url=comp.get('opencorporates_url', ''),
                        entity_type=comp.get('company_type'),
                        filing_number=comp.get('company_number')
                    )
                    records.append(record)
                    
                self.logger.info(f"Parsed {len(records)} companies from OpenCorporates")
            elif response.status_code == 401:
                self.logger.error("OpenCorporates requires API key. Set OPENCORPORATES_API_KEY environment variable.")
                raise ValueError("OpenCorporates requires an API key. Get one at https://opencorporates.com/api_accounts/new")
            elif response.status_code == 429:
                self.logger.warning("OpenCorporates: Rate limit exceeded. Wait and try again.")
                raise ValueError("OpenCorporates rate limit exceeded. Try again later.")
            else:
                self.logger.warning(f"OpenCorporates API returned {response.status_code}: {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            self.logger.error("OpenCorporates: Request timed out")
        except Exception as e:
            self.logger.error(f"OpenCorporates error: {e}")
        
        return records[:limit]
    
    def is_available(self) -> bool:
        return True


# ============================================================================
# SEC EDGAR API (Completely FREE - No API key needed)
# ============================================================================

# SIC Code to Industry mapping for company categorization
SIC_CODES = {
    "0100": "Agricultural Production - Crops",
    "0200": "Agricultural Production - Livestock",
    "1000": "Metal Mining",
    "1311": "Crude Petroleum & Natural Gas",
    "1381": "Drilling Oil & Gas Wells",
    "1400": "Mining & Quarrying of Nonmetallic Minerals",
    "1500": "Building Construction",
    "1600": "Heavy Construction",
    "1700": "Construction - Special Trade Contractors",
    "2000": "Food & Kindred Products",
    "2011": "Meat Packing Plants",
    "2080": "Beverages",
    "2300": "Apparel & Other Textile Products",
    "2500": "Furniture & Fixtures",
    "2600": "Paper & Allied Products",
    "2700": "Printing & Publishing",
    "2800": "Chemicals & Allied Products",
    "2834": "Pharmaceutical Preparations",
    "2836": "Biological Products",
    "2911": "Petroleum Refining",
    "3000": "Rubber & Misc. Plastics Products",
    "3300": "Primary Metal Industries",
    "3400": "Fabricated Metal Products",
    "3500": "Industrial Machinery & Equipment",
    "3571": "Electronic Computers",
    "3572": "Computer Storage Devices",
    "3576": "Computer Communications Equipment",
    "3600": "Electronic & Other Electric Equipment",
    "3674": "Semiconductors & Related Devices",
    "3690": "Misc. Electrical Equipment & Supplies",
    "3711": "Motor Vehicles & Car Bodies",
    "3714": "Motor Vehicle Parts & Accessories",
    "3720": "Aircraft & Parts",
    "3800": "Instruments & Related Products",
    "3812": "Navigation & Guidance Systems",
    "3825": "Instruments for Measuring Electricity",
    "3841": "Surgical & Medical Instruments",
    "3845": "Electromedical Equipment",
    "4011": "Railroads",
    "4210": "Trucking & Courier Services",
    "4400": "Water Transportation",
    "4500": "Transportation by Air",
    "4512": "Air Transportation - Scheduled",
    "4700": "Transportation Services",
    "4812": "Radiotelephone Communications",
    "4813": "Telephone Communications",
    "4833": "Television Broadcasting",
    "4841": "Cable & Other Pay TV Services",
    "4899": "Communications Services NEC",
    "4911": "Electric Services",
    "4922": "Natural Gas Transmission",
    "4931": "Electric & Other Services Combined",
    "4950": "Sanitary Services",
    "5000": "Wholesale Trade - Durable Goods",
    "5047": "Medical & Hospital Equipment",
    "5100": "Wholesale Trade - Nondurable Goods",
    "5122": "Drugs & Proprietary Products",
    "5200": "Building Materials & Garden Supplies",
    "5300": "General Merchandise Stores",
    "5400": "Food Stores",
    "5500": "Automotive Dealers & Service Stations",
    "5600": "Apparel & Accessory Stores",
    "5700": "Furniture & Home Furnishings Stores",
    "5812": "Eating Places",
    "5900": "Miscellaneous Retail",
    "5912": "Drug Stores & Proprietary Stores",
    "5940": "Sporting Goods & Bicycle Shops",
    "5961": "Catalog & Mail-Order Houses",
    "6000": "Depository Institutions",
    "6020": "Commercial Banks",
    "6021": "National Commercial Banks",
    "6022": "State Commercial Banks",
    "6035": "Savings Institutions",
    "6141": "Personal Credit Institutions",
    "6153": "Short-Term Business Credit",
    "6159": "Misc. Business Credit Institutions",
    "6162": "Mortgage Bankers & Loan Correspondents",
    "6163": "Loan Brokers",
    "6199": "Finance Services",
    "6200": "Security & Commodity Brokers",
    "6211": "Security Brokers & Dealers",
    "6282": "Investment Advice",
    "6311": "Life Insurance",
    "6321": "Accident & Health Insurance",
    "6324": "Hospital & Medical Service Plans",
    "6331": "Fire, Marine & Casualty Insurance",
    "6351": "Surety Insurance",
    "6361": "Title Insurance",
    "6399": "Insurance Carriers NEC",
    "6411": "Insurance Agents, Brokers & Service",
    "6500": "Real Estate",
    "6510": "Real Estate Operators & Lessors",
    "6512": "Operators of Nonresidential Buildings",
    "6519": "Real Property Lessors NEC",
    "6531": "Real Estate Agents & Managers",
    "6552": "Land Subdividers & Developers",
    "6700": "Holding & Other Investment Offices",
    "6770": "Blank Checks",
    "6792": "Oil Royalty Traders",
    "6794": "Patent Owners & Lessors",
    "6795": "Mineral Royalty Traders",
    "6798": "Real Estate Investment Trusts",
    "6799": "Investors NEC",
    "7000": "Hotels & Other Lodging Places",
    "7011": "Hotels & Motels",
    "7200": "Personal Services",
    "7300": "Business Services",
    "7310": "Advertising",
    "7311": "Advertising Agencies",
    "7320": "Consumer Credit Reporting & Collection",
    "7330": "Mailing, Reproduction & Stenographic",
    "7350": "Misc. Equipment Rental & Leasing",
    "7359": "Equipment Rental & Leasing NEC",
    "7361": "Employment Agencies",
    "7363": "Help Supply Services",
    "7370": "Computer & Data Processing Services",
    "7371": "Computer Programming Services",
    "7372": "Prepackaged Software",
    "7373": "Computer Integrated Systems Design",
    "7374": "Computer Processing & Data Preparation",
    "7375": "Information Retrieval Services",
    "7376": "Computer Facilities Management",
    "7377": "Computer Rental & Leasing",
    "7378": "Computer Maintenance & Repair",
    "7379": "Computer Related Services NEC",
    "7380": "Miscellaneous Business Services",
    "7381": "Detective & Armored Car Services",
    "7384": "Photofinishing Laboratories",
    "7389": "Business Services NEC",
    "7500": "Auto Repair, Services & Parking",
    "7510": "Automotive Rentals, No Drivers",
    "7600": "Miscellaneous Repair Services",
    "7812": "Motion Picture & Video Production",
    "7819": "Services Allied to Motion Pictures",
    "7900": "Amusement & Recreation Services",
    "7990": "Misc. Amusement & Recreation Services",
    "7997": "Membership Sports & Recreation Clubs",
    "8000": "Health Services",
    "8011": "Offices of Doctors of Medicine",
    "8050": "Nursing & Personal Care Facilities",
    "8051": "Skilled Nursing Care Facilities",
    "8060": "Hospitals",
    "8062": "General Medical & Surgical Hospitals",
    "8071": "Medical Laboratories",
    "8082": "Home Health Care Services",
    "8090": "Misc. Health & Allied Services",
    "8093": "Specialty Outpatient Facilities NEC",
    "8111": "Legal Services",
    "8200": "Educational Services",
    "8300": "Social Services",
    "8351": "Child Day Care Services",
    "8700": "Engineering & Management Services",
    "8711": "Engineering Services",
    "8731": "Commercial Physical Research",
    "8734": "Testing Laboratories",
    "8741": "Management Services",
    "8742": "Management Consulting Services",
    "8744": "Facilities Support Services",
    "8900": "Services NEC",
    "9995": "Non-Operating Establishments",
}

def get_industry_from_sic(sic_code: str) -> str:
    """Get industry name from SIC code."""
    if not sic_code:
        return None
    # Clean SIC code
    sic_code = str(sic_code).strip().zfill(4)
    # Try exact match first
    if sic_code in SIC_CODES:
        return SIC_CODES[sic_code]
    # Try first 2 digits for broader category
    broad_sic = sic_code[:2] + "00"
    if broad_sic in SIC_CODES:
        return SIC_CODES[broad_sic]
    return None


class SECEdgarScraper(BaseScraper):
    """
    SEC EDGAR API Scraper
    
    SEC EDGAR provides free API access to all SEC filings.
    We can get recent company registrations (Form S-1, 10-K, etc.)
    
    API: https://www.sec.gov/cgi-bin/browse-edgar
    Rate limit: 10 requests per second (very generous)
    
    Extracts:
    - Company name, address, phone
    - EIN, CIK, State of incorporation
    - SIC code and industry category
    - Filing details (file number, film number, fiscal year end)
    - Business and mailing addresses
    """
    
    API_BASE = "https://efts.sec.gov/LATEST/search-index"
    FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
    COMPANY_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
    
    def __init__(self):
        super().__init__("Registration Agent", "US", self.API_BASE)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LeadGenDashboard/1.0 (contact@example.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
    
    def _extract_company_details(self, cik: str, filing_url: str = None) -> dict:
        """
        Fetch detailed company info from SEC company page and filing index.
        
        Extracts all available data including:
        - EIN, State of Incorporation, Fiscal Year End
        - SIC code and industry category
        - File Number, Film Number, Act
        - CF Office (SEC division)
        - Business and Mailing addresses with phone
        
        Returns dict with all available fields.
        """
        details = {}
        
        try:
            # First, extract data from the filing index page if available
            if filing_url:
                details.update(self._extract_from_filing_index(filing_url))
            
            # Then fetch the company page for additional details
            params = {
                'action': 'getcompany',
                'CIK': cik,
                'type': '',
                'dateb': '',
                'owner': 'include',
                'count': 1
            }
            
            response = self.session.get(self.COMPANY_URL, params=params, timeout=15)
            if response.status_code != 200:
                return details
            
            soup = BeautifulSoup(response.text, 'lxml')
            text = soup.get_text()
            
            # Extract EIN
            ein_match = re.search(r'EIN[.:\s]*(\d{2}[-]?\d{7})', text, re.IGNORECASE)
            if ein_match:
                details['ein'] = ein_match.group(1)
            
            # Extract State of Incorporation
            state_match = re.search(r'State of Inc(?:orp)?[.:\s]*([A-Z]{2})', text, re.IGNORECASE)
            if state_match:
                details['state_of_incorporation'] = state_match.group(1).upper()
                if 'state' not in details:
                    details['state'] = state_match.group(1).upper()
            
            # Extract Fiscal Year End
            fy_match = re.search(r'Fiscal Year End[.:\s]*(\d{4})', text, re.IGNORECASE)
            if fy_match:
                details['fiscal_year_end'] = fy_match.group(1)
            
            # Extract SIC Code and Industry
            sic_match = re.search(r'SIC[.:\s]*(\d{4})\s*[-–]?\s*([A-Za-z\s&,]+?)(?:\n|$|<)', text, re.IGNORECASE)
            if sic_match:
                details['sic_code'] = sic_match.group(1)
                industry_from_page = sic_match.group(2).strip()
                if industry_from_page:
                    details['industry_category'] = industry_from_page
                else:
                    details['industry_category'] = get_industry_from_sic(sic_match.group(1))
            elif 'sic_code' in details and not details.get('industry_category'):
                details['industry_category'] = get_industry_from_sic(details['sic_code'])
            
            # Extract CF Office (SEC Division)
            cf_match = re.search(r'(?:CF Office|Office)[.:\s]*(\d+\s+[A-Za-z\s&]+?)(?:\n|$|<)', text, re.IGNORECASE)
            if cf_match:
                details['cf_office'] = cf_match.group(1).strip()
            
            # Extract Business Address
            business_addr = self._extract_address_block(soup, 'Business Address')
            if business_addr:
                details['business_address'] = business_addr.get('address')
                if business_addr.get('phone'):
                    details['business_phone'] = business_addr['phone']
                    details['phone'] = business_addr['phone']
            
            # Extract Mailing Address
            mailing_addr = self._extract_address_block(soup, 'Mailing Address')
            if mailing_addr:
                details['mailing_address'] = mailing_addr.get('address')
            
            # Fallback address extraction
            if not details.get('business_address'):
                addr = self._extract_fallback_address(soup, text)
                if addr:
                    details['business_address'] = addr.get('address')
                    if addr.get('phone') and not details.get('phone'):
                        details['phone'] = addr['phone']
                        details['business_phone'] = addr['phone']
            
            self.logger.debug(f"Extracted details for CIK {cik}: {list(details.keys())}")
            
        except Exception as e:
            self.logger.debug(f"Error fetching company details: {e}")
        
        return details
    
    def _extract_from_filing_index(self, filing_url: str) -> dict:
        """
        Extract detailed info from a filing index page.
        This is where most of the footer data lives.
        """
        details = {}
        
        try:
            response = self.session.get(filing_url, timeout=15)
            if response.status_code != 200:
                return details
            
            soup = BeautifulSoup(response.text, 'lxml')
            text = soup.get_text()
            
            # Extract File Number (e.g., "001-35764")
            file_match = re.search(r'File No[.:\s]*(\d{3}[-]?\d{5})', text, re.IGNORECASE)
            if file_match:
                details['sec_file_number'] = file_match.group(1)
            
            # Extract Film Number
            film_match = re.search(r'Film No[.:\s]*(\d+)', text, re.IGNORECASE)
            if film_match:
                details['film_number'] = film_match.group(1)
            
            # Extract Act number
            act_match = re.search(r'\bAct[.:\s]*(\d+)\b', text, re.IGNORECASE)
            if act_match:
                details['sec_act'] = act_match.group(1)
            
            # Extract SIC from filing page
            sic_match = re.search(r'SIC[.:\s]*(\d{4})\s*([A-Za-z\s&,]+)?', text, re.IGNORECASE)
            if sic_match:
                details['sic_code'] = sic_match.group(1)
                if sic_match.group(2):
                    details['industry_category'] = sic_match.group(2).strip()
            
            # Extract CF Office from filing
            cf_match = re.search(r'CF Office[.:\s]*(\d+\s*[A-Za-z\s&]+)', text, re.IGNORECASE)
            if cf_match:
                details['cf_office'] = cf_match.group(1).strip()
            
            # Extract State of Incorporation
            state_match = re.search(r'State of Inc(?:orp)?[.:\s]*([A-Z]{2})', text, re.IGNORECASE)
            if state_match:
                details['state_of_incorporation'] = state_match.group(1).upper()
            
            # Extract Fiscal Year End
            fy_match = re.search(r'Fiscal Year End[.:\s]*(\d{4})', text, re.IGNORECASE)
            if fy_match:
                details['fiscal_year_end'] = fy_match.group(1)
            
            # Extract EIN if available
            ein_match = re.search(r'EIN[.:\s]*(\d{2}[-]?\d{7})', text, re.IGNORECASE)
            if ein_match:
                details['ein'] = ein_match.group(1)
            
        except Exception as e:
            self.logger.debug(f"Error extracting from filing index: {e}")
        
        return details
    
    def _extract_address_block(self, soup, address_type: str) -> dict:
        """
        Extract a specific address block (Business or Mailing) from the page.
        Returns dict with 'address' and optionally 'phone'.
        """
        result = {}
        
        try:
            # Look for the address type header
            addr_header = soup.find(text=re.compile(address_type, re.IGNORECASE))
            if not addr_header:
                return result
            
            # Get parent element and look for address content
            parent = addr_header.find_parent()
            if not parent:
                return result
            
            # Get all text after the header
            text = parent.get_text()
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            
            # Filter out the header and other labels
            addr_parts = []
            phone = None
            
            for line in lines:
                # Skip labels
                if any(x in line.lower() for x in ['address', 'mail', 'business']):
                    continue
                
                # Check for phone (usually in format ###-###-#### or (###) ###-####)
                phone_match = re.search(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', line)
                if phone_match and not line.replace('-', '').replace('.', '').startswith('000'):
                    phone = phone_match.group(1)
                    # Don't include phone in address
                    line = re.sub(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', '', line).strip()
                
                # Keep address lines that start with alphanumeric
                if line and re.match(r'^[\d\w]', line):
                    addr_parts.append(line)
            
            if addr_parts:
                result['address'] = ', '.join(addr_parts[:5])  # Max 5 parts
            if phone:
                result['phone'] = phone
            
        except Exception as e:
            pass
        
        return result
    
    def _extract_fallback_address(self, soup, text: str) -> dict:
        """
        Fallback method to extract address from page if structured extraction fails.
        """
        result = {}
        
        try:
            # Look for street address patterns in table cells
            for cell in soup.find_all(['td', 'div', 'p']):
                cell_text = cell.get_text().strip()
                # Look for typical street address format
                if re.match(r'^\d+\s+[A-Z]', cell_text, re.IGNORECASE):
                    if len(cell_text) < 200:  # Reasonable length
                        # Check for phone at end
                        phone_match = re.search(r',?\s*(\d{3}[-.]?\d{3}[-.]?\d{4})\s*$', cell_text)
                        if phone_match:
                            result['phone'] = phone_match.group(1)
                            cell_text = re.sub(r',?\s*\d{3}[-.]?\d{3}[-.]?\d{4}\s*$', '', cell_text)
                        
                        result['address'] = cell_text.replace('\n', ', ').strip()
                        break
        except Exception as e:
            pass
        
        return result
    
    def fetch_new_businesses(self, limit: int = 50, filing_type: str = "10-K", fast_mode: bool = False, company_search: str = None, state_code: str = None) -> List[BusinessRecord]:
        """
        Fetch recent company filings from SEC EDGAR with full details.
        Uses modern JSON API for state-specific searches, legacy Atom for global.
        """
        state_log = f" for state: {state_code}" if state_code else ""
        self.logger.info(f"Fetching from SEC EDGAR (filing type: {filing_type}, fast_mode: {fast_mode}{state_log})")
        records = []
        
        try:
            if state_code:
                # Use modern JSON Search API for reliable state filtering
                search_url = "https://efts.sec.gov/LATEST/search-index"
                # Combine search terms
                q = company_search if company_search else "*"
                
                params = {
                    'q': q,
                    'locationCodes': state_code.upper(),
                    'forms': filing_type or "10-K,S-1,10-Q",
                    'from': 0,
                    'size': min(limit, 100)
                }
                
                # SEC requires a user-agent to avoid 403
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json'
                }
                
                response = self.session.get(search_url, params=params, headers=headers, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    hits = data.get('hits', {}).get('hits', [])
                    
                    for i, hit in enumerate(hits):
                        source = hit.get('_source', {})
                        # Extract company name (usually index 0 in display_names)
                        full_display_name = source.get('display_names', ['Unknown'])[0]
                        # Clean name: remove CIK/Ticker at end
                        company_name = re.sub(r'\s*\(.*?\)\s*\(CIK.*?\)', '', full_display_name).strip()
                        company_name = re.sub(r'\s*\(.*?\)', '', company_name).strip()
                        
                        cik = source.get('ciks', [None])[0]
                        adsh = source.get('adsh') # Accession number
                        filing_date = source.get('file_date', datetime.now().strftime("%Y-%m-%d"))
                        form = source.get('form', 'SEC Filing')
                        
                        # Construct URL
                        url = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{int(cik) if cik else '0'}/{adsh.replace('-', '')}/{source.get('file_name')}" if adsh and cik else ""
                        
                        # Fetch details if not fast_mode
                        details = {}
                        if cik and not fast_mode:
                            time.sleep(0.3)
                            details = self._extract_company_details(cik, url)
                            
                        # Fallback for state/city/address
                        biz_locations = source.get('biz_locations', [])
                        location_str = biz_locations[0] if biz_locations else (state_code.upper() if state_code else "US")
                        
                        # Get industry from SIC code if not already present
                        industry = details.get('industry_category')
                        if not industry and details.get('sic_code'):
                            industry = get_industry_from_sic(details.get('sic_code'))
                        
                        # Determine final state
                        rec_state = details.get('state_of_incorporation', details.get('state'))
                        if not rec_state:
                            rec_state = state_code.upper() if state_code else "US"

                        record = BusinessRecord(
                            business_name=company_name,
                            filing_date=filing_date,
                            state=rec_state,
                            status='SEC Filing',
                            url=url,
                            entity_type=form,
                            address=details.get('business_address') or location_str,
                            phone=details.get('business_phone'),
                            cik=cik,
                            ein=details.get('ein'),
                            sic_code=details.get('sic_code'),
                            industry_category=industry or detect_business_category(company_name),
                            fiscal_year_end=details.get('fiscal_year_end'),
                            state_of_incorporation=details.get('state_of_incorporation') or state_code.upper(),
                            sec_file_number=details.get('sec_file_number'),
                            film_number=details.get('film_number'),
                            sec_act=details.get('sec_act'),
                            cf_office=details.get('cf_office'),
                            business_address=details.get('business_address'),
                            business_phone=details.get('business_phone'),
                            mailing_address=details.get('mailing_address'),
                        )
                        records.append(record)
                        self.logger.info(f"[{i+1}/{len(hits)}] Found {company_name} in {state_code.upper()}")
                else:
                    self.logger.error(f"Modern SEC search failed: {response.status_code}")
                    # Fallback to legacy if modern fails? Handled by caller (D)
            
            else:
                # Use the legacy atom feed for global recent filings (it still works for global)
                params = {
                    'action': 'getcurrent',
                    'type': filing_type if not company_search else '',
                    'company': company_search or '',
                    'count': min(limit, 100),
                    'output': 'atom'
                }
                
                response = self.session.get(self.FILINGS_URL, params=params, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml-xml')
                    entries = soup.find_all('entry')
                    
                    for i, entry in enumerate(entries[:limit]):
                        try:
                            title = entry.find('title')
                            if title:
                                title_text = title.text
                                parts = title_text.split(' - ', 1)
                                if len(parts) > 1:
                                    company_part = parts[1]
                                    company_name = re.sub(r'\s*\(\d+\).*', '', company_part).strip()
                                    cik_match = re.search(r'\((\d+)\)', company_part)
                                    cik = cik_match.group(1) if cik_match else None
                                else:
                                    company_name = title_text
                                    cik = None
                                
                                updated = entry.find('updated')
                                filing_date = updated.text[:10] if updated else datetime.now().strftime("%Y-%m-%d")
                                url = entry.find('link').get('href', '') if entry.find('link') else ''
                                
                                details = {}
                                if cik and not fast_mode:
                                    time.sleep(0.3)
                                    details = self._extract_company_details(cik, url)
                                
                                industry = details.get('industry_category') or detect_business_category(company_name)
                                rec_state = details.get('state_of_incorporation', details.get('state', 'US'))

                                record = BusinessRecord(
                                    business_name=company_name,
                                    filing_date=filing_date,
                                    state=rec_state,
                                    status='SEC Filing',
                                    url=url,
                                    entity_type=filing_type,
                                    address=details.get('business_address', details.get('address')),
                                    phone=details.get('business_phone', details.get('phone')),
                                    cik=cik,
                                    ein=details.get('ein'),
                                    sic_code=details.get('sic_code'),
                                    industry_category=industry,
                                    fiscal_year_end=details.get('fiscal_year_end'),
                                    state_of_incorporation=details.get('state_of_incorporation'),
                                    sec_file_number=details.get('sec_file_number'),
                                    film_number=details.get('film_number'),
                                    sec_act=details.get('sec_act'),
                                    cf_office=details.get('cf_office'),
                                    business_address=details.get('business_address'),
                                    business_phone=details.get('business_phone'),
                                    mailing_address=details.get('mailing_address'),
                                )
                                records.append(record)
                        except Exception as e:
                            continue
                
                self.logger.info(f"Found {len(records)} companies from Global SEC feed")
                
        except Exception as e:
            self.logger.error(f"SEC EDGAR error: {e}")
        
        return records[:int(limit)]
    
    def is_available(self) -> bool:
        try:
            response = self.session.head(self.FILINGS_URL, timeout=10)
            return response.status_code == 200
        except:
            return True  # Assume available, let fetch handle errors


# ============================================================================
# State-Specific EDGAR Scraper
# ============================================================================

class StateSpecificEdgarScraper(SECEdgarScraper):
    """
    State-Specific SEC EDGAR Scraper
    
    Extends SECEdgarScraper to filter results by state of incorporation.
    Fetches EDGAR filings and returns only companies incorporated in the target state.
    
    Usage:
        scraper = StateSpecificEdgarScraper("CA")  # Only CA companies
        records = scraper.fetch_new_businesses(limit=50)
    """
    
    STATE_NAMES = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    }
    
    def __init__(self, state_code: str):
        """
        Initialize scraper for a specific state.
        
        Args:
            state_code: Two-letter state code (e.g., "CA", "NY", "DE")
        """
        # Convert to uppercase and validate
        self.target_state_code = state_code.upper()
        
        if self.target_state_code not in self.STATE_NAMES:
            raise ValueError(f"Invalid state code: {state_code}")
        
        # Initialize parent class
        super().__init__()
        
        # Override state info
        state_name = self.STATE_NAMES[self.target_state_code]
        self.state_name = state_name
        self.state_code = self.target_state_code
    
    def fetch_new_businesses(self, limit: int = 50, filing_type: str = "10-K", fast_mode: bool = False, company_search: str = None) -> List[BusinessRecord]:
        """
        Fetch SEC EDGAR filings for companies incorporated in the target state.
        
        Args:
            limit: Maximum number of results
            filing_type: Type of filing (10-K, S-1, 10-Q, etc.)
            fast_mode: If True, skip detailed company info fetch for speed
            
        Returns:
            List of BusinessRecord objects - only for companies incorporated in target state
        """
        self.logger.info(f"Fetching from SEC EDGAR for {self.state_name} (filing type: {filing_type})")
        
        # Fetch all records from parent class
        all_records = super().fetch_new_businesses(
            limit=limit * 3, 
            filing_type=filing_type, 
            fast_mode=fast_mode, 
            company_search=company_search
        )
        
        # Filter to only companies incorporated in target state
        filtered_records = []
        for record in all_records:
            # Check state_of_incorporation field
            if record.state_of_incorporation and record.state_of_incorporation.upper() == self.target_state_code:
                filtered_records.append(record)
            # Fallback: check state field if state_of_incorporation is missing
            elif not record.state_of_incorporation and record.state.upper() == self.target_state_code:
                filtered_records.append(record)
            
            # Stop early if we have enough
            if len(filtered_records) >= limit:
                break
        
        # Ensure all records have correct state set
        for record in filtered_records:
            if not record.state or record.state == "US":
                record.state = self.target_state_code
            if not record.state_of_incorporation:
                record.state_of_incorporation = self.target_state_code
        
        self.logger.info(f"Filtered to {len(filtered_records)} companies incorporated in {self.state_name}")
        
        return filtered_records[:limit]


# ============================================================================
# Sample Business Data Scraper (For testing when APIs are blocked)
# ============================================================================

class SampleDataScraper(BaseScraper):
    """
    Sample real-looking business data for testing.
    Uses realistic company name patterns and recent dates.
    NOTE: This is sample data, not live scraped data.
    """
    
    def __init__(self, state_code: str = 'FL'):
        super().__init__("Sample Data", state_code, "local://sample")
        self.state = state_code
    
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """Generate sample realistic business records."""
        import hashlib
        
        # Real-sounding business name patterns
        prefixes = ['Summit', 'Coastal', 'Premier', 'Apex', 'Horizon', 'Sterling', 
                    'Vanguard', 'Zenith', 'Catalyst', 'Pinnacle', 'Momentum', 'Skyline']
        
        industries = ['Technologies', 'Solutions', 'Consulting', 'Investments', 
                      'Services', 'Holdings', 'Capital', 'Ventures', 'Group', 
                      'Enterprises', 'Partners', 'Associates', 'Development', 'Advisors']
        
        entity_types = {
            'FL': 'Florida Limited Liability Company',
            'DE': 'Delaware Corporation', 
            'CA': 'California LLC',
            'TX': 'Texas Limited Partnership',
            'NY': 'New York Corporation'
        }
        
        records = []
        base_date = datetime.now()
        
        for i in range(min(limit, 30)):
            # Create deterministic but varied names
            seed = f"{self.state}-{i}-{base_date.strftime('%Y%m')}"
            hash_val = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
            
            prefix_idx = hash_val % len(prefixes)
            industry_idx = (hash_val // 100) % len(industries)
            
            company_name = f"{prefixes[prefix_idx]} {industries[industry_idx]} LLC"
            
            # Recent filing dates
            days_ago = i % 14
            filing_date = (base_date - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            
            # Generate realistic filing number
            filing_num = f"L{base_date.year}{str(hash_val)[:8]}"
            
            record = BusinessRecord(
                business_name=company_name,
                filing_date=filing_date,
                state=self.state,
                status='Active',
                url=f"https://example.com/filing/{filing_num}",
                entity_type=entity_types.get(self.state, 'LLC'),
                filing_number=filing_num
            )
            records.append(record)
        
        self.logger.info(f"Generated {len(records)} sample businesses for {self.state}")
        return records
    
    def is_available(self) -> bool:
        return True


# ============================================================================
# Scraper Registry
# ============================================================================

# States with dedicated scrapers
REAL_SCRAPERS = {
    'FL': FloridaScraper,
    'CA': CaliforniaScraper,
    'DE': DelawareScraper,
    'NY': NewYorkScraper,
    'TX': TexasScraper,
}

# All US state codes for sample data generation
ALL_US_STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]


class StateOpenCorporatesScraper(RealScraperBase):
    """
    OpenCorporates-based scraper for states without dedicated scrapers.
    Uses OpenCorporates API to fetch business data for any US state.
    """
    
    STATE_NAMES = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    }
    
    def __init__(self, state_code: str):
        self.state_code = state_code.upper()
        state_name = self.STATE_NAMES.get(self.state_code, state_code)
        super().__init__(state_name, self.state_code, "https://api.opencorporates.com")
        self.api_key = os.environ.get('OPENCORPORATES_API_KEY')
        
    def fetch_new_businesses(self, limit: int = 50) -> List[BusinessRecord]:
        """Fetch businesses from this state via OpenCorporates."""
        oc = OpenCorporatesScraper(self.api_key)
        jurisdiction = f"us_{self.state_code.lower()}"
        return oc.fetch_new_businesses(limit=limit, jurisdiction=jurisdiction)
    
    def is_available(self) -> bool:
        """OpenCorporates is always available (though may require API key for full access)."""
        return True


def get_real_scraper(state_code: str) -> Optional[RealScraperBase]:
    """
    Get a scraper instance for a state.
    
    REAL DATA ONLY - Uses dedicated state scrapers when available,
    falls back to OpenCorporates for other states.
    """
    state_code = state_code.upper()
    if state_code in REAL_SCRAPERS:
        return REAL_SCRAPERS[state_code]()
    # For states without dedicated scrapers, use OpenCorporates
    if state_code in ALL_US_STATES:
        return StateOpenCorporatesScraper(state_code)
    return None


def get_available_states() -> List[str]:
    """Get list of all US states - all supported via OpenCorporates fallback."""
    return ALL_US_STATES


if __name__ == "__main__":
    # Test the scrapers
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Florida Scraper...")
    fl = FloridaScraper()
    if fl.is_available():
        records = fl.fetch_new_businesses(limit=5)
        for r in records:
            print(f"  - {r.business_name} ({r.filing_date})")
    else:
        print("  Florida not available")
    
    print("\nTesting OpenCorporates...")
    oc = OpenCorporatesScraper()
    records = oc.fetch_new_businesses(limit=5, jurisdiction="us_de")
    for r in records:
        print(f"  - {r.business_name} ({r.state})")

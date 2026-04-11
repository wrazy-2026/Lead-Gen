"""
Business Data Enrichment Module
================================
Attempts to find contact information for businesses using various methods:
1. Google Places lookup (Serper) for category, phone, website
2. LLM classification gate  – STEP 4 (OpenAI or Groq)
3. Apify Skip Trace        – STEP 5 (costs money, gated behind LLM YES)
4. Website scraping for contact info
5. Email pattern guessing based on domain

Note: For production use, consider using paid APIs like:
- Hunter.io (email finding)
- Clearbit (company enrichment)
- Apollo.io (contact database)
- ZoomInfo (business intelligence)
"""

import os
import re
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, List
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin
import time
import random

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# STEP 4: LLM Classification Gate
# ---------------------------------------------------------------------------
# Set ONE of these env-var pairs:
#   OPENAI_API_KEY  → uses OpenAI gpt-4o-mini (cheap, fast)
#   GROQ_API_KEY    → uses Groq llama-3.1-8b-instant (free tier available)
#
# If neither key is set the gate defaults to PASS-THROUGH ("YES") with a
# warning, so the pipeline still works during development.
# ---------------------------------------------------------------------------

_LLM_SYSTEM_PROMPT = (
    "You are a data classifier. Look at this business name, state, and "
    "Google Maps category. Is this a local home service, trade, or local "
    "physical business? Reply exactly with the word YES or NO."
)


def verify_local_service(business_data: dict) -> bool:
    """
    STEP 4 – LLM Classification Gate.

    Calls a lightweight LLM to decide whether a lead represents a local
    service / trade / physical business (YES) or should be discarded (NO).

    Args:
        business_data: Dict containing at minimum:
            - ``business_name``      (str)
            - ``state``              (str, two-letter code)
            - ``places_category``    (str|None, from Serper Places)

    Returns:
        True  → lead is a local service business (send to Apify)
        False → lead is NOT a local service business (discard / flag)
    """
    business_name    = business_data.get("business_name", "").strip()
    state            = business_data.get("state", "").strip()
    places_category  = business_data.get("places_category") or "Unknown"

    if not business_name:
        logger.warning("[LLM-GATE] Empty business_name – defaulting to NO.")
        return False

    user_message = (
        f"Business name: {business_name}\n"
        f"State: {state}\n"
        f"Google Maps category: {places_category}"
    )

    # ── OpenAI path ─────────────────────────────────────────────────────────
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if openai_key:
        try:
            import requests as _req
            response = _req.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {openai_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",          # Cheap, fast model
                    "messages": [
                        {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                        {"role": "user",   "content": user_message},
                    ],
                    "max_tokens": 5,
                    "temperature": 0,
                },
                timeout=10,
            )
            if response.status_code == 200:
                answer = (
                    response.json()
                    .get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    .strip()
                    .upper()
                )
                logger.info(
                    f"[LLM-GATE/OpenAI] '{business_name}' → {answer}"
                )
                return answer == "YES"
            else:
                logger.warning(
                    f"[LLM-GATE/OpenAI] API error {response.status_code}: {response.text[:200]}"
                )
        except Exception as exc:
            logger.warning(f"[LLM-GATE/OpenAI] Request failed: {exc}")

    # ── Groq path ────────────────────────────────────────────────────────────
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if groq_key:
        try:
            import requests as _req
            response = _req.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.1-8b-instant",
                    "messages": [
                        {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                        {"role": "user",   "content": user_message},
                    ],
                    "max_tokens": 5,
                    "temperature": 0,
                },
                timeout=10,
            )
            if response.status_code == 200:
                answer = (
                    response.json()
                    .get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    .strip()
                    .upper()
                )
                logger.info(
                    f"[LLM-GATE/Groq] '{business_name}' → {answer}"
                )
                return answer == "YES"
            else:
                logger.warning(
                    f"[LLM-GATE/Groq] API error {response.status_code}: {response.text[:200]}"
                )
        except Exception as exc:
            logger.warning(f"[LLM-GATE/Groq] Request failed: {exc}")

    # ── No LLM key configured ────────────────────────────────────────────────
    logger.warning(
        "[LLM-GATE] Neither OPENAI_API_KEY nor GROQ_API_KEY is set. "
        "Defaulting to PASS-THROUGH (YES). Set a key for real classification."
    )
    return True  # Pass-through so the pipeline keeps working during dev


# Common user agents for web scraping
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]


@dataclass
class ContactInfo:
    """Container for enriched contact information."""
    email: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    owner_name: Optional[str] = None
    linkedin: Optional[str] = None
    address: Optional[str] = None
    enrichment_source: Optional[str] = None
    confidence_score: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            'email': self.email,
            'phone': self.phone,
            'website': self.website,
            'owner_name': self.owner_name,
            'linkedin': self.linkedin,
            'address': self.address,
            'enrichment_source': self.enrichment_source,
            'confidence_score': self.confidence_score
        }
    
    def is_empty(self) -> bool:
        return not any([self.email, self.phone, self.website, self.owner_name])


class BusinessEnricher:
    """
    Enriches business data with contact information.
    
    Uses multiple strategies:
    1. Website discovery via search
    2. Contact page scraping
    3. Email pattern matching
    """
    
    # Email regex pattern
    EMAIL_PATTERN = re.compile(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        re.IGNORECASE
    )
    
    # Phone regex patterns (US format)
    PHONE_PATTERNS = [
        re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),  # (123) 456-7890
        re.compile(r'\+1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}'),  # +1 123 456 7890
        re.compile(r'1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}'),  # 1-123-456-7890
    ]
    
    # Common contact page paths
    CONTACT_PATHS = [
        '/contact', '/contact-us', '/contact.html', '/contactus',
        '/about', '/about-us', '/about.html',
        '/connect', '/reach-us', '/get-in-touch',
    ]
    
    # Common owner/founder title patterns
    OWNER_PATTERNS = [
        re.compile(r'(?:CEO|Chief Executive Officer|Founder|Owner|President|Principal)[\s:]+([A-Z][a-z]+\s+[A-Z][a-z]+)', re.IGNORECASE),
        re.compile(r'([A-Z][a-z]+\s+[A-Z][a-z]+)[\s,]+(?:CEO|Chief Executive Officer|Founder|Owner|President|Principal)', re.IGNORECASE),
    ]
    
    def __init__(self, timeout: int = 10, delay: float = 1.0):
        """
        Initialize the enricher.
        
        Args:
            timeout: Request timeout in seconds
            delay: Delay between requests to be polite
        """
        self.timeout = timeout
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
    
    def _make_request(self, url: str) -> Optional[requests.Response]:
        """Make a safe HTTP request with error handling."""
        try:
            time.sleep(self.delay * random.uniform(0.5, 1.5))  # Random delay
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.debug(f"Request failed for {url}: {e}")
            return None
    
    def _extract_emails(self, text: str, soup: BeautifulSoup = None) -> List[str]:
        """Extract email addresses from text and HTML."""
        emails = set()
        
        # From plain text
        emails.update(self.EMAIL_PATTERN.findall(text))
        
        # From mailto links
        if soup:
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').split('?')[0]
                    if self.EMAIL_PATTERN.match(email):
                        emails.add(email)
        
        # Filter out common non-business emails
        filtered = []
        exclude_patterns = ['example.com', 'test.com', 'email.com', 'domain.com', 
                          'yourcompany', 'company.com', 'noreply', 'no-reply']
        
        for email in emails:
            if not any(pattern in email.lower() for pattern in exclude_patterns):
                filtered.append(email)
        
        return filtered
    
    def _extract_phones(self, text: str) -> List[str]:
        """Extract phone numbers from text."""
        phones = set()
        
        for pattern in self.PHONE_PATTERNS:
            matches = pattern.findall(text)
            phones.update(matches)
        
        # Clean and normalize
        cleaned = []
        for phone in phones:
            # Remove non-digits except +
            digits = re.sub(r'[^\d+]', '', phone)
            if len(digits) >= 10:
                cleaned.append(phone)
        
        return cleaned[:3]  # Return top 3
    
    def _extract_owner_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Try to extract owner/founder name from page."""
        text = soup.get_text()
        
        for pattern in self.OWNER_PATTERNS:
            match = pattern.search(text)
            if match:
                return match.group(1).strip()
        
        # Check meta tags
        for meta in soup.find_all('meta'):
            if meta.get('name', '').lower() in ['author', 'owner']:
                content = meta.get('content', '')
                if content and len(content) < 50:  # Reasonable name length
                    return content
        
        return None
    
    def _find_contact_page(self, base_url: str, soup: BeautifulSoup) -> Optional[str]:
        """Find the contact page URL."""
        # Check navigation links
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            text = link.get_text().lower()
            
            if 'contact' in href or 'contact' in text:
                return urljoin(base_url, link['href'])
        
        # Try common paths
        for path in self.CONTACT_PATHS:
            test_url = urljoin(base_url, path)
            response = self._make_request(test_url)
            if response and response.status_code == 200:
                return test_url
        
        return None
    
    def _search_for_website(self, business_name: str, state: str) -> Optional[str]:
        """
        Try to find a business website.
        
        Note: For production, use Google Custom Search API or similar.
        This is a basic implementation.
        """
        # Clean business name for search
        clean_name = re.sub(r'\s+(LLC|Inc\.?|Corp\.?|Corporation|LP|LLP|PC)$', '', 
                           business_name, flags=re.IGNORECASE)
        clean_name = clean_name.strip()
        
        # Try direct domain guesses
        domain_guesses = []
        
        # Convert name to potential domain
        name_parts = clean_name.lower().split()
        if len(name_parts) >= 1:
            # Try single word
            domain_guesses.append(f"https://www.{name_parts[0]}.com")
            # Try combined words
            combined = ''.join(name_parts[:3])
            domain_guesses.append(f"https://www.{combined}.com")
            # Try hyphenated
            hyphenated = '-'.join(name_parts[:3])
            domain_guesses.append(f"https://www.{hyphenated}.com")
        
        for domain in domain_guesses:
            response = self._make_request(domain)
            if response and response.status_code == 200:
                return domain
        
        return None
    
    def enrich_business(self, business_name: str, state: str, 
                       existing_url: str = None) -> ContactInfo:
        """
        Attempt to enrich a business with contact information.
        
        Args:
            business_name: Name of the business
            state: State code (e.g., "CA")
            existing_url: Known URL for the business (if any)
            
        Returns:
            ContactInfo object with found data
        """
        info = ContactInfo()
        
        try:
            # Step 1: Find or use website
            website = existing_url
            if not website or 'sunbiz.org' in website or 'dos.ny.gov' in website:
                # These are state filing sites, not business sites
                website = self._search_for_website(business_name, state)
            
            if not website:
                logger.info(f"Could not find website for: {business_name}")
                info.enrichment_source = "no_website_found"
                return info
            
            info.website = website
            
            # Step 2: Fetch homepage
            response = self._make_request(website)
            if not response:
                info.enrichment_source = "website_unreachable"
                return info
            
            soup = BeautifulSoup(response.text, 'lxml')
            homepage_text = soup.get_text()
            
            # Step 3: Extract from homepage
            emails = self._extract_emails(homepage_text, soup)
            phones = self._extract_phones(homepage_text)
            owner = self._extract_owner_name(soup)
            
            # Step 4: Try contact page for more info
            contact_url = self._find_contact_page(website, soup)
            if contact_url and contact_url != website:
                contact_response = self._make_request(contact_url)
                if contact_response:
                    contact_soup = BeautifulSoup(contact_response.text, 'lxml')
                    contact_text = contact_soup.get_text()
                    
                    # Merge findings
                    emails.extend(self._extract_emails(contact_text, contact_soup))
                    phones.extend(self._extract_phones(contact_text))
                    if not owner:
                        owner = self._extract_owner_name(contact_soup)
            
            # Step 5: Compile results
            if emails:
                # Prefer info@ or contact@ emails
                priority_emails = [e for e in emails if any(p in e.lower() for p in ['info@', 'contact@', 'hello@', 'mail@'])]
                info.email = priority_emails[0] if priority_emails else emails[0]
            
            if phones:
                info.phone = phones[0]
            
            info.owner_name = owner
            info.enrichment_source = "web_scraping"
            
            # Calculate confidence
            found_fields = sum([bool(info.email), bool(info.phone), bool(info.owner_name)])
            info.confidence_score = found_fields / 3.0
            
            logger.info(f"Enriched {business_name}: email={info.email}, phone={info.phone}")
            
        except Exception as e:
            logger.error(f"Error enriching {business_name}: {e}")
            info.enrichment_source = f"error: {str(e)}"
        
        return info
    
    def enrich_batch(self, businesses: List[dict], max_count: int = 10) -> List[dict]:
        """
        Enrich a batch of businesses.
        
        Args:
            businesses: List of business dicts with 'business_name' and 'state'
            max_count: Maximum number to enrich (to avoid rate limits)
            
        Returns:
            List of enriched business dicts
        """
        enriched = []
        
        for i, biz in enumerate(businesses[:max_count]):
            logger.info(f"Enriching {i+1}/{min(len(businesses), max_count)}: {biz.get('business_name')}")
            
            info = self.enrich_business(
                biz.get('business_name', ''),
                biz.get('state', ''),
                biz.get('url')
            )
            
            # Merge enrichment data
            enriched_biz = biz.copy()
            enriched_biz.update(info.to_dict())
            enriched.append(enriched_biz)
        
        return enriched


class MockEnricher:
    """
    Generates realistic mock contact data for testing.
    Use this when you don't want to hit real websites.
    """
    
    FIRST_NAMES = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa',
                   'William', 'Jennifer', 'James', 'Amanda', 'Richard', 'Jessica', 'Thomas']
    
    LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
                  'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson', 'Anderson']
    
    EMAIL_DOMAINS = ['gmail.com', 'yahoo.com', 'outlook.com']
    
    def enrich_business(self, business_name: str, state: str, 
                       existing_url: str = None) -> ContactInfo:
        """Generate mock contact info."""
        import random
        
        # Generate owner name
        first = random.choice(self.FIRST_NAMES)
        last = random.choice(self.LAST_NAMES)
        owner_name = f"{first} {last}"
        
        # Generate email
        company_part = business_name.lower().split()[0][:10]
        email_choice = random.choice([
            f"info@{company_part}.com",
            f"contact@{company_part}.com",
            f"{first.lower()}.{last.lower()}@{random.choice(self.EMAIL_DOMAINS)}",
            f"{first.lower()[0]}{last.lower()}@{company_part}.com",
        ])
        
        # Generate phone
        area_codes = {'DE': '302', 'CA': '415', 'TX': '512', 'NY': '212', 'FL': '305'}
        area = area_codes.get(state, '555')
        phone = f"({area}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
        
        # Generate website
        website = f"https://www.{company_part}.com"
        
        return ContactInfo(
            email=email_choice,
            phone=phone,
            website=website,
            owner_name=owner_name,
            enrichment_source="mock_data",
            confidence_score=0.8
        )
    
    def enrich_batch(self, businesses: List[dict], max_count: int = 50) -> List[dict]:
        """Enrich batch with mock data."""
        enriched = []
        
        for biz in businesses[:max_count]:
            info = self.enrich_business(
                biz.get('business_name', ''),
                biz.get('state', ''),
                biz.get('url')
            )
            
            enriched_biz = biz.copy()
            enriched_biz.update(info.to_dict())
            enriched.append(enriched_biz)
        
        return enriched


class ApifySkipTraceEnricher:
    """
    Enricher using Apify Skip Trace API.
    
    This provides comprehensive contact information including:
    - Multiple emails and phone numbers
    - Address history
    - Relatives and associates
    
    Pricing: $7.00 per 1,000 results
    Docs: https://apify.com/one-api/skip-trace
    """
    
    API_BASE = "https://api.apify.com/v2"
    
    def __init__(self, api_token: str = None):
        """
        Initialize with Apify API token.
        
        Args:
            api_token: Apify API token. If not provided, uses APIFY_TOKEN env var.
        """
        import os
        self.api_token = api_token or os.environ.get('APIFY_TOKEN', '')
        self.session = requests.Session()
        
    def _run_actor(self, input_data: dict) -> Optional[str]:
        """Start the skip trace actor and return run ID."""
        url = f"{self.API_BASE}/acts/one-api~skip-trace/runs?token={self.api_token}"
        
        try:
            response = self.session.post(url, json=input_data, timeout=30)
            if response.status_code == 201:
                return response.json()['data']['id']
            else:
                logger.error(f"Failed to start actor: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Error starting skip trace actor: {e}")
        return None
    
    def _check_status(self, run_id: str) -> tuple:
        """Check actor run status. Returns (status, dataset_id)."""
        url = f"{self.API_BASE}/actor-runs/{run_id}?token={self.api_token}"
        
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()['data']
                return data['status'], data.get('defaultDatasetId')
        except Exception as e:
            logger.error(f"Error checking status: {e}")
        return None, None
    
    def _get_results(self, dataset_id: str) -> Optional[List[dict]]:
        """Get results from dataset."""
        url = f"{self.API_BASE}/datasets/{dataset_id}/items?token={self.api_token}"
        
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Error getting results: {e}")
        return None
    
    def _wait_for_results(self, run_id: str, max_wait: int = 180) -> Optional[List[dict]]:
        """Wait for actor to complete and return results."""
        import time
        
        # Shorter intervals for faster response
        intervals = [3, 3, 5, 5, 5, 10, 10, 10, 15, 15, 20, 20, 30, 30]  
        waited = 0
        
        for interval in intervals:
            if waited >= max_wait:
                break
            time.sleep(interval)
            waited += interval
            
            try:
                status, dataset_id = self._check_status(run_id)
                logger.debug(f"Skip trace status: {status} (waited {waited}s)")
                
                if status == "SUCCEEDED" and dataset_id:
                    return self._get_results(dataset_id)
                elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                    logger.error(f"Skip trace run failed: {status}")
                    return None
                elif status == "RUNNING":
                    continue  # Still processing
            except Exception as e:
                logger.warning(f"Error checking status: {e}")
                continue
        
        logger.warning(f"Skip trace timed out after {waited}s")
        return None
    
    def skip_trace_by_name(self, name: str, city: str = None, state: str = None, 
                          max_results: int = 1) -> Optional[List[dict]]:
        """
        Skip trace by person/business name.
        
        Args:
            name: Person or business name
            city: Optional city for more accurate results
            state: Optional state code (e.g., "FL")
            max_results: Maximum results per name (default 1)
            
        Returns:
            List of skip trace results or None
        """
        # Build search query - API format: "Name; City, State"
        if city and state:
            query = f"{name}; {city}, {state}"
        elif state:
            query = f"{name}; {state}"
        else:
            query = name
        
        input_data = {
            "name": [query],
            "max_results": max_results
        }
        
        logger.info(f"Skip tracing: {query}")
        run_id = self._run_actor(input_data)
        
        if run_id:
            return self._wait_for_results(run_id)
        return None
    
    def skip_trace_by_address(self, street: str, city: str, state: str, 
                             zipcode: str = None, max_results: int = 1) -> Optional[List[dict]]:
        """
        Skip trace by address.
        
        Args:
            street: Street address
            city: City name
            state: State code
            zipcode: Optional ZIP code
            max_results: Maximum results
            
        Returns:
            List of skip trace results or None
        """
        # API format: "Street; City, State Zip"
        if zipcode:
            query = f"{street}; {city}, {state} {zipcode}"
        else:
            query = f"{street}; {city}, {state}"
        
        input_data = {
            "street_citystatezip": [query],
            "max_results": max_results
        }
        
        logger.info(f"Skip tracing address: {query}")
        run_id = self._run_actor(input_data)
        
        if run_id:
            return self._wait_for_results(run_id)
        return None
    
    def skip_trace_by_phone(self, phone: str, max_results: int = 1) -> Optional[List[dict]]:
        """
        Skip trace by phone number.
        
        Args:
            phone: Phone number (any format)
            max_results: Maximum results
            
        Returns:
            List of skip trace results or None
        """
        input_data = {
            "phone_number": [phone],
            "max_results": max_results
        }
        
        logger.info(f"Skip tracing phone: {phone}")
        run_id = self._run_actor(input_data)
        
        if run_id:
            return self._wait_for_results(run_id)
        return None
    
    def enrich_business(self, business_name: str, state: str, 
                       city: str = None, address: str = None) -> ContactInfo:
        """
        Enrich a business using skip trace.
        
        Args:
            business_name: Name of the business  
            state: State code (e.g., "CA")
            city: Optional city name
            address: Optional street address
            
        Returns:
            ContactInfo object with found data
        """
        info = ContactInfo()
        results = None
        
        # Try by name first
        results = self.skip_trace_by_name(business_name, city, state)
        
        # If no results and we have address, try that
        if not results and address and city:
            results = self.skip_trace_by_address(address, city, state)
        
        if results and len(results) > 0:
            # Parse the first result
            data = results[0]
            
            # Get primary email (prefer Email-1)
            info.email = data.get('Email-1') or data.get('Email-2')
            
            # Get primary phone
            info.phone = data.get('Phone-1') or data.get('Phone-2')
            
            # Get address
            street = data.get('Street Address', '')
            locality = data.get('Address Locality', '')
            region = data.get('Address Region', '')
            postal = data.get('Postal Code', '')
            if street:
                info.address = f"{street}, {locality}, {region} {postal}".strip(', ')
            
            # Get owner/person name
            first_name = data.get('First Name', '')
            last_name = data.get('Last Name', '')
            if first_name or last_name:
                info.owner_name = f"{first_name} {last_name}".strip()
            
            info.enrichment_source = "apify_skip_trace"
            info.confidence_score = 0.85
            
            logger.info(f"Skip trace found: {info.owner_name}, {info.email}, {info.phone}")
        else:
            info.enrichment_source = "apify_skip_trace_no_results"
            info.confidence_score = 0.0
            logger.info(f"No skip trace results for: {business_name}")
        
        return info
    
    def skip_trace_batch(self, queries: List[str], max_results: int = 1) -> Optional[List[dict]]:
        """
        Skip trace multiple names in a single API call (more efficient).
        
        Args:
            queries: List of search queries (e.g., ["John Smith; Miami, FL", "Jane Doe; NY"])
            max_results: Max results per query
            
        Returns:
            List of all results
        """
        if not queries:
            return None
            
        input_data = {
            "name": queries,
            "max_results": max_results
        }
        
        logger.info(f"Batch skip tracing {len(queries)} queries")
        run_id = self._run_actor(input_data)
        
        if run_id:
            # Longer timeout for batch processing
            max_wait = min(60 + (len(queries) * 15), 300)  # 15s per query, max 5 min
            return self._wait_for_results(run_id, max_wait=max_wait)
        return None
    
    def enrich_batch(self, businesses: List[dict], max_count: int = 50) -> List[dict]:
        """
        Enrich a batch of businesses using skip trace.
        
        Uses batch API call for efficiency - processes all names in one request.
        
        Note: Each lookup costs credits. Budget accordingly.
        At $7.00/1000 results, 50 lookups = ~$0.35
        
        Args:
            businesses: List of business dicts with 'business_name', 'state', etc.
            max_count: Maximum businesses to enrich
            
        Returns:
            List of enriched business dicts
        """
        batch = businesses[:max_count]
        enriched = []
        
        # Build query list for batch processing
        queries = []
        for biz in batch:
            name = biz.get('business_name', '')
            state = biz.get('state', '')
            city = biz.get('city', '')
            
            if city and state:
                query = f"{name}; {city}, {state}"
            elif state:
                query = f"{name}; {state}"
            else:
                query = name
            queries.append(query)
        
        # Make single batch API call
        results = self.skip_trace_batch(queries) or []
        
        # Create a lookup dict from results (match by input query)
        results_map = {}
        for r in results:
            input_given = r.get('Input Given', '').lower()
            results_map[input_given] = r
        
        # Match results back to businesses
        for i, biz in enumerate(batch):
            enriched_biz = biz.copy()
            query = queries[i].lower()
            
            # Try to find matching result
            data = results_map.get(query)
            
            if data:
                # Extract all available fields from Apify response
                enriched_biz['email'] = data.get('Email-1') or data.get('Email-2')
                enriched_biz['phone'] = data.get('Phone-1') or data.get('Phone-2')
                
                # Email fields
                enriched_biz['email_1'] = data.get('Email-1')
                enriched_biz['email_2'] = data.get('Email-2')
                enriched_biz['email_3'] = data.get('Email-3')
                enriched_biz['email_4'] = data.get('Email-4')
                enriched_biz['email_5'] = data.get('Email-5')
                
                # Phone fields
                enriched_biz['phone_1'] = data.get('Phone-1')
                enriched_biz['phone_2'] = data.get('Phone-2')
                
                # Name fields
                first_name = data.get('First Name', '')
                last_name = data.get('Last Name', '')
                enriched_biz['first_name'] = first_name
                enriched_biz['last_name'] = last_name
                if first_name or last_name:
                    enriched_biz['owner_name'] = f"{first_name} {last_name}".strip()
                
                # Age
                enriched_biz['dob'] = data.get('DOB') or data.get('Date of Birth')
                enriched_biz['age'] = data.get('Age')
                
                # Address fields
                street = data.get('Street Address', '')
                locality = data.get('Address Locality', '')
                region = data.get('Address Region', '')
                postal = data.get('Postal Code', '')
                
                enriched_biz['street_address'] = street
                enriched_biz['address_locality'] = locality
                enriched_biz['address_region'] = region
                enriched_biz['postal_code'] = postal
                
                if street:
                    enriched_biz['address'] = f"{street}, {locality}, {region} {postal}".strip(', ')
                
                enriched_biz['enrichment_source'] = 'apify_skip_trace'
                enriched_biz['confidence_score'] = 0.85
            else:
                enriched_biz['enrichment_source'] = 'apify_skip_trace_no_results'
                enriched_biz['confidence_score'] = 0.0
            
            enriched.append(enriched_biz)
        
        logger.info(f"Batch enrichment complete: {len([e for e in enriched if e.get('email')])} with email")
        return enriched


# Singleton instances
_enricher: Optional[BusinessEnricher] = None
_mock_enricher: Optional[MockEnricher] = None
_apify_enricher: Optional[ApifySkipTraceEnricher] = None


def get_enricher(use_mock: bool = False, use_apify: bool = False):
    """
    Get enricher instance.

    Args:
        use_mock: Use mock enricher (for testing)
        use_apify: Use Apify Skip Trace API (costs credits)

    Returns:
        Enricher instance
    """
    global _enricher, _mock_enricher, _apify_enricher

    if use_apify:
        if _apify_enricher is None:
            _apify_enricher = ApifySkipTraceEnricher()
        return _apify_enricher
    elif use_mock:
        if _mock_enricher is None:
            _mock_enricher = MockEnricher()
        return _mock_enricher
    else:
        if _enricher is None:
            _enricher = BusinessEnricher()
        return _enricher


class EnrichmentService:
    """
    Unified enrichment service for pipeline usage.

    STEP 5 – Updated pipeline:
      1. Serper Places lookup  → Google Maps ``category``, ``phone``, ``website``
      2. LLM gate (STEP 4)     → verify_local_service() → YES / NO
      3. [YES only] Apify      → skip-trace for deep contact data
      4. [NO]       Flag lead as "unqualified" and skip Apify (saves money)

    Thread-safety: all external API calls are stateless HTTP requests;
    the Apify/enricher instances are created once and reused safely across
    threads (requests.Session is not thread-safe for *concurrent* calls so
    each public method creates its own short-lived session where needed).
    """

    def __init__(self, api_token: str = None):
        self.apify = ApifySkipTraceEnricher(api_token)

    # -------------------------------------------------------------------------
    # STEP 5: Gated enrichment pipeline
    # -------------------------------------------------------------------------

    def enrich_local_lead(self, data: dict) -> dict:
        """
        Full local-service enrichment pipeline for a single lead.

        Pipeline:
          1. Serper Places  → attach ``places_category``, ``places_phone``,
                              ``places_website`` fields to ``data``
          2. LLM gate       → verify_local_service(data)
             - NO  → return data with ``llm_qualified=False`` and
                     ``status='unqualified'``; Apify NOT called
             - YES → proceed to step 3
          3. Apify skip-trace → deep contact enrichment

        Args:
            data: Dict with at minimum ``business_name``, ``state``.
                  Can also include ``city``, ``address``, ``url``.

        Returns:
            Enriched dict. Check ``data['llm_qualified']`` (bool) and
            ``data['status']`` (``'enriched'`` | ``'unqualified'`` |
            ``'error'``) for pipeline outcome.
        """
        result = data.copy()
        business_name = result.get("business_name", "")
        state         = result.get("state", "")
        city          = result.get("city") or result.get("address_locality") or ""

        # ── Step 1: Google Places via Serper ──────────────────────────────────
        try:
            from serper_service import get_serper_service
            serper = get_serper_service()
            if serper.is_configured() and business_name:
                places_data = serper.search_google_places(business_name, city, state)
                result["places_category"]  = places_data.get("category")
                result["places_phone"]     = places_data.get("phoneNumber")
                result["places_website"]   = places_data.get("website")
                result["places_address"]   = places_data.get("address")
                result["places_rating"]    = places_data.get("rating")

                # Prefer Places phone/website over existing empty fields
                if not result.get("phone") and result.get("places_phone"):
                    result["phone"] = result["places_phone"]
                if not result.get("website") and result.get("places_website"):
                    result["website"] = result["places_website"]
            else:
                result["places_category"] = None
        except Exception as exc:
            logger.warning(f"[PIPELINE] Serper Places failed for '{business_name}': {exc}")
            result["places_category"] = None

        # ── Step 2: LLM Classification Gate ──────────────────────────────────
        try:
            is_local = verify_local_service(result)
        except Exception as exc:
            logger.error(f"[PIPELINE] LLM gate error for '{business_name}': {exc}")
            is_local = True  # Fail-open: uncertain → still enrich

        result["llm_qualified"] = is_local

        if not is_local:
            result["status"] = "unqualified"
            logger.info(
                f"[PIPELINE] '{business_name}' flagged as UNQUALIFIED by LLM gate. "
                "Apify skip-trace SKIPPED."
            )
            return result

        # ── Step 3: Apify Skip-Trace (gated behind YES) ───────────────────────
        logger.info(
            f"[PIPELINE] '{business_name}' QUALIFIED. Running Apify skip-trace..."
        )
        skip_result = self.skip_trace(result)
        if skip_result:
            result.update(skip_result)
            result["status"] = "enriched"
        else:
            result["status"] = "enriched_no_skip_trace"

        return result

    def enrich_local_batch(
        self,
        leads: List[dict],
        max_count: int = 50,
    ) -> List[dict]:
        """
        Gated enrichment pipeline for a list of leads.

        Runs ``enrich_local_lead`` for each lead.  Because each call makes
        independent HTTP requests, this is safe to call from multiple threads
        (e.g. inside a ThreadPoolExecutor) without any additional locking.

        Args:
            leads:     List of lead dicts (same schema as ``enrich_local_lead``)
            max_count: Cap on how many leads to process

        Returns:
            List of enriched dicts (same order as input, unqualified leads
            are included but have ``status='unqualified'``).
        """
        results = []
        batch = leads[:max_count]

        for i, lead in enumerate(batch):
            name = lead.get("business_name", "<unknown>")
            logger.info(
                f"[PIPELINE-BATCH] Processing {i + 1}/{len(batch)}: '{name}'"
            )
            try:
                enriched = self.enrich_local_lead(lead)
            except Exception as exc:
                logger.error(f"[PIPELINE-BATCH] Error enriching '{name}': {exc}")
                enriched = {**lead, "status": "error", "error": str(exc)}
            results.append(enriched)

        qualified   = sum(1 for r in results if r.get("llm_qualified"))
        unqualified = len(results) - qualified
        logger.info(
            f"[PIPELINE-BATCH] Complete: {qualified} qualified, "
            f"{unqualified} unqualified out of {len(results)} leads."
        )
        return results

    # -------------------------------------------------------------------------
    # Legacy single skip-trace (used internally by enrich_local_lead)
    # -------------------------------------------------------------------------

    def skip_trace(self, data: dict) -> Optional[dict]:
        """
        Perform skip trace lookup.

        Args:
            data: Dict with keys: first_name, last_name, state, business_name

        Returns:
            Dict with enriched contact info or None
        """
        first_name    = data.get("first_name", "")
        last_name     = data.get("last_name", "")
        state         = data.get("state", "")
        business_name = data.get("business_name", "")

        # Build name query
        name = f"{first_name} {last_name}".strip()
        if not name and business_name:
            name = business_name

        if not name:
            return None

        try:
            results = self.apify.skip_trace_by_name(name, state=state, max_results=1)
            if results and len(results) > 0:
                result = results[0]

                # Format address
                street   = result.get("Street Address", "")
                locality = result.get("Address Locality", "")
                region   = result.get("Address Region", "")
                postal   = result.get("Postal Code", "")
                address  = (
                    f"{street}, {locality}, {region} {postal}".strip(", ")
                    if street else None
                )

                return {
                    "phone_1":  result.get("Phone-1") or result.get("Phone-2"),
                    "phone_2":  result.get("Phone-2") or result.get("Phone-3"),
                    "email_1":  result.get("Email-1") or result.get("Email-2"),
                    "email_2":  result.get("Email-2") or result.get("Email-3"),
                    "address":  address,
                    "age":      result.get("Age"),
                    "raw":      result,
                }
        except Exception as exc:
            logger.error(f"Skip trace error: {exc}")

        return None


if __name__ == "__main__":
    # Test the enricher
    logging.basicConfig(level=logging.INFO)

    # Test with mock
    mock = MockEnricher()
    result = mock.enrich_business("Summit Technologies LLC", "CA")
    print(f"Mock result: {result.to_dict()}")

    # Test the new LLM gate directly
    print("\n--- LLM Gate Test ---")
    test_cases = [
        {"business_name": "Joe's Plumbing LLC",       "state": "FL", "places_category": "Plumber"},
        {"business_name": "Sunshine Masonry Inc",      "state": "TX", "places_category": "Masonry Contractor"},
        {"business_name": "Apex Capital Holdings LLC", "state": "DE", "places_category": "Financial Services"},
        {"business_name": "Green Tree Service",        "state": "CA", "places_category": "Tree Service"},
    ]
    for tc in test_cases:
        verdict = verify_local_service(tc)
        print(f"  {tc['business_name']:40s} → {'YES ✓' if verdict else 'NO  ✗'}")

    # Test the full gated pipeline (needs APIFY_TOKEN + SERPER_API_KEY in env)
    # service = EnrichmentService()
    # enriched = service.enrich_local_lead({
    #     "business_name": "Mike's HVAC Services",
    #     "state": "FL",
    #     "city": "Tampa",
    # })
    # print(f"Pipeline result: {enriched}")


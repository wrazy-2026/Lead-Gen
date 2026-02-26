"""
Serper Google Search API Integration
=====================================
Uses the Serper API to search Google for business information,
specifically to find owner names and website/domain info.

API: https://google.serper.dev/search
"""

import os
import re
import requests
import logging
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SerperResult:
    """Container for Serper search results."""
    owner_name: Optional[str] = None
    website: Optional[str] = None
    domain: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    description: Optional[str] = None
    business_category: Optional[str] = None  # Detected or searched business category
    confidence: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            'serper_owner_name': self.owner_name,
            'serper_website': self.website,
            'serper_domain': self.domain,
            'phone': self.phone,
            'address': self.address,
            'description': self.description,
            'business_category': self.business_category,
            'serper_confidence': self.confidence
        }


# Business name keyword patterns to detect industry categories
BUSINESS_CATEGORY_PATTERNS = {
    # Technology & Software
    r'\b(software|tech|technologies|digital|app|apps|application|cyber|cloud|saas|ai\b|data|analytics|computing|it\b|solutions)\b': 'Technology & Software',
    r'\b(web|internet|online|e-?commerce|platform)\b': 'Internet & E-Commerce',
    
    # Healthcare & Medical
    r'\b(health|healthcare|medical|clinic|hospital|dental|pharma|pharmaceutical|biotech|therapeutics|surgery|ortho|cardio)\b': 'Healthcare & Medical',
    r'\b(wellness|fitness|gym|yoga|nutrition|diet)\b': 'Health & Wellness',
    
    # Finance & Insurance
    r'\b(bank|banking|financial|finance|capital|investment|fund|asset|wealth|credit|loan|mortgage|insurance)\b': 'Finance & Insurance',
    r'\b(accounting|cpa|tax|bookkeeping|payroll)\b': 'Accounting & Tax Services',
    
    # Legal Services
    r'\b(law|legal|attorney|lawyer|litigation|counsel)\b': 'Legal Services',
    
    # Real Estate & Construction
    r'\b(real\s*estate|realty|property|properties|brokerage|housing|home|homes|apartment)\b': 'Real Estate',
    r'\b(construction|build|builder|building|contractor|roofing|plumbing|electric|hvac|renovation|remodel)\b': 'Construction & Contracting',
    
    # Manufacturing & Industrial
    r'\b(manufacturing|manufacturer|factory|industrial|production|fabricat|machine|equipment)\b': 'Manufacturing',
    r'\b(aerospace|auto|automotive|vehicle|motor|aviation|aircraft)\b': 'Automotive & Aerospace',
    
    # Retail & Consumer
    r'\b(retail|store|shop|shopping|market|mart|outlet|boutique)\b': 'Retail',
    r'\b(food|restaurant|cafe|coffee|catering|bakery|bar|grill|pizza|dining)\b': 'Food & Beverage',
    r'\b(apparel|clothing|fashion|wear|garment|textile)\b': 'Fashion & Apparel',
    
    # Professional Services
    r'\b(consult|consulting|advisory|advisor|agency|management|services|solutions|group|partners)\b': 'Professional Services',
    r'\b(marketing|advertising|media|creative|design|brand|pr|public\s*relations)\b': 'Marketing & Advertising',
    
    # Education & Training
    r'\b(education|school|academy|college|university|training|learning|tutoring|coaching)\b': 'Education & Training',
    
    # Entertainment & Media
    r'\b(entertainment|media|film|movie|music|game|gaming|studio|production|publishing)\b': 'Entertainment & Media',
    
    # Transportation & Logistics
    r'\b(transport|transportation|logistics|shipping|freight|trucking|delivery|courier|supply\s*chain)\b': 'Transportation & Logistics',
    
    # Agriculture & Farming
    r'\b(farm|farming|agricultural|agriculture|crop|livestock|dairy|organic|garden)\b': 'Agriculture',
    
    # Energy & Utilities
    r'\b(energy|power|oil|gas|petroleum|solar|wind|renewable|electric|utility)\b': 'Energy & Utilities',
    
    # Hospitality & Travel
    r'\b(hotel|hospitality|travel|tourism|vacation|resort|lodge|inn|motel)\b': 'Hospitality & Travel',
    
    # Cleaning & Maintenance
    r'\b(cleaning|clean|janitorial|maintenance|landscaping|lawn|pest|sanitation)\b': 'Cleaning & Maintenance Services',
    
    # Personal Services
    r'\b(salon|spa|beauty|barber|hair|nail|massage|tattoo)\b': 'Personal Care & Beauty',
    
    # Non-profit & Religious
    r'\b(nonprofit|non-profit|foundation|charity|church|ministries|mission)\b': 'Non-Profit & Religious',
    
    # Security Services
    r'\b(security|guard|protection|alarm|surveillance|safe)\b': 'Security Services',
    
    # Telecommunications
    r'\b(telecom|telecommunications|wireless|mobile|cellular|phone)\b': 'Telecommunications',
}


def detect_business_category(business_name: str) -> Optional[str]:
    """
    Detect business category from business name using keyword patterns.
    
    Args:
        business_name: The business name to analyze
        
    Returns:
        Detected business category or None if no match
    """
    if not business_name:
        return None
    
    name_lower = business_name.lower()
    
    for pattern, category in BUSINESS_CATEGORY_PATTERNS.items():
        if re.search(pattern, name_lower, re.IGNORECASE):
            return category
    
    return None


@dataclass
class DomainOwnerResult:
    """Container for domain owner lookup results."""
    domain: str = ''
    registrant_name: Optional[str] = None
    registrant_org: Optional[str] = None
    registrant_email: Optional[str] = None
    registrant_phone: Optional[str] = None
    admin_name: Optional[str] = None
    admin_email: Optional[str] = None
    tech_name: Optional[str] = None
    tech_email: Optional[str] = None
    registrar: Optional[str] = None
    creation_date: Optional[str] = None
    expiration_date: Optional[str] = None
    updated_date: Optional[str] = None
    name_servers: Optional[List[str]] = None
    status: Optional[str] = None
    # Owner info from web search
    owner_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    owner_title: Optional[str] = None
    company_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    emails: Optional[List[str]] = None
    phones: Optional[List[str]] = None
    address: Optional[str] = None
    confidence: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            'domain': self.domain,
            'registrant_name': self.registrant_name,
            'registrant_org': self.registrant_org,
            'registrant_email': self.registrant_email,
            'registrant_phone': self.registrant_phone,
            'admin_name': self.admin_name,
            'admin_email': self.admin_email,
            'tech_name': self.tech_name,
            'tech_email': self.tech_email,
            'registrar': self.registrar,
            'creation_date': self.creation_date,
            'expiration_date': self.expiration_date,
            'updated_date': self.updated_date,
            'name_servers': self.name_servers,
            'status': self.status,
            'owner_name': self.owner_name,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'owner_title': self.owner_title,
            'company_name': self.company_name,
            'linkedin_url': self.linkedin_url,
            'emails': self.emails,
            'phones': self.phones,
            'address': self.address,
            'confidence': self.confidence
        }


class SerperService:
    """
    Service for searching Google via Serper API.
    
    Uses Serper's search endpoint to find business owner names,
    websites, and other relevant information.
    """
    
    API_URL = "https://google.serper.dev/search"
    
    # Words that should NOT appear in owner names (case-insensitive)
    INVALID_NAME_WORDS = {
        'at', 'and', 'of', 'the', 'for', 'in', 'on', 'is', 'to', 'from', 'by', 'with',
        'ceo', 'cfo', 'coo', 'cto', 'president', 'founder', 'owner', 'chief', 'executive',
        'officer', 'director', 'manager', 'llc', 'inc', 'corp', 'company', 'co', 'ltd',
        'former', 'current', 'was', 'has', 'been', 'are', 'new', 'november', 'december',
        'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
        'september', 'october', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
        'saturday', 'sunday', 'today', 'yesterday', 'tomorrow', 'business', 'services',
        'group', 'holdings', 'partners', 'enterprises', 'industries', 'solutions'
    }
    
    # Common first names (subset for validation)
    COMMON_FIRST_NAMES = {
        'james', 'john', 'robert', 'michael', 'william', 'david', 'richard', 'joseph',
        'thomas', 'charles', 'christopher', 'daniel', 'matthew', 'anthony', 'mark',
        'donald', 'steven', 'paul', 'andrew', 'joshua', 'kenneth', 'kevin', 'brian',
        'george', 'timothy', 'ronald', 'edward', 'jason', 'jeffrey', 'ryan', 'jacob',
        'gary', 'nicholas', 'eric', 'jonathan', 'stephen', 'larry', 'justin', 'scott',
        'brandon', 'benjamin', 'samuel', 'raymond', 'gregory', 'frank', 'alexander',
        'patrick', 'jack', 'dennis', 'jerry', 'tyler', 'aaron', 'jose', 'adam', 'nathan',
        'mary', 'patricia', 'jennifer', 'linda', 'elizabeth', 'barbara', 'susan',
        'jessica', 'sarah', 'karen', 'nancy', 'lisa', 'betty', 'margaret', 'sandra',
        'ashley', 'dorothy', 'kimberly', 'emily', 'donna', 'michelle', 'carol', 'amanda',
        'melissa', 'deborah', 'stephanie', 'rebecca', 'sharon', 'laura', 'cynthia',
        'kathleen', 'amy', 'angela', 'shirley', 'anna', 'brenda', 'pamela', 'emma',
        'nicole', 'helen', 'samantha', 'katherine', 'christine', 'debra', 'rachel',
        'carolyn', 'janet', 'catherine', 'maria', 'heather', 'diane', 'ruth', 'julie'
    }
    
    def __init__(self, api_key: str = None):
        """
        Initialize Serper service.
        
        Args:
            api_key: Serper API key. Defaults to SERPER_API_KEY env var.
        """
        self.api_key = api_key or os.environ.get('SERPER_API_KEY', '')
        self.session = requests.Session()
        
    def is_configured(self) -> bool:
        """Check if API key is configured."""
        return bool(self.api_key)
    
    def search(self, query: str, num_results: int = 10) -> Optional[Dict]:
        """
        Perform a Google search via Serper API.
        
        Args:
            query: Search query string
            num_results: Number of results to return
            
        Returns:
            JSON response from Serper API or None on error
        """
        if not self.is_configured():
            logger.warning("Serper API key not configured")
            return None
        
        headers = {
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json'
        }
        
        payload = {
            'q': query,
            'num': num_results
        }
        
        try:
            response = self.session.post(
                self.API_URL,
                headers=headers,
                json=payload,
                timeout=10  # Reduced timeout to prevent stream disconnection
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Serper API error: {response.status_code} - {response.text}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Serper request failed: {e}")
            return None
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception:
            return ''
    
    def _is_valid_name(self, name: str) -> bool:
        """
        Check if a string looks like a valid person's name.
        
        Args:
            name: Potential name string
            
        Returns:
            True if it looks like a valid name
        """
        if not name or len(name) < 3:
            return False
        
        words = name.lower().split()
        
        # Must have at least 2 words (first + last name)
        if len(words) < 2:
            return False
        
        # Check each word
        for word in words:
            # Skip if it's in the invalid words list
            if word in self.INVALID_NAME_WORDS:
                return False
            
            # Must be alphanumeric (allow hyphens for hyphenated names)
            clean_word = word.replace('-', '').replace("'", '')
            if not clean_word.isalpha():
                return False
            
            # Each word should be 2-15 characters
            if len(word) < 2 or len(word) > 15:
                return False
        
        # Total name shouldn't be too long
        if len(name) > 40:
            return False
        
        return True
    
    def _extract_owner_from_text(self, text: str) -> Optional[str]:
        """
        Try to extract owner name from text with strict validation.
        
        Args:
            text: Text to search for owner names
            
        Returns:
            Valid owner name or None
        """
        # Patterns ordered by reliability
        patterns = [
            # "owned by John Smith"
            r'owned\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            # "founded by Jane Doe"
            r'founded\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            # "CEO John Smith"
            r'CEO[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            # "owner John Smith"
            r'owner[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            # "John Smith, CEO"
            r'([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(?:is\s+)?(?:the\s+)?(?:CEO|founder|owner|president)',
            # "John Smith owns"
            r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:owns|founded|started|operates)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean and validate
                name = match.strip()
                # Capitalize properly
                name = ' '.join(word.capitalize() for word in name.split())
                
                if self._is_valid_name(name):
                    return name
        
        return None
    
    def _extract_owner_from_linkedin(self, text: str, title: str) -> Optional[str]:
        """
        Extract owner name from LinkedIn search results.
        
        Args:
            text: Snippet text
            title: Title of the search result
            
        Returns:
            Valid owner name or None
        """
        # LinkedIn titles often have format: "Name - Title - Company"
        if 'linkedin.com' in text.lower() or 'linkedin' in title.lower():
            # Try to extract from title pattern: "FirstName LastName - Title"
            match = re.match(r'^([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[-|–]', title)
            if match:
                name = match.group(1).strip()
                if self._is_valid_name(name):
                    return name
        
        return None
    
    def search_business_owner(self, business_name: str, state: str = None, 
                              address: str = None, phone: str = None) -> SerperResult:
        """
        Search for business owner information using business details.
        
        Args:
            business_name: Name of the business
            state: Optional state for better results
            address: Optional address for context
            phone: Optional phone number for context
            
        Returns:
            SerperResult with found information
        """
        result = SerperResult()
        
        if not business_name:
            return result
        
        # Clean business name (remove LLC, Inc, etc. for better search)
        clean_name = re.sub(r'\s*(LLC|Inc\.?|Corp\.?|Co\.?|Ltd\.?|Limited|Corporation)\s*$', '', business_name, flags=re.IGNORECASE).strip()
        
        # Build search query with business details
        # Using business name + address/state for targeted search
        query_parts = [f'"{clean_name}"']
        if address:
            # Use city/state from address if available
            query_parts.append(address.split(',')[0] if ',' in address else address)
        elif state:
            query_parts.append(state)
        query_parts.append('owner OR founder OR CEO')
        
        query = ' '.join(query_parts)
        logger.info(f"Serper query: {query}")
        
        # Perform search
        search_results = self.search(query)
        
        if not search_results:
            return result
        
        # Extract information from organic results
        organic = search_results.get('organic', [])
        knowledge_graph = search_results.get('knowledgeGraph', {})
        
        # Try knowledge graph first (most reliable)
        if knowledge_graph:
            # Check for owner/founder in attributes
            attributes = knowledge_graph.get('attributes', {})
            for key, value in attributes.items():
                key_lower = key.lower()
                if any(term in key_lower for term in ['founder', 'owner', 'ceo', 'president']):
                    # Validate the name
                    if self._is_valid_name(value):
                        result.owner_name = value
                        result.confidence = 0.95
                        break
            
            # Get website from knowledge graph
            if knowledge_graph.get('website'):
                result.website = knowledge_graph['website']
                result.domain = self._extract_domain(result.website)
        
        # Process organic results
        for item in organic[:5]:  # Check first 5 results
            title = item.get('title', '')
            snippet = item.get('snippet', '')
            link = item.get('link', '')
            
            # Try LinkedIn extraction first
            if not result.owner_name:
                owner = self._extract_owner_from_linkedin(snippet + ' ' + link, title)
                if owner:
                    result.owner_name = owner
                    result.confidence = 0.85
            
            # Try to extract owner from snippet
            if not result.owner_name:
                owner = self._extract_owner_from_text(snippet)
                if owner:
                    result.owner_name = owner
                    result.confidence = 0.75
            
            # Get website if not already found
            if not result.website and link:
                # Skip social media and directory sites
                skip_domains = ['facebook.com', 'linkedin.com', 'twitter.com', 'yelp.com', 
                               'yellowpages.com', 'bbb.org', 'bizapedia.com', 'opencorporates.com',
                               'dnb.com', 'zoominfo.com', 'manta.com', 'buzzfile.com']
                domain = self._extract_domain(link)
                if domain and not any(skip in domain for skip in skip_domains):
                    result.website = link
                    result.domain = domain
        
        # If no owner found, try LinkedIn-specific search
        if not result.owner_name:
            linkedin_query = f'site:linkedin.com "{clean_name}" owner OR founder OR CEO'
            linkedin_results = self.search(linkedin_query, num_results=5)
            
            if linkedin_results:
                for item in linkedin_results.get('organic', [])[:3]:
                    title = item.get('title', '')
                    snippet = item.get('snippet', '')
                    link = item.get('link', '')
                    
                    owner = self._extract_owner_from_linkedin(snippet + ' ' + link, title)
                    if owner:
                        result.owner_name = owner
                        result.confidence = 0.70
                        break
        
        # Detect business category from business name
        result.business_category = detect_business_category(business_name)
        
        # If no category from name, try to extract from search results
        if not result.business_category and organic:
            for item in organic[:3]:
                snippet = item.get('snippet', '').lower()
                # Look for industry keywords in snippets
                for pattern, category in BUSINESS_CATEGORY_PATTERNS.items():
                    if re.search(pattern, snippet, re.IGNORECASE):
                        result.business_category = category
                        break
                if result.business_category:
                    break
        
        logger.info(f"Serper result for '{business_name}': owner={result.owner_name}, website={result.website}, category={result.business_category}, confidence={result.confidence}")
        return result
    
    def search_business_domain(self, business_name: str, state: str = None, 
                               city: str = None) -> SerperResult:
        """
        Search specifically for a business's website/domain.
        
        Args:
            business_name: Name of the business
            state: Optional state for better results
            city: Optional city for better results
            
        Returns:
            SerperResult with website and domain information
        """
        result = SerperResult()
        
        if not business_name:
            return result
        
        # Clean business name (remove LLC, Inc, etc.)
        clean_name = re.sub(r'\s*(LLC|Inc\.?|Corp\.?|Co\.?|Ltd\.?|Limited|Corporation|L\.?L\.?C\.?)\s*$', '', business_name, flags=re.IGNORECASE).strip()
        
        # Build search query focused on finding website
        query_parts = [f'"{clean_name}"']
        if city:
            query_parts.append(city)
        if state:
            query_parts.append(state)
        query_parts.append('website OR official site')
        
        query = ' '.join(query_parts)
        logger.info(f"Domain search query: {query}")
        
        # Perform search
        search_results = self.search(query)
        
        if not search_results:
            # Try simpler query
            simple_query = f'"{clean_name}" {state if state else ""}'
            search_results = self.search(simple_query)
        
        if not search_results:
            return result
        
        # Sites to skip (social, directories, etc.)
        skip_domains = [
            'facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com',
            'yelp.com', 'yellowpages.com', 'bbb.org', 'bizapedia.com', 
            'opencorporates.com', 'dnb.com', 'zoominfo.com', 'manta.com', 
            'buzzfile.com', 'crunchbase.com', 'glassdoor.com', 'indeed.com',
            'wikipedia.org', 'bloomberg.com', 'sec.gov', 'sunbiz.org',
            'google.com', 'youtube.com', 'reddit.com', 'quora.com'
        ]
        
        # Check knowledge graph first
        knowledge_graph = search_results.get('knowledgeGraph', {})
        if knowledge_graph.get('website'):
            website = knowledge_graph['website']
            domain = self._extract_domain(website)
            if domain and not any(skip in domain for skip in skip_domains):
                result.website = website
                result.domain = domain
                result.confidence = 0.95
                logger.info(f"Found domain from knowledge graph: {domain}")
                return result
        
        # Check organic results
        for item in search_results.get('organic', [])[:7]:
            link = item.get('link', '')
            title = item.get('title', '').lower()
            
            if not link:
                continue
                
            domain = self._extract_domain(link)
            
            # Skip social/directory sites
            if any(skip in domain for skip in skip_domains):
                continue
            
            # Boost confidence if business name appears in domain
            clean_name_lower = clean_name.lower().replace(' ', '')
            domain_base = domain.split('.')[0] if domain else ''
            
            # Check if it's likely the business's actual site
            title_match = any(word in title for word in clean_name.lower().split() if len(word) > 3)
            domain_match = any(word in domain_base for word in clean_name.lower().split() if len(word) > 3)
            
            if domain_match or title_match:
                result.website = link
                result.domain = domain
                result.confidence = 0.85 if domain_match else 0.70
                logger.info(f"Found domain from organic results: {domain}")
                return result
        
        # If still no result, take first non-skipped domain
        for item in search_results.get('organic', [])[:5]:
            link = item.get('link', '')
            if link:
                domain = self._extract_domain(link)
                if domain and not any(skip in domain for skip in skip_domains):
                    result.website = link
                    result.domain = domain
                    result.confidence = 0.50
                    logger.info(f"Found domain (lower confidence): {domain}")
                    return result
        
        return result
    
    def search_business_batch(self, businesses: List[dict]) -> List[SerperResult]:
        """
        Search for multiple businesses.
        
        Args:
            businesses: List of business dicts with 'business_name', 'state', 'address', 'phone'
            
        Returns:
            List of SerperResult objects
        """
        results = []
        
        for biz in businesses:
            result = self.search_business_owner(
                biz.get('business_name', ''),
                biz.get('state', ''),
                biz.get('address', ''),
                biz.get('phone', '') or biz.get('business_phone', '')
            )
            results.append(result)
        
        return results
    
    def lookup_domain_owner(self, domain: str) -> DomainOwnerResult:
        """
        Look up domain owner information using multiple advanced search techniques.
        
        Uses a comprehensive multi-step approach:
        1. WHOIS data extraction via Google search
        2. Knowledge Graph and direct owner search
        3. LinkedIn profile discovery
        4. Contact page email/phone extraction
        5. About/Team page owner extraction
        6. Press releases and news for executive names
        7. Social media profile aggregation
        8. SEC/Business registry cross-reference
        
        Args:
            domain: Domain name (e.g., 'example.com')
            
        Returns:
            DomainOwnerResult with comprehensive owner information
        """
        result = DomainOwnerResult(domain=domain)
        
        if not domain:
            return result
        
        # Clean domain (remove http://, www., etc.)
        domain = domain.lower().strip()
        domain = re.sub(r'^https?://', '', domain)
        domain = re.sub(r'^www\.', '', domain)
        domain = domain.split('/')[0]  # Remove path
        result.domain = domain
        company_name_guess = domain.split('.')[0].replace('-', ' ').replace('_', ' ').title()
        
        # ================================
        # Step 1: WHOIS lookup search
        # ================================
        whois_queries = [
            f'"{domain}" WHOIS registrant organization',
            f'"{domain}" domain registration owner',
            f'"{domain}" registered to'
        ]
        
        for whois_query in whois_queries:
            whois_results = self.search(whois_query, num_results=8)
            if not whois_results:
                continue
                
            for item in whois_results.get('organic', []):
                snippet = item.get('snippet', '')
                
                # Extract registrar info
                registrar_patterns = [
                    r'(?:registrar|registered\s+(?:by|through|with))[:\s]+([A-Za-z0-9\s\-\.]+?)(?:\.|,|$)',
                    r'(?:domain\s+registrar)[:\s]+([A-Za-z0-9\s\-\.]+)',
                ]
                for pattern in registrar_patterns:
                    if not result.registrar:
                        match = re.search(pattern, snippet, re.IGNORECASE)
                        if match:
                            result.registrar = match.group(1).strip()[:50]
                
                # Extract dates with multiple patterns
                date_patterns = [
                    (r'(?:created|creation|registered|registration)[:\s]+(\d{4}[-/]\d{2}[-/]\d{2})', 'creation_date'),
                    (r'(?:created|registration)[:\s]+(\w+\s+\d{1,2},?\s+\d{4})', 'creation_date'),
                    (r'(?:expires?|expiration)[:\s]+(\d{4}[-/]\d{2}[-/]\d{2})', 'expiration_date'),
                    (r'(?:expires?|expiration)[:\s]+(\w+\s+\d{1,2},?\s+\d{4})', 'expiration_date'),
                    (r'(?:updated|modified|last\s+update)[:\s]+(\d{4}[-/]\d{2}[-/]\d{2})', 'updated_date'),
                ]
                for pattern, field in date_patterns:
                    if not getattr(result, field):
                        match = re.search(pattern, snippet, re.IGNORECASE)
                        if match:
                            setattr(result, field, match.group(1))
                
                # Extract registrant organization with multiple patterns
                org_patterns = [
                    r'(?:registrant\s+organization|registered\s+to|domain\s+owner|owned\s+by)[:\s]+([A-Za-z0-9\s\-\.\,\']+?(?:LLC|Inc|Corp|Ltd|Company|Co\.)?)',
                    r'(?:organization)[:\s]+([A-Za-z0-9\s\-\.\,]+)',
                ]
                for pattern in org_patterns:
                    if not result.registrant_org:
                        match = re.search(pattern, snippet, re.IGNORECASE)
                        if match:
                            org = match.group(1).strip()[:100]
                            if len(org) > 3 and not any(skip in org.lower() for skip in ['whois', 'privacy', 'proxy', 'redacted']):
                                result.registrant_org = org
                                result.company_name = org
            
            if result.registrar or result.registrant_org:
                break  # Found enough from WHOIS
        
        # ================================
        # Step 2: Knowledge Graph + Direct Owner Search
        # ================================
        owner_queries = [
            f'"{domain}" owner founder CEO',
            f'"{company_name_guess}" company owner founder',
            f'who owns {domain}',
            f'"{domain}" executive leadership team'
        ]
        
        for owner_query in owner_queries:
            owner_results = self.search(owner_query, num_results=10)
            if not owner_results:
                continue
                
            knowledge_graph = owner_results.get('knowledgeGraph', {})
            
            # Check knowledge graph (highest confidence)
            if knowledge_graph:
                if not result.company_name:
                    result.company_name = knowledge_graph.get('title')
                
                # Check description for owner hints
                description = knowledge_graph.get('description', '').lower()
                if 'founder' in description or 'ceo' in description or 'owner' in description:
                    # Try to extract name from description
                    desc_match = re.search(r'(?:founded|owned|led)\s+by\s+([A-Z][a-z]+\s+[A-Z][a-z]+)', knowledge_graph.get('description', ''))
                    if desc_match and self._is_valid_name(desc_match.group(1)):
                        result.owner_name = desc_match.group(1)
                        result.confidence = 0.90
                
                attributes = knowledge_graph.get('attributes', {})
                for key, value in attributes.items():
                    key_lower = key.lower()
                    if any(term in key_lower for term in ['founder', 'owner', 'ceo', 'president', 'chief executive']):
                        if self._is_valid_name(value):
                            result.owner_name = value
                            name_parts = value.split()
                            if len(name_parts) >= 2:
                                result.first_name = name_parts[0]
                                result.last_name = ' '.join(name_parts[1:])
                            result.owner_title = key.title()
                            result.confidence = 0.95
                            break
            
            # Process organic results
            for item in owner_results.get('organic', [])[:7]:
                title = item.get('title', '')
                snippet = item.get('snippet', '')
                link = item.get('link', '')
                
                # Extract owner name from text with enhanced patterns
                if not result.owner_name or result.confidence < 0.80:
                    owner = self._extract_owner_from_text(snippet)
                    if owner:
                        result.owner_name = owner
                        name_parts = owner.split()
                        if len(name_parts) >= 2:
                            result.first_name = name_parts[0]
                            result.last_name = ' '.join(name_parts[1:])
                        result.confidence = max(result.confidence, 0.75)
                
                # Extract title/position
                title_patterns = [
                    r'([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(CEO|Founder|Owner|President|Chief\s+Executive)',
                    r'(CEO|Founder|Owner|President)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                ]
                for pattern in title_patterns:
                    match = re.search(pattern, snippet)
                    if match:
                        if not result.owner_title:
                            groups = match.groups()
                            if 'CEO' in groups or 'Founder' in groups or 'Owner' in groups or 'President' in groups:
                                result.owner_title = next((g for g in groups if g in ['CEO', 'Founder', 'Owner', 'President', 'Chief Executive']), None)
                
                # Extract company name from title
                if not result.company_name and domain.split('.')[0].lower() in title.lower():
                    result.company_name = title.split(' - ')[0].split(' | ')[0].strip()
            
            if result.owner_name and result.confidence >= 0.80:
                break  # Found confident owner
        
        # ================================
        # Step 3: LinkedIn Advanced Search
        # ================================
        linkedin_queries = [
            f'site:linkedin.com/in "{company_name_guess}" founder OR CEO OR owner',
            f'site:linkedin.com "{domain}" company owner',
            f'site:linkedin.com/company "{company_name_guess}"'
        ]
        
        for linkedin_query in linkedin_queries:
            linkedin_results = self.search(linkedin_query, num_results=8)
            if not linkedin_results:
                continue
                
            for item in linkedin_results.get('organic', [])[:5]:
                title = item.get('title', '')
                snippet = item.get('snippet', '')
                link = item.get('link', '')
                
                # Extract owner from LinkedIn profile
                owner = self._extract_owner_from_linkedin(snippet + ' ' + link, title)
                if owner and (not result.owner_name or result.confidence < 0.85):
                    result.owner_name = owner
                    name_parts = owner.split()
                    if len(name_parts) >= 2:
                        result.first_name = name_parts[0]
                        result.last_name = ' '.join(name_parts[1:])
                    if 'linkedin.com/in/' in link:
                        result.linkedin_url = link
                    result.confidence = max(result.confidence, 0.85)
                
                # Extract title from LinkedIn
                if not result.owner_title:
                    title_match = re.search(r'[-–|]\s*(CEO|Founder|Owner|President|Chief\s+\w+\s+Officer)', title, re.IGNORECASE)
                    if title_match:
                        result.owner_title = title_match.group(1)
            
            if result.linkedin_url:
                break  # Found LinkedIn profile
        
        # ================================
        # Step 4: Contact Page Deep Search
        # ================================
        contact_queries = [
            f'site:{domain} contact email phone',
            f'site:{domain} "contact us" OR "get in touch"',
            f'site:{domain} "@{domain.split(".")[0]}"'  # Look for company email pattern
        ]
        
        emails_found = []
        phones_found = []
        
        for contact_query in contact_queries:
            contact_results = self.search(contact_query, num_results=8)
            if not contact_results:
                continue
                
            for item in contact_results.get('organic', []):
                snippet = item.get('snippet', '')
                title = item.get('title', '')
                
                # Enhanced email extraction with validation
                email_patterns = [
                    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                    r'([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|@)\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
                ]
                for pattern in email_patterns:
                    matches = re.findall(pattern, snippet + ' ' + title)
                    for match in matches:
                        email = match if isinstance(match, str) else '@'.join(match)
                        # Validate email format and skip generic ones
                        if email and '@' in email:
                            email_lower = email.lower()
                            if email_lower not in [e.lower() for e in emails_found]:
                                # Prioritize company domain emails
                                if domain.split('.')[0] in email_lower:
                                    emails_found.insert(0, email)  # Add to front
                                elif not any(skip in email_lower for skip in ['example.com', 'email.com', 'domain.com', 'sentry']):
                                    emails_found.append(email)
                
                # Enhanced phone extraction with international format support
                phone_patterns = [
                    r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
                    r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',
                    r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',
                ]
                for pattern in phone_patterns:
                    phone_matches = re.findall(pattern, snippet)
                    for phone in phone_matches:
                        # Clean phone number
                        clean_phone = re.sub(r'[^\d+]', '', phone)
                        if len(clean_phone) >= 10 and phone not in phones_found:
                            phones_found.append(phone)
        
        if emails_found:
            result.emails = emails_found[:5]  # Limit to 5
        if phones_found:
            result.phones = phones_found[:3]  # Limit to 3
        
        # ================================
        # Step 5: About/Team Page Deep Analysis
        # ================================
        about_queries = [
            f'site:{domain} "about us" team leadership',
            f'site:{domain} "our team" OR "meet the team"',
            f'site:{domain} "our story" founder',
            f'site:{domain} management team executives'
        ]
        
        for about_query in about_queries:
            if result.owner_name and result.confidence >= 0.85:
                break  # Already have confident owner
                
            about_results = self.search(about_query, num_results=8)
            if not about_results:
                continue
                
            for item in about_results.get('organic', []):
                snippet = item.get('snippet', '')
                title = item.get('title', '')
                
                # Extract owner with enhanced patterns
                owner_patterns = [
                    r'(?:founded\s+by|started\s+by|created\s+by)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                    r'(?:CEO|founder|owner|president)[,:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                    r'([A-Z][a-z]+\s+[A-Z][a-z]+)[,\s]+(?:CEO|founder|owner|president)',
                    r'(?:led\s+by|headed\s+by)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                ]
                
                if not result.owner_name or result.confidence < 0.75:
                    for pattern in owner_patterns:
                        match = re.search(pattern, snippet, re.IGNORECASE)
                        if match:
                            name = match.group(1).strip()
                            if self._is_valid_name(name):
                                result.owner_name = name
                                name_parts = name.split()
                                if len(name_parts) >= 2:
                                    result.first_name = name_parts[0]
                                    result.last_name = ' '.join(name_parts[1:])
                                result.confidence = max(result.confidence, 0.75)
                                break
        
        # ================================
        # Step 6: Press Releases & News Search
        # ================================
        if not result.owner_name or result.confidence < 0.80:
            news_queries = [
                f'"{company_name_guess}" CEO OR founder press release',
                f'"{domain}" company announcement executive',
                f'"{company_name_guess}" raises funding founder'
            ]
            
            for news_query in news_queries:
                news_results = self.search(news_query, num_results=8)
                if not news_results:
                    continue
                    
                for item in news_results.get('organic', [])[:5]:
                    snippet = item.get('snippet', '')
                    
                    # Look for CEO/founder mentions in news
                    news_patterns = [
                        r'(?:CEO|chief\s+executive)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                        r'([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(?:CEO|chief\s+executive|founder)',
                        r'(?:said|announced|stated)\s+([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(?:CEO|founder)',
                    ]
                    
                    for pattern in news_patterns:
                        match = re.search(pattern, snippet)
                        if match:
                            name = match.group(1).strip()
                            if self._is_valid_name(name):
                                result.owner_name = name
                                name_parts = name.split()
                                if len(name_parts) >= 2:
                                    result.first_name = name_parts[0]
                                    result.last_name = ' '.join(name_parts[1:])
                                result.confidence = max(result.confidence, 0.80)
                                break
                
                if result.owner_name and result.confidence >= 0.80:
                    break
        
        # ================================
        # Step 7: Social Media Aggregation
        # ================================
        social_queries = [
            f'"{company_name_guess}" site:twitter.com OR site:facebook.com founder',
            f'"{domain}" official twitter facebook instagram'
        ]
        
        for social_query in social_queries:
            social_results = self.search(social_query, num_results=5)
            if not social_results:
                continue
                
            for item in social_results.get('organic', []):
                link = item.get('link', '')
                snippet = item.get('snippet', '')
                
                # Extract social handles and potential owner names
                if 'twitter.com' in link and not result.owner_name:
                    # Twitter bio often contains owner info
                    owner = self._extract_owner_from_text(snippet)
                    if owner and self._is_valid_name(owner):
                        result.owner_name = owner
                        result.confidence = max(result.confidence, 0.65)
        
        # ================================
        # Step 8: SEC/Business Registry Cross-Reference
        # ================================
        if not result.owner_name or result.confidence < 0.85:
            sec_queries = [
                f'site:sec.gov "{company_name_guess}" OR "{domain}"',
                f'"{company_name_guess}" business registration owner officer'
            ]
            
            for sec_query in sec_queries:
                sec_results = self.search(sec_query, num_results=5)
                if not sec_results:
                    continue
                    
                for item in sec_results.get('organic', []):
                    snippet = item.get('snippet', '')
                    
                    # Look for officer names in SEC filings
                    officer_patterns = [
                        r'(?:director|officer|executive)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                        r'([A-Z][a-z]+\s+[A-Z][a-z]+)[,\s]+(?:director|officer|executive)',
                    ]
                    
                    for pattern in officer_patterns:
                        match = re.search(pattern, snippet)
                        if match:
                            name = match.group(1).strip()
                            if self._is_valid_name(name):
                                result.owner_name = name
                                name_parts = name.split()
                                if len(name_parts) >= 2:
                                    result.first_name = name_parts[0]
                                    result.last_name = ' '.join(name_parts[1:])
                                result.confidence = max(result.confidence, 0.85)
                                break
        
        # Final confidence adjustment based on data completeness
        data_points = sum([
            bool(result.owner_name),
            bool(result.company_name),
            bool(result.emails),
            bool(result.phones),
            bool(result.linkedin_url),
        ])
        if data_points >= 4:
            result.confidence = min(result.confidence + 0.05, 1.0)
        
        logger.info(f"Enhanced domain lookup for '{domain}': owner={result.owner_name}, company={result.company_name}, emails={result.emails}, phones={result.phones}, linkedin={result.linkedin_url}, confidence={result.confidence}")
        return result


# Singleton instance
_serper_service: Optional[SerperService] = None


def get_serper_service() -> SerperService:
    """Get or create Serper service singleton."""
    global _serper_service
    
    if _serper_service is None:
        _serper_service = SerperService()
    
    return _serper_service


if __name__ == "__main__":
    # Test the service
    logging.basicConfig(level=logging.INFO)
    
    service = SerperService()
    
    if service.is_configured():
        result = service.search_business_owner("Apple Inc", "California")
        print(f"Result: {result.to_dict()}")
    else:
        print("Serper API key not configured. Set SERPER_API_KEY environment variable.")

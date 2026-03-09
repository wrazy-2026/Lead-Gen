import requests
import time
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from typing import List

from scrapers.base_scraper import BaseScraper, BusinessRecord

logger = logging.getLogger(__name__)

class GlobalEdgarScraper(BaseScraper):
    """
    Consolidated SEC EDGAR Scraper for all 50 states.
    Refactored from edgar_full_usa_scraper.py
    """
    
    HEADERS = {
        'User-Agent': 'LeadGenDashboard/2.0 (contact@example.com)',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    }

    STATES = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
        "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
        "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
        "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
        "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
        "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
        "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
        "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
        "DC": "District of Columbia", "PR": "Puerto Rico"
    }

    def __init__(self):
        super().__init__("Discovery Monitor", "US_ALL", "https://www.sec.gov")
        self.sic_map = {}
        self._load_sic_map()

    def _load_sic_map(self):
        # We can use the mapping from the script or just leave it empty for now
        self.sic_map = {
            "1000": "Metal Mining",
            "1311": "Crude Petroleum & Natural Gas",
            "1520": "General Building Contractors",
            "2834": "Pharmaceutical Preparations",
            "3571": "Electronic Computers",
            "4941": "Water Supply",
            "6021": "National Commercial Banks",
            "7372": "Prepackaged Software"
        }

    def get_industry_category(self, sic_code):
        return self.sic_map.get(str(sic_code), "Unknown Industry")

    def fetch_new_businesses(self, limit: int = 5) -> List[BusinessRecord]:
        """
        Main entry point for ScraperManager.
        Note: For a global scraper, 'limit' is usually limit per state if it iterates over all states.
        """
        all_records = []
        for state_code in self.STATES.keys():
            try:
                state_records = self.fetch_for_state(state_code, limit=limit)
                all_records.extend(state_records)
                if len(all_records) >= limit * 50: # Safety cap
                    break
            except Exception as e:
                logger.error(f"Error scraping state {state_code}: {e}")
        return all_records

    def fetch_for_state(self, state_code: str, limit: int = 5) -> List[BusinessRecord]:
        companies = self.get_company_list_by_state(state_code)
        companies = companies[:limit]
        
        records = []
        for company in companies:
            details = self.get_company_details(company)
            if not details:
                continue
                
            record = BusinessRecord(
                business_name=company['business_name'],
                filing_date=details.get('filing_date', datetime.now().strftime('%Y-%m-%d')),
                state=state_code,
                status="Active",
                url=company['url'],
                cik=company['cik'],
                ein=details.get('ein'),
                sic_code=details.get('sic_code'),
                industry_category=details.get('industry_category', 'Unknown Industry'),
                address=details.get('address'),
                phone=details.get('phone'),
                business_address=details.get('address'),
                business_phone=details.get('phone'),
                filing_number=details.get('filing_number')
            )
            records.append(record)
            time.sleep(0.2) # Rate limit
        return records

    def get_company_list_by_state(self, state_code):
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?company=&match=starts-with&filenum=&State={state_code}&Country=&SIC=&myowner=exclude&action=getcompany"
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            table = soup.find('table', {'summary': 'Results'})
            if not table:
                tables = soup.find_all('table')
                for t in tables:
                    if "CIK" in t.text and "Company" in t.text:
                        table = t
                        break
            
            if not table:
                return []

            companies = []
            rows = table.find_all('tr')[1:] # Skip header
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    cik_link = cols[0].find('a')
                    if not cik_link: continue
                    
                    cik = cik_link.text.strip()
                    company_name = cols[1].text.strip()
                    company_url = "https://www.sec.gov" + cik_link['href']
                    
                    companies.append({
                        'cik': cik,
                        'business_name': company_name,
                        'state': state_code,
                        'url': company_url
                    })
            return companies
        except Exception as e:
            logger.error(f"Error fetching companies for {state_code}: {e}")
            return []

    def get_company_details(self, company_info):
        url = company_info['url']
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            ident_info = soup.find('div', class_='identInfo') or soup.find('p', class_='identInfo')
            details = {
                'phone': '',
                'address': '',
                'ein': '',
                'sic_code': '',
                'industry_category': 'Unknown Industry',
                'filing_date': datetime.now().strftime('%Y-%m-%d'),
                'filing_number': ''
            }
            
            if ident_info:
                text = ident_info.get_text(separator='|')
                ein_match = re.search(r'IRS No\.:\s*(\d+)', text)
                if ein_match:
                    details['ein'] = ein_match.group(1)
                sic_match = re.search(r'SIC:\s*(\d+)', text)
                if sic_match:
                    details['sic_code'] = sic_match.group(1)
                    details['industry_category'] = self.get_industry_category(details['sic_code'])

            mailers = soup.find_all('div', class_='mailer')
            business_addr_div = None
            for m in mailers:
                if "Business Address" in m.text:
                    business_addr_div = m
                    break
            if not business_addr_div and len(mailers) > 0:
                business_addr_div = mailers[-1]

            if business_addr_div:
                addr_lines = [span.text.strip() for span in business_addr_div.find_all('span', class_='mailerAddress')]
                details['address'] = ", ".join(addr_lines)
                phone_match = re.search(r'(\d{3}-\d{3}-\d{4})', business_addr_div.get_text())
                if phone_match:
                    details['phone'] = phone_match.group(1)

            filing_table = soup.find('table', class_='tableFile2')
            if filing_table:
                rows = filing_table.find_all('tr')
                if len(rows) > 1:
                    cols = rows[1].find_all('td')
                    if len(cols) >= 4:
                        details['filing_date'] = cols[3].text.strip()
                        details['filing_number'] = cols[2].text.strip().split('\n')[0]
            return details
        except Exception as e:
            logger.error(f"Error fetching details for {company_info['cik']}: {e}")
            return {}

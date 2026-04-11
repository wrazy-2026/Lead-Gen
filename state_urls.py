"""
US Secretary of State Business Search URLs
==========================================
This file contains the business search/lookup URLs for all 50 US states.
Use these as starting points when developing state-specific scrapers.

IMPORTANT NOTES:
----------------
1. URLs may change over time - always verify before implementing
2. Many states require CAPTCHA or registration to search
3. Some states offer API access - prefer APIs over scraping
4. Always check robots.txt and Terms of Service
5. Implement rate limiting to avoid IP blocking
6. Consider using commercial data providers for production:
   - OpenCorporates (https://opencorporates.com)
   - Cobalt Intelligence (https://cobaltintelligence.com)
   - InfoGroup/Infousa

DIFFICULTY RATINGS:
-------------------
⭐ Easy - Basic HTML tables, minimal protection
⭐⭐ Medium - JavaScript rendering, forms required
⭐⭐⭐ Hard - CAPTCHA, anti-bot protection
⭐⭐⭐⭐ Very Hard - Login required, heavy protection
⭐⭐⭐⭐⭐ Blocked - Strong anti-scraping (use APIs instead)

Last Updated: 2024
"""

# ============================================================================
# ALL 50 STATE BUSINESS SEARCH URLs
# ============================================================================

STATE_URLS = {
    # -------------------------------------------------------------------------
    # ALABAMA (AL) ⭐⭐
    # -------------------------------------------------------------------------
    "AL": {
        "name": "Alabama",
        "url": "https://arc-sos.state.al.us/CGI/CORPNAME.MBR/INPUT",
        "notes": "Secretary of State Business Entity Search",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # ALASKA (AK) ⭐⭐
    # -------------------------------------------------------------------------
    "AK": {
        "name": "Alaska",
        "url": "https://www.commerce.alaska.gov/cbp/Main/Search/Entities",
        "notes": "Division of Corporations, Business and Professional Licensing",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # ARIZONA (AZ) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "AZ": {
        "name": "Arizona",
        "url": "https://ecorp.azcc.gov/BusinessSearch/BusinessSearch",
        "notes": "Arizona Corporation Commission - eCorp",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # ARKANSAS (AR) ⭐⭐
    # -------------------------------------------------------------------------
    "AR": {
        "name": "Arkansas",
        "url": "https://www.sos.arkansas.gov/corps/search_all.php",
        "notes": "Secretary of State Business & Commercial Services",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # CALIFORNIA (CA) ⭐⭐⭐⭐
    # -------------------------------------------------------------------------
    "CA": {
        "name": "California",
        "url": "https://bizfileonline.sos.ca.gov/search/business",
        "notes": "California Secretary of State - bizfile Online. Has CAPTCHA.",
        "api_url": "https://businesssearch.sos.ca.gov/",
        "difficulty": 4
    },
    
    # -------------------------------------------------------------------------
    # COLORADO (CO) ⭐⭐
    # -------------------------------------------------------------------------
    "CO": {
        "name": "Colorado",
        "url": "https://www.sos.state.co.us/biz/BusinessEntityCriteriaExt.do",
        "notes": "Colorado Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # CONNECTICUT (CT) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "CT": {
        "name": "Connecticut",
        "url": "https://service.ct.gov/business/s/onlinebusinesssearch",
        "notes": "Connecticut Business Registry Search",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # DELAWARE (DE) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "DE": {
        "name": "Delaware",
        "url": "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
        "notes": "Delaware Division of Corporations - Very popular for incorporations",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # FLORIDA (FL) ⭐⭐
    # -------------------------------------------------------------------------
    "FL": {
        "name": "Florida",
        "url": "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName",
        "notes": "Florida Division of Corporations - Sunbiz",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # GEORGIA (GA) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "GA": {
        "name": "Georgia",
        "url": "https://ecorp.sos.ga.gov/BusinessSearch",
        "notes": "Georgia Corporations Division",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # HAWAII (HI) ⭐⭐
    # -------------------------------------------------------------------------
    "HI": {
        "name": "Hawaii",
        "url": "https://hbe.ehawaii.gov/documents/search.html",
        "notes": "Hawaii Business Express",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # IDAHO (ID) ⭐⭐
    # -------------------------------------------------------------------------
    "ID": {
        "name": "Idaho",
        "url": "https://sosbiz.idaho.gov/search/business",
        "notes": "Idaho Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # ILLINOIS (IL) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "IL": {
        "name": "Illinois",
        "url": "https://apps.ilsos.gov/corporatellc/",
        "notes": "Illinois Secretary of State - Corporation/LLC Search",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # INDIANA (IN) ⭐⭐
    # -------------------------------------------------------------------------
    "IN": {
        "name": "Indiana",
        "url": "https://bsd.sos.in.gov/publicbusinesssearch",
        "notes": "Indiana Secretary of State - INBiz",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # IOWA (IA) ⭐⭐
    # -------------------------------------------------------------------------
    "IA": {
        "name": "Iowa",
        "url": "https://sos.iowa.gov/search/business/(S(...))/search.aspx",
        "notes": "Iowa Secretary of State - Fast Track Filing",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # KANSAS (KS) ⭐⭐
    # -------------------------------------------------------------------------
    "KS": {
        "name": "Kansas",
        "url": "https://www.kansas.gov/bess/flow/main?execution=e1s1",
        "notes": "Kansas Business Entity Search",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # KENTUCKY (KY) ⭐⭐
    # -------------------------------------------------------------------------
    "KY": {
        "name": "Kentucky",
        "url": "https://app.sos.ky.gov/ftsearch/",
        "notes": "Kentucky Secretary of State - Business Filings",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # LOUISIANA (LA) ⭐⭐
    # -------------------------------------------------------------------------
    "LA": {
        "name": "Louisiana",
        "url": "https://coraweb.sos.la.gov/commercialsearch/CommercialSearch.aspx",
        "notes": "Louisiana Secretary of State - Commercial Database",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # MAINE (ME) ⭐⭐
    # -------------------------------------------------------------------------
    "ME": {
        "name": "Maine",
        "url": "https://icrs.informe.org/nei-sos-icrs/ICRS?MainPage=x",
        "notes": "Maine Secretary of State - Corporate Registry",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # MARYLAND (MD) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "MD": {
        "name": "Maryland",
        "url": "https://egov.maryland.gov/BusinessExpress/EntitySearch",
        "notes": "Maryland Business Express",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # MASSACHUSETTS (MA) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "MA": {
        "name": "Massachusetts",
        "url": "https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx",
        "notes": "Massachusetts Secretary of the Commonwealth",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # MICHIGAN (MI) ⭐⭐
    # -------------------------------------------------------------------------
    "MI": {
        "name": "Michigan",
        "url": "https://cofs.lara.state.mi.us/SearchApi/Search/Search",
        "notes": "Michigan LARA - Corporations Online Filing System",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # MINNESOTA (MN) ⭐⭐
    # -------------------------------------------------------------------------
    "MN": {
        "name": "Minnesota",
        "url": "https://mblsportal.sos.state.mn.us/Business/Search",
        "notes": "Minnesota Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # MISSISSIPPI (MS) ⭐⭐
    # -------------------------------------------------------------------------
    "MS": {
        "name": "Mississippi",
        "url": "https://corp.sos.ms.gov/corp/portal/c/page/corpBusinessIdSearch/portal.aspx",
        "notes": "Mississippi Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # MISSOURI (MO) ⭐⭐
    # -------------------------------------------------------------------------
    "MO": {
        "name": "Missouri",
        "url": "https://bsd.sos.mo.gov/BusinessEntity/BESearch.aspx",
        "notes": "Missouri Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # MONTANA (MT) ⭐⭐
    # -------------------------------------------------------------------------
    "MT": {
        "name": "Montana",
        "url": "https://biz.sosmt.gov/search",
        "notes": "Montana Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # NEBRASKA (NE) ⭐⭐
    # -------------------------------------------------------------------------
    "NE": {
        "name": "Nebraska",
        "url": "https://www.nebraska.gov/sos/corp/corpsearch.cgi",
        "notes": "Nebraska Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # NEVADA (NV) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "NV": {
        "name": "Nevada",
        "url": "https://esos.nv.gov/EntitySearch/OnlineEntitySearch",
        "notes": "Nevada Secretary of State - Popular for incorporations",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # NEW HAMPSHIRE (NH) ⭐⭐
    # -------------------------------------------------------------------------
    "NH": {
        "name": "New Hampshire",
        "url": "https://quickstart.sos.nh.gov/online/BusinessInquire",
        "notes": "New Hampshire Secretary of State - QuickStart",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # NEW JERSEY (NJ) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "NJ": {
        "name": "New Jersey",
        "url": "https://www.njportal.com/DOR/BusinessNameSearch/",
        "notes": "New Jersey Division of Revenue",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # NEW MEXICO (NM) ⭐⭐
    # -------------------------------------------------------------------------
    "NM": {
        "name": "New Mexico",
        "url": "https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch",
        "notes": "New Mexico Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # NEW YORK (NY) ⭐⭐⭐⭐
    # -------------------------------------------------------------------------
    "NY": {
        "name": "New York",
        "url": "https://apps.dos.ny.gov/publicInquiry/",
        "notes": "New York Department of State - Division of Corporations",
        "difficulty": 4
    },
    
    # -------------------------------------------------------------------------
    # NORTH CAROLINA (NC) ⭐⭐
    # -------------------------------------------------------------------------
    "NC": {
        "name": "North Carolina",
        "url": "https://www.sosnc.gov/online_services/search/by_title/_Business_Registration",
        "notes": "North Carolina Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # NORTH DAKOTA (ND) ⭐⭐
    # -------------------------------------------------------------------------
    "ND": {
        "name": "North Dakota",
        "url": "https://firststop.sos.nd.gov/search/business",
        "notes": "North Dakota Secretary of State - First Stop",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # OHIO (OH) ⭐⭐
    # -------------------------------------------------------------------------
    "OH": {
        "name": "Ohio",
        "url": "https://businesssearch.ohiosos.gov/",
        "notes": "Ohio Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # OKLAHOMA (OK) ⭐⭐
    # -------------------------------------------------------------------------
    "OK": {
        "name": "Oklahoma",
        "url": "https://www.sos.ok.gov/corp/corpInquiryFind.aspx",
        "notes": "Oklahoma Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # OREGON (OR) ⭐⭐
    # -------------------------------------------------------------------------
    "OR": {
        "name": "Oregon",
        "url": "https://sos.oregon.gov/business/pages/find.aspx",
        "notes": "Oregon Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # PENNSYLVANIA (PA) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "PA": {
        "name": "Pennsylvania",
        "url": "https://www.corporations.pa.gov/search/corpsearch",
        "notes": "Pennsylvania Department of State",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # RHODE ISLAND (RI) ⭐⭐
    # -------------------------------------------------------------------------
    "RI": {
        "name": "Rhode Island",
        "url": "https://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSearch.aspx",
        "notes": "Rhode Island Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # SOUTH CAROLINA (SC) ⭐⭐
    # -------------------------------------------------------------------------
    "SC": {
        "name": "South Carolina",
        "url": "https://businessfilings.sc.gov/BusinessFiling/Entity/Search",
        "notes": "South Carolina Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # SOUTH DAKOTA (SD) ⭐⭐
    # -------------------------------------------------------------------------
    "SD": {
        "name": "South Dakota",
        "url": "https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx",
        "notes": "South Dakota Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # TENNESSEE (TN) ⭐⭐
    # -------------------------------------------------------------------------
    "TN": {
        "name": "Tennessee",
        "url": "https://tnbear.tn.gov/Ecommerce/FilingSearch.aspx",
        "notes": "Tennessee Secretary of State - TNBEAR",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # TEXAS (TX) ⭐⭐⭐
    # -------------------------------------------------------------------------
    "TX": {
        "name": "Texas",
        "url": "https://mycpa.cpa.state.tx.us/coa/",
        "notes": "Texas Comptroller of Public Accounts - Also check SOS at sos.state.tx.us",
        "sos_url": "https://www.sos.state.tx.us/corp/sosda/index.shtml",
        "difficulty": 3
    },
    
    # -------------------------------------------------------------------------
    # UTAH (UT) ⭐⭐
    # -------------------------------------------------------------------------
    "UT": {
        "name": "Utah",
        "url": "https://secure.utah.gov/bes/index.html",
        "notes": "Utah Division of Corporations",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # VERMONT (VT) ⭐⭐
    # -------------------------------------------------------------------------
    "VT": {
        "name": "Vermont",
        "url": "https://bizfilings.vermont.gov/online/BusinessInquire",
        "notes": "Vermont Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # VIRGINIA (VA) ⭐⭐
    # -------------------------------------------------------------------------
    "VA": {
        "name": "Virginia",
        "url": "https://cis.scc.virginia.gov/EntitySearch/Index",
        "notes": "Virginia State Corporation Commission",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # WASHINGTON (WA) ⭐⭐
    # -------------------------------------------------------------------------
    "WA": {
        "name": "Washington",
        "url": "https://ccfs.sos.wa.gov/#/BusinessSearch",
        "notes": "Washington Secretary of State - Corporations Division",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # WEST VIRGINIA (WV) ⭐⭐
    # -------------------------------------------------------------------------
    "WV": {
        "name": "West Virginia",
        "url": "https://apps.sos.wv.gov/business/corporations/",
        "notes": "West Virginia Secretary of State",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # WISCONSIN (WI) ⭐⭐
    # -------------------------------------------------------------------------
    "WI": {
        "name": "Wisconsin",
        "url": "https://www.wdfi.org/apps/CorpSearch/Search.aspx",
        "notes": "Wisconsin Department of Financial Institutions",
        "difficulty": 2
    },
    
    # -------------------------------------------------------------------------
    # WYOMING (WY) ⭐⭐
    # -------------------------------------------------------------------------
    "WY": {
        "name": "Wyoming",
        "url": "https://wyobiz.wyo.gov/Business/FilingSearch.aspx",
        "notes": "Wyoming Secretary of State - Popular for privacy-focused incorporations",
        "difficulty": 2
    }
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_state_url(state_code: str) -> str:
    """Get the business search URL for a state."""
    state = STATE_URLS.get(state_code.upper())
    return state["url"] if state else None


def get_state_info(state_code: str) -> dict:
    """Get full state information."""
    return STATE_URLS.get(state_code.upper())


def get_easy_states() -> list:
    """Get list of states rated as 'easy' to scrape (difficulty 1-2)."""
    return [
        code for code, info in STATE_URLS.items() 
        if info.get("difficulty", 5) <= 2
    ]


def get_all_states() -> list:
    """Get list of all state codes."""
    return list(STATE_URLS.keys())


def print_all_urls():
    """Print all state URLs in a formatted table."""
    print("\n" + "="*80)
    print("US SECRETARY OF STATE BUSINESS SEARCH URLs")
    print("="*80)
    
    for code, info in sorted(STATE_URLS.items()):
        difficulty = "⭐" * info.get("difficulty", 2)
        print(f"\n{code} - {info['name']} {difficulty}")
        print(f"   URL: {info['url']}")
        if info.get("notes"):
            print(f"   Note: {info['notes']}")


# ============================================================================
# ALTERNATIVE DATA SOURCES
# ============================================================================

ALTERNATIVE_SOURCES = """
COMMERCIAL DATA PROVIDERS (Recommended for Production)
=======================================================

1. OpenCorporates (https://opencorporates.com)
   - Largest open database of companies
   - API access available
   - Covers all US states + international
   - Free tier + paid plans
   
2. Cobalt Intelligence (https://cobaltintelligence.com)
   - Real-time Secretary of State data
   - API specifically for new business filings
   - Handles CAPTCHA and anti-bot measures
   
3. Dun & Bradstreet (https://www.dnb.com)
   - Comprehensive business data
   - Enterprise-level solution
   - Expensive but thorough

4. InfoGroup/Data.com
   - Marketing-focused business data
   - Good for lead generation
   
5. State-Specific APIs
   - Some states offer official APIs
   - California: https://businesssearch.sos.ca.gov/
   - Check each state for data.gov resources

GOVERNMENT DATA PORTALS
=======================
- https://www.data.gov (Federal business datasets)
- Many states have open data portals with business information
"""


if __name__ == "__main__":
    print_all_urls()
    print("\n" + ALTERNATIVE_SOURCES)

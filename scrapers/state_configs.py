"""
State SOS Configuration Registry
=================================
Data-driven configuration for all 50 US states + DC + PR Secretary of State
business search portals.

Each config contains:
 - SOS website URL and search endpoint
 - Search strategy (suffix_search, date_search, api_json, api_xml, asp_form)
 - Trade-keyword suffixes for local service business discovery
 - CSS selectors / field mappings for parsing results
 - Rate limit settings per state
 - Whether Playwright (JS rendering) is required
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

# ============================================================================
# SEARCH TERMS (ALPHABET + GENERIC SUFFIXES)
# These strings are fed into the SOS search to iterate effectively through ALL businesses.
# ============================================================================

import string

# Business-name suffixes used by the Universal Search Matrix.
# These are used by ALL states regardless of primary strategy.
BUSINESS_SUFFIXES: List[str] = [
    "LLC", "Inc", "Corp", "Services", "Consulting", "Management", "Holdings",
]

# Extended trade suffixes: business suffixes + alphabet scan.
TRADE_SUFFIXES: List[str] = BUSINESS_SUFFIXES + [
    "Company", "Solutions", "Group", "Partners",
] + list(string.ascii_uppercase)

# ============================================================================
# SEARCH MATRIX CLASSIFICATION
# ============================================================================
# These sets drive which primary strategy each state uses.

# States that support Advanced Search with date-range filtering (last 7-30 days)
DATE_RANGE_STATES = {
    "CA", "CO", "GA", "ID", "IL", "IN", "IA", "KS", "KY", "LA",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
    "TN", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

# States where sequential entity-ID iteration works best
SEQUENTIAL_ID_STATES = {
    "FL", "TX", "DE", "NY", "AL", "AK", "AZ", "AR", "CT", "DC",
    "HI", "MD", "ME", "PR",
}

# Specialized portal overrides (handled by dedicated scrapers)
SPECIALIZED_STATES = {"FL", "TX", "NY"}


@dataclass
class StateSOSConfig:
    """Configuration for a single state's SOS portal."""

    state_code: str
    state_name: str
    sos_url: str                          # Main SOS business search page
    search_endpoint: str = ""             # API / form action URL
    search_strategy: str = "suffix_search"  # suffix_search | date_search | api_json | api_xml | asp_form
    requires_js: bool = False             # True = needs Playwright/headless browser
    rate_limit_delay: float = 2.0         # Seconds between requests
    max_results_per_query: int = 50       # Max rows the SOS returns per page
    result_selector: str = ""             # CSS selector for result rows
    name_selector: str = ""               # CSS selector for business name within a row
    date_selector: str = ""               # CSS selector for filing date within a row
    detail_link_selector: str = ""        # CSS selector for detail page link
    notes: str = ""                       # Human-readable notes about this SOS portal
    extra: Dict = field(default_factory=dict)  # State-specific params (viewstate keys, etc.)


# ============================================================================
# STATE CONFIGURATIONS
# ============================================================================

STATE_CONFIGS: Dict[str, StateSOSConfig] = {
    # ── ALABAMA ──
    "AL": StateSOSConfig(
        state_code="AL", state_name="Alabama",
        sos_url="https://www.sos.alabama.gov/government-records/business-entity-records",
        search_endpoint="https://arc-sos.state.al.us/cgi/corpdetl.mbr/output",
        search_strategy="sequential_id",
        rate_limit_delay=2.0,
        result_selector="table tr",
        name_selector="td:nth-child(1)",
        date_selector="td:nth-child(3)",
        notes="Alabama SOS has a CGI-based search — sequential ID iteration",
    ),
    # ── ALASKA ──
    "AK": StateSOSConfig(
        state_code="AK", state_name="Alaska",
        sos_url="https://www.commerce.alaska.gov/cbp/main/search/entities",
        search_endpoint="https://www.commerce.alaska.gov/cbp/main/search/entities",
        search_strategy="sequential_id",
        rate_limit_delay=2.0,
        notes="Alaska Commerce search portal — sequential ID iteration",
    ),
    # ── ARIZONA ──
    "AZ": StateSOSConfig(
        state_code="AZ", state_name="Arizona",
        sos_url="https://ecorp.azcc.gov/EntitySearch/Index",
        search_endpoint="https://ecorp.azcc.gov/EntitySearch/Index",
        search_strategy="sequential_id",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Arizona Corporation Commission — sequential ID iteration",
    ),
    # ── ARKANSAS ──
    "AR": StateSOSConfig(
        state_code="AR", state_name="Arkansas",
        sos_url="https://www.sos.arkansas.gov/corps/search_all.php",
        search_endpoint="https://www.sos.arkansas.gov/corps/search_all.php",
        search_strategy="sequential_id",
        rate_limit_delay=2.0,
        notes="Arkansas SOS PHP search — sequential ID iteration",
    ),
    # ── CALIFORNIA ──
    "CA": StateSOSConfig(
        state_code="CA", state_name="California",
        sos_url="https://bizfileonline.sos.ca.gov/search/business",
        search_endpoint="https://bizfileonline.sos.ca.gov/api/Records/businesssearch",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=3.0,
        notes="California — date-range search via JSON API (hCaptcha possible)",
    ),
    # ── COLORADO ──
    "CO": StateSOSConfig(
        state_code="CO", state_name="Colorado",
        sos_url="https://www.sos.state.co.us/biz/BusinessEntityCriteriaExt.do",
        search_endpoint="https://www.sos.state.co.us/biz/BusinessEntityResults.do",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Colorado SOS — date-range search",
    ),
    # ── CONNECTICUT ──
    "CT": StateSOSConfig(
        state_code="CT", state_name="Connecticut",
        sos_url="https://service.ct.gov/business/s/onlinebusinesssearch",
        search_endpoint="https://service.ct.gov/business/s/onlinebusinesssearch",
        search_strategy="sequential_id",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Connecticut Salesforce portal — sequential ID iteration",
    ),
    # ── DELAWARE ──
    "DE": StateSOSConfig(
        state_code="DE", state_name="Delaware",
        sos_url="https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
        search_endpoint="https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
        search_strategy="sequential_id",
        rate_limit_delay=3.0,
        notes="Delaware ICIS — sequential ID via ASP.NET ViewState",
    ),
    # ── FLORIDA ──
    "FL": StateSOSConfig(
        state_code="FL", state_name="Florida",
        sos_url="https://search.sunbiz.org/Inquiry/CorporationSearch/ByName",
        search_endpoint="https://search.sunbiz.org/Inquiry/CorporationSearch/SearchByName",
        search_strategy="suffix_search",
        rate_limit_delay=2.0,
        result_selector="table.search-results tr",
        name_selector="td:nth-child(1) a",
        date_selector="td:nth-child(3)",
        detail_link_selector="td:nth-child(1) a",
        notes="Florida Sunbiz - most reliable SOS portal",
    ),
    # ── GEORGIA ──
    "GA": StateSOSConfig(
        state_code="GA", state_name="Georgia",
        sos_url="https://ecorp.sos.ga.gov/BusinessSearch",
        search_endpoint="https://ecorp.sos.ga.gov/BusinessSearch",
        search_strategy="suffix_search",
        requires_js=True,
        rate_limit_delay=2.0,
        result_selector="table tr",
        name_selector="td:nth-child(1)",
        date_selector="td:nth-child(3)",
        notes="Georgia SOS — requires Playwright (JS) — dedicated scraper",
    ),
    # ── HAWAII ──
    "HI": StateSOSConfig(
        state_code="HI", state_name="Hawaii",
        sos_url="https://hbe.ehawaii.gov/documents/search.html",
        search_endpoint="https://hbe.ehawaii.gov/documents/search.html",
        search_strategy="sequential_id",
        rate_limit_delay=2.0,
        notes="Hawaii eHawaii portal — sequential ID iteration",
    ),
    # ── IDAHO ──
    "ID": StateSOSConfig(
        state_code="ID", state_name="Idaho",
        sos_url="https://sosbiz.idaho.gov/search/business",
        search_endpoint="https://sosbiz.idaho.gov/api/Records/businesssearch",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Idaho Tyler Technologies API — date-range search",
    ),
    # ── ILLINOIS ──
    "IL": StateSOSConfig(
        state_code="IL", state_name="Illinois",
        sos_url="https://apps.ilsos.gov/corporatellc/",
        search_endpoint="https://apps.ilsos.gov/corporatellc/CorporateLlcController",
        search_strategy="suffix_search",
        rate_limit_delay=2.0,
        result_selector="table tr",
        name_selector="td:nth-child(1)",
        date_selector="td:nth-child(3)",
        notes="Illinois SOS — Java servlet form POST — dedicated scraper",
    ),
    # ── INDIANA ──
    "IN": StateSOSConfig(
        state_code="IN", state_name="Indiana",
        sos_url="https://bsd.sos.in.gov/publicbusinesssearch",
        search_endpoint="https://bsd.sos.in.gov/publicbusinesssearch",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Indiana — date-range search",
    ),
    # ── IOWA ──
    "IA": StateSOSConfig(
        state_code="IA", state_name="Iowa",
        sos_url="https://sos.iowa.gov/search/business/(S(x))/search.aspx",
        search_endpoint="https://sos.iowa.gov/search/business/(S(x))/search.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Iowa SOS — date-range search via ASP.NET",
    ),
    # ── KANSAS ──
    "KS": StateSOSConfig(
        state_code="KS", state_name="Kansas",
        sos_url="https://www.kansas.gov/bess/flow/main?execution=e1s1",
        search_endpoint="https://www.kansas.gov/bess/flow/main",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Kansas — date-range search",
    ),
    # ── KENTUCKY ──
    "KY": StateSOSConfig(
        state_code="KY", state_name="Kentucky",
        sos_url="https://web.sos.ky.gov/bussearchnprofile/(S(x))/default.aspx",
        search_endpoint="https://web.sos.ky.gov/bussearchnprofile/(S(x))/default.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Kentucky — date-range search via ASP.NET",
    ),
    # ── LOUISIANA ──
    "LA": StateSOSConfig(
        state_code="LA", state_name="Louisiana",
        sos_url="https://coraweb.sos.la.gov/CommercialSearch/CommercialSearch.aspx",
        search_endpoint="https://coraweb.sos.la.gov/CommercialSearch/CommercialSearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Louisiana CORA — date-range search",
    ),
    # ── MAINE ──
    "ME": StateSOSConfig(
        state_code="ME", state_name="Maine",
        sos_url="https://icrs.informe.org/nei-sos-icrs/ICRS?MainPage=x",
        search_endpoint="https://icrs.informe.org/nei-sos-icrs/ICRS",
        search_strategy="sequential_id",
        rate_limit_delay=2.0,
        notes="Maine InforME portal — sequential ID iteration",
    ),
    # ── MARYLAND ──
    "MD": StateSOSConfig(
        state_code="MD", state_name="Maryland",
        sos_url="https://egov.maryland.gov/BusinessExpress/EntitySearch",
        search_endpoint="https://egov.maryland.gov/BusinessExpress/EntitySearch",
        search_strategy="sequential_id",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Maryland eGov portal — sequential ID iteration",
    ),
    # ── MASSACHUSETTS ──
    "MA": StateSOSConfig(
        state_code="MA", state_name="Massachusetts",
        sos_url="https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx",
        search_endpoint="https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Massachusetts — date-range search via ASP.NET",
    ),
    # ── MICHIGAN ──
    "MI": StateSOSConfig(
        state_code="MI", state_name="Michigan",
        sos_url="https://cofs.lara.state.mi.us/SearchApi/Search/Search",
        search_endpoint="https://cofs.lara.state.mi.us/SearchApi/Search/Search",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Michigan LARA JSON API — date-range search",
    ),
    # ── MINNESOTA ──
    "MN": StateSOSConfig(
        state_code="MN", state_name="Minnesota",
        sos_url="https://mblsportal.sos.state.mn.us/Business/Search",
        search_endpoint="https://mblsportal.sos.state.mn.us/Business/Search",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Minnesota SOS — date-range search",
    ),
    # ── MISSISSIPPI ──
    "MS": StateSOSConfig(
        state_code="MS", state_name="Mississippi",
        sos_url="https://corp.sos.ms.gov/corp/portal/c/page/corpBusinessIdSearch/portal.aspx",
        search_endpoint="https://corp.sos.ms.gov/corp/portal/c/page/corpBusinessIdSearch/portal.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Mississippi — date-range search",
    ),
    # ── MISSOURI ──
    "MO": StateSOSConfig(
        state_code="MO", state_name="Missouri",
        sos_url="https://bsd.sos.mo.gov/BusinessEntity/BESearch.aspx",
        search_endpoint="https://bsd.sos.mo.gov/BusinessEntity/BESearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Missouri — date-range search",
    ),
    # ── MONTANA ──
    "MT": StateSOSConfig(
        state_code="MT", state_name="Montana",
        sos_url="https://biz.sosmt.gov/search",
        search_endpoint="https://biz.sosmt.gov/api/Records/businesssearch",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Montana Tyler Technologies API — date-range search",
    ),
    # ── NEBRASKA ──
    "NE": StateSOSConfig(
        state_code="NE", state_name="Nebraska",
        sos_url="https://www.nebraska.gov/sos/corp/corpsearch.cgi",
        search_endpoint="https://www.nebraska.gov/sos/corp/corpsearch.cgi",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Nebraska — date-range search",
    ),
    # ── NEVADA ──
    "NV": StateSOSConfig(
        state_code="NV", state_name="Nevada",
        sos_url="https://esos.nv.gov/EntitySearch/OnlineEntitySearch",
        search_endpoint="https://esos.nv.gov/EntitySearch/OnlineEntitySearch",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=3.0,
        notes="Nevada eSOS — date-range search (aggressive bot detection)",
    ),
    # ── NEW HAMPSHIRE ──
    "NH": StateSOSConfig(
        state_code="NH", state_name="New Hampshire",
        sos_url="https://quickstart.sos.nh.gov/online/BusinessInquire",
        search_endpoint="https://quickstart.sos.nh.gov/online/BusinessInquire",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="New Hampshire — date-range search",
    ),
    # ── NEW JERSEY ──
    "NJ": StateSOSConfig(
        state_code="NJ", state_name="New Jersey",
        sos_url="https://www.njportal.com/DOR/BusinessNameSearch",
        search_endpoint="https://www.njportal.com/DOR/BusinessNameSearch",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="New Jersey — date-range search",
    ),
    # ── NEW MEXICO ──
    "NM": StateSOSConfig(
        state_code="NM", state_name="New Mexico",
        sos_url="https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch",
        search_endpoint="https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="New Mexico SOS — date-range search",
    ),
    # ── NEW YORK ──
    "NY": StateSOSConfig(
        state_code="NY", state_name="New York",
        sos_url="https://appext20.dos.ny.gov/corp_public/CORPSEARCH.ENTITY_SEARCH_ENTRY",
        search_endpoint="https://appext20.dos.ny.gov/corp_public/CORPSEARCH.ENTITY_SEARCH_ENTRY",
        search_strategy="sequential_id",
        requires_js=True,
        rate_limit_delay=3.0,
        notes="New York DOS — sequential ID + keyword search (specialized portal)",
    ),
    # ── NORTH CAROLINA ──
    "NC": StateSOSConfig(
        state_code="NC", state_name="North Carolina",
        sos_url="https://www.sosnc.gov/online_services/search/by_title/_Business_Registration",
        search_endpoint="https://www.sosnc.gov/online_services/search/by_title/_Business_Registration",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="North Carolina SOS — date-range search",
    ),
    # ── NORTH DAKOTA ──
    "ND": StateSOSConfig(
        state_code="ND", state_name="North Dakota",
        sos_url="https://firststop.sos.nd.gov/search/business",
        search_endpoint="https://firststop.sos.nd.gov/api/Records/businesssearch",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="North Dakota Tyler Technologies API — date-range search",
    ),
    # ── OHIO ──
    "OH": StateSOSConfig(
        state_code="OH", state_name="Ohio",
        sos_url="https://businesssearch.ohiosos.gov/",
        search_endpoint="https://businesssearch.ohiosos.gov/",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Ohio SOS — date-range search",
    ),
    # ── OKLAHOMA ──
    "OK": StateSOSConfig(
        state_code="OK", state_name="Oklahoma",
        sos_url="https://www.sos.ok.gov/corp/corpInquiryFind.aspx",
        search_endpoint="https://www.sos.ok.gov/corp/corpInquiryFind.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Oklahoma — date-range search",
    ),
    # ── OREGON ──
    "OR": StateSOSConfig(
        state_code="OR", state_name="Oregon",
        sos_url="http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.login",
        search_endpoint="http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.do_name_srch",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Oregon — date-range search",
    ),
    # ── PENNSYLVANIA ──
    "PA": StateSOSConfig(
        state_code="PA", state_name="Pennsylvania",
        sos_url="https://www.corporations.pa.gov/search/corpsearch",
        search_endpoint="https://www.corporations.pa.gov/search/corpsearch",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Pennsylvania — date-range search",
    ),
    # ── RHODE ISLAND ──
    "RI": StateSOSConfig(
        state_code="RI", state_name="Rhode Island",
        sos_url="http://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSearch.aspx",
        search_endpoint="http://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Rhode Island — date-range search",
    ),
    # ── SOUTH CAROLINA ──
    "SC": StateSOSConfig(
        state_code="SC", state_name="South Carolina",
        sos_url="https://businessfilings.sc.gov/BusinessFiling/Entity/Search",
        search_endpoint="https://businessfilings.sc.gov/BusinessFiling/Entity/Search",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="South Carolina SOS — date-range search",
    ),
    # ── SOUTH DAKOTA ──
    "SD": StateSOSConfig(
        state_code="SD", state_name="South Dakota",
        sos_url="https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx",
        search_endpoint="https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="South Dakota — date-range search",
    ),
    # ── TENNESSEE ──
    "TN": StateSOSConfig(
        state_code="TN", state_name="Tennessee",
        sos_url="https://tnbear.tn.gov/Ecommerce/FilingSearch.aspx",
        search_endpoint="https://tnbear.tn.gov/Ecommerce/FilingSearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Tennessee BEAR — date-range search",
    ),
    # ── TEXAS ──
    "TX": StateSOSConfig(
        state_code="TX", state_name="Texas",
        sos_url="https://mycpa.cpa.state.tx.us/coa/coaSearchBtn",
        search_endpoint="https://mycpa.cpa.state.tx.us/coa/coaSearchBtn",
        search_strategy="sequential_id",
        rate_limit_delay=3.0,
        notes="Texas — sequential ID + SEC EDGAR fallback (specialized portal)",
    ),
    # ── UTAH ──
    "UT": StateSOSConfig(
        state_code="UT", state_name="Utah",
        sos_url="https://secure.utah.gov/bes/",
        search_endpoint="https://secure.utah.gov/bes/action/search",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Utah — date-range search",
    ),
    # ── VERMONT ──
    "VT": StateSOSConfig(
        state_code="VT", state_name="Vermont",
        sos_url="https://bizfilings.vermont.gov/online/BusinessInquire",
        search_endpoint="https://bizfilings.vermont.gov/online/BusinessInquire",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Vermont — date-range search",
    ),
    # ── VIRGINIA ──
    "VA": StateSOSConfig(
        state_code="VA", state_name="Virginia",
        sos_url="https://cis.scc.virginia.gov/EntitySearch/Index",
        search_endpoint="https://cis.scc.virginia.gov/EntitySearch/Index",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="Virginia SCC — date-range search",
    ),
    # ── WASHINGTON ──
    "WA": StateSOSConfig(
        state_code="WA", state_name="Washington",
        sos_url="https://ccfs.sos.wa.gov/#/AdvancedSearch",
        search_endpoint="https://ccfs.sos.wa.gov/api/BusinessSearch",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Washington SOS JSON API — date-range search",
    ),
    # ── WEST VIRGINIA ──
    "WV": StateSOSConfig(
        state_code="WV", state_name="West Virginia",
        sos_url="https://apps.wv.gov/SOS/BusinessEntitySearch/",
        search_endpoint="https://apps.wv.gov/SOS/BusinessEntitySearch/",
        search_strategy="date_search",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="West Virginia SOS — date-range search",
    ),
    # ── WISCONSIN ──
    "WI": StateSOSConfig(
        state_code="WI", state_name="Wisconsin",
        sos_url="https://www.wdfi.org/apps/CorpSearch/Search.aspx",
        search_endpoint="https://www.wdfi.org/apps/CorpSearch/Search.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Wisconsin WDFI — date-range search",
    ),
    # ── WYOMING ──
    "WY": StateSOSConfig(
        state_code="WY", state_name="Wyoming",
        sos_url="https://wyobiz.wyo.gov/Business/FilingSearch.aspx",
        search_endpoint="https://wyobiz.wyo.gov/Business/FilingSearch.aspx",
        search_strategy="date_search",
        rate_limit_delay=2.0,
        notes="Wyoming — date-range search",
    ),
    # ── DISTRICT OF COLUMBIA ──
    "DC": StateSOSConfig(
        state_code="DC", state_name="District of Columbia",
        sos_url="https://corponline.dcra.dc.gov/BizEntity.aspx/NavigateToSearch",
        search_endpoint="https://corponline.dcra.dc.gov/BizEntity.aspx/NavigateToSearch",
        search_strategy="sequential_id",
        requires_js=True,
        rate_limit_delay=2.0,
        notes="DC DCRA portal — sequential ID iteration",
    ),
    # ── PUERTO RICO ──
    "PR": StateSOSConfig(
        state_code="PR", state_name="Puerto Rico",
        sos_url="https://prcorpfiling.f1hst.com/CorporationSearch.aspx",
        search_endpoint="https://prcorpfiling.f1hst.com/CorporationSearch.aspx",
        search_strategy="sequential_id",
        rate_limit_delay=2.0,
        notes="Puerto Rico Corp filing — sequential ID iteration",
    ),
}


def get_state_config(state_code: str) -> StateSOSConfig:
    """Get SOS config for a state code. Raises KeyError if not found."""
    return STATE_CONFIGS[state_code.upper()]


def get_all_state_codes() -> List[str]:
    """Return all configured state codes."""
    return list(STATE_CONFIGS.keys())


def get_states_by_strategy(strategy: str) -> List[str]:
    """Return state codes that use a given search strategy."""
    return [k for k, v in STATE_CONFIGS.items() if v.search_strategy == strategy]


def get_states_requiring_js() -> List[str]:
    """Return state codes that require Playwright/headless browser."""
    return [k for k, v in STATE_CONFIGS.items() if v.requires_js]

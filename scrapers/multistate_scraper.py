#!/usr/bin/env python3
"""
Multi-State SOS Scraper – Playwright-based

Supports scraping from multiple US State Secretary of State websites.
Each state has its own configuration for selectors, URLs, and parsing logic.

Supported States:
- FL: Florida Sunbiz (fully working)
- GA: Georgia eCorp
- NY: New York DOS
- PA: Pennsylvania Corporations
- CA: California BizFile
- TX: Texas SOS

Usage:
    scraper = MultiStateScraper(states=['FL', 'GA'], on_log=print)
    await scraper.scrape(keywords=['plumber', 'hvac'], max_per_keyword=20)
"""

import asyncio
import json
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Optional, Callable, Set
from dataclasses import dataclass, field

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class StateConfig:
    """Configuration for scraping a specific state's SOS website."""
    code: str
    name: str
    search_url: str
    search_input_selector: str
    search_button_selector: str
    results_table_selector: str
    row_selector: str
    name_selector: str
    doc_number_selector: str
    status_selector: str
    date_selector: str
    detail_link_selector: str
    next_page_selector: str = ""
    requires_captcha: bool = False
    wait_after_search: int = 2000  # ms to wait after search
    rate_limit_delay: float = 1.5  # seconds between requests
    extra: Dict = field(default_factory=dict)


# State configurations
STATE_CONFIGS: Dict[str, StateConfig] = {
    "FL": StateConfig(
        code="FL",
        name="Florida",
        search_url="https://search.sunbiz.org/Inquiry/CorporationSearch/ByName",
        search_input_selector="#SearchTerm",
        search_button_selector="input[type='submit'][value='Search Now']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        next_page_selector="a[title='Next List']",
        wait_after_search=2000,
    ),
    "GA": StateConfig(
        code="GA",
        name="Georgia",
        search_url="https://ecorp.sos.ga.gov/BusinessSearch",
        search_input_selector="#txtBusinessName",
        search_button_selector="#btnSearch",
        results_table_selector="table#grid_businessSearchResults",
        row_selector="table#grid_businessSearchResults tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        next_page_selector=".k-pager-nav.k-link:has-text('>')",
        wait_after_search=3000,
        extra={"requires_js": True}
    ),
    "NY": StateConfig(
        code="NY",
        name="New York",
        search_url="https://apps.dos.ny.gov/publicInquiry/",
        search_input_selector="#search-businessname",
        search_button_selector="button[type='submit']",
        results_table_selector="table.dataTable",
        row_selector="table.dataTable tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(4)",
        date_selector="td:nth-child(3)",
        detail_link_selector="td:nth-child(1) a",
        next_page_selector=".paginate_button.next",
        wait_after_search=3000,
    ),
    "PA": StateConfig(
        code="PA",
        name="Pennsylvania",
        search_url="https://www.corporations.pa.gov/search/corpsearch",
        search_input_selector="#txtSearchTerms",
        search_button_selector="#btnSearch",
        results_table_selector="table#dataTable",
        row_selector="table#dataTable tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "CA": StateConfig(
        code="CA",
        name="California",
        search_url="https://bizfileonline.sos.ca.gov/search/business",
        search_input_selector="input[formcontrolname='SearchValue']",
        search_button_selector="button[type='submit']",
        results_table_selector="table.p-datatable-table",
        row_selector="table.p-datatable-table tbody tr",
        name_selector="td:nth-child(1)",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(4)",
        date_selector="td:nth-child(3)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=4000,
        requires_captcha=True,
        extra={"angular_app": True}
    ),
    "TX": StateConfig(
        code="TX",
        name="Texas",
        search_url="https://mycpa.cpa.state.tx.us/coa/Index.html",
        search_input_selector="#taxpayerName",
        search_button_selector="input[type='submit']",
        results_table_selector="table.results",
        row_selector="table.results tbody tr",
        name_selector="td:nth-child(1)",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
        extra={"requires_login": False}  # Using public comptroller search
    ),
    # ═══════════════════════════════════════════════════════════════════
    # NEW STATES - Batch 2 (18 total active states)
    # ═══════════════════════════════════════════════════════════════════
    "OH": StateConfig(
        code="OH",
        name="Ohio",
        search_url="https://businesssearch.ohiosos.gov/",
        search_input_selector="#BusinessName",
        search_button_selector="button[type='submit']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "NC": StateConfig(
        code="NC",
        name="North Carolina",
        search_url="https://www.sosnc.gov/online_services/search/by_title/_Business_Registration",
        search_input_selector="#SearchCriteria",
        search_button_selector="input[type='submit']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "IL": StateConfig(
        code="IL",
        name="Illinois",
        search_url="https://apps.ilsos.gov/corporatellc/",
        search_input_selector="#corporateName",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "NJ": StateConfig(
        code="NJ",
        name="New Jersey",
        search_url="https://www.njportal.com/DOR/BusinessNameSearch",
        search_input_selector="#BusinessName",
        search_button_selector="button[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1)",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "VA": StateConfig(
        code="VA",
        name="Virginia",
        search_url="https://cis.scc.virginia.gov/EntitySearch/Index",
        search_input_selector="#EntityName",
        search_button_selector="button[type='submit']",
        results_table_selector="table#results",
        row_selector="table#results tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "WA": StateConfig(
        code="WA",
        name="Washington",
        search_url="https://ccfs.sos.wa.gov/#/BusinessSearch",
        search_input_selector="input[placeholder*='Business Name']",
        search_button_selector="button.search-button",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1)",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3500,
    ),
    "AZ": StateConfig(
        code="AZ",
        name="Arizona",
        search_url="https://ecorp.azcc.gov/EntitySearch/Index",
        search_input_selector="#EntityName",
        search_button_selector="button[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "MA": StateConfig(
        code="MA",
        name="Massachusetts",
        search_url="https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx",
        search_input_selector="#MainContent_txtEntityName",
        search_button_selector="#MainContent_btnSearch",
        results_table_selector="table#MainContent_grdSearchResults",
        row_selector="table#MainContent_grdSearchResults tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "MI": StateConfig(
        code="MI",
        name="Michigan",
        search_url="https://cofs.lara.state.mi.us/corpweb/CorpSearch/CorpSearch.aspx",
        search_input_selector="#txtEntityName",
        search_button_selector="#btnSearch",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "TN": StateConfig(
        code="TN",
        name="Tennessee",
        search_url="https://tnbear.tn.gov/Ecommerce/FilingSearch.aspx",
        search_input_selector="#MainContent_txtSearchName",
        search_button_selector="#MainContent_btnSearch",
        results_table_selector="table#MainContent_gvSearchResults",
        row_selector="table#MainContent_gvSearchResults tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "IN": StateConfig(
        code="IN",
        name="Indiana",
        search_url="https://bsd.sos.in.gov/publicbusinesssearch",
        search_input_selector="#businessName",
        search_button_selector="button[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "CO": StateConfig(
        code="CO",
        name="Colorado",
        search_url="https://www.sos.state.co.us/biz/BusinessEntityCriteriaExt.do",
        search_input_selector="#entityName",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    # ═══════════════════════════════════════════════════════════════════
    # STANDARD HTML STATES - Batch 3 (No JS required)
    # ═══════════════════════════════════════════════════════════════════
    "OK": StateConfig(
        code="OK",
        name="Oklahoma",
        search_url="https://www.sos.ok.gov/corp/corpInquiryFind.aspx",
        search_input_selector="#ctl00_DefaultContent_CorpNameSearch1_txtBusinessName",
        search_button_selector="#ctl00_DefaultContent_CorpNameSearch1_btnSearch",
        results_table_selector="table#ctl00_DefaultContent_CorpNameSearch1_GridView1",
        row_selector="table#ctl00_DefaultContent_CorpNameSearch1_GridView1 tr:not(:first-child)",
        name_selector="td:nth-child(1)",
        doc_number_selector="td:nth-child(2) a",
        status_selector="td:nth-child(4)",
        date_selector="td:nth-child(3)",
        detail_link_selector="td:nth-child(2) a",
        wait_after_search=2000,
    ),
    "MO": StateConfig(
        code="MO",
        name="Missouri",
        search_url="https://bsd.sos.mo.gov/BusinessEntity/BESearch.aspx",
        search_input_selector="#ctl00_ctl00_ContentPlaceHolderMain_ContentPlaceHolderMainSingle_ppBESearch_bsPanel_txtBE",
        search_button_selector="#ctl00_ctl00_ContentPlaceHolderMain_ContentPlaceHolderMainSingle_ppBESearch_bsPanel_stdbtnSearch_LinkStandardButton",
        results_table_selector="table.rgMasterTable",
        row_selector="table.rgMasterTable tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "SC": StateConfig(
        code="SC",
        name="South Carolina",
        search_url="https://businessfilings.sc.gov/BusinessFiling/Entity/Search",
        search_input_selector="#SearchTerm",
        search_button_selector="button[type='submit']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "UT": StateConfig(
        code="UT",
        name="Utah",
        search_url="https://businessregistration.utah.gov/EntitySearch/OnlineEntitySearch",
        search_input_selector="#searchTerm",
        search_button_selector="button[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "VT": StateConfig(
        code="VT",
        name="Vermont",
        search_url="https://bizfilings.vermont.gov/online/BusinessInquire",
        search_input_selector="#SearchTerm",
        search_button_selector="button[type='submit']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "WI": StateConfig(
        code="WI",
        name="Wisconsin",
        search_url="https://www.wdfi.org/apps/CorpSearch/Search.aspx",
        search_input_selector="#txtEntityName",
        search_button_selector="#btnSearch",
        results_table_selector="table#grdSearchResults",
        row_selector="table#grdSearchResults tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "NE": StateConfig(
        code="NE",
        name="Nebraska",
        search_url="https://www.nebraska.gov/sos/corp/corpsearch.cgi",
        search_input_selector="input[name='corpname']",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2000,
    ),
    "NH": StateConfig(
        code="NH",
        name="New Hampshire",
        search_url="https://quickstart.sos.nh.gov/online/BusinessInquire",
        search_input_selector="#SearchTerm",
        search_button_selector="button[type='submit']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "KS": StateConfig(
        code="KS",
        name="Kansas",
        search_url="https://www.kansas.gov/bess/flow/main?execution=e1s1",
        search_input_selector="#Name",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "KY": StateConfig(
        code="KY",
        name="Kentucky",
        search_url="https://web.sos.ky.gov/ftshow/default.aspx",
        search_input_selector="#txtName",
        search_button_selector="#btnSearch",
        results_table_selector="table#grdBEName",
        row_selector="table#grdBEName tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "LA": StateConfig(
        code="LA",
        name="Louisiana",
        search_url="https://coraweb.sos.la.gov/CommercialSearch/CommercialSearch.aspx",
        search_input_selector="#ctl00_ContentPlaceHolder1_txtName",
        search_button_selector="#ctl00_ContentPlaceHolder1_btnSearch",
        results_table_selector="table#ctl00_ContentPlaceHolder1_gvResults",
        row_selector="table#ctl00_ContentPlaceHolder1_gvResults tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "AR": StateConfig(
        code="AR",
        name="Arkansas",
        search_url="https://www.sos.arkansas.gov/corps/search_all.php",
        search_input_selector="input[name='ESSION_search']",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2000,
    ),
    "SD": StateConfig(
        code="SD",
        name="South Dakota",
        search_url="https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx",
        search_input_selector="#MainContent_txtSearchName",
        search_button_selector="#MainContent_btnSearch",
        results_table_selector="table#MainContent_gvSearchResults",
        row_selector="table#MainContent_gvSearchResults tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "OR": StateConfig(
        code="OR",
        name="Oregon",
        search_url="http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.login",
        search_input_selector="input[name='p_name']",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(4)",
        date_selector="td:nth-child(3)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2000,
    ),
    "RI": StateConfig(
        code="RI",
        name="Rhode Island",
        search_url="http://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSearch.aspx",
        search_input_selector="#MainContent_txtEntityName",
        search_button_selector="#MainContent_btnSearch",
        results_table_selector="table#MainContent_grdSearchResults",
        row_selector="table#MainContent_grdSearchResults tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "MS": StateConfig(
        code="MS",
        name="Mississippi",
        search_url="https://corp.sos.ms.gov/corp/portal/c/page/corpBusinessIdSearch/portal.aspx",
        search_input_selector="#txtBusName",
        search_button_selector="#btnSearch",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "NM": StateConfig(
        code="NM",
        name="New Mexico",
        search_url="https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch",
        search_input_selector="#SearchTerm",
        search_button_selector="button[type='submit']",
        results_table_selector="table.table",
        row_selector="table.table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "ME": StateConfig(
        code="ME",
        name="Maine",
        search_url="https://icrs.informe.org/nei-sos-icrs/ICRS?MainPage=x",
        search_input_selector="input[name='SearchID']",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "DE": StateConfig(
        code="DE",
        name="Delaware",
        search_url="https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
        search_input_selector="#txtEntityName",
        search_button_selector="#btnSearch",
        results_table_selector="table#grdSearchResults",
        row_selector="table#grdSearchResults tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=3000,
    ),
    "HI": StateConfig(
        code="HI",
        name="Hawaii",
        search_url="https://hbe.ehawaii.gov/documents/search.html",
        search_input_selector="#searchbar",
        search_button_selector="button[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "AL": StateConfig(
        code="AL",
        name="Alabama",
        search_url="https://arc-sos.state.al.us/cgi/corpdetl.mbr/output",
        search_input_selector="input[name='corpname']",
        search_button_selector="input[type='submit']",
        results_table_selector="table",
        row_selector="table tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(4)",
        date_selector="td:nth-child(3)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2000,
    ),
    "AK": StateConfig(
        code="AK",
        name="Alaska",
        search_url="https://www.commerce.alaska.gov/cbp/main/search/entities",
        search_input_selector="#search",
        search_button_selector="button[type='submit']",
        results_table_selector="table",
        row_selector="table tbody tr",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
    "IA": StateConfig(
        code="IA",
        name="Iowa",
        search_url="https://sos.iowa.gov/search/business/(S(x))/search.aspx",
        search_input_selector="#txtNameSearch",
        search_button_selector="#btnSearch",
        results_table_selector="table#grdResults",
        row_selector="table#grdResults tr:not(:first-child)",
        name_selector="td:nth-child(1) a",
        doc_number_selector="td:nth-child(2)",
        status_selector="td:nth-child(3)",
        date_selector="td:nth-child(4)",
        detail_link_selector="td:nth-child(1) a",
        wait_after_search=2500,
    ),
}


class MultiStateScraper:
    """
    Playwright-driven multi-state SOS scraper.
    
    Supports scraping from multiple state SOS websites simultaneously.
    """
    
    DEFAULT_KEYWORDS = [
        "HVAC", "plumber", "roofer", "cleaning", "remodeling",
        "electrician", "painter", "landscaper", "contractor",
    ]
    
    def __init__(
        self,
        states: List[str] = None,
        output_dir: str = "./sos_data",
        headless: bool = True,
        timeout: int = 60000,
        on_log: Optional[Callable[[str], None]] = None,
    ):
        self.states = states or ["FL"]
        self.output_dir = output_dir
        self.headless = headless
        self.timeout = timeout
        self.businesses: List[Dict] = []
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self._playwright = None
        self._log_cb = on_log or (lambda msg: None)
        self._stop_requested = False
        os.makedirs(output_dir, exist_ok=True)
    
    def _log(self, msg: str):
        logger.info(msg)
        self._log_cb(msg)
    
    def stop(self):
        """Request scraping to stop."""
        self._stop_requested = True
        self._log("Stop requested...")
    
    async def start_browser(self):
        """Initialize the Playwright browser."""
        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        await self.context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>false});"
        )
        self._log("Browser started.")
    
    async def stop_browser(self):
        """Close the browser."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._log("Browser stopped.")
    
    async def scrape(
        self,
        keywords: List[str] = None,
        max_per_keyword: int = 20,
    ) -> List[Dict]:
        """
        Main scraping entry point.
        
        Args:
            keywords: List of search keywords
            max_per_keyword: Max results per keyword per state
            
        Returns:
            List of scraped business records
        """
        keywords = keywords or self.DEFAULT_KEYWORDS
        self.businesses = []
        self._stop_requested = False
        
        await self.start_browser()
        
        try:
            for state_code in self.states:
                if self._stop_requested:
                    break
                    
                if state_code not in STATE_CONFIGS:
                    self._log(f"No configuration for state: {state_code}")
                    continue
                
                config = STATE_CONFIGS[state_code]
                self._log(f"\n{'='*60}")
                self._log(f"SCRAPING {config.name} ({config.code})")
                self._log(f"{'='*60}")
                
                state_results = await self._scrape_state(config, keywords, max_per_keyword)
                self.businesses.extend(state_results)
                
        finally:
            await self.stop_browser()
        
        self._log(f"\nTotal businesses scraped: {len(self.businesses)}")
        return self.businesses
    
    async def _scrape_state(
        self,
        config: StateConfig,
        keywords: List[str],
        max_per_keyword: int,
    ) -> List[Dict]:
        """Scrape a single state's SOS website."""
        results = []
        page = await self.context.new_page()
        page.set_default_timeout(self.timeout)
        
        try:
            # Navigate to search page
            # Use domcontentloaded for JS-heavy sites (Firebase, Angular, etc.)
            wait_strategy = "domcontentloaded" if config.extra.get("requires_js") else "networkidle"
            self._log(f"Navigating to {config.search_url}")
            try:
                await page.goto(config.search_url, wait_until=wait_strategy, timeout=30000)
            except Exception as nav_err:
                # Fallback: try domcontentloaded if networkidle times out
                self._log(f"  Navigation timeout, trying fallback...")
                await page.goto(config.search_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1)
            
            for keyword in keywords:
                if self._stop_requested:
                    break
                
                self._log(f"\nKEYWORD: {keyword}")
                keyword_results = await self._search_keyword(page, config, keyword, max_per_keyword)
                results.extend(keyword_results)
                
                # Rate limiting
                await asyncio.sleep(config.rate_limit_delay)
        
        except Exception as e:
            self._log(f"ERROR scraping {config.name}: {str(e)}")
        
        finally:
            await page.close()
        
        return results
    
    async def _search_keyword(
        self,
        page: Page,
        config: StateConfig,
        keyword: str,
        max_results: int,
    ) -> List[Dict]:
        """Search for a keyword and parse results."""
        results = []
        
        try:
            # Navigate back to search page if needed
            if config.search_url not in page.url:
                await page.goto(config.search_url, wait_until="networkidle")
                await asyncio.sleep(1)
            
            # Clear and fill search input
            search_input = await page.query_selector(config.search_input_selector)
            if not search_input:
                self._log(f"  Could not find search input: {config.search_input_selector}")
                return results
            
            await search_input.fill("")
            await search_input.fill(keyword)
            self._log(f"  Typed: {keyword}")
            
            # Click search button
            search_btn = await page.query_selector(config.search_button_selector)
            if search_btn:
                await search_btn.click()
                self._log("  Clicked search button")
            else:
                # Try Enter key
                await search_input.press("Enter")
                self._log("  Pressed Enter")
            
            # Wait for results
            await asyncio.sleep(config.wait_after_search / 1000)
            
            try:
                await page.wait_for_selector(config.results_table_selector, timeout=10000)
                self._log("  Results table loaded")
            except:
                self._log("  No results table found")
                return results
            
            # Parse results (with pagination)
            page_num = 1
            while len(results) < max_results:
                if self._stop_requested:
                    break
                
                page_results = await self._parse_results_page(page, config, keyword)
                if not page_results:
                    break
                
                results.extend(page_results)
                self._log(f"  Page {page_num}: Found {len(page_results)} businesses (total: {len(results)})")
                
                # Check for next page
                if len(results) >= max_results:
                    break
                
                if config.next_page_selector:
                    next_btn = await page.query_selector(config.next_page_selector)
                    if next_btn and await next_btn.is_visible():
                        try:
                            await next_btn.click()
                            await asyncio.sleep(2)
                            page_num += 1
                        except:
                            break
                    else:
                        break
                else:
                    break
            
        except Exception as e:
            self._log(f"  ERROR searching '{keyword}': {str(e)}")
        
        return results[:max_results]
    
    async def _parse_results_page(
        self,
        page: Page,
        config: StateConfig,
        keyword: str,
    ) -> List[Dict]:
        """Parse a single page of results."""
        results = []
        
        try:
            rows = await page.query_selector_all(config.row_selector)
            
            for row in rows:
                try:
                    # Extract business name
                    name_el = await row.query_selector(config.name_selector)
                    if not name_el:
                        continue
                    
                    name = (await name_el.inner_text()).strip()
                    
                    # Skip navigation links and empty names
                    if not name or name.lower() in ['previous', 'next', 'first', 'last']:
                        continue
                    
                    # Extract other fields
                    doc_number = ""
                    status = ""
                    filing_date = ""
                    detail_url = ""
                    
                    doc_el = await row.query_selector(config.doc_number_selector)
                    if doc_el:
                        doc_number = (await doc_el.inner_text()).strip()
                    
                    status_el = await row.query_selector(config.status_selector)
                    if status_el:
                        status = (await status_el.inner_text()).strip()
                    
                    date_el = await row.query_selector(config.date_selector)
                    if date_el:
                        filing_date = (await date_el.inner_text()).strip()
                    
                    link_el = await row.query_selector(config.detail_link_selector)
                    if link_el:
                        href = await link_el.get_attribute("href")
                        if href:
                            if href.startswith("http"):
                                detail_url = href
                            else:
                                # Relative URL - construct full URL
                                base = config.search_url.rsplit("/", 1)[0]
                                detail_url = f"{base}/{href.lstrip('/')}"
                    
                    business = {
                        "name": name,
                        "document_number": doc_number,
                        "status": status,
                        "filing_date": filing_date,
                        "state": config.code,
                        "category": keyword,
                        "detail_url": detail_url,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    
                    results.append(business)
                    
                except Exception as e:
                    continue
            
        except Exception as e:
            self._log(f"  Parse error: {str(e)}")
        
        return results
    
    def get_results(self) -> List[Dict]:
        """Get all scraped results."""
        return self.businesses
    
    def save_results(self, filename: str = None):
        """Save results to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.output_dir}/sos_results_{timestamp}.json"
        
        with open(filename, "w") as f:
            json.dump(self.businesses, f, indent=2)
        
        self._log(f"Saved {len(self.businesses)} results to {filename}")
        return filename


# ============================================================================
# State-specific scraper implementations for sites that need special handling
# ============================================================================

class FloridaSunbizScraper(MultiStateScraper):
    """
    Specialized Florida Sunbiz scraper with enhanced parsing.
    Uses the existing sunbiz_scraper_fixed.py logic.
    """
    
    def __init__(self, **kwargs):
        super().__init__(states=["FL"], **kwargs)
    
    async def _parse_results_page(self, page: Page, config: StateConfig, keyword: str) -> List[Dict]:
        """Enhanced parsing for Florida Sunbiz."""
        results = []
        
        try:
            # Florida-specific: Look for links with SearchResultDetail in href
            rows = await page.query_selector_all("table tbody tr")
            
            for row in rows:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3:
                        continue
                    
                    # Get name from first cell's link
                    name_link = await cells[0].query_selector("a[href*='SearchResultDetail']")
                    if not name_link:
                        continue
                    
                    name = (await name_link.inner_text()).strip()
                    href = await name_link.get_attribute("href") or ""
                    
                    # Validate: skip navigation links
                    if name.lower() in ['previous on list', 'next on list', 'return to list']:
                        continue
                    
                    # Extract document number
                    doc_number = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""
                    
                    # Validate document number format (FL format: letter + 8+ digits)
                    if not re.match(r'^[A-Z]\d{8,}$', doc_number):
                        # Use the name as-is, it's the actual business name
                        pass
                    
                    status = (await cells[2].inner_text()).strip() if len(cells) > 2 else ""
                    filing_date = (await cells[3].inner_text()).strip() if len(cells) > 3 else ""
                    
                    detail_url = ""
                    if href:
                        if href.startswith("http"):
                            detail_url = href
                        else:
                            detail_url = f"https://search.sunbiz.org{href}"
                    
                    business = {
                        "name": name,
                        "document_number": doc_number,
                        "status": status,
                        "filing_date": filing_date,
                        "state": "FL",
                        "category": keyword,
                        "detail_url": detail_url,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    
                    results.append(business)
                    
                except Exception as e:
                    continue
            
        except Exception as e:
            self._log(f"  FL Parse error: {str(e)}")
        
        return results


class GeorgiaScraper(MultiStateScraper):
    """
    Specialized Georgia eCorp scraper.
    Georgia uses Kendo UI grid with AJAX loading.
    """
    
    def __init__(self, **kwargs):
        super().__init__(states=["GA"], **kwargs)
    
    async def _search_keyword(self, page: Page, config: StateConfig, keyword: str, max_results: int) -> List[Dict]:
        """Georgia-specific search handling."""
        results = []
        
        try:
            # Georgia uses Firebase App Check - use domcontentloaded instead of networkidle
            await page.goto(config.search_url, wait_until="domcontentloaded", timeout=30000)
            
            # Wait for the search form to be ready (instead of networkidle)
            try:
                await page.wait_for_selector("#txtBusinessName", timeout=15000)
            except Exception:
                # If form not ready, wait a bit more
                await asyncio.sleep(3)
            
            await asyncio.sleep(1)
            
            # Clear and fill business name
            name_input = await page.query_selector("#txtBusinessName")
            if name_input:
                await name_input.fill(keyword)
            
            # Click search
            search_btn = await page.query_selector("#btnSearch")
            if search_btn:
                await search_btn.click()
            
            # Wait for Kendo grid to load
            await asyncio.sleep(3)
            
            # Parse results from Kendo grid
            rows = await page.query_selector_all(".k-grid-content table tbody tr")
            
            for row in rows[:max_results]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 4:
                        continue
                    
                    name = (await cells[0].inner_text()).strip()
                    control_num = (await cells[1].inner_text()).strip()
                    status = (await cells[2].inner_text()).strip()
                    filing_date = (await cells[3].inner_text()).strip()
                    
                    # Get detail link
                    link = await cells[0].query_selector("a")
                    detail_url = ""
                    if link:
                        href = await link.get_attribute("href")
                        if href:
                            detail_url = f"https://ecorp.sos.ga.gov{href}" if not href.startswith("http") else href
                    
                    business = {
                        "name": name,
                        "document_number": control_num,
                        "status": status,
                        "filing_date": filing_date,
                        "state": "GA",
                        "category": keyword,
                        "detail_url": detail_url,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    
                    results.append(business)
                    
                except Exception:
                    continue
            
            self._log(f"  Found {len(results)} businesses for '{keyword}'")
            
        except Exception as e:
            self._log(f"  ERROR: {str(e)}")
        
        return results


class NewYorkScraper(MultiStateScraper):
    """
    Specialized New York DOS scraper.
    NY uses DataTables with server-side processing.
    """
    
    def __init__(self, **kwargs):
        super().__init__(states=["NY"], **kwargs)
    
    async def _search_keyword(self, page: Page, config: StateConfig, keyword: str, max_results: int) -> List[Dict]:
        """New York-specific search handling."""
        results = []
        
        try:
            await page.goto("https://apps.dos.ny.gov/publicInquiry/", wait_until="networkidle")
            await asyncio.sleep(2)
            
            # NY has a specific search form structure
            name_input = await page.query_selector("#search-businessname")
            if name_input:
                await name_input.fill(keyword)
            
            # Submit search
            submit_btn = await page.query_selector("button[type='submit'], input[type='submit']")
            if submit_btn:
                await submit_btn.click()
            else:
                await name_input.press("Enter")
            
            await asyncio.sleep(3)
            
            # Parse DataTables results
            rows = await page.query_selector_all("table.dataTable tbody tr")
            
            for row in rows[:max_results]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3:
                        continue
                    
                    name = (await cells[0].inner_text()).strip()
                    dos_id = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""
                    filing_date = (await cells[2].inner_text()).strip() if len(cells) > 2 else ""
                    status = (await cells[3].inner_text()).strip() if len(cells) > 3 else ""
                    
                    link = await cells[0].query_selector("a")
                    detail_url = ""
                    if link:
                        href = await link.get_attribute("href")
                        if href:
                            detail_url = f"https://apps.dos.ny.gov{href}" if not href.startswith("http") else href
                    
                    business = {
                        "name": name,
                        "document_number": dos_id,
                        "status": status,
                        "filing_date": filing_date,
                        "state": "NY",
                        "category": keyword,
                        "detail_url": detail_url,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    
                    results.append(business)
                    
                except Exception:
                    continue
            
            self._log(f"  Found {len(results)} businesses for '{keyword}'")
            
        except Exception as e:
            self._log(f"  ERROR: {str(e)}")
        
        return results


class PennsylvaniaScraper(MultiStateScraper):
    """
    Specialized Pennsylvania Corporations scraper.
    """
    
    def __init__(self, **kwargs):
        super().__init__(states=["PA"], **kwargs)


class CaliforniaScraper(MultiStateScraper):
    """
    Specialized California BizFile scraper.
    California uses Angular with potential hCaptcha.
    """
    
    def __init__(self, **kwargs):
        super().__init__(states=["CA"], **kwargs)
    
    async def _search_keyword(self, page: Page, config: StateConfig, keyword: str, max_results: int) -> List[Dict]:
        """California-specific search handling."""
        results = []
        
        try:
            await page.goto("https://bizfileonline.sos.ca.gov/search/business", wait_until="networkidle")
            await asyncio.sleep(3)
            
            # California's Angular form
            search_input = await page.query_selector("input[formcontrolname='SearchValue']")
            if search_input:
                await search_input.fill(keyword)
            else:
                # Fallback selector
                search_input = await page.query_selector("input[placeholder*='Search']")
                if search_input:
                    await search_input.fill(keyword)
            
            # Click search button
            submit_btn = await page.query_selector("button[type='submit']")
            if submit_btn:
                await submit_btn.click()
            
            await asyncio.sleep(4)  # CA takes longer to load
            
            # Check for captcha
            captcha = await page.query_selector(".h-captcha, .g-recaptcha")
            if captcha:
                self._log("  WARNING: Captcha detected - cannot proceed automatically")
                return results
            
            # Parse results
            rows = await page.query_selector_all("table tbody tr, .p-datatable-tbody tr")
            
            for row in rows[:max_results]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3:
                        continue
                    
                    name = (await cells[0].inner_text()).strip()
                    entity_num = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""
                    filing_date = (await cells[2].inner_text()).strip() if len(cells) > 2 else ""
                    status = (await cells[3].inner_text()).strip() if len(cells) > 3 else ""
                    
                    business = {
                        "name": name,
                        "document_number": entity_num,
                        "status": status,
                        "filing_date": filing_date,
                        "state": "CA",
                        "category": keyword,
                        "detail_url": f"https://bizfileonline.sos.ca.gov/search/business/{entity_num}" if entity_num else "",
                        "scraped_at": datetime.now().isoformat(),
                    }
                    
                    results.append(business)
                    
                except Exception:
                    continue
            
            self._log(f"  Found {len(results)} businesses for '{keyword}'")
            
        except Exception as e:
            self._log(f"  ERROR: {str(e)}")
        
        return results


class TexasScraper(MultiStateScraper):
    """
    Texas Comptroller scraper (public search, no login required).
    """
    
    def __init__(self, **kwargs):
        super().__init__(states=["TX"], **kwargs)
    
    async def _search_keyword(self, page: Page, config: StateConfig, keyword: str, max_results: int) -> List[Dict]:
        """Texas-specific search handling using Comptroller public search."""
        results = []
        
        try:
            # Texas Comptroller public search
            await page.goto("https://mycpa.cpa.state.tx.us/coa/Index.html", wait_until="networkidle")
            await asyncio.sleep(2)
            
            # Fill taxpayer name search
            name_input = await page.query_selector("#taxpayerName")
            if name_input:
                await name_input.fill(keyword)
            
            # Submit
            submit_btn = await page.query_selector("input[type='submit']")
            if submit_btn:
                await submit_btn.click()
            
            await asyncio.sleep(3)
            
            # Parse results
            rows = await page.query_selector_all("table tbody tr")
            
            for row in rows[:max_results]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue
                    
                    name = (await cells[0].inner_text()).strip()
                    taxpayer_num = (await cells[1].inner_text()).strip() if len(cells) > 1 else ""
                    status = (await cells[2].inner_text()).strip() if len(cells) > 2 else "Active"
                    
                    business = {
                        "name": name,
                        "document_number": taxpayer_num,
                        "status": status,
                        "filing_date": "",
                        "state": "TX",
                        "category": keyword,
                        "detail_url": "",
                        "scraped_at": datetime.now().isoformat(),
                    }
                    
                    results.append(business)
                    
                except Exception:
                    continue
            
            self._log(f"  Found {len(results)} businesses for '{keyword}'")
            
        except Exception as e:
            self._log(f"  ERROR: {str(e)}")
        
        return results


# ============================================================================
# Factory function to get the right scraper for a state
# ============================================================================

def get_scraper_for_states(states: List[str], **kwargs) -> MultiStateScraper:
    """
    Factory function to get the appropriate scraper for given states.
    
    For single-state scraping of specialized states, returns state-specific scraper.
    For multi-state or generic states, returns MultiStateScraper.
    """
    if len(states) == 1:
        state = states[0].upper()
        if state == "FL":
            return FloridaSunbizScraper(**kwargs)
        elif state == "GA":
            return GeorgiaScraper(**kwargs)
        elif state == "NY":
            return NewYorkScraper(**kwargs)
        elif state == "PA":
            return PennsylvaniaScraper(**kwargs)
        elif state == "CA":
            return CaliforniaScraper(**kwargs)
        elif state == "TX":
            return TexasScraper(**kwargs)
    
    return MultiStateScraper(states=states, **kwargs)


# ============================================================================
# CLI for testing
# ============================================================================

if __name__ == "__main__":
    import sys
    
    async def main():
        states = sys.argv[1].split(",") if len(sys.argv) > 1 else ["FL"]
        keywords = sys.argv[2].split(",") if len(sys.argv) > 2 else ["plumber", "hvac"]
        
        scraper = get_scraper_for_states(states, headless=True, on_log=print)
        results = await scraper.scrape(keywords=keywords, max_per_keyword=10)
        
        print(f"\n{'='*60}")
        print(f"Total results: {len(results)}")
        for r in results[:10]:
            print(f"  - {r['name']} ({r['state']}) - {r['status']}")
    
    asyncio.run(main())

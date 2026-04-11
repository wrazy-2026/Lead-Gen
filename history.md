# Changelog / History

## April 12, 2026 (Latest Update)

### Standard HTML State Scrapers - 29 States Active

Added scrapers for all Standard HTML states (no JavaScript required):

**Active States (29):**
FL, IL, NC, MA, TN, MO, WI, CO, SC, AL, LA, KY, OR, OK, UT, IA, AR, MS, KS, NM, NE, HI, NH, ME, RI, DE, SD, AK, VT

**Still Disabled (21):**
- CA, NY, GA, PA, TX, AZ (CAPTCHA/login/SSL issues)
- OH, NJ, VA, WA, MI, IN (JS required but may work)
- NV, WV, MD, CT, MN, ID, MT, ND, WY (CAPTCHA or complex JS)

**Files Updated:**
- `scrapers/multistate_scraper.py` - Added 23 new StateConfig entries
- `app_flask.py` - Updated ACTIVE_SCRAPER_STATES with 29 states
- `templates/florida_scraper.html` - Marked 29 states as active

---

## April 12, 2026

### Knowledge Base Page Added

New `/knowledgebase` page with all 50 state Secretary of State business search websites:
- Color-coded status (Working, JSON API, Standard HTML, JS Required, CAPTCHA/Login)
- Search filter to find states quickly
- Direct links to each state's SOS portal
- Notes on scraping difficulty for each state

Access from sidebar: **Scrapers > Knowledge Base**

---

## April 12, 2026

### State Scraper Status Update

**Only Florida (FL) is currently working.** Other state scrapers are disabled due to:

| State | Issue |
|-------|-------|
| GA | Firebase App Check blocking page load |
| PA | SSL certificate error (ERR_CERT_COMMON_NAME_INVALID) |
| NY | Form selectors changed, search elements not found |
| CA | hCaptcha protection on bizfileonline.sos.ca.gov |
| TX | Selectors not matching page structure |
| AZ | Now requires login (Arizona Business Center) |

The StateConfig entries exist in `scrapers/multistate_scraper.py` but the websites have changed or added anti-bot protection. Each state needs individual debugging and selector updates.

---

## April 12, 2026

### Multi-State SOS Scraper Expansion - 18 States

**18 States Now Active**

Added 12 new state scrapers to the multistate_scraper.py:

**Batch 1 (6 states):** FL, CA, TX, NY, PA, GA
**Batch 2 (12 states):** OH, NC, IL, NJ, VA, WA, AZ, MA, MI, TN, IN, CO

**Files Updated:**
- `scrapers/multistate_scraper.py` - Added StateConfig for 12 new states with CSS selectors
- `app_flask.py` - Updated ACTIVE_SCRAPER_STATES to include all 18 states  
- `templates/florida_scraper.html` - Marked new states as active (clickable)

---

## April 12, 2026

### Multi-State SOS Scraper - Full Implementation

**6 States Initial Release: FL, CA, TX, NY, PA, GA**

Built multi-state Playwright scrapers for the top 5 US states by population plus Georgia:
- **Florida (FL)** - Sunbiz.org - Fully working
- **California (CA)** - bizfileonline.sos.ca.gov - Angular app, may have captcha
- **Texas (TX)** - mycpa.cpa.state.tx.us - Comptroller public search
- **New York (NY)** - apps.dos.ny.gov - DataTables-based
- **Pennsylvania (PA)** - corporations.pa.gov - Standard form
- **Georgia (GA)** - ecorp.sos.ga.gov - Kendo UI grid

**New Files Created:**
- `scrapers/multistate_scraper.py` - Comprehensive Playwright-based scraper with:
  - `STATE_CONFIGS` dict with selectors for each state
  - `MultiStateScraper` class for generic scraping
  - Specialized classes: `FloridaSunbizScraper`, `GeorgiaScraper`, `NewYorkScraper`, `CaliforniaScraper`, `TexasScraper`
  - `get_scraper_for_states()` factory function

**Backend Updates:**
- `app_flask.py`:
  - Updated `_florida_run_scrape()` to accept states parameter
  - Updated `_florida_async_scrape()` to use MultiStateScraper for non-FL states
  - Added `ACTIVE_SCRAPER_STATES` set
  - New endpoint: `/api/florida/active-states`

**Frontend Updates:**
- `templates/florida_scraper.html`:
  - States are now clickable buttons (like keywords) with Select All/Clear
  - Multi-select: can scrape multiple states at once
  - Updated `startScrape()` to pass selected states array to API
  - 6 states marked as active, 44 as "Coming Soon"

---

## April 12, 2026

### Multi-State SOS Scraper with Filters

**Major Feature: Expanded to All 50 US States**
- Renamed "Florida Sunbiz Scraper" to "SOS Business Scraper"
- Added state dropdown selector with all 50 US states (FL active, others coming soon)
- UI now shows state count badge and supports future state scrapers

**Filter System:**
- Added Status filter (Active/Inactive) - filters by business status
- Added Category filter - dynamically populated from scraped data
- Added State filter - filter by state when multiple states are scraped
- "Clear Filters" button to reset all filters
- Filtered export - CSV and JSON export only the filtered data

**UI Improvements:**
- Collapsible log panel with toggle button
- Added State column to results table
- Export buttons now export filtered data instead of all data
- Client-side CSV/JSON generation for filtered exports

**Files Modified:**
- `templates/florida_scraper.html` - Complete rewrite of JavaScript to `sosScraper()` with:
  - `availableStates` array for all 50 states
  - `filteredBusinesses` computed property
  - Filter state variables (`filterStatus`, `filterCategory`, `filterState`)
  - `exportFiltered()` function with client-side CSV/JSON generation
  - `uniqueCategories` and `uniqueStates` computed properties

---

## April 12, 2026

### Florida Sunbiz Scraper Improvements

**Fixed Issues:**
1. **Business Name Extraction**: Fixed extraction to properly capture business names instead of navigation links ("Previous On List", "Next On List", "Return to List"). Added filtering to exclude navigation elements and validate proper business entries.

2. **Data Persistence**: Scraped data now persists in Firestore database (`florida_leads` collection). Data no longer disappears after page refresh.

3. **Business URL Added**: Each scraped business now includes a clickable link (`detail_url`) to view the full business details on the Sunbiz website.

**Files Modified:**
- `Florida/sunbiz_scraper_fixed.py` - Fixed `_parse_results_table()` to filter out navigation links, added `detail_url` to fallback data
- `app_flask.py` - Added `_florida_save_to_db()`, `_florida_load_from_db()`, `/api/florida/persisted` endpoint, updated download functions
- `templates/florida_scraper.html` - Added Link column, auto-load persisted data on page load

**New Features:**
- Data automatically loads from database on page refresh
- New "Link" column shows clickable link to Sunbiz detail page for each business
- CSV and JSON downloads now work even after page refresh (pulls from database)

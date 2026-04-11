# Changelog / History

## April 12, 2026 (Latest)

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

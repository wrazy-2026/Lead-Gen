# Changelog / History

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

# Florida Sunbiz Scraper - Fixed Version

This is the corrected version of the Sunbiz scraper that properly handles the multi-step scraping process:

1. **Search for keyword** (e.g., "hvac")
2. **Navigate to search results page** (like the URL you provided)
3. **Extract each business link** from the results table
4. **Open each business detail page** to get complete information
5. **Parse detailed data** (filing date, address, registered agent, entity type)
6. **Sort results** by filing date (newest to oldest)

## Key Improvements

### Problem Fixed
The original scraper was trying to extract data directly from the search page without opening individual business detail pages. The Sunbiz website requires clicking on each business to view complete information.

### Solution
The fixed scraper now:
- Performs a keyword search
- Extracts all business links from the search results table
- Opens each business detail page individually
- Parses complete business information including filing date
- Returns to the search results and continues with the next business
- Sorts all results by filing date (newest to oldest)

## Installation

```bash
# Install dependencies
pip install playwright pandas

# Install Chromium browser
playwright install chromium
```

## Usage

### Basic Usage

```python
import asyncio
from sunbiz_scraper_fixed import FixedSunbizScraper

async def main():
    scraper = FixedSunbizScraper(headless=True)
    
    # Scrape all categories
    await scraper.scrape_all_categories(max_per_category=50)
    
    # Sort by date (newest to oldest)
    sorted_businesses = scraper.sort_by_date(ascending=False)
    scraper.businesses = sorted_businesses
    
    # Save results
    scraper.save_to_json("businesses.json")
    scraper.save_to_csv("businesses.csv")
    
    # Display summary
    summary = scraper.get_summary()
    print(summary)

asyncio.run(main())
```

### Command Line

```bash
python sunbiz_scraper_fixed.py
```

## How It Works

### Step 1: Search
```python
search_url = await scraper.search_keyword(page, "hvac")
# Navigates to: https://search.sunbiz.org/Inquiry/CorporationSearch/ByName
# Fills in "hvac" and submits the search
```

### Step 2: Extract Results
```python
results = await scraper.extract_results_from_page(page, "hvac")
# Parses the search results table
# Extracts: name, document_number, status, detail_url
# Returns list of businesses with links to detail pages
```

### Step 3: Get Details
```python
business = await scraper.get_business_details(page, business)
# Opens the business detail page
# Extracts: filing_date, address, registered_agent, entity_type
```

### Step 4: Sort & Save
```python
sorted_businesses = scraper.sort_by_date(ascending=False)
scraper.save_to_json()
scraper.save_to_csv()
```

## Output Format

### JSON Output
```json
[
  {
    "name": "HVAC CONTRACTORS, CORP",
    "document_number": "P13000077080",
    "status": "Active",
    "category": "HVAC",
    "detail_url": "/Inquiry/CorporationSearch/SearchResults?...",
    "filing_date": "01/15/2013",
    "address": "123 Main St, Miami, FL 33101",
    "registered_agent": "John Smith",
    "entity_type": "Corporation",
    "scraped_date": "2024-04-07T16:30:00.000000"
  }
]
```

### CSV Output
| name | document_number | status | category | filing_date | address | registered_agent | entity_type |
| --- | --- | --- | --- | --- | --- | --- | --- |
| HVAC CONTRACTORS, CORP | P13000077080 | Active | HVAC | 01/15/2013 | 123 Main St, Miami, FL 33101 | John Smith | Corporation |

## Configuration Options

```python
scraper = FixedSunbizScraper(
    output_dir="./sunbiz_data",      # Where to save output files
    headless=True,                    # Run browser in headless mode
    use_proxy="http://proxy:8080",   # Optional proxy URL
    timeout=60000                     # Page load timeout in ms
)
```

## Home Service Categories

The scraper searches for:
- HVAC
- Plumber
- Roofer
- Cleaning
- Remodeling
- Electrician
- Painter
- Landscaper
- Carpenter
- Handyman
- Contractor
- Construction
- Solar
- Pool service
- Pest control
- Tree service
- Pressure washing
- Drywall
- Flooring
- Masonry
- Gutter
- Appliance repair
- Water damage
- Mold

## Troubleshooting

### CAPTCHA Blocking
If you encounter CAPTCHA errors:
1. Run with `headless=False` to see what's happening
2. Use a residential proxy
3. Increase delays between requests

### No Data Extracted
1. Check that Sunbiz website is accessible
2. Verify the search page HTML structure hasn't changed
3. Run with `headless=False` to debug

### Slow Performance
1. Reduce `max_per_category` to scrape fewer results
2. Increase delays between requests
3. Use fewer keywords

## Advanced Features

### Scrape Specific Keywords Only

```python
scraper = FixedSunbizScraper()
await scraper.initialize_browser()
page = await scraper.context.new_page()

# Scrape only HVAC and plumbing
hvac_results = await scraper.scrape_keyword(page, "HVAC", max_results=100)
plumbing_results = await scraper.scrape_keyword(page, "plumber", max_results=100)

scraper.businesses = hvac_results + plumbing_results
scraper.save_to_json()

await page.close()
await scraper.close_browser()
```

### Use Proxy

```python
scraper = FixedSunbizScraper(
    use_proxy="http://residential-proxy.example.com:8080"
)
await scraper.scrape_all_categories()
```

### Custom Data Analysis

```python
import pandas as pd

df = pd.read_csv("sunbiz_data/sunbiz_businesses.csv")

# Filter by status
active = df[df["status"] == "Active"]

# Group by category
by_category = df.groupby("category").size()

# Filter by date
from datetime import datetime, timedelta
recent = df[df["filing_date"] > (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")]
```

## Performance Notes

- Each business detail page takes ~0.5-1 second to load
- Scraping 50 results per category takes approximately 5-10 minutes
- Total time for all 24 categories with 50 results each: ~2-3 hours
- Adjust `max_per_category` to control total runtime

## Legal & Ethical

- Sunbiz data is public record
- Respect rate limits and server load
- Use data responsibly and in compliance with applicable laws
- Do not use for spam or harassment

---

**Version:** 2.1 (Fixed)  
**Last Updated:** April 2026

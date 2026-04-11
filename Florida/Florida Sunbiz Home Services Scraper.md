# Florida Sunbiz Home Services Scraper

A comprehensive Python scraper designed to extract business information from the Florida Sunbiz registry (https://search.sunbiz.org) for all home service categories. The scraper automatically identifies and extracts data for HVAC, plumbing, roofing, cleaning, remodeling, and hundreds of other home service businesses, sorting results from newest to oldest.

## Features

- **Multi-Category Scraping:** Automatically searches 20+ home service keywords including HVAC, plumbing, roofing, cleaning, electrician, painter, landscaper, and more.
- **Newest-to-Oldest Sorting:** All results are automatically sorted by filing date with the newest businesses appearing first.
- **Detailed Business Information:** Extracts name, document number, status, filing date, and category for each business.
- **Multiple Output Formats:** Saves data to both JSON and CSV formats for easy analysis.
- **Rate Limiting:** Built-in delays between requests to avoid overwhelming the server.
- **Error Handling:** Comprehensive error logging and graceful failure handling.
- **Two Scraper Versions:** Basic version using requests/BeautifulSoup and advanced version using Playwright for JavaScript rendering.

## Installation

### Prerequisites
- Python 3.8+
- pip or conda

### Basic Installation

```bash
pip install requests beautifulsoup4 pandas aiohttp
```

### Advanced Installation (with Playwright)

```bash
pip install requests beautifulsoup4 pandas aiohttp playwright
playwright install chromium
```

## Usage

### Basic Scraper (Requests + BeautifulSoup)

```python
import asyncio
from sunbiz_scraper import SunbizScraper

async def main():
    async with SunbizScraper(output_dir="./sunbiz_data") as scraper:
        # Scrape all home service categories
        businesses = await scraper.scrape_all_categories(max_per_category=100)
        
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

### Advanced Scraper (Playwright)

```python
import asyncio
from sunbiz_scraper_advanced import AdvancedSunbizScraper

async def main():
    scraper = AdvancedSunbizScraper(
        output_dir="./sunbiz_data",
        headless=True,
        use_proxy=None  # Optional: "http://proxy:8080"
    )
    
    # Scrape all categories
    await scraper.scrape_all_categories(max_per_category=100)
    
    # Sort by date
    sorted_businesses = scraper.sort_by_date(ascending=False)
    scraper.businesses = sorted_businesses
    
    # Save results
    scraper.save_to_json()
    scraper.save_to_csv()

asyncio.run(main())
```

### Command Line Usage

```bash
# Run basic scraper
python sunbiz_scraper.py

# Run advanced scraper
python sunbiz_scraper_advanced.py
```

## Home Service Categories Included

The scraper searches for businesses in the following categories:

- **HVAC:** HVAC, heating, cooling, air conditioning
- **Plumbing:** Plumber, plumbing, pipe
- **Roofing:** Roofer, roofing, roof
- **Cleaning:** Cleaning, cleaner, janitorial
- **Remodeling:** Remodeling, remodeler, renovation, contractor
- **Electrical:** Electrician, electrical, electric
- **Painting:** Painting, painter, paint
- **Landscaping:** Landscaping, landscaper, lawn, garden
- **Carpentry:** Carpentry, carpenter, wood
- **Masonry:** Masonry, mason, concrete
- **Flooring:** Flooring, floor
- **Drywall:** Drywall, insulation
- **Windows & Doors:** Window, door
- **Gutters:** Gutter, gutter cleaning
- **Pressure Washing:** Pressure washing, power wash
- **Tree Service:** Tree service, tree removal
- **Pest Control:** Pest control, termite
- **Pool Service:** Pool service, pool cleaning
- **Appliance Repair:** Appliance repair, appliance
- **Handyman:** Handyman, home repair, home maintenance
- **Construction:** Construction, builder
- **Solar:** Solar, solar panel
- **Water Damage:** Water damage, restoration
- **Mold Removal:** Mold, mold removal

## Output Format

### JSON Output

```json
[
  {
    "name": "ABC HVAC Services LLC",
    "document_number": "L12345678",
    "status": "ACTIVE",
    "filing_date": "2024-01-15",
    "category": "HVAC",
    "scraped_date": "2024-04-07T16:30:00.000000"
  },
  {
    "name": "XYZ Plumbing Inc",
    "document_number": "L12345679",
    "status": "ACTIVE",
    "filing_date": "2024-01-14",
    "category": "plumber",
    "scraped_date": "2024-04-07T16:30:00.000000"
  }
]
```

### CSV Output

| name | document_number | status | filing_date | category | scraped_date |
| --- | --- | --- | --- | --- | --- |
| ABC HVAC Services LLC | L12345678 | ACTIVE | 2024-01-15 | HVAC | 2024-04-07T16:30:00 |
| XYZ Plumbing Inc | L12345679 | ACTIVE | 2024-01-14 | plumber | 2024-04-07T16:30:00 |

## Advanced Features

### Using Proxies

```python
scraper = AdvancedSunbizScraper(
    use_proxy="http://residential-proxy.example.com:8080"
)
```

### Custom Category Search

```python
async with SunbizScraper() as scraper:
    # Search specific categories
    hvac_results = await scraper.search_by_keyword("HVAC", max_results=200)
    plumbing_results = await scraper.search_by_keyword("plumber", max_results=200)
```

### Data Analysis

```python
import pandas as pd

# Load results
df = pd.read_csv("sunbiz_data/sunbiz_businesses.csv")

# Group by category
category_counts = df.groupby("category").size()
print(category_counts)

# Filter by status
active_only = df[df["status"] == "ACTIVE"]
print(f"Active businesses: {len(active_only)}")

# Sort by date
df["filing_date"] = pd.to_datetime(df["filing_date"])
newest_first = df.sort_values("filing_date", ascending=False)
```

## Troubleshooting

### CAPTCHA Blocking

If you encounter CAPTCHA errors:

1. **Use the Advanced Scraper:** The Playwright version handles JavaScript-rendered pages better.
2. **Add Proxies:** Rotate residential proxies to avoid IP-based blocking.
3. **Increase Delays:** Add longer delays between requests:
   ```python
   await asyncio.sleep(5)  # Increase from 1-2 seconds
   ```

### No Results Returned

1. **Check Internet Connection:** Ensure you have stable internet access.
2. **Verify Keywords:** Test keywords manually on https://search.sunbiz.org.
3. **Check Sunbiz Status:** Verify the Sunbiz website is not under maintenance.

### Memory Issues with Large Datasets

If scraping all categories returns too much data:

```python
# Limit results per category
await scraper.scrape_all_categories(max_per_category=25)

# Or scrape specific categories
for keyword in ["HVAC", "plumber", "roofer"]:
    results = await scraper.search_by_keyword(keyword, max_results=50)
```

## Performance Tips

1. **Use Async:** The scrapers use async/await for concurrent requests.
2. **Batch Processing:** Process results in batches to avoid memory issues.
3. **Rate Limiting:** Respect Sunbiz's rate limits with appropriate delays.
4. **Proxy Rotation:** Use residential proxies to avoid IP bans.

## Legal & Ethical Considerations

- **Terms of Service:** Ensure your use complies with Sunbiz's terms of service.
- **Rate Limiting:** Do not overwhelm the server with excessive requests.
- **Data Usage:** Use scraped data responsibly and in accordance with applicable laws.
- **Public Data:** Sunbiz data is public record, but verify usage rights in your jurisdiction.

## API Integration

To integrate this scraper into your application:

```python
from sunbiz_scraper import SunbizScraper

async def get_home_service_businesses(category: str, limit: int = 50):
    """API endpoint to get home service businesses."""
    async with SunbizScraper() as scraper:
        results = await scraper.search_by_keyword(category, max_results=limit)
        return scraper.sort_by_date(ascending=False)
```

## Support & Contributing

For issues, suggestions, or contributions, please refer to the project documentation or contact the development team.

## License

This project is provided as-is for educational and research purposes.

---

**Last Updated:** April 2026  
**Version:** 2.0

# Illinois Secretary of State Business Scraper Guide

## Overview

This professional scraper extracts business data from the Illinois Secretary of State corporate registry. It handles reCAPTCHA protection, JavaScript rendering, and provides comprehensive logging for debugging.

## What It Does

The scraper searches the Illinois SOS database for home service businesses (HVAC, plumbing, roofing, electrical, cleaning) and extracts:

- Business Name
- File Number
- Entity Type (LLC, Corporation, etc.)
- Status (Active, Inactive, etc.)
- Filing Date
- Registered Agent
- Principal Officer
- Business Address
- County
- Jurisdiction

## Why the Original Scraper Failed

The original scraper had several issues:

1. **Wrong Element ID:** Looking for `#corporateName` instead of `#searchValue`
2. **No CAPTCHA Handling:** Illinois SOS uses reCAPTCHA Enterprise which blocks automated requests
3. **No JavaScript Rendering:** Used simple HTTP requests instead of browser automation
4. **No Error Handling:** Failed silently without proper logging

## Solution: Playwright + 2Captcha

This new scraper uses:

- **Playwright:** For browser automation and JavaScript rendering
- **2Captcha:** For solving reCAPTCHA protection
- **Async/Await:** For efficient concurrent processing
- **Comprehensive Logging:** For debugging and monitoring

## Setup Instructions

### Step 1: Install Dependencies

```bash
pip install playwright aiohttp python-dotenv
python -m playwright install chromium
```

### Step 2: Set Up 2Captcha Account

1. Sign up at https://2captcha.com
2. Get your API key from the dashboard
3. Add credits to your account (starts at $0.50 minimum)
4. Create `.env` file with your key:

```
TWOCAPTCHA_API_KEY=your_api_key_here
```

### Step 3: Verify Setup

```bash
python -c "from playwright.async_api import async_playwright; print('Playwright OK')"
python -c "import aiohttp; print('aiohttp OK')"
```

## Usage

### Basic Usage

```python
import asyncio
from illinois_sos_scraper import IllinoisSOSScraper

async def main():
    scraper = IllinoisSOSScraper(headless=True)
    
    try:
        await scraper.initialize()
        
        # Search for HVAC businesses
        results = await scraper.search_businesses("HVAC", search_method="Business Name")
        
        # Save results
        scraper.save_results(results)
        
    finally:
        await scraper.close()

asyncio.run(main())
```

### Batch Processing Multiple Keywords

```python
import asyncio
from illinois_sos_scraper import IllinoisSOSScraper

async def main():
    keywords = ["HVAC", "Plumbing", "Roofing", "Electrical", "Cleaning"]
    
    scraper = IllinoisSOSScraper(headless=True)
    
    try:
        await scraper.initialize()
        
        # Scrape all keywords
        businesses = await scraper.scrape_keywords(keywords)
        
        # Save results
        scraper.save_results(businesses, output_dir="illinois_businesses")
        
        print(f"Total businesses scraped: {len(businesses)}")
        
    finally:
        await scraper.close()

asyncio.run(main())
```

### Different Search Methods

```python
import asyncio
from illinois_sos_scraper import IllinoisSOSScraper

async def main():
    scraper = IllinoisSOSScraper()
    
    try:
        await scraper.initialize()
        
        # Search by business name
        results1 = await scraper.search_businesses("HVAC", search_method="Business Name")
        
        # Search by keyword
        results2 = await scraper.search_businesses("air conditioning", search_method="Keyword")
        
        # Search by partial word
        results3 = await scraper.search_businesses("HVAC", search_method="Partial Word")
        
    finally:
        await scraper.close()

asyncio.run(main())
```

## Search Methods Available

| Method | Use Case | Example |
| --- | --- | --- |
| Business Name | Exact business name | "HVAC Solutions LLC" |
| Keyword | Full keyword search | "air conditioning" |
| Partial Word | Partial name matching | "HVAC" |
| Registered Agent | Search by agent name | "John Smith" |
| President | Search by president name | "Jane Doe" |
| Secretary | Search by secretary name | "Bob Johnson" |
| Manager | Search by manager name | "Alice Brown" |
| File Number | Exact file number | "0123456789" |

## Output Format

### JSON Output (`businesses.json`)

```json
[
  {
    "name": "ABC HVAC SOLUTIONS LLC",
    "file_number": "0123456789",
    "entity_type": "Limited Liability Company",
    "status": "Active",
    "filing_date": "2023-01-15",
    "registered_agent": "John Smith",
    "principal_officer": "Jane Doe",
    "address": "123 Main St, Chicago, IL 60601",
    "county": "Cook",
    "jurisdiction": "Illinois",
    "category": "HVAC",
    "scraped_at": "2025-04-07T18:00:00.000000"
  }
]
```

### CSV Output (`businesses.csv`)

```
name,file_number,entity_type,status,filing_date,registered_agent,principal_officer,address,county,jurisdiction,category,scraped_at
"ABC HVAC SOLUTIONS LLC","0123456789","Limited Liability Company","Active","2023-01-15","John Smith","Jane Doe","123 Main St, Chicago, IL 60601","Cook","Illinois","HVAC","2025-04-07T18:00:00.000000"
```

## Cost Estimation

### 2Captcha Costs

- **reCAPTCHA v2:** $0.50 per 1000 CAPTCHAs
- **Minimum deposit:** $0.50
- **Per business search:** ~$0.0005 (if CAPTCHA required)

### Example Costs

| Searches | CAPTCHA Rate | Cost |
| --- | --- | --- |
| 100 | 50% | $0.025 |
| 1,000 | 50% | $0.25 |
| 10,000 | 50% | $2.50 |
| 100,000 | 50% | $25.00 |

**Note:** Not every search triggers CAPTCHA. The rate depends on Illinois SOS rate limiting.

## Troubleshooting

### Problem: "TWOCAPTCHA_API_KEY not set"

**Solution:** Create `.env` file with your API key:
```
TWOCAPTCHA_API_KEY=your_key_here
```

### Problem: "CAPTCHA solving timeout"

**Possible causes:**
- 2Captcha service is slow
- Your account has insufficient credits
- Network connectivity issues

**Solution:**
1. Check your 2Captcha account balance
2. Increase timeout in code: `timeout=600` (10 minutes)
3. Check your internet connection

### Problem: "Results table not found"

**Possible causes:**
- Search returned no results
- Page structure changed
- CAPTCHA solving failed silently

**Solution:**
1. Run with `headless=False` to see what's happening
2. Check the browser window for errors
3. Verify search term is valid

### Problem: "Browser stopped"

**Possible causes:**
- Playwright not installed properly
- Chromium not installed

**Solution:**
```bash
python -m playwright install chromium
```

### Problem: "Connection refused"

**Possible causes:**
- Illinois SOS website is down
- Network connectivity issues
- IP is blocked

**Solution:**
1. Check if website is accessible: https://apps.ilsos.gov/businessentitysearch/
2. Try again later
3. Use a VPN or proxy if IP is blocked

## Performance Tips

### 1. Use Headless Mode

```python
scraper = IllinoisSOSScraper(headless=True)  # Faster
```

### 2. Batch Processing

Process multiple keywords efficiently:
```python
keywords = ["HVAC", "Plumbing", "Roofing", ...]
businesses = await scraper.scrape_keywords(keywords)
```

### 3. Rate Limiting

The scraper includes 2-second delays between searches to avoid overwhelming the server:
```python
await asyncio.sleep(2)  # Between searches
```

### 4. Parallel Processing

For very large keyword lists, use asyncio tasks:
```python
tasks = [scraper.search_businesses(kw) for kw in keywords]
results = await asyncio.gather(*tasks)
```

## Advanced Usage

### Custom Search with Logging

```python
import asyncio
import logging
from illinois_sos_scraper import IllinoisSOSScraper

# Set up logging
logging.basicConfig(level=logging.DEBUG)

async def main():
    scraper = IllinoisSOSScraper(headless=False)  # Show browser
    
    try:
        await scraper.initialize()
        
        # Search with custom parameters
        results = await scraper.search_businesses(
            keyword="HVAC",
            search_method="Business Name"
        )
        
        print(f"Found {len(results)} businesses")
        
        # Print statistics
        print(f"Searches: {scraper.stats['searches_performed']}")
        print(f"Results: {scraper.stats['results_found']}")
        print(f"Businesses: {scraper.stats['businesses_scraped']}")
        print(f"Errors: {scraper.stats['errors']}")
        
    finally:
        await scraper.close()

asyncio.run(main())
```

### Save to Custom Location

```python
# Save to specific directory
scraper.save_results(businesses, output_dir="/path/to/data")

# Files will be saved as:
# /path/to/data/businesses.json
# /path/to/data/businesses.csv
```

## Integration with Web Dashboard

You can integrate this scraper into your web dashboard:

```python
# In your web app backend
from illinois_sos_scraper import IllinoisSOSScraper

@app.route('/api/scrape-illinois', methods=['POST'])
async def scrape_illinois():
    data = request.json
    keywords = data.get('keywords', ['HVAC'])
    
    scraper = IllinoisSOSScraper()
    
    try:
        await scraper.initialize()
        businesses = await scraper.scrape_keywords(keywords)
        scraper.save_results(businesses)
        
        return jsonify({
            'success': True,
            'count': len(businesses),
            'stats': scraper.stats
        })
    finally:
        await scraper.close()
```

## Best Practices

1. **Always use try/finally** to ensure browser is closed
2. **Set headless=False** during development to debug issues
3. **Monitor 2Captcha credits** to avoid running out mid-scrape
4. **Use rate limiting** to avoid overwhelming the server
5. **Log everything** for debugging and monitoring
6. **Test with small batches** before running large scrapes
7. **Save results regularly** in case of interruption
8. **Handle errors gracefully** and continue processing

## Next Steps

1. **Install dependencies** and set up 2Captcha
2. **Test with single keyword** (e.g., "HVAC")
3. **Monitor logs** for any issues
4. **Scale to full keyword list** once working
5. **Integrate with dashboard** for real-time scraping
6. **Schedule regular updates** for data freshness

## Support & Resources

- **Illinois SOS Website:** https://apps.ilsos.gov/businessentitysearch/
- **Playwright Docs:** https://playwright.dev/python/
- **2Captcha Docs:** https://2captcha.com/api/python
- **Python Async:** https://docs.python.org/3/library/asyncio.html

---

**Version:** 1.0  
**Last Updated:** April 2026  
**Status:** Production Ready

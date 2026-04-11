# Illinois Secretary of State Business Entity Search - Analysis

## Website URL
https://apps.ilsos.gov/businessentitysearch/

## Key Findings

### 1. Page Structure
- **Form ID:** `index`
- **Search Method Radio Buttons:**
  - Business Name: `#name`
  - Registered Agent: `#agentsearch`
  - President: `#presidentsearch`
  - Secretary: `#secretarysearch`
  - Manager: `#managersearch`
  - File Number: `#fileNumber`
  - Keyword: `#keyWord`
  - Partial Word: `#partialWord`

- **Search Input Field:** `#searchValue` (text input)
- **Submit Button:** `#btnSearch` (submit button)

### 2. Anti-Bot Protection
**reCAPTCHA Enterprise** is implemented on the search form
- Triggers immediately after form submission
- Requires solving CAPTCHA to proceed
- This is why the original scraper failed - it couldn't handle the CAPTCHA

### 3. Search Method
The form uses POST request with the following parameters:
- `name` (radio selected): "s" for Business Name search
- `searchValue`: The search term (e.g., "HVAC")
- `btnSearch`: Submit button

### 4. Why Original Scraper Failed
```
Error: "Could not find search input: #corporateName"
```
- The original scraper was looking for `#corporateName` which doesn't exist
- The correct field ID is `#searchValue`
- The CAPTCHA protection blocks automated requests

## Solution Approaches

### Option 1: Use Playwright with CAPTCHA Solving
- Use Playwright to handle JavaScript rendering
- Integrate 2Captcha or Anti-Captcha service to solve reCAPTCHA
- More reliable but requires CAPTCHA service API key and costs

### Option 2: Use Puppeteer with Stealth Plugin
- Use puppeteer-extra with stealth plugin to bypass detection
- May work without CAPTCHA solving
- Less reliable but cheaper

### Option 3: Direct API Call (If Available)
- Check if Illinois SOS has an API endpoint
- Some state registries have backend APIs that don't require CAPTCHA
- Most reliable if available

### Option 4: Apify Actor
- Use Apify's pre-built business scraper
- Handles CAPTCHA and anti-scraping automatically
- Costs money but very reliable

## Recommended Solution

**Best Option: Playwright + 2Captcha Integration**

This approach:
1. Uses Playwright for JavaScript rendering
2. Handles form interactions properly
3. Solves reCAPTCHA using 2Captcha service
4. Extracts search results
5. Parses individual business records

## Implementation Plan

1. **Install Dependencies:**
   ```bash
   pip install playwright 2captcha-python
   python -m playwright install
   ```

2. **Set Up 2Captcha Account:**
   - Sign up at https://2captcha.com
   - Get API key
   - Add credits

3. **Create Scraper:**
   - Initialize Playwright browser
   - Navigate to search page
   - Fill in search form
   - Solve CAPTCHA using 2Captcha
   - Extract results
   - Parse business details

4. **Handle Results:**
   - Parse search results table
   - Extract business name, file number, status
   - Click on individual records for details
   - Extract full business information

## Expected Results

After fixing the scraper, you should be able to extract:
- Business Name
- File Number
- Entity Type (LLC, Corporation, etc.)
- Status (Active, Inactive, etc.)
- Filing Date
- Registered Agent
- Business Address
- Principal Officer Names

## Challenges

1. **CAPTCHA Protection** - Requires solving service
2. **Rate Limiting** - Illinois SOS may rate limit requests
3. **Dynamic Content** - Results loaded via JavaScript
4. **Session Management** - May need to maintain session across requests
5. **Data Extraction** - Results table structure needs careful parsing

## Next Steps

1. Create professional Playwright-based scraper
2. Integrate 2Captcha for CAPTCHA solving
3. Implement proper error handling
4. Add logging for debugging
5. Test with sample searches
6. Scale to full keyword list

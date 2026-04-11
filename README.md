# 🚀 LeadGen Pro - Business Lead Generation Platform

<div align="center">

![LeadGen Pro](https://img.shields.io/badge/LeadGen-Pro-ff6b9d?style=for-the-badge&logo=rocket&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776ab?style=for-the-badge&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0+-000000?style=for-the-badge&logo=flask&logoColor=white)
![React](https://img.shields.io/badge/React-18+-61dafb?style=for-the-badge&logo=react&logoColor=white)
![Deployed](https://img.shields.io/badge/Render-Deployed-46e3b7?style=for-the-badge&logo=render&logoColor=white)

**Discover freshly registered businesses in real-time. Track SEC filings, state registrations, and enrich leads with comprehensive contact data. Automatically export to GoHighLevel CRM.**

[Live Demo](https://lead-generation-dashboard.onrender.com) • [Features](#features) • [Getting Started](#quick-start) • [API Reference](#api-reference)

</div>

---

## 🔄 Pipeline Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  1. FETCH LEADS │────▶│ 2. FIND DOMAINS  │────▶│ 3. FIND OWNERS  │────▶│ 4. ENRICH DATA  │────▶│ 5. EXPORT GHL   │
│  (State Sites)  │     │  (Serper/Google) │     │ (Serper/Domain) │     │ (Apify SkipTrace)│     │   (Webhook)     │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │                       │                       │
        ▼                       ▼                       ▼                       ▼                       ▼
   All 50 States          Domain Discovery        Owner Identification     Contact Enrichment     GHL Contacts
   SEC EDGAR               Website URLs            Names & Titles          Emails & Phones       Custom Tags
   OpenCorporates          Google Search           LinkedIn Profiles       Social Profiles       Automations
```

---

## ✨ Features

### 🎨 Stunning Modern UI
- **Particle Network Landing Page** - Interactive particles with pinkish gradient hero section
- **Google One-Tap Sign-In** - Automatic account picker for seamless authentication
- **React + Vite Dashboard** - Modern React 18 pipeline control panel with real-time progress
- **Visual Pipeline Flow** - See your data progress through each step with animated connectors
- **Autopilot Mode** - Run all 5 steps automatically with one click

### 🔍 Data Scraping (All 50 US States)
- **SEC EDGAR Integration** - Real-time SEC filings (10-K, 10-Q, S-1, 8-K)
- **All 50 State SOS Sites** - Secretary of State business registrations nationwide
- **OpenCorporates API** - Additional business data from multiple jurisdictions
- **Parallel Scraping** - Concurrent execution for faster data collection

### 🌐 Domain Discovery (NEW)
- **Google Search via Serper** - Find company websites by business name and location
- **Domain Extraction** - Extract clean domain names from search results
- **Website Validation** - Verify discovered domains are active

### 👤 Owner Identification
- **Serper Business Search** - Find owner names via Google knowledge panel
- **LinkedIn Discovery** - Find owner LinkedIn profiles
- **WHOIS Integration** - Domain registrant information
- **Title Detection** - Identify CEO, Owner, Founder, etc.

### 📊 Lead Enrichment
- **Apify Skip Tracing** - Professional contact data enrichment
- **Email Discovery** - Find verified business emails
- **Phone Number Lookup** - Direct and mobile numbers
- **Social Profile Aggregation** - LinkedIn, Facebook, Twitter profiles

### 🚀 GoHighLevel Integration (NEW)
- **Webhook Export** - Push contacts directly to GHL sub-accounts
- **Custom Field Mapping** - Map all scraped data to GHL contact fields
- **Source Tagging** - Auto-tag contacts by source (e.g., `source:FL_SOS`, `source:SEC_EDGAR`)
- **Automation Triggers** - Trigger GHL workflows on new contact import

### 📤 Additional Exports
- **Google Sheets Export** - One-click export with Service Account support
- **CSV Download** - Export all leads to CSV file
- **Auto-Export** - Automatic Google Sheets push after enrichment

### 👥 User Management
- **Google OAuth 2.0** - Secure authentication
- **Role-Based Access** - Admin and Client dashboards
- **Admin Controls** - User management, scraping history, system settings

---

## 🖼️ Screenshots

### Landing Page
Beautiful particle network animation with pinkish gradient hero section and Google One-Tap sign-in.

### Client Dashboard
Modern, user-friendly interface with:
- Real-time lead statistics
- 3-step scraping process (Fetch → Find Owners → Enrich)
- Export to Google Sheets
- Interactive data tables

### Admin Dashboard
Comprehensive admin panel with:
- User management
- Scraper status monitoring
- Database management
- Activity logs
- System settings

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Google Cloud Project with OAuth configured
- (Optional) Serper API key for enrichment
- (Optional) Apify token for advanced enrichment

### 1. Clone & Install

```bash
git clone https://github.com/yourusername/lead-generation-dashboard.git
cd lead-generation-dashboard
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file:

```env
# Required
SECRET_KEY=your-secret-key-here
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret
ADMIN_EMAIL=samadly728@gmail.com

# Optional - Enrichment
SERPER_API_KEY=your-serper-api-key
APIFY_TOKEN=your-apify-token

# Optional - Google Sheets Export
GOOGLE_CREDENTIALS_JSON={"type":"service_account",...}
GOOGLE_SHEET_ID=your-spreadsheet-id
```

### 3. Run Locally

```bash
python app_flask.py
```

Visit `http://localhost:5000`

### 4. Production Deployment (Render)

The app is configured for Render deployment:

```yaml
# render.yaml already configured
gunicorn app_flask:app
```

### 5. Production Deployment (Google Cloud Run) ⚠️ IMPORTANT

**Service Name:** `leadgen-dashboard`  
**Live URL:** https://leadgen-dashboard-1022135430610.us-central1.run.app/

```bash
# ALWAYS use this exact command to deploy:
cd "C:\Users\HP\Downloads\New Regestered Bussinesses"
gcloud run deploy leadgen-dashboard --source . --region us-central1 --allow-unauthenticated --memory 1Gi --timeout 300
```

⚠️ **DO NOT** use any other service name (e.g., `lead-gen-app`). The correct service is `leadgen-dashboard`.

**Environment Variables (set in Cloud Run console):**
- `DATABASE_URL` - PostgreSQL connection string (for persistent data)
- `GOOGLE_CLIENT_ID` - OAuth client ID
- `GOOGLE_CLIENT_SECRET` - OAuth client secret
- `SECRET_KEY` - Flask session secret
- `SERPER_API_KEY` - Serper API key for domain/owner lookup
- `APIFY_API_KEY` - Apify API key for enrichment

---

## 📁 Project Structure

```
lead-generation-dashboard/
├── app_flask.py              # Main Flask application
├── auth.py                   # Google OAuth & user management
├── database.py               # SQLite/PostgreSQL operations
├── google_sheets.py          # Google Sheets export
├── serper_service.py         # Serper API integration
├── enrichment.py             # Lead enrichment (Apify, web search)
├── scraper_manager.py        # Scraper plugin manager
├── requirements.txt          # Python dependencies
├── render.yaml               # Render deployment config
├── Procfile                  # Gunicorn startup
├── scrapers/
│   ├── base_scraper.py       # Abstract scraper class
│   ├── real_scrapers.py      # SEC, State, OpenCorporates scrapers
│   └── mock_scraper.py       # Testing data generator
├── templates/
│   ├── landing.html          # Particle network landing page
│   ├── admin_dashboard.html  # Admin control panel
│   ├── client_dashboard.html # Client dashboard
│   ├── react_dashboard.html  # React-powered client UI
│   └── ...                   # Other templates
└── static/
    └── react/                # Compiled React assets
```

---

## 🔌 API Reference

### Fetch Leads
```http
POST /api/fetch-leads
Content-Type: application/json

{
  "limit": 100,
  "states": ["FL", "DE", "CA", "TX", "NY"]
}
```

### Find Owners (Serper)
```http
POST /api/fetch-owners
Content-Type: application/json

{
  "ids": [1, 2, 3]  // Optional - defaults to recent leads
}
```

### Enrich Leads (Apify)
```http
POST /api/enrich-leads
Content-Type: application/json

{
  "ids": [1, 2, 3]  // Optional - defaults to leads with owners
}
```

---

## 🔧 Configuration

### Google OAuth Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project
3. Enable **Google+ API** and **Google Sheets API**
4. Configure OAuth Consent Screen
5. Create OAuth 2.0 Client ID (Web Application)
6. Add authorized redirect URI: `https://your-domain.com/auth/callback`
7. Copy Client ID and Secret to environment variables

### Google Sheets Export

**Option 1: Service Account (Recommended)**
1. Create Service Account in Google Cloud
2. Download JSON key
3. Set `GOOGLE_CREDENTIALS_JSON` environment variable
4. Share target spreadsheet with service account email

**Option 2: OAuth Flow**
1. User authorizes on first export
2. Tokens stored in session

---

## 👨‍💼 Admin Features

The admin dashboard (`samadly728@gmail.com`) provides:

| Tab | Features |
|-----|----------|
| **Users** | View all registered users, roles, login history |
| **Scrapers** | Monitor scraper status, API availability |
| **Database** | Clear old leads, database statistics |
| **Settings** | System configuration, API keys status |
| **Logs** | Activity logs, error tracking |

---

## 🛡️ Security

- **Google OAuth 2.0** - Industry-standard authentication
- **Role-Based Access Control** - Admin vs Client permissions
- **Environment Variables** - Secrets never in code
- **CORS Protection** - Flask-CORS enabled
- **Session Management** - Secure Flask sessions

---

## 📄 License

MIT License - feel free to use for personal or commercial projects.

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open Pull Request

---

<div align="center">

**Built with ❤️ for lead generation professionals**

[Report Bug](https://github.com/SamAdly2023/lead-generation-dashboard/issues) • [Request Feature](https://github.com/SamAdly2023/lead-generation-dashboard/issues)

</div>
        return records
```

### 2. Register the Scraper

```python
# In your app or scraper_manager.py
from scrapers.california_scraper import CaliforniaScraper

manager = ScraperManager()
manager.register("CA", CaliforniaScraper())
```

## Using OpenCorporates API (Recommended)

For production use, consider using the OpenCorporates API instead of scraping:

```python
from scrapers.example_scraper import OpenCorporatesScraper

# Get your API key from https://opencorporates.com
scraper = OpenCorporatesScraper("CA", api_key="your-api-key")
records = scraper.fetch_new_businesses(limit=50)
```

## State URLs Reference

All 50 state Secretary of State business search URLs are documented in [state_urls.py](state_urls.py). Use these as starting points when developing real scrapers.

### Easy States to Scrape (⭐⭐)
- Florida, Ohio, Indiana, Kentucky, and most others

### Difficult States (⭐⭐⭐⭐)
- California, New York (CAPTCHA, anti-bot protection)

## Configuration

### Database

The SQLite database (`leads.db`) is created automatically on first run. To use a different location:

```python
from database import Database
db = Database("path/to/your/database.db")
```

### Google Sheets

Without `service_account.json`, the app uses a mock exporter that simulates exports. Add your credentials to enable real exports.

## API Reference

### ScraperManager

```python
manager = ScraperManager(use_mock_fallback=True)

# Register a scraper
manager.register("CA", CaliforniaScraper())

# Fetch from all states
records = manager.fetch_all(limit_per_state=50)

# Fetch from specific states
records = manager.fetch_all(states=["CA", "TX", "NY"])
```

### Database

```python
db = get_database()

# Save records
inserted, duplicates = db.save_records(records)

# Get all leads
df = db.get_all_leads()

# Filter by state
df = db.get_leads_by_state("California")

# Get recent leads
df = db.get_recent_leads(days=7)
```

### Google Sheets

```python
exporter = GoogleSheetsExporter("service_account.json")
result = exporter.export_dataframe(
    df,
    spreadsheet_id="your-sheet-id",
    worksheet_name="Leads",
    append=True
)
```

## Troubleshooting

### "No module named 'scrapers'"
Make sure you're running from the project root directory.

### Google Sheets export fails
1. Verify `service_account.json` exists
2. Check that the sheet is shared with the service account email
3. Ensure the Spreadsheet ID is correct

### No data returned
The mock scraper generates random data. Real scrapers need to be configured for each state.

## 📋 Recent Updates

### April 12, 2026 - Florida Sunbiz Scraper Improvements
- **Fixed**: Business name extraction now properly captures company names instead of navigation links
- **Added**: Data persistence - scraped data is saved to database and persists after page refresh
- **Added**: Business URL link column - each record includes a clickable link to the Sunbiz detail page
- **Improved**: CSV/JSON downloads now work after page refresh by pulling from saved database

See [history.md](history.md) for full changelog.

## Legal Considerations

- Always check `robots.txt` and Terms of Service before scraping
- Implement rate limiting to avoid IP blocking
- Consider using official APIs when available
- For production, use commercial data providers

## License

MIT License - See LICENSE file for details

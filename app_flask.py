"""
Lead Generation Dashboard - Flask Application
==============================================
A modern web application for tracking newly registered businesses.

Features:
- Google OAuth authentication
- Role-based access (Admin/Client)
- Fetch new business registrations
- View and filter leads
- Export leads to Google Sheets or CSV
- Local SQLite storage

To run locally:
    python app_flask.py

For production (Gunicorn):
    gunicorn app_flask:app
"""

import os
import io
import csv
import json
import time
import asyncio
import logging
import threading
import traceback
import re
import uuid
import queue
from functools import wraps

import datetime
from datetime import datetime, timedelta
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, Response, session, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from firebase_setup import initialize_firebase
try:
    initialize_firebase() 
except Exception as e:
    print(f"Startup: Firebase initialization warning: {e}")


# Import application modules
from scraper_manager import ScraperManager
from database import Database, get_database
from google_sheets import GoogleSheetsExporter, MockGoogleSheetsExporter, GoogleSheetsAPIExporter
from scrapers.real_scrapers import (
    FloridaScraper, CaliforniaScraper, DelawareScraper, 
    NewYorkScraper, TexasScraper, SECEdgarScraper,
    get_real_scraper, ALL_US_STATES, StateSpecificEdgarScraper
)
from enrichment import get_enricher, BusinessEnricher, ApifySkipTraceEnricher
from serper_service import SerperService, get_serper_service, detect_business_category
from gemini_service import get_gemini_service
from auth import init_oauth, User, admin_required, login_required_custom, oauth, ADMIN_EMAIL, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
from state_urls import STATE_URLS

# All 52 US Jurisdictions (50 States + DC + PR)
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico", "US": "United States (Federal)"
}


def df_to_records(df):
    """
    Convert DataFrame to list of records with NaN values replaced by None.
    This ensures proper handling in Jinja templates.
    """
    if df.empty:
        return []
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    return df.to_dict('records')


def _split_owner_name(full_name):
    """Split owner full name into first and last names."""
    if not full_name:
        return None, None
    cleaned = str(full_name).strip()
    if not cleaned:
        return None, None
    parts = cleaned.split()
    first_name = parts[0] if parts else None
    last_name = ' '.join(parts[1:]) if len(parts) > 1 else None
    return first_name, last_name


def _normalize_date(value):
    """Return YYYY-MM-DD for several common DOB formats; otherwise None."""
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    formats = [
        '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d',
        '%b %d, %Y', '%B %d, %Y'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def _safe_int(value):
    """Parse integer values safely."""
    if value is None or value == '':
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _dedupe_non_empty(values):
    """Return a unique list preserving order and dropping empty values."""
    out = []
    seen = set()
    for item in values or []:
        value = str(item).strip() if item is not None else ''
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _extract_city_from_address(address):
    """Best-effort city extraction from an address string."""
    if not address:
        return None
    parts = [p.strip() for p in str(address).split(',') if p.strip()]
    if len(parts) >= 2:
        return parts[1]
    return None


# ============================================================================
# BACKGROUND TASK MANAGEMENT
# ============================================================================

# Store for background fetch/scrape jobs
fetch_jobs = {}

class FetchJob:
    """Track background scraping job progress."""
    def __init__(self, job_id: str, states: list, limit: int):
        self.job_id = job_id
        self.states = states
        self.limit = limit
        self.status = 'running'  # running, completed, failed
        self.scraped_count = 0
        self.saved_count = 0
        self.error = None
        self.started_at = datetime.now()
        self.completed_at = None
        self.results_summary = ""

    def to_dict(self):
        return {
            'job_id': self.job_id,
            'states': self.states,
            'limit': self.limit,
            'status': self.status,
            'scraped_count': self.scraped_count,
            'saved_count': self.saved_count,
            'error': self.error,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'results_summary': self.results_summary
        }


# Store for background enrichment tasks
enrichment_tasks = {}

class EnrichmentTask:
    """Track background enrichment progress."""
    def __init__(self, task_id: str, total_leads: int):
        self.task_id = task_id
        self.total = total_leads
        self.processed = 0
        self.enriched = 0
        self.failed = 0
        self.status = 'running'  # running, completed, failed
        self.error = None
        self.results = {}        # Track individual lead outcomes {lead_id: result_data}
        self.started_at = datetime.now()
        self.completed_at = None
    
    def to_dict(self):
        return {
            'task_id': self.task_id,
            'total': self.total,
            'processed': self.processed,
            'enriched': self.enriched,
            'failed': self.failed,
            'status': self.status,
            'error': self.error,
            'results': self.results,
            'progress_percent': int((self.processed / self.total * 100)) if self.total > 0 else 0,
            'elapsed_seconds': (datetime.now() - self.started_at).total_seconds()
        }


# Store for background export tasks
ghl_export_tasks = {}

class GHLExportTask:
    """Track background GHL export progress."""
    def __init__(self, task_id: str, total_leads: int):
        self.task_id = task_id
        self.total = total_leads
        self.processed = 0
        self.success = 0
        self.failed = 0
        self.status = 'running'  # running, completed, failed
        self.error = None
        self.started_at = datetime.now()
        self.completed_at = None
    
    def to_dict(self):
        return {
            'task_id': self.task_id,
            'total': self.total,
            'processed': self.processed,
            'success': self.success,
            'failed': self.failed,
            'status': self.status,
            'error': self.error,
            'progress_percent': int((self.processed / self.total * 100)) if self.total > 0 else 0,
            'elapsed_seconds': int((datetime.now() - self.started_at).total_seconds())
        }

# ============================================================================
# APP CONFIGURATION
# ============================================================================

# ============================================================
# SERVER-SIDE LEAD CACHE
# Loads data ONCE at/after startup and caches for 30 minutes.
# This drastically reduces Firestore reads, preventing 429 errors.
# All read-heavy routes use this cache instead of querying Firestore directly.
# ============================================================
import threading as _threading
import copy as _copy

_lead_cache = {
    'leads': [],          # list of dicts
    'total': 0,
    'enriched': 0,
    'stats': {},
    'users': [],
    'timestamp': None,
    'lock': _threading.Lock()
}
CACHE_TIMEOUT = 1800  # 30 minutes - long enough to survive heavy traffic
DATA_CACHE_TIMEOUT = 1800

# Alias for backward compat
_stats_cache = {
    'data': None,
    'timestamp': None,
    'total_leads': 0,
    'enriched_count': 0,
    'total_leads_timestamp': None,
    'enriched_timestamp': None
}

def _is_cache_valid():
    """Check if lead cache is still fresh."""
    if _lead_cache['timestamp'] is None:
        return False
    age = (datetime.now() - _lead_cache['timestamp']).total_seconds()
    return age < CACHE_TIMEOUT

def _refresh_lead_cache(force=False):
    """Load leads from Firestore into memory cache. Thread-safe. Returns True on success."""
    global _lead_cache, _stats_cache
    if not force and _is_cache_valid():
        return True
    
    with _lead_cache['lock']:
        # Double-check after acquiring lock
        if not force and _is_cache_valid():
            return True
        try:
            df = db.get_all_leads(limit=2000)
            if df.empty:
                # Fallback to direct Firestore stream if wrapped DB call times out.
                direct_records = _load_leads_direct_from_firestore(limit=2000)
                if direct_records:
                    df = pd.DataFrame(direct_records)
            stats = db.get_stats() or {}
            total = stats.get('total_leads', len(df))
            
            enriched_count = 0
            if not df.empty:
                has_domain = df['website'].notna() & (df['website'] != '') & (df['website'] != 'Not Found') if 'website' in df.columns else pd.Series([False]*len(df))
                has_email = df['email'].notna() & (df['email'] != '') if 'email' in df.columns else pd.Series([False]*len(df))
                has_phone = df['phone'].notna() & (df['phone'] != '') if 'phone' in df.columns else pd.Series([False]*len(df))
                enriched_count = int((has_domain | has_email | has_phone).sum())
            
            records = df_to_records(df) if not df.empty else []
            
            _lead_cache['leads'] = records
            _lead_cache['total'] = total
            _lead_cache['enriched'] = enriched_count
            _lead_cache['stats'] = stats
            _lead_cache['timestamp'] = datetime.now()
            
            # Sync to stats cache as well
            _stats_cache['total_leads'] = total
            _stats_cache['enriched_count'] = enriched_count
            _stats_cache['total_leads_timestamp'] = datetime.now()
            _stats_cache['enriched_timestamp'] = datetime.now()
            
            print(f"[Cache] Refreshed: {total} total leads, {enriched_count} enriched, {len(records)} loaded.")
            return True
        except Exception as e:
            print(f"[Cache] Failed to refresh: {e}")
            # Keep stale data if any
            return False


def _load_leads_direct_from_firestore(limit=2000):
    """Read leads directly from Firestore, bypassing timeout decorators."""
    try:
        if not hasattr(db, 'leads_ref'):
            return _load_leads_from_backup_json(limit=limit)
        docs = list(db.leads_ref.limit(limit).stream(timeout=12.0))
        records = []
        for doc in docs:
            row = doc.to_dict() or {}
            if 'id' not in row:
                row['id'] = doc.id
            records.append(row)
        if records:
            records.sort(key=lambda r: str(r.get('filing_date') or ''), reverse=True)
        return records
    except Exception as e:
        print(f"[LeadsDirect] Firestore fallback read failed: {e}")
        return _load_leads_from_backup_json(limit=limit)


def _load_leads_from_backup_json(limit=2000):
    """Emergency fallback: read bundled JSON so UI stays usable during Firestore outages."""
    try:
        backup_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'all_states_leads.json')
        if not os.path.exists(backup_path):
            return []

        with open(backup_path, 'r', encoding='utf-8') as f:
            rows = json.load(f)

        if not isinstance(rows, list):
            return []

        records = []
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            rec = dict(row)
            rec.setdefault('id', f'backup_{idx}')
            records.append(rec)

        records.sort(key=lambda r: str(r.get('filing_date') or ''), reverse=True)
        return records[:limit]
    except Exception as e:
        print(f"[LeadsBackup] JSON fallback read failed: {e}")
        return []

def _background_cache_refresh():
    """Trigger a cache refresh in a background thread so it doesn't block requests."""
    t = _threading.Thread(target=_refresh_lead_cache, args=(True,), daemon=True)
    t.start()

def get_cached_leads(refresh_if_empty=True):
    """Return leads from cache, refreshing if empty or stale."""
    if not _is_cache_valid() and refresh_if_empty:
        _refresh_lead_cache()
    return _copy.copy(_lead_cache['leads'])

def get_cached_total():
    """Return cached total lead count."""
    if not _is_cache_valid():
        _refresh_lead_cache()
    return _lead_cache['total']

def get_cached_enriched():
    """Return cached enriched lead count."""
    if not _is_cache_valid():
        _refresh_lead_cache()
    return _lead_cache['enriched']

def get_cached_stats():
    """Return cached stats dict."""
    if not _is_cache_valid():
        _refresh_lead_cache()
    return _lead_cache['stats']

def invalidate_cache():
    """Invalidate cache after a data-changing operation."""
    _lead_cache['timestamp'] = None


app = Flask(__name__)
CORS(app)
logger = logging.getLogger(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Health check endpoint - must be registered EARLY before any DB calls
@app.route('/_health')
@app.route('/_ah/health')
def health_check():
    """Health check for Cloud Run startup/liveness probes."""
    return jsonify({'status': 'ok'}), 200

app.config['GOOGLE_CLIENT_ID'] = GOOGLE_CLIENT_ID
app.config['GOOGLE_CLIENT_SECRET'] = GOOGLE_CLIENT_SECRET
app.config['ADMIN_EMAIL'] = ADMIN_EMAIL
app.config['GEMINI_API_KEY'] = os.environ.get('GEMINI_API_KEY', '')
app.config['SERPER_API_KEY'] = os.environ.get('SERPER_API_KEY', '')

# Session and Security Configuration
if os.environ.get('K_SERVICE') or os.environ.get('GOOGLE_CLOUD_PROJECT'):
    app.config['PREFERRED_URL_SCHEME'] = 'https'
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    # Production settings
    app.config['SESSION_COOKIE_SECURE'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
else:
    # Development settings (works on localhost/HTTP)
    app.config['SESSION_COOKIE_SECURE'] = False
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_HTTPONLY'] = True

# REQUIRED FOR FIREBASE AUTH POPUPS: Allow the site to be an opener for cross-origin popups
@app.after_request
def add_security_headers(response):
    """Add security and cache-busting headers to all responses."""
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin-allow-popups'
    
    # Cache-busting headers to ensure UI updates are seen immediately
    if response.mimetype == 'text/html':
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, proxy-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        response.headers['Surrogate-Control'] = 'no-store'
    
    return response

# Initialize OAuth
init_oauth(app)

try:
    # Database instance (Firestore)
    db = get_database()
except Exception as e:
    print(f"CRITICAL: Failed to initialize database: {e}")
    # Create a mock/empty db object so the app doesn't crash on import
    from unittest.mock import MagicMock
    db = MagicMock()

def api_login_required(f):
    """Decorator for API routes that returns JSON error instead of redirect."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required', 'redirect': '/'}), 401
        return f(*args, **kwargs)
    return decorated_function

# Clean up any placeholder data at startup
try:
    if hasattr(db, 'cleanup_placeholder_leads'):
        cleaned = db.cleanup_placeholder_leads()
        if cleaned > 0:
            print(f"Startup: Cleaned {cleaned} placeholder/invalid leads from database")
except Exception as e:
    print(f"Startup cleanup error (non-critical): {e}")

# Pre-load leads into memory cache in background thread (non-blocking)
# This means the first page load will use cache within seconds
try:
    _background_cache_refresh()
except Exception as e:
    print(f"Startup cache refresh failed (non-critical): {e}")

# Scraper manager (real scrapers only)
scraper_manager = ScraperManager(use_mock_fallback=False)



# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_sheets_exporter():
    """Get Google Sheets exporter."""
    # Use API exporter for simpler setup via env vars
    exporter = GoogleSheetsAPIExporter()
    if not exporter.is_configured():
        # Fallback to older exporter if needed, or mock
        old_exporter = GoogleSheetsExporter()
        if old_exporter.is_configured():
            return old_exporter
        return MockGoogleSheetsExporter()
    return exporter


def get_export_leads_df(limit: int = 5000) -> pd.DataFrame:
    """Load leads for export with the same resilient fallback chain as UI pages."""
    try:
        df = db.get_all_leads(limit=limit)
        if not df.empty:
            return df
    except Exception:
        pass

    try:
        cached = get_cached_leads(refresh_if_empty=True)
        if cached:
            return pd.DataFrame(cached)
    except Exception:
        pass

    try:
        direct = _load_leads_direct_from_firestore(limit=limit)
        if direct:
            return pd.DataFrame(direct)
    except Exception:
        pass

    try:
        backup = _load_leads_from_backup_json(limit=limit)
        if backup:
            return pd.DataFrame(backup)
    except Exception:
        pass

    return pd.DataFrame()


def auto_export_to_sheet(leads, sheet_id=None):
    """
    Automatically export leads to Google Sheet if configured.
    Prioritizes OAuth (user account) over Service Account to avoid 'Drive storage quota exceeded' errors.
    """
    # Load spreadsheet ID from settings
    google_settings = load_google_settings()
    target_sheet_id = sheet_id or google_settings.get('spreadsheet_id') or os.environ.get('GOOGLE_SHEET_ID') or '1IjXeEEEli4Oyjve7jS0TIgoUO4wfYPkp5LpltmJ9zak'
    if not target_sheet_id:
        return {'success': False, 'error': 'GOOGLE_SHEET_ID not set'}
        
    try:
        # 1. Try OAuth (User Account) First - Use the 'best' available token
        # This fixes the "Drive storage quota exceeded" issue which usually happens on service accounts
        token_dict = None
        try:
            token_dict = get_best_google_token()
        except:
            pass

        exporter_api = GoogleSheetsAPIExporter(token_dict=token_dict)
        if exporter_api.is_authenticated():
            # Convert to DataFrame if list of dicts
            if isinstance(leads, list):
                df = pd.DataFrame(leads)
            else:
                df = leads
            
            if df.empty:
                return {'success': False, 'error': 'No data to export'}
            
            # Use defined column order
            result = exporter_api.export_dataframe(df, target_sheet_id, worksheet_name='Leads', append=True)
            print(f"Auto-export result (OAuth API): {result}")
            return result

        # 2. Try Service Account only as a secondary fallback
        # (Using a separate instance without token_dict will force it to check for service account files)
        service_account_exporter = GoogleSheetsAPIExporter()
        if service_account_exporter.is_configured():
             # Convert to DataFrame if list of dicts
            if isinstance(leads, list):
                df = pd.DataFrame(leads)
            else:
                df = leads
                
            if df.empty:
                return {'success': False, 'error': 'No data to export'}
                
            result = service_account_exporter.export_dataframe(df, target_sheet_id, worksheet_name='Leads', append=True)
            print(f"Auto-export result (Service Account): {result}")
            return result
            
        return {'success': False, 'error': 'No Google Sheets configuration found (OAuth or Service Account)'}

    except Exception as e:
        import traceback
        print(f"Auto export error: {e}")
        print(traceback.format_exc())
        return {'success': False, 'error': str(e)}


# GHL Settings storage path
GHL_SETTINGS_FILE = os.path.join(os.path.dirname(__file__), 'ghl_settings.json')

def load_ghl_settings():
    """Load GoHighLevel settings from file."""
    try:
        if os.path.exists(GHL_SETTINGS_FILE):
            with open(GHL_SETTINGS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading GHL settings: {e}")
    return {
        'webhook_url': os.environ.get('GHL_WEBHOOK_URL', ''), 
        'api_key': os.environ.get('GHL_API_KEY', ''),
        'location_id': os.environ.get('GHL_LOCATION_ID', ''),
        'tag': 'lead_scraper'
    }


def save_ghl_settings_to_file(webhook_url=None, tag='lead_scraper', api_key=None, location_id=None):
    """Save GoHighLevel settings to file."""
    try:
        settings = load_ghl_settings()
        if webhook_url is not None: settings['webhook_url'] = webhook_url
        if tag is not None: settings['tag'] = tag
        if api_key is not None: settings['api_key'] = api_key
        if location_id is not None: settings['location_id'] = location_id
        
        with open(GHL_SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving GHL settings: {e}")
        return False


# Google Settings storage path
GOOGLE_SETTINGS_FILE = os.path.join(os.path.dirname(__file__), 'google_settings.json')

def load_google_settings():
    """Load Google Sheets settings from DB or file."""
    # 1. Try Firestore FIRST
    try:
        db_sid = db.get_setting('google_spreadsheet_id')
        if db_sid and db_sid.strip():
            return {'spreadsheet_id': db_sid.strip()}
    except Exception as e:
        print(f"Error getting setting from DB: {e}")
        
    # 2. Try JSON file SECOND
    try:
        if os.path.exists(GOOGLE_SETTINGS_FILE):
            with open(GOOGLE_SETTINGS_FILE, 'r') as f:
                data = json.load(f)
                if data.get('spreadsheet_id') and data.get('spreadsheet_id').strip():
                    return data
    except Exception as e:
        print(f"Error loading Google settings: {e}")
        
    # 3. Default fallback
    env_sid = os.environ.get('GOOGLE_SHEET_ID')
    if env_sid and env_sid.strip():
        return {'spreadsheet_id': env_sid.strip()}
        
    return {'spreadsheet_id': '1IjXeEEEli4Oyjve7jS0TIgoUO4wfYPkp5LpltmJ9zak'}


def save_google_settings_to_file(spreadsheet_id):
    """Save Google Sheets settings to DB and file."""
    # 1. Save to Firestore
    db_success = False
    try:
        db_success = db.save_setting('google_spreadsheet_id', spreadsheet_id)
    except Exception as e:
        print(f"Error saving setting to DB: {e}")
        
    # 2. Save to JSON file
    try:
        settings = {'spreadsheet_id': spreadsheet_id}
        with open(GOOGLE_SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving Google settings: {e}")
        return db_success


def get_dashboard_stats():
    """Get statistics for the dashboard - uses in-memory cache to avoid Firestore quota."""
    try:
        stats = get_cached_stats()
        total_leads = get_cached_total()

        # If cache shows 0, try a live count from Firestore to avoid stale-zero display
        if total_leads == 0:
            try:
                live_count = db.get_leads_count()
                if live_count and live_count > 0:
                    total_leads = live_count
                    _lead_cache['total'] = live_count
            except Exception as e:
                print(f"[Dashboard] Live count fallback failed: {e}")

        return {
            'total_leads': total_leads,
            'new_today': stats.get('new_today', 0),
            'this_week': stats.get('this_week', 0),
            'states_count': stats.get('states_count', 0),
            'last_fetch': stats.get('last_fetch', 'Never'),
            'leads_by_state': stats.get('leads_by_state', [])
        }
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        # Try one more live attempt
        try:
            live_count = db.get_leads_count()
        except:
            live_count = _lead_cache['total']
        return {
            'total_leads': live_count or _lead_cache['total'],
            'new_today': 0,
            'this_week': 0,
            'states_count': 0,
            'last_fetch': 'Never',
            'leads_by_state': []
        }



def get_state_stats(force_refresh=False):
    """Get leads count by state - Optimized and Cached."""
    global _stats_cache
    now = datetime.now()
    
    # Return cached data if valid and not forced
    if not force_refresh and (_stats_cache['data'] is not None and 
        _stats_cache['timestamp'] is not None and 
        (now - _stats_cache['timestamp']).total_seconds() < CACHE_TIMEOUT):
        return _stats_cache['data']

    try:
        if force_refresh:
            _stats_cache['timestamp'] = None # Clear timestamp to force fetch

        # Fallback path: derive state counts from available records.
        state_counts = {code: 0 for code in US_STATES.keys()}

        # Try DB read first.
        df = db.get_all_leads(limit=5000)
        if not df.empty and 'state' in df.columns:
            counts = df['state'].astype(str).str.upper().value_counts().to_dict()
            for code, count in counts.items():
                if code in state_counts:
                    state_counts[code] = int(count)
        else:
            # Use the same resilient source chain as /leads route.
            records = get_cached_leads(refresh_if_empty=True)
            if not records:
                records = _load_leads_direct_from_firestore(limit=5000)
            if not records:
                records = _load_leads_from_backup_json(limit=5000)

            for row in records:
                code = str((row or {}).get('state') or '').upper().strip()
                if code in state_counts:
                    state_counts[code] += 1
        
        total = sum(state_counts.values()) or 1
        
        stats = []
        for code, count in sorted(state_counts.items(), key=lambda x: (x[1], US_STATES.get(x[0], x[0])), reverse=True):
            stats.append({
                'code': code,
                'name': US_STATES.get(code, code),
                'count': count,
                'percentage': round((count / total) * 100, 1)
            })
        
        _stats_cache['data'] = stats
        _stats_cache['timestamp'] = now
        return stats
    except Exception as e:
        logger.error(f"Error refreshing stats: {e}")
        return _stats_cache['data'] if _stats_cache['data'] else []


# ============================================================================
# PUBLIC ROUTES
# ============================================================================

@app.route('/favicon.ico')
@app.route('/favicon.svg')
def favicon():
    """Serve favicon."""
    return app.send_static_file('favicon.svg')


@app.route('/new-dashboard')
@login_required_custom
def new_dashboard():
    """Serve the new React Dashboard."""
    return render_template('react_dashboard.html')


@app.route('/')
def landing():
    """Landing page - public."""
    # If they just logged out, don't auto-redirect
    if session.get('logout_success'):
        # Keep the flag for the template to potentially disable auto-prompt
        pass
    elif request.args.get('preview') != '1' and current_user.is_authenticated:
        if current_user.is_admin:
            return redirect(url_for('admin_dashboard'))
        return redirect(url_for('client_dashboard'))
    
    return render_template('landing.html', current_year=datetime.now().year, logout_success=session.get('logout_success', False))


@app.route('/privacy')
def privacy():
    """Privacy Policy page."""
    return render_template('privacy.html')


@app.route('/terms')
def terms():
    """Terms of Service page."""
    return render_template('terms.html')


@app.route('/login')
@app.route('/auth/login')
def auth_login():
    """Universal login/signup page."""
    if current_user.is_authenticated:
        if current_user.is_admin:
            return redirect(url_for('admin_dashboard'))
        return redirect(url_for('client_dashboard'))
    return render_template('auth_page.html')

@app.route('/auth/firebase-login', methods=['POST'])
def firebase_login():
    """Verify Firebase ID token and login via Flask-Login."""
    import time
    start_time = time.time()
    try:
        from firebase_admin import auth as firebase_auth
        
        # Log request details for debugging
        print(f"[Auth] Firebase login request received from {request.remote_addr}")
        
        data = request.get_json(silent=True)
        if not data:
            print(f"[Auth] Error: No JSON data received in request")
            return jsonify({'success': False, 'error': 'No data provided'}), 400
            
        id_token = data.get('idToken')
        if not id_token:
            print(f"[Auth] Error: Missing idToken in request data")
            return jsonify({'success': False, 'error': 'No token provided'}), 400
            
        # Verify the ID token
        print(f"[{time.time()-start_time:.2f}s] [Auth] Verifying token (length: {len(id_token)})...")
        try:
            decoded_token = firebase_auth.verify_id_token(id_token)
        except Exception as ve:
            print(f"[{time.time()-start_time:.2f}s] [Auth] Token verification failed: {ve}")
            return jsonify({'success': False, 'error': f'Token verification failed: {str(ve)}'}), 401
            
        uid = decoded_token['uid']
        email = decoded_token.get('email')
        name = decoded_token.get('name', email)
        picture = decoded_token.get('picture')
        
        if not email:
            print(f"[{time.time()-start_time:.2f}s] [Auth] Error: No email found in token for UID {uid}")
            return jsonify({'success': False, 'error': 'No email found in token'}), 400
            
        # Create or update user
        print(f"[{time.time()-start_time:.2f}s] [Auth] Calling User.create_or_update for {email}...")
        user = User.create_or_update(
            email=email,
            name=name,
            picture=picture
        )
        print(f"[{time.time()-start_time:.2f}s] [Auth] User object received: {getattr(user, 'id', 'NO_ID')}")
        
        # Log in the user
        print(f"[{time.time()-start_time:.2f}s] [Auth] Calling login_user...")
        try:
            login_user(user, remember=True)
            print(f"[{time.time()-start_time:.2f}s] [Auth] login_user success")
        except Exception as lue:
            print(f"[{time.time()-start_time:.2f}s] [Auth] login_user CRASHED: {lue}")
            raise lue
            
        # Cache to session
        print(f"[{time.time()-start_time:.2f}s] [Auth] Setting session data...")
        session[f'user_data_{user.id}'] = {
            'email': user.email,
            'name': user.name,
            'picture': user.picture,
            'is_admin': user.is_admin
        }
        
        # Ensure session is modified
        session.modified = True
        print(f"[{time.time()-start_time:.2f}s] [Auth] Session data set")
        
        # Determine redirect URL
        redirect_url = url_for('admin_dashboard') if user.is_admin else url_for('client_dashboard')
        print(f"[{time.time()-start_time:.2f}s] [Auth] Login successful. Redirecting to {redirect_url}")
        
        return jsonify({
            'success': True,
            'redirect': redirect_url,
            'user': {
                'email': user.email,
                'name': user.name,
                'is_admin': user.is_admin
            }
        })
        
    except Exception as e:
        import traceback
        print(f"[Auth] Firebase login error after {time.time()-start_time:.2f}s: {e}")
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/auth/google-sheets')
@login_required_custom
def auth_google_sheets():
    """Initiate Google Sheets OAuth - only called when user wants to export."""
    redirect_uri = url_for('auth_sheets_callback', _external=True)
    return oauth.google_sheets.authorize_redirect(
        redirect_uri,
        access_type='offline',
        prompt='consent'
    )


@app.route('/auth/sheets-callback')
@login_required_custom
def auth_sheets_callback():
    """Handle Google Sheets OAuth callback - saves Sheets token."""
    try:
        token = oauth.google_sheets.authorize_access_token()
        if token:
            token_info = {
                'token': token.get('access_token'),
                'refresh_token': token.get('refresh_token'),
                'token_uri': 'https://oauth2.googleapis.com/token',
                'client_id': app.config.get('GOOGLE_CLIENT_ID'),
                'client_secret': app.config.get('GOOGLE_CLIENT_SECRET'),
                'scopes': token.get('scope', '').split(' ')
            }
            session['google_token'] = token_info
            if current_user.is_admin:
                db.save_setting('google_admin_token', token_info)
            flash('Google Sheets connected successfully! You can now export data.', 'success')
        next_url = session.pop('sheets_auth_next', url_for('leads'))
        return redirect(next_url)
    except Exception as e:
        print(f"Sheets OAuth error: {e}")
        flash('Google Sheets authorization failed. Please try again.', 'error')
        return redirect(url_for('leads'))


@app.route('/logout')
@login_required
def logout():
    """Log out the user."""
    logout_user()
    session.clear()
    session['google_token'] = None
    session['logout_success'] = True
    flash('You have been logged out.', 'info')
    return redirect(url_for('landing'))


@app.route('/auth/google-one-tap', methods=['POST'])
def google_one_tap_callback():
    """Handle Google One Tap sign-in callback."""
    session.pop('logout_success', None)
    try:
        from google.oauth2 import id_token
        from google.auth.transport import requests as google_requests
        
        # Get the credential token from the request
        credential = request.form.get('credential')
        
        if not credential:
            flash('No credential received', 'error')
            return redirect(url_for('landing'))
        
        # Verify the token
        idinfo = id_token.verify_oauth2_token(
            credential, 
            google_requests.Request(), 
            app.config['GOOGLE_CLIENT_ID']
        )
        
        # Get user info from verified token
        email = idinfo.get('email')
        name = idinfo.get('name', email)
        picture = idinfo.get('picture')
        
        if not email:
            flash('Could not get email from Google', 'error')
            return redirect(url_for('landing'))
        
        # Create or update user
        user = User.create_or_update(
            email=email,
            name=name,
            picture=picture
        )
        
        # Log in the user
        login_user(user)
        
        flash(f'Welcome, {user.name}!', 'success')
        
        # Redirect based on role
        if user.is_admin:
            return redirect(url_for('admin_dashboard'))
        return redirect(url_for('client_dashboard'))
        
    except ValueError as e:
        print(f"Google One Tap verification error: {e}")
        flash('Invalid credential. Please try again.', 'error')
        return redirect(url_for('landing'))
    except Exception as e:
        print(f"Google One Tap error: {e}")
        flash('Authentication failed. Please try again.', 'error')
        return redirect(url_for('landing'))


# ============================================================================
# ADMIN ROUTES
# ============================================================================

@app.route('/admin')
@admin_required
def admin_dashboard():
    """Admin dashboard - admin only."""
    import time
    t0 = time.time()
    print(f"[Admin] Dashboard access started...")
    
    try:
        print(f"[Admin] Fetching stats...")
        stats = get_dashboard_stats()
        print(f"[Admin] Stats fetched in {time.time()-t0:.2f}s")
        
        t1 = time.time()
        print(f"[Admin] Fetching all users...")
        users = User.get_all_users()
        print(f"[Admin] Users fetched in {time.time()-t1:.2f}s")
        
        t2 = time.time()
        print(f"[Admin] Checking Google Sheets config...")
        exporter = GoogleSheetsExporter()
        sheets_configured = exporter.is_configured()
        print(f"[Admin] Sheets config checked in {time.time()-t2:.2f}s")
        
        # Mock activity logs
        logs = [
            {'time': datetime.now().strftime('%Y-%m-%d %H:%M'), 'level': 'info', 'message': 'Admin dashboard accessed'},
            {'time': (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M'), 'level': 'info', 'message': 'System startup complete'},
        ]
        
        print(f"[Admin] Dashboard data prepared in {time.time()-t0:.2f}s. Rendering template...")
        return render_template('admin_dashboard.html',
                              stats=stats,
                              users=users,
                              logs=logs,
                              sheets_configured=sheets_configured,
                              config=app.config)
    except Exception as e:
        print(f"[Admin] Dashboard CRASHED: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Error loading dashboard: {str(e)}", "error")
        return redirect(url_for('landing'))


@app.route('/admin/clear-old-leads', methods=['POST'])
@admin_required
def admin_clear_old_leads():
    """Clear leads older than 30 days."""
    try:
        count = db.clear_old_leads(days=30)
        flash(f'Cleared {count} leads older than 30 days', 'success')
    except Exception as e:
        flash(f'Error clearing old leads: {str(e)}', 'error')
    return redirect(url_for('admin_dashboard'))


@app.route('/admin/clear-all-leads', methods=['POST'])
@admin_required
def admin_clear_all_leads():
    """Clear all leads from database."""
    try:
        db.clear_all_leads()
        flash('All leads have been cleared', 'success')
    except Exception as e:
        flash(f'Error clearing leads: {str(e)}', 'error')
    return redirect(url_for('admin_dashboard'))


@app.route('/admin/update-user-role', methods=['POST'])
@admin_required
def admin_update_user_role():
    """Update a user's role (admin or client). Admin-only."""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        new_role = data.get('role')  # 'admin' or 'client'
        
        if not user_id or new_role not in ('admin', 'client'):
            return jsonify({'success': False, 'error': 'Invalid parameters'}), 400
        
        # Prevent self-demotion
        if user_id == current_user.id and new_role == 'client':
            return jsonify({'success': False, 'error': 'You cannot demote your own account.'}), 403
        
        is_admin = (new_role == 'admin')
        
        # Update in Firestore
        ref = User._get_users_ref()
        if not ref:
            return jsonify({'success': False, 'error': 'Database unavailable'}), 500
        
        ref.document(user_id).update({'is_admin': is_admin})
        return jsonify({'success': True, 'message': f'User role updated to {new_role}'})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# CLIENT ROUTES
# ============================================================================

@app.route('/client')
@login_required_custom
def client_dashboard():
    """Redirect old /client to /fetch."""
    return redirect(url_for('fetch_leads'))


@app.route('/old-client')
@login_required_custom
def old_client_dashboard():
    """Client dashboard - authenticated users."""
    stats = get_dashboard_stats()
    state_stats = get_state_stats()
    
    try:
        df = db.get_all_leads()
        recent_leads = df_to_records(df.head(10))
    except Exception:
        recent_leads = []
    
    saved_searches = []  # TODO: Implement saved searches
    export_history = session.get('export_history', [])
    
    return render_template('client_dashboard.html',
                          stats=stats,
                          state_stats=state_stats,
                          recent_leads=recent_leads,
                          saved_searches=saved_searches,
                          export_history=export_history)


# ============================================================================
# PROTECTED ROUTES (Require Authentication)
# ============================================================================

@app.route('/dashboard')
@login_required_custom
def dashboard():
    """Main dashboard page."""
    stats = get_dashboard_stats()
    state_stats = get_state_stats()
    
    try:
        df = db.get_all_leads(limit=10)
        recent_leads = df_to_records(df)
    except Exception as e:
        print(f"Error fetching recent leads for dashboard: {e}")
        recent_leads = []
    
    return render_template('dashboard.html', 
                          stats=stats,
                          state_stats=state_stats,
                          recent_leads=recent_leads)


@app.route('/states-report')
@login_required_custom
def states_report():
    """Detailed report showing lead distribution across all states."""
    force_refresh = request.args.get('refresh') == 'true'
    try:
        stats = get_state_stats(force_refresh=force_refresh)
        total_leads = sum(int((s or {}).get('count', 0)) for s in (stats or []))
        return render_template('states_report.html',
                              stats=stats,
                              total_leads=total_leads)
    except Exception as e:
        logger.error(f"Error rendering states-report: {e}")
        return "Internal Server Error", 500



@app.route('/leads')
@login_required_custom
def leads():
    """All leads listing page with pagination - served from in-memory cache."""
    state_filter = request.args.get('state', '')
    status_filter = request.args.get('status', '')
    search_query = request.args.get('search', '')
    page = request.args.get('page', 1, type=int)
    per_page = 50
    
    try:
        page = request.args.get('page', 1, type=int)
        per_page = 50
        
        # Prefer cache to reduce read cost.
        all_cached = get_cached_leads()

        # Hard fallback: if cache is empty, read directly from Firestore.
        if not all_cached:
            all_cached = _load_leads_direct_from_firestore(limit=5000)
            if all_cached:
                _lead_cache['leads'] = all_cached
                _lead_cache['total'] = len(all_cached)
                _lead_cache['timestamp'] = datetime.now()

        # Emergency fallback: use local backup JSON if all data sources are unavailable.
        if not all_cached:
            all_cached = _load_leads_from_backup_json(limit=5000)
            if all_cached:
                _lead_cache['leads'] = all_cached
                _lead_cache['total'] = len(all_cached)
                _lead_cache['timestamp'] = datetime.now()

        total_leads = get_cached_total() or len(all_cached)
        
        has_filters = bool(state_filter or status_filter or search_query)
        
        if has_filters:
            # Filter in memory
            filtered = all_cached
            if state_filter:
                filtered = [l for l in filtered if l.get('state','').upper() == state_filter.upper()]
            if status_filter:
                filtered = [l for l in filtered if status_filter.lower() in str(l.get('status','')).lower()]
            if search_query:
                filtered = [l for l in filtered if search_query.lower() in str(l.get('business_name','')).lower()]
            
            total_filtered = len(filtered)
            start = (page - 1) * per_page
            end = start + per_page
            all_leads = filtered[start:end]
            has_next = end < total_filtered
            has_prev = page > 1
            total_leads = total_filtered
        else:
            # Paginate from cache
            start = (page - 1) * per_page
            end = start + per_page
            all_leads = all_cached[start:end]
            has_next = end < total_leads
            has_prev = page > 1
        
    except Exception as e:
        import traceback
        print(f"Error getting leads: {e}")
        print(traceback.format_exc())
        all_leads = []
        total_leads = 0
        has_next = False
        has_prev = False
        page = 1
    
    return render_template('leads.html', 
                          leads=all_leads, 
                          total_leads=total_leads, 
                          page=page, 
                          has_next=has_next, 
                          has_prev=has_prev)




@app.route('/fetch')
@login_required_custom
def fetch_leads():
    """Fetch leads page."""
    serper_configured = False
    try:
        serper_configured = get_serper_service().is_configured()
    except Exception:
        serper_configured = False

    apify_configured = bool(os.environ.get('APIFY_TOKEN') or os.environ.get('APIFY_API_TOKEN'))
    if not apify_configured:
        try:
            # Fall back to configured enricher token if present.
            apify_configured = bool(get_enricher(use_mock=False, use_apify=True).api_token)
        except Exception:
            apify_configured = False

    return render_template(
        'fetch.html',
        all_states=sorted(US_STATES.keys()),
        state_names=US_STATES,
        serper_configured=serper_configured,
        apify_configured=apify_configured,
        last_results=session.get('last_scraper_results', []),
        last_state_results=session.get('last_state_results', {}),
        last_fetch_time=session.get('last_fetch_time')
    )


@app.route('/scraper/dashboard')
@login_required_custom
def scraper_dashboard():
    """Redirect to the unified Fetch Leads hub."""
    flash('All scraping is now managed from the Fetch Leads hub.', 'info')
    return redirect(url_for('fetch_leads'))

# Store for background scrape jobs
_scrape_jobs = {}

# ============================================================================
# LIVE LOG STREAMING  (SSE - Server-Sent Events)
# ============================================================================
import queue as _queue
from collections import deque

# Per-job log queues: { job_id: queue.Queue }  — entries are plain strings
_scrape_log_queues: dict = {}
_LOG_QUEUE_MAX = 500   # max buffered log lines per job

# Persistent app-log ring buffer (latest 2000 lines kept in memory)
_app_logs: deque = deque(maxlen=2000)

# Per-job persistent log: { job_id: [line, ...] }
_job_log_history: dict = {}


def _log_emit(job_id: str, message: str):
    """Push a timestamped log line into the job's queue AND persistent log. Thread-safe."""
    if not job_id:
        return
    q = _scrape_log_queues.get(job_id)
    ts = datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}]  {message}"
    if q is not None:
        try:
            q.put_nowait(line)
        except _queue.Full:
            pass  # drop silently if buffer full
    # Also save to persistent history
    _job_log_history.setdefault(job_id, []).append(line)
    _app_logs.append(line)


def _log_done(job_id: str):
    """Signal that the scrape job has finished streaming."""
    if not job_id:
        return
    q = _scrape_log_queues.get(job_id)
    if q:
        try:
            q.put_nowait('__DONE__')
        except _queue.Full:
            pass


@app.route('/fetch/stream/<job_id>')
@login_required_custom
def fetch_log_stream(job_id):
    """
    SSE endpoint — streams log lines for a running scrape job.
    The browser opens this immediately after submitting the fetch form.
    Sends newline-delimited `data: <line>\n\n` SSE frames.
    Connection closes automatically when the job emits __DONE__.
    """
    def _generate():
        # Create or reuse the queue for this job
        if job_id not in _scrape_log_queues:
            _scrape_log_queues[job_id] = _queue.Queue(maxsize=_LOG_QUEUE_MAX)

        q = _scrape_log_queues[job_id]
        timeout_seconds = 600   # max 10 min stream
        deadline = time.time() + timeout_seconds

        # Send a heartbeat immediately so the browser connection is confirmed
        yield 'data: [--] Log stream connected. Waiting for scraper...\n\n'

        while time.time() < deadline:
            try:
                line = q.get(timeout=1.0)
                if line == '__DONE__':
                    yield 'data: [✓] Pipeline complete.\n\n'
                    yield 'event: done\ndata: done\n\n'
                    break
                # Escape any newlines inside the message itself
                safe = line.replace('\n', ' ').replace('\r', '')
                yield f'data: {safe}\n\n'
            except _queue.Empty:
                # Keep-alive comment — prevents proxy/CDN from closing idle connections
                yield ': keep-alive\n\n'
        else:
            yield 'data: [!] Stream timeout reached.\n\n'
            yield 'event: done\ndata: timeout\n\n'

        # Cleanup after stream ends
        _scrape_log_queues.pop(job_id, None)

    return Response(
        _generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',    # disable nginx buffering
            'Connection': 'keep-alive',
        }
    )

@app.route('/api/scraper/global', methods=['POST'])
@login_required_custom
def trigger_global_scraper():
    """Trigger the global scraper in a background thread. Returns job_id immediately."""
    import uuid
    try:
        data = request.get_json(silent=True) or {}
        limit = int(data.get('limit', 10))
    except Exception:
        limit = 10

    from scraper_manager import get_manager
    manager = get_manager()

    global_scraper = manager.get_scraper("SEC_GLOBAL")
    if not global_scraper:
        return jsonify({"success": False, "error": "Discovery engine not available. Please use the Fetch Leads page instead."}), 200

    job_id = str(uuid.uuid4())[:8]
    _scrape_jobs[job_id] = {'status': 'running', 'inserted': 0, 'duplicates': 0, 'error': None}

    def _run_scrape():
        try:
            records = global_scraper.fetch_new_businesses(limit=limit)
            _db = get_database()
            inserted, duplicates, ids = _db.save_records(records)
            invalidate_cache()
            _scrape_jobs[job_id].update({'status': 'done', 'inserted': inserted, 'duplicates': duplicates})
        except Exception as e:
            _scrape_jobs[job_id].update({'status': 'error', 'error': str(e)})

    t = threading.Thread(target=_run_scrape, daemon=True)
    t.start()

    # Wait up to 45 seconds for the job to finish before returning
    # This keeps Cloud Run happy while still giving the scraper a chance to complete
    for _ in range(45):
        time.sleep(1)
        job = _scrape_jobs.get(job_id, {})
        if job.get('status') != 'running':
            break

    job = _scrape_jobs.get(job_id, {})
    if job.get('status') == 'done':
        return jsonify({
            "success": True,
            "inserted": job['inserted'],
            "duplicates": job['duplicates'],
            "total_skipped": max(0, limit - job['inserted'] - job['duplicates'])
        })
    elif job.get('status') == 'error':
        return jsonify({"success": False, "error": job.get('error', 'Unknown error')})
    else:
        # Still running — return partial success message
        return jsonify({
            "success": False,
            "error": "The discovery run is still in progress. Leads will be saved automatically. Check the All Leads page in a few minutes."
        })



@app.route('/api/cron/daily-scrape')
def cron_daily_scrape():
    """
    Endpoint for Cloud Scheduler to trigger daily scraping.
    Security: In production, check for a secret header.
    """
    # auth_token = request.headers.get('X-Cloud-Scheduler-Auth')
    # if auth_token != os.environ.get('CRON_SECRET'):
    #     return "Unauthorized", 401

    limit = 10
    from scraper_manager import get_manager
    manager = get_manager()
    global_scraper = manager.get_scraper("SEC_GLOBAL")
    records = global_scraper.fetch_new_businesses(limit=limit)
    db = get_database()
    inserted, duplicates, ids = db.save_records(records)
    
    return f"Daily scrape complete. Inserted {inserted} new leads."


@app.route('/fetch_status/<job_id>')
@login_required_custom
def fetch_status(job_id):
    """Retrieve the current status of a background fetch job."""
    job = fetch_jobs.get(job_id)
    if not job: return jsonify({'error': 'Job not found'}), 404
    return jsonify(job.to_dict())


@app.route('/fetch', methods=['POST'])
@login_required_custom
def do_fetch():
    """Starts a discovery pipeline in the background and returns a job_id immediately."""
    try:
        states = request.form.getlist('states')
        if not states: states = ['FL']
        limit = int(request.form.get('limit', 20))
        use_sec = request.form.get('use_sec') == 'on'
        use_serper = request.form.get('use_serper') == 'on'
        use_apify = request.form.get('use_apify') == 'on'
    except Exception as e:
        flash(f"Invalid input: {e}", "error")
        return redirect(url_for('fetch_leads'))

    # Unique job ID for SSE streaming and status tracking
    job_id = uuid.uuid4().hex[:10]
    _scrape_log_queues[job_id] = queue.Queue(maxsize=_LOG_QUEUE_MAX)
    
    # Store job metadata
    job = FetchJob(job_id, states, limit)
    fetch_jobs[job_id] = job

    # Launch the actual heavy lifting in a background thread
    # This prevents the initial POST from timing out (503)
    thread = threading.Thread(
        target=_run_fetch_pipeline_async,
        args=(job_id, states, limit, use_sec, use_serper, use_apify),
        daemon=True
    )
    thread.start()

    # Client-side JS in fetch.html will receive this and start tailing /fetch/stream/<job_id>
    return jsonify({
        'status': 'started',
        'job_id': job_id,
        'message': 'Pipeline execution initiated. Gathering logs...'
    })


def _run_fetch_pipeline_async(job_id, states, limit, use_sec, use_serper, use_apify):
    """The full scraping pipeline executed in a safe background thread."""
    from datetime import datetime
    all_records = []
    saved = 0
    duplicates = 0
    scraped_total = 0
    new_lead_ids = []

    job = fetch_jobs.get(job_id)

    def _log(msg: str):
        _log_emit(job_id, msg)
        print(f"[JOB-{job_id}] {msg}")

    try:
        _log(f"🚀 Discovery Pipeline Launched  |  Target States: {', '.join(states)}")
        _log(f"   Settings → Limit: {limit}/state  |  Recent filings only")
        
        # --------------------------------------------------------------------
        # STEP 1: SCRAPE LEAD DATA
        # --------------------------------------------------------------------
        from scraper_manager import get_manager
        manager = get_manager()
        
        # 1A: Primary State SOS Scrapers (Parallelized)
        _log("📡 Step 1A — Launching State SOS Scrapers (Primary Hub)...")
        sos_records = manager.fetch_all(
            states=states, 
            limit_per_state=limit, 
            log_callback=lambda m: _log_emit(job_id, f"   {m}")
        )
        all_records.extend(sos_records)

        # 1B: Targeted SEC Fallback (if requested)
        if use_sec:
            existing_states = set(r.state.upper() for r in all_records if r.state)
            missing = [s for s in states if s.upper() not in existing_states]
            if missing:
                _log(f"⬇  Step 1B — SEC fallback for missing states: {', '.join(missing)}")
                from scrapers.real_scrapers import SECEdgarScraper
                sec_scraper = SECEdgarScraper()
                for s in missing:
                    try:
                        _log_emit(job_id, f"   [{s}] SEC Search...")
                        recs = sec_scraper.fetch_new_businesses(limit=limit, state_code=s)
                        if recs:
                            all_records.extend(recs)
                            _log_emit(job_id, f"   [{s}] ✓ Added {len(recs)} records via SEC")
                    except Exception as ex:
                        _log_emit(job_id, f"   [{s}] SEC Error: {ex}")

        # --------------------------------------------------------------------
        # STEP 2: DEDUPLICATION & DATABASE STORAGE
        # --------------------------------------------------------------------
        _log(f"💾 Step 2 — Processing {len(all_records)} raw records...")
        db = get_database()

        scraped_total = len(all_records)
        
        # Use the existing save_records method which handles deduplication internally
        inserted, dup_count, inserted_ids = db.save_records(all_records)
        saved = inserted
        duplicates = dup_count
        new_lead_ids = inserted_ids

        # Update job progress
        if job:
            job.scraped_count = scraped_total
            job.saved_count = saved
        
        _log(f"✅ Step 2 complete. {saved} new leads saved into DB. ({duplicates} duplicates skipped)")

        # --------------------------------------------------------------------
        # STEP 3: OWNER & CONTACT ENRICHMENT (Asynchronous Threads)
        # --------------------------------------------------------------------
        if saved > 0 and (use_serper or use_apify):
            _log("✨ Step 3 — Starting Enrichment Loop for new leads...")
            
            # Fetch enriched lead data from DB to ensure we have latest objects
            leads_to_enrich = []
            if new_lead_ids:
                leads_df = db.get_leads_by_ids(new_lead_ids)
                if not leads_df.empty:
                    leads_to_enrich = df_to_records(leads_df)

            if leads_to_enrich:
                processed = 0
                enriched_count = 0
                total_to_enrich = len(leads_to_enrich)
                
                from concurrent.futures import ThreadPoolExecutor, as_completed
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(_perform_enrichment_on_single_lead_task, l, use_serper, use_apify, job_id) for l in leads_to_enrich]
                    for f in as_completed(futures):
                        processed += 1
                        if f.result(): enriched_count += 1
                        if job: job.results_summary = f"Progress: {processed}/{total_to_enrich} enriched."
                
                _log(f"🎯 Step 3 complete. {enriched_count} leads enriched successfully.")

        if job:
            job.status = 'completed'
            job.results_summary = f"Workflow finished. Scraped: {scraped_total}, New Saved: {saved}."
            job.completed_at = datetime.now()
        _log("🏁 Discovery Pipeline Finalized.")

    except Exception as e:
        if job:
            job.status = 'failed'
            job.error = str(e)
        _log(f"✗ Pipeline CRASH: {e}")
        traceback.print_exc()
    finally:
        # Signal the SSE stream to close cleanly
        _log_done(job_id)
        # Invalidate cache
        global _stats_cache
        _stats_cache['timestamp'] = None


def _perform_enrichment_on_single_lead_task(lead, use_serper, use_apify, job_id):
    """
    Enriches a specific lead using the gated EnrichmentService pipeline:
      1. Serper Places  → category, phone, website
      2. LLM Gate       → verify_local_service() → YES / NO
      3. Apify skip-trace (only if LLM says YES)
    """
    from datetime import datetime
    try:
        db = get_database()
        l_id = lead.get('id')
        name = lead.get('business_name')
        state = lead.get('state')

        _log_emit(job_id, f"   🔹 Enriching: {name} ({state})")

        from enrichment import EnrichmentService
        enricher = EnrichmentService()

        enriched = enricher.enrich_local_lead(lead)

        # Collect fields that were populated by the pipeline
        update_data = {}
        for key in ('owner_name', 'website', 'phone', 'email',
                     'places_category', 'places_phone', 'places_website',
                     'places_address', 'places_rating',
                     'phone_1', 'phone_2', 'email_1', 'email_2',
                     'address', 'age', 'llm_qualified', 'status'):
            val = enriched.get(key)
            if val is not None:
                update_data[key] = val

        qualified = enriched.get('llm_qualified', True)
        if not qualified:
            _log_emit(job_id, f"   ⛔ {name} → UNQUALIFIED (LLM gate). Apify skipped.")

        if update_data:
            update_data['enriched_at'] = datetime.now().isoformat()
            db.update_lead_enrichment(l_id, update_data)
            return True
    except Exception as ex:
        _log_emit(job_id, f"   ⚠️ Enrichment error for '{lead.get('business_name')}': {ex}")
    return False


@app.route('/fetch/report')
@login_required_custom
def fetch_report():
    """Show a post-fetch report instead of error page."""
    report = session.get('last_fetch_report')
    if not report:
        flash('No recent fetch report found. Run a new fetch to see details.', 'info')
        return redirect(url_for('fetch_leads'))
    return render_template('fetch_report.html', report=report)


@app.route('/logs')
@login_required_custom
def app_logs_page():
    """View persistent application logs (fetch pipeline runs etc.)."""
    # Gather all job logs, most recent first
    jobs_summary = []
    for jid, lines in sorted(_job_log_history.items(), key=lambda x: x[0], reverse=True):
        jobs_summary.append({'job_id': jid, 'line_count': len(lines), 'lines': lines})
    return render_template('logs.html', jobs=jobs_summary, global_logs=list(_app_logs))


@app.route('/api/logs')
@login_required_custom
def api_logs():
    """JSON endpoint returning latest app logs."""
    return jsonify({'logs': list(_app_logs), 'count': len(_app_logs)})


@app.route('/search')
@login_required_custom
def search_page():
    """Redirect to the unified Fetch Leads hub (was Custom Search / Live Scrape)."""
    return redirect(url_for('fetch_leads'))


@app.route('/export')
@login_required_custom
def export_page():
    """Export configuration page."""
    # Use API exporter which supports OAuth tokens
    token_dict = get_best_google_token()
    exporter = GoogleSheetsAPIExporter(token_dict=token_dict)
    sheets_configured = exporter.is_configured()
    sheets_authenticated = exporter.is_authenticated()
    
    try:
        total_leads = db.get_leads_count()
    except Exception:
        total_leads = 0
    
    export_history = session.get('export_history', [])
    google_settings = load_google_settings()
    
    return render_template('export.html', 
                          sheets_configured=sheets_configured,
                          sheets_authenticated=sheets_authenticated,
                          total_leads=total_leads,
                          export_history=export_history,
                          google_settings=google_settings)


@app.route('/export/csv')
@login_required_custom
def export_csv():
    """Export leads as CSV file."""
    try:
        df = get_export_leads_df(limit=5000)
        
        if df.empty:
            flash('No leads to export', 'warning')
            return redirect(url_for('export_page'))
        
        all_leads = df.to_dict('records')
        
        # Create CSV in memory with all available columns
        output = io.StringIO()
        fieldnames = ['business_name', 'state', 'phone', 'address', 'filing_date', 'url', 'ein',
                      'tin',
                      'industry_category', 'owner_name', 'first_name', 'last_name',
                      'phone_1', 'phone_2', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'dob',
                      'age', 'website', 'status', 'entity_type', 'filing_number', 'sic_code',
                      'business_address', 'business_phone', 'mailing_address', 'cik',
                      'sec_file_number', 'film_number', 'sec_act', 'cf_office', 'fiscal_year_end',
                      'state_of_incorporation']
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        
        writer.writeheader()
        for lead in all_leads:
            writer.writerow(lead)
        
        # Update export history
        history = session.get('export_history', [])
        history.insert(0, {
            'type': 'csv',
            'records': len(all_leads),
            'date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        session['export_history'] = history[:10]  # Keep last 10
        
        # Return CSV file
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=leads_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
    except Exception as e:
        flash(f'Error exporting CSV: {str(e)}', 'error')
        return redirect(url_for('export_page'))


@app.route('/api/google/quota')
@admin_required
def get_google_quota():
    """Fetch storage quota for the service account."""
    try:
        from google_sheets import GoogleSheetsAPIExporter
        exporter = GoogleSheetsAPIExporter()
        return jsonify(exporter.get_quota_info())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/debug/quota')
def debug_quota():
    try:
        from google_sheets import GoogleSheetsAPIExporter
        exporter = GoogleSheetsAPIExporter()
        # Use the new method but keep the debug output format
        quota_details = exporter.get_quota_info()
        
        from googleapiclient.discovery import build
        creds = exporter._get_credentials()
        drive = build('drive', 'v3', credentials=creds)
        
        # Get all files
        results = drive.files().list(
            fields="files(id, name, mimeType, size)",
            pageSize=100
        ).execute()
        files = results.get('files', [])
        
        # Try empty trash
        drive.files().emptyTrash().execute()
        
        return {
            'quota': quota_details,
            'files_count': len(files),
            'files_sample': files[:10],
            'empty_trash': 'completed'
        }
    except Exception as e:
        return {'error': str(e)}

def get_best_google_token():
    """
    Get the best available Google OAuth token.
    Priority: 1. Session, 2. Firestore (Admin), 3. local token.json
    """
    # 1. Try Session
    token = session.get('google_token')
    if token:
        return token
        
    # 2. Try Firestore (Admin token)
    token = db.get_setting('google_admin_token')
    if token:
        return token
        
    # 3. Try local file (Fallback)
    if os.path.exists('token.json'):
        try:
            import json
            with open('token.json', 'r') as f:
                return json.load(f)
        except Exception:
            pass
            
    return None

@app.route('/export/sheets/direct')
@login_required_custom
def export_sheets_direct():
    """Export leads directly to a new Google Sheet."""
    try:
        df = get_export_leads_df(limit=5000)
        logger.info(f"[SheetsDirect] Prepared dataframe rows={len(df)} cols={len(df.columns) if not df.empty else 0}")
        
        if df.empty:
            flash('No leads to export', 'warning')
            return redirect(url_for('leads'))
        
        # Use the API exporter with the BEST available token (Session -> Firestore -> File)
        token_dict = get_best_google_token()
        exporter = GoogleSheetsAPIExporter(token_dict=token_dict)
        oauth_exporter = GoogleSheetsExporter()
        
        if not exporter.is_configured():
            flash('Google Sheets not configured. Go to Settings to connect your Google account.', 'warning')
            return redirect(url_for('settings'))

        if not exporter.is_authenticated():
            # Start OAuth flow and retry export after callback.
            session['pending_export'] = {
                'spreadsheet_id': load_google_settings().get('spreadsheet_id', ''),
                'worksheet_name': 'Leads'
            }
            try:
                redirect_uri = request.host_url.rstrip('/') + url_for('oauth2callback')
                auth_url = oauth_exporter.get_authorization_url(redirect_uri)
                return redirect(auth_url)
            except Exception as auth_e:
                logger.error(f"[SheetsDirect] OAuth URL generation failed: {auth_e}")
                flash('Google OAuth is not configured. Please set valid GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in Cloud Run.', 'error')
                return redirect(url_for('settings'))
        
        # Load settings to see if we have a target spreadsheet
        google_settings = load_google_settings()
        spreadsheet_id = google_settings.get('spreadsheet_id')

        # If spreadsheet is configured, write directly to a deterministic worksheet/tab.
        if spreadsheet_id:
            result = exporter.export_dataframe(df, spreadsheet_id, worksheet_name='Leads', append=True)
            result['spreadsheet_url'] = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        else:
            title = f"Business Leads Export - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
            result = exporter.create_new_spreadsheet(
                title=title,
                spreadsheet_id=None,
                df=df,
                append=False,
                worksheet_name='Leads'
            )

        logger.info(f"[SheetsDirect] Export result success={result.get('success')} rows={result.get('rows_exported')} error={result.get('error')}")
        
        if result.get('success'):
            # Update export history
            history = session.get('export_history', [])
            history.insert(0, {
                'type': 'sheets',
                'records': len(df),
                'date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                'url': result.get('spreadsheet_url', '')
            })
            session['export_history'] = history[:10]
            
            flash(f'Successfully exported {len(df)} leads to Google Sheets! <a href="{result.get("spreadsheet_url", "")}" target="_blank" class="underline">Open Sheet</a>', 'success')
            return redirect(result.get('spreadsheet_url', ''))
        else:
            error_msg = f'Export failed: {result.get("error", "Unknown error")}'
            if result.get("details"):
                error_msg += f"<br><br><strong>TO FIX THIS:</strong><br>{result.get('details').replace(chr(10), '<br>')}"
            flash(error_msg, 'error')
            return redirect(url_for('leads'))
            
    except Exception as e:
        flash(f'Error exporting to Sheets: {str(e)}', 'error')
        return redirect(url_for('leads'))

@app.route('/export/sheets', methods=['POST'])
@login_required_custom
def export_sheets():
    """Export leads to Google Sheets."""
    try:
        spreadsheet_id = request.form.get('spreadsheet_id')
        worksheet_name = (request.form.get('worksheet_name', 'Leads') or 'Leads').strip()
        
        if not spreadsheet_id:
            flash('Please enter a Spreadsheet ID', 'error')
            return redirect(url_for('export_page'))
        
        # Use API exporter for exports
        # Priority: Session -> Firestore -> local token.json
        token_dict = get_best_google_token()
        api_exporter = GoogleSheetsAPIExporter(token_dict=token_dict)
        oauth_exporter = GoogleSheetsExporter()
        
        if not oauth_exporter.is_configured():
            flash('Google Sheets not configured. Go to Settings to set up.', 'warning')
            return redirect(url_for('settings'))
        
        # Check if authenticated (token exists in session, file, or environment)
        if not api_exporter.is_authenticated():
            session['pending_export'] = {
                'spreadsheet_id': spreadsheet_id,
                'worksheet_name': worksheet_name
            }
            # Use OAuth exporter to start auth flow
            redirect_uri = request.host_url.rstrip('/') + url_for('oauth2callback')
            auth_url = oauth_exporter.get_authorization_url(redirect_uri)
            return redirect(auth_url)
        
        df = get_export_leads_df(limit=5000)
        logger.info(f"[SheetsForm] Prepared dataframe rows={len(df)} cols={len(df.columns) if not df.empty else 0} target_sheet={spreadsheet_id} worksheet={worksheet_name}")

        if df.empty:
            flash('No leads available to export right now. Please refresh leads and try again.', 'warning')
            return redirect(url_for('export_page'))
        
        result = api_exporter.export_dataframe(df, spreadsheet_id, worksheet_name)
        logger.info(f"[SheetsForm] Export result success={result.get('success')} rows={result.get('rows_exported')} error={result.get('error')}")
        
        if result.get('success'):
            # Update export history
            history = session.get('export_history', [])
            history.insert(0, {
                'type': 'sheets',
                'records': len(df),
                'date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            })
            session['export_history'] = history[:10]
            
            flash(f'Successfully exported {len(df)} leads to Google Sheets', 'success')
            return redirect(url_for('leads'))
        else:
            error_msg = f'Export failed: {result.get("error", "Unknown error")}'
            if result.get("details"):
                error_msg += f"<br><br><strong>TO FIX THIS:</strong><br>{result.get('details').replace(chr(10), '<br>')}"
            flash(error_msg, 'error')
            return redirect(url_for('export_page'))
    except Exception as e:
        flash(f'Error exporting to Sheets: {str(e)}', 'error')
    
    return redirect(url_for('export_page'))


@app.route('/oauth2callback')
def oauth2callback():
    """Handle Google Sheets OAuth callback."""
    try:
        exporter = GoogleSheetsExporter()
        # Use the full callback URL, but construct the redirect_uri for token exchange
        redirect_uri = request.host_url.rstrip('/') + url_for('oauth2callback')
        authorization_response = request.url
        success = exporter.handle_oauth_callback(authorization_response, redirect_uri)
        
        if success:
            # Persist token beyond single instance so Cloud Run exports stay authenticated.
            try:
                if os.path.exists('token.json'):
                    with open('token.json', 'r', encoding='utf-8') as f:
                        token_payload = json.load(f)
                    session['google_token'] = token_payload
                    # Save for subsequent requests/instances.
                    db.save_setting('google_admin_token', token_payload)
            except Exception as token_e:
                print(f"[OAuth] Token persistence warning: {token_e}")

            flash('Successfully connected to Google Sheets!', 'success')
            pending = session.pop('pending_export', None)
            if pending:
                # Use API exporter for the actual export
                token_dict = get_best_google_token()
                api_exporter = GoogleSheetsAPIExporter(token_dict=token_dict)
                df = get_export_leads_df(limit=5000)
                result = api_exporter.export_dataframe(
                    df, 
                    pending['spreadsheet_id'], 
                    pending['worksheet_name']
                )
                if result.get('success'):
                    flash(f'Exported {len(df)} leads to Google Sheets', 'success')
                else:
                    flash(f'Export failed: {result.get("error", "Unknown error")}', 'error')
        else:
            flash('Failed to complete Google authentication', 'error')
    except Exception as e:
        flash(f'OAuth error: {str(e)}', 'error')
    
    return redirect(url_for('settings'))


@app.route('/settings/google/connect')
@login_required_custom
def google_connect():
    """Start Google Sheets OAuth flow."""
    try:
        exporter = GoogleSheetsExporter()
        # Get the redirect URI and attempt OAuth URL generation directly.
        redirect_uri = request.host_url.rstrip('/') + url_for('oauth2callback')
        auth_url = exporter.get_authorization_url(redirect_uri)
        return redirect(auth_url)
    except Exception as e:
        flash(f'Error starting Google auth: {str(e)}', 'error')
        return redirect(url_for('settings'))


@app.route('/settings/google/disconnect', methods=['POST'])
@login_required_custom
def google_disconnect():
    """Disconnect Google Sheets."""
    try:
        # Remove stored OAuth token
        token_path = 'token.json'
        if os.path.exists(token_path):
            os.remove(token_path)
            flash('Disconnected from Google Sheets.', 'success')
        else:
            flash('No Google connection to disconnect.', 'info')
    except Exception as e:
        flash(f'Error disconnecting: {str(e)}', 'error')
    
    return redirect(url_for('settings'))


@app.route('/settings/cleanup-placeholders', methods=['POST'])
@login_required_custom  
def cleanup_placeholder_data():
    """Clean up placeholder data from database."""
    try:
        cleaned = db.cleanup_placeholder_leads()
        flash(f'Cleaned up {cleaned} placeholder/invalid records from database.', 'success')
    except Exception as e:
        flash(f'Error during cleanup: {str(e)}', 'error')
    
    return redirect(url_for('settings'))


@app.route('/settings/google/cleanup', methods=['POST'])
@admin_required
def google_drive_cleanup():
    """Emergency purge of service account Google Drive."""
    try:
        from google_sheets import GoogleSheetsAPIExporter
        exporter = GoogleSheetsAPIExporter()
        
        # Perform full purge (keep_count=0)
        success = exporter.purge_service_account_drive()
        
        if success:
            flash('Successfully purged all files from service account Google Drive and emptied trash. Storage quota should be restored.', 'success')
        else:
            flash('Failed to perform Google Drive cleanup. Check logs for details.', 'error')
    except Exception as e:
        flash(f'Error during Google Drive cleanup: {str(e)}', 'error')
    
    return redirect(url_for('settings'))


@app.route('/settings')
@login_required_custom
def settings():
    """System settings page."""
    # Load settings
    ghl_settings = load_ghl_settings()
    google_settings = load_google_settings()
    
    # Check Google Sheets status
    exporter = GoogleSheetsExporter()
    sheets_configured = exporter.is_configured()
    sheets_authenticated = exporter.is_authenticated()
    
    try:
        total_records = db.get_leads_count()
    except Exception:
        total_records = 0
    
    # Get database type and info for display
    db_type = db.db_type if hasattr(db, 'db_type') else 'sqlite'
    db_url = db.db_url if hasattr(db, 'db_url') else None
    
    # Detect Google Cloud SQL
    is_cloud_sql = db_url and '/cloudsql/' in db_url
    
    if is_cloud_sql:
        db_type_display = 'Google Cloud SQL (PostgreSQL)'
    elif db_type == 'firestore':
        db_type_display = 'Google Cloud Firestore (Persistent)'
    else:
        db_type_display = {
            'postgres': 'PostgreSQL (Persistent)',
            'sqlite': 'SQLite (Local/Ephemeral)'
        }.get(db_type, db_type)
    
    db_is_persistent = db_type in ['postgres', 'firestore']
    
    db_stats = {
        'total_records': total_records,
        'db_type': db_type,
        'db_type_display': db_type_display,
        'is_persistent': db_is_persistent,
        'is_cloud_sql': is_cloud_sql
    }
    
    return render_template('settings.html', 
                          sheets_configured=sheets_configured,
                          sheets_authenticated=sheets_authenticated,
                          db_stats=db_stats,
                          ghl_settings=ghl_settings,
                          google_settings=google_settings)


@app.route('/settings/google/save', methods=['POST'])
@login_required_custom
def save_google_settings():
    """Save Google Sheets settings."""
    try:
        spreadsheet_id = request.form.get('spreadsheet_id', '').strip()
        if save_google_settings_to_file(spreadsheet_id):
            flash('Google settings saved successfully!', 'success')
        else:
            flash('Error saving Google settings', 'error')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
    return redirect(url_for('settings'))


@app.route('/settings/ghl/save', methods=['POST'])
@login_required_custom
def save_ghl_settings():
    """Save GHL settings."""
    try:
        webhook_url = request.form.get('ghl_webhook_url', '').strip()
        api_key = request.form.get('ghl_api_key', '').strip()
        location_id = request.form.get('ghl_location_id', '').strip()
        tag = request.form.get('ghl_tag', 'lead_scraper').strip()
        
        if save_ghl_settings_to_file(webhook_url, tag, api_key, location_id):
            flash('GHL settings saved successfully!', 'success')
        else:
            flash('Error saving GHL settings', 'error')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
    return redirect(url_for('settings'))


@app.route('/api/export/ghl', methods=['POST'])
@login_required_custom
def export_to_ghl():
    """Export leads to GoHighLevel (background task)."""
    try:
        settings = load_ghl_settings()
        api_key = settings.get('api_key')
        location_id = settings.get('location_id')
        
        if not api_key:
            return jsonify({'success': False, 'message': 'Please add GHL API Key in settings first'}), 400
            
        # Get leads to export from cache (avoids Firestore read)
        leads_list = get_cached_leads()
        
        if not leads_list:
            return jsonify({'success': False, 'message': 'No leads to export'}), 400
            
        import uuid
        task_id = str(uuid.uuid4())[:8]
        task = GHLExportTask(task_id, len(leads_list))
        ghl_export_tasks[task_id] = task
        
        def run_ghl_export():
            try:
                from ghl_service import GHLService
                ghls = GHLService(api_key, location_id)
                tag = settings.get('tag', 'lead_scraper')
                
                for lead in leads_list:
                    if task.status != 'running': break
                    
                    success, result = ghls.create_contact(lead, tag=tag)
                    task.processed += 1
                    if success:
                        task.success += 1
                    else:
                        task.failed += 1
                    
                    # Sleep to respect rate limits (GHL is 100req/min for V1, 10req/sec for V2 usually)
                    # We'll stick to a safe 0.5s
                    time.sleep(0.5)
                
                task.status = 'completed'
                task.completed_at = datetime.datetime.now()
            except Exception as e:
                task.status = 'failed'
                task.error = str(e)
                print(f"GHL export task error: {e}")

        # Start thread
        thread = threading.Thread(target=run_ghl_export)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True, 
            'task_id': task_id,
            'message': f"Export started in background for {len(leads_list)} leads."
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/export/ghl/status/<task_id>')
@login_required_custom
def ghl_export_status(task_id):
    """Get the status of a background GHL export task."""
    task = ghl_export_tasks.get(task_id)
    if not task:
        return jsonify({'success': False, 'message': 'Task not found'}), 404
        
    return jsonify({
        'success': True,
        'task': task.to_dict()
    })


@app.route('/api/export/ghl/test', methods=['POST'])
@login_required_custom
def test_ghl_connection():
    """Test GHL connection with provided credentials."""
    try:
        data = request.json
        api_key = data.get('api_key')
        location_id = data.get('location_id')
        
        if not api_key:
            return jsonify({'success': False, 'message': 'API Key is required'})
            
        from ghl_service import GHLService
        ghls = GHLService(api_key, location_id)
        
        # Try a very simple test - maybe search or just try to create a dummy contact
        test_lead = {
            'business_name': 'Test Project (Lead Scraper)',
            'email': 'test@example.com',
            'phone': '1234567890'
        }
        
        success, message = ghls.create_contact(test_lead, tag='test_connection')
        
        if success:
            return jsonify({'success': True, 'message': 'Successfully connected and sent test contact'})
        else:
            return jsonify({'success': False, 'message': f"Connection failed: {message}"})
            
    except Exception as e:
        return jsonify({'success': False, 'message': f"Error: {str(e)}"})


@app.route('/settings/ghl/test', methods=['POST'])
@login_required_custom
def api_get_ghl_settings():
    """Get GHL settings for React frontend."""
    settings = load_ghl_settings()
    return jsonify({
        'webhookUrl': settings.get('webhook_url', ''),
        'tag': settings.get('tag', 'lead_scraper')
    })


@app.route('/settings/clear-database', methods=['POST'])
@admin_required
def clear_database():
    """Clear all data from the database - admin only."""
    try:
        db.clear_all_leads()
        flash('All data has been cleared', 'success')
    except Exception as e:
        flash(f'Error clearing database: {str(e)}', 'error')
    
    return redirect(url_for('settings'))


# ============================================================================
# DOMAIN LOOKUP ROUTES
# ============================================================================

@app.route('/knowledgebase')
@login_required_custom
def knowledgebase():
    """Knowledge base page with state SOS website info."""
    return render_template('knowledgebase.html')


@app.route('/domain-lookup')
@login_required_custom
def domain_lookup_page():
    """Domain owner lookup page."""
    return render_template('domain_lookup.html')


@app.route('/api/domain-lookup', methods=['POST'])
@login_required_custom
def api_domain_lookup():
    """
    API endpoint to lookup domain owner information.
    Uses Serper to search for domain registration and owner details.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        domain = data.get('domain', '').strip()
        
        if not domain:
            return jsonify({'success': False, 'error': 'Domain is required'}), 400
        
        # Clean domain
        domain = domain.lower()
        domain = domain.replace('https://', '').replace('http://', '').replace('www.', '')
        domain = domain.split('/')[0]  # Remove path
        
        # Validate domain format
        import re
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9-]*\.[a-zA-Z]{2,}$', domain):
            return jsonify({'success': False, 'error': 'Invalid domain format'}), 400
        
        # Use Serper service to lookup domain
        serper = get_serper_service()
        
        if not serper.is_configured():
            return jsonify({
                'success': False, 
                'error': 'Serper API not configured. Set SERPER_API_KEY environment variable.'
            }), 400
        
        # Perform domain lookup
        result = serper.lookup_domain_owner(domain)
        
        # Optionally, enhance with Apify skip trace if owner name found
        use_apify = data.get('use_apify', False)
        if use_apify and result.owner_name:
            try:
                from enrichment import get_enricher
                apify = get_enricher(use_mock=False, use_apify=True)
                apify_result = apify.skip_trace_by_name(result.owner_name, max_results=1)
                
                if apify_result and len(apify_result) > 0:
                    apify_data = apify_result[0]
                    # Merge Apify data with Serper results
                    if apify_data.get('Email-1') and not result.emails:
                        result.emails = [apify_data.get('Email-1')]
                        if apify_data.get('Email-2'):
                            result.emails.append(apify_data.get('Email-2'))
                    if apify_data.get('Phone-1') and not result.phones:
                        result.phones = [apify_data.get('Phone-1')]
                        if apify_data.get('Phone-2'):
                            result.phones.append(apify_data.get('Phone-2'))
                    if apify_data.get('Street Address') and not result.address:
                        street = apify_data.get('Street Address', '')
                        city = apify_data.get('Address Locality', '')
                        state = apify_data.get('Address Region', '')
                        postal = apify_data.get('Postal Code', '')
                        result.address = f"{street}, {city}, {state} {postal}".strip(', ')
            except Exception as e:
                print(f"Apify enrichment error for domain lookup: {e}")
        
        return jsonify({
            'success': True,
            'result': result.to_dict()
        })
        
    except Exception as e:
        print(f"Domain lookup error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# ENRICHMENT ROUTES
# ============================================================================

@app.route('/enrich')
@login_required_custom
def enrich_page():
    """Enrichment configuration page - served from in-memory lead cache."""
    try:
        total_leads = get_cached_total()
        enriched_count = get_cached_enriched()
        all_leads = get_cached_leads()
        unenriched_count = max(0, total_leads - enriched_count)
        
    except Exception as e:
        print(f"Error in enrich_page: {e}")
        total_leads = 0
        enriched_count = 0
        unenriched_count = 0
        all_leads = []
    
    return render_template('enrich.html',
                          total_leads=total_leads,
                          enriched_count=enriched_count,
                          unenriched_count=unenriched_count,
                          all_leads=all_leads)


@app.route('/enrich/run', methods=['POST'])
@login_required_custom
def run_enrichment():
    """Run enrichment on selected leads - uses background task for Apify."""
    try:
        max_count = int(request.form.get('max_count', 10))
        use_apify = request.form.get('use_apify') == 'true'
        
        unenriched_df = db.get_unenriched_leads(limit=max_count)
        
        if unenriched_df.empty:
            flash('No leads to enrich', 'warning')
            return redirect(url_for('enrich_page'))
        
        leads_list = unenriched_df.to_dict('records')
        
        if use_apify:
            # For Apify, use background task with batch processing
            import uuid
            task_id = str(uuid.uuid4())[:8]
            task = EnrichmentTask(task_id, len(leads_list))
            enrichment_tasks[task_id] = task
            
            def run_apify_enrichment():
                try:
                    enricher = get_enricher(use_mock=False, use_apify=True)
                    
                    # Use batch enrichment for efficiency
                    enriched_results = enricher.enrich_batch(leads_list, max_count=len(leads_list))
                    
                    # Update database with results
                    for i, enriched_biz in enumerate(enriched_results):
                        task.processed += 1
                        lead_id = leads_list[i].get('id')
                        
                        try:
                            result_data = {
                                'email': enriched_biz.get('email'),
                                'phone': enriched_biz.get('phone'),
                                'owner_name': enriched_biz.get('owner_name'),
                                'address': enriched_biz.get('address'),
                                'enrichment_source': enriched_biz.get('enrichment_source'),
                                'confidence_score': enriched_biz.get('confidence_score', 0)
                            }
                            db.update_lead_enrichment(lead_id, result_data)
                            task.results[lead_id] = result_data
                            task.enriched += 1
                        except Exception as e:
                            print(f"Error updating lead {lead_id}: {e}")
                            task.failed += 1
                    
                    task.status = 'completed'
                    task.completed_at = datetime.datetime.now()
                    
                except Exception as e:
                    task.status = 'failed'
                    task.error = str(e)
                    print(f"Enrichment task error: {e}")
            
            # Start background thread
            thread = threading.Thread(target=run_apify_enrichment)
            thread.daemon = True
            thread.start()
            
            # Return task ID for tracking
            if request.headers.get('Accept') == 'application/json':
                return jsonify({'success': True, 'task_id': task_id})
            
            flash(f'Enrichment started in background (Task: {task_id}). Processing {len(leads_list)} leads...', 'info')
            return redirect(url_for('enrich_page'))
        
        else:
            # For web search enrichment, process synchronously (faster)
            enricher = get_enricher(use_mock=False, use_apify=False)
            enriched_count = 0
            
            for lead in leads_list:
                try:
                    contact_info = enricher.enrich_business(
                        lead.get('business_name', ''),
                        lead.get('state', ''),
                        lead.get('url')
                    )
                    
                    if not contact_info.is_empty():
                        success = db.update_lead_enrichment(lead['id'], contact_info.to_dict())
                        if success:
                            enriched_count += 1
                except Exception as e:
                    print(f"Error enriching lead {lead.get('id')}: {e}")
            
            flash(f'Successfully enriched {enriched_count} leads with contact information', 'success')
        
    except Exception as e:
        flash(f'Error during enrichment: {str(e)}', 'error')
    
    return redirect(url_for('enrich_page'))


@app.route('/enrich/status/<task_id>')
@login_required_custom
def enrichment_status(task_id):
    """Get the status of a background enrichment task."""
    task = enrichment_tasks.get(task_id)
    
    if not task:
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    
    return jsonify({'success': True, 'task': task.to_dict()})


@app.route('/enrich/single/<int:lead_id>', methods=['POST'])
@login_required_custom
def enrich_single(lead_id):
    """Enrich a single lead."""
    try:
        lead = db.get_lead_by_id(lead_id)
        if not lead:
            return jsonify({'success': False, 'error': 'Lead not found'}), 404
        
        # Check if Apify should be used
        use_apify = request.json.get('use_apify', False) if request.is_json else False
        enricher = get_enricher(use_mock=False, use_apify=use_apify)
        
        contact_info = enricher.enrich_business(
            lead.get('business_name', ''),
            lead.get('state', ''),
            lead.get('url')
        )
        
        if not contact_info.is_empty():
            db.update_lead_enrichment(lead_id, contact_info.to_dict())
            return jsonify({
                'success': True,
                'data': contact_info.to_dict()
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No contact information found'
            })
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/enrich/domains', methods=['POST'])
@login_required_custom
def enrich_domains():
    """Trigger background task to find business domains using Gemini API."""
    try:
        # Get only leads that need enrichment (limit to 1000 for safety)
        leads_df = db.get_unenriched_leads(limit=1000)
        if leads_df.empty:
            return jsonify({'success': False, 'error': 'No leads need domain discovery at this time.'})
            
        leads_list = leads_df.to_dict('records')
        
        import uuid
        task_id = str(uuid.uuid4())[:8]
        task = EnrichmentTask(task_id, len(leads_list))
        enrichment_tasks[task_id] = task
        
        def run_gemini_search():
            try:
                gemini = get_gemini_service()
                if not gemini or not gemini.model:
                    task.status = 'failed'
                    task.error = "AI Discovery service not configured. Please check system settings."
                    return

                for lead in leads_list:
                    if task.status != 'running': break # Allow cancellation if needed
                    
                    lead_id = lead.get('id')
                    business_name = lead.get('business_name')
                    state = lead.get('state')
                    address = lead.get('address')
                    
                    # Update processed count
                    task.processed += 1
                    
                    # Only search if website is missing or generic
                    current_url = lead.get('website') or lead.get('url') or ''
                    is_generic = any(x in current_url.lower() for x in ['sunbiz.org', 'sec.gov', 'dos.ny.gov', 'delaware.gov'])
                    
                    if not current_url or is_generic:
                        domain = gemini.find_business_domain(business_name, state, address)
                        if domain and domain != "Not Found":
                            db.update_lead_enrichment(lead_id, {'website': domain})
                            task.results[lead_id] = {'website': domain}
                            task.enriched += 1
                        else:
                            # Mark as not found to avoid re-searching
                            db.update_lead_enrichment(lead_id, {'website': 'Not Found'})
                            task.results[lead_id] = {'website': 'Not Found'}
                    else:
                        # Skip already enriched
                        task.results[lead_id] = {'website': current_url}
                        task.enriched += 1
                
                task.status = 'completed'
                task.completed_at = datetime.datetime.now()
            except Exception as e:
                task.status = 'failed'
                task.error = str(e)
                print(f"Gemini domain task error: {e}")

        # Start thread
        thread = threading.Thread(target=run_gemini_search)
        thread.daemon = True
        thread.start()
        
        return jsonify({'success': True, 'task_id': task_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/leads/enriched')
@api_login_required
def api_enriched_leads():
    """API endpoint to get enriched leads."""
    try:
        df = db.get_enriched_leads()
        leads = df.to_dict('records') if not df.empty else []
        return jsonify({
            'success': True,
            'count': len(leads),
            'leads': leads
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500



@app.route('/api/leads/duplicates/preview')
@api_login_required
def api_leads_duplicates():
    """Identify duplicate leads by business_name + state."""
    try:
        # Optimization: only fetch necessary fields for duplicate check
        # Use select() to reduce bandwidth and memory
        docs = db.leads_ref.select(['business_name', 'state', 'id']).limit(2000).stream(timeout=30.0)
        data = []
        for doc in docs:
            d = doc.to_dict()
            d['id'] = doc.id
            data.append(d)
            
        if not data:
            return jsonify({'success': True, 'count': 0, 'duplicates': []})
            
        df = pd.DataFrame(data)

        # Find duplicates by business_name + state
        dup_mask = df.duplicated(subset=['business_name', 'state'], keep=False)
        dup_df = df[dup_mask].copy()

        # Clean NaN for JSON serialization
        duplicates = []
        for rec in dup_df.to_dict('records'):
            clean = {k: (v.item() if hasattr(v, 'item') else v) for k, v in rec.items()}
            duplicates.append(clean)

        return jsonify({
            'success': True,
            'count': len(duplicates),
            'duplicates': duplicates
        })
    except Exception as e:
        logger.error(f"Error finding duplicates: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# NEW API ROUTES (REACT FRONTEND)
# ============================================================================

import concurrent.futures

@app.route('/api/fetch-leads', methods=['POST'])
@api_login_required
def api_fetch_leads():
    try:
        vals = request.get_json(force=True, silent=True) or {}
        limit = int(vals.get('limit', 100))
        
        all_records = []
        
        # PRIMARY: SEC EDGAR (most reliable - no anti-bot protection)
        # Use multiple filing types to get more diverse businesses
        sec_filing_types = ['10-K', 'S-1', '10-Q', '8-K', 'S-11', '10-K/A', '20-F', 'N-1A']
        records_per_type = max(10, limit // len(sec_filing_types) + 3)
        
        print(f"Fetching {limit} leads from SEC EDGAR...")
        
        def scrape_sec(filing_type):
            try:
                sec = SECEdgarScraper()
                records = sec.fetch_new_businesses(limit=records_per_type, filing_type=filing_type, fast_mode=True)
                return records or []
            except Exception as e:
                print(f"SEC {filing_type} Error: {e}")
                return []

        # Use ThreadPoolExecutor for parallel SEC scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(scrape_sec, f_type) for f_type in sec_filing_types]
            
            try:
                for future in concurrent.futures.as_completed(futures, timeout=25):
                    try:
                        records = future.result(timeout=10)
                        if records:
                            all_records.extend(records)
                            print(f"SEC EDGAR: Collected {len(all_records)} records...")
                            if len(all_records) >= limit:
                                break
                    except Exception as e:
                        print(f"SEC thread error: {e}")
            except concurrent.futures.TimeoutError:
                print(f"SEC scraping timed out with {len(all_records)} results")

        # Trim to limit
        all_records = all_records[:limit]
        
        if not all_records:
            print("SEC EDGAR returned no results, trying fallback...")
            # Fallback: Try a single synchronous request
            try:
                sec = SECEdgarScraper()
                all_records = sec.fetch_new_businesses(limit=min(limit, 20), filing_type='10-K') or []
            except Exception as e:
                print(f"Fallback SEC error: {e}")
        
        if not all_records:
            error_reasons = []
            error_reasons.append("SEC EDGAR: May require different filing types or network issues")
            error_reasons.append("State scrapers: Many state SOS sites use CAPTCHA, JavaScript rendering, or rate limiting")
            error_reasons.append("OpenCorporates: API rate limits or network timeouts")
            error_reasons.append("Network: Check internet connection and firewall settings")
            
            return jsonify({
                'error': "Failed to scrape any real leads",
                'message': "All scrapers failed to return data. This is common due to anti-bot measures on government sites.",
                'reasons': error_reasons,
                'suggestion': "Try again later, or check the server logs for specific errors.",
                'count': 0,
                'ids': [],
                'sources_scraped': len(sec_filing_types)
            }), 200  # Return 200 so frontend handles it gracefully
        
        saved, duplicates, new_lead_ids = db.save_records(all_records)
        
        return jsonify({
            'message': f"Saved {saved} new leads from SEC EDGAR",
            'count': saved,
            'ids': new_lead_ids,
            'sources_scraped': len(sec_filing_types)
        })
    except Exception as e:
        import traceback
        print(f"API Fetch Error: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


@app.route('/api/clear-leads', methods=['POST'])
@api_login_required
def api_clear_leads():
    """Clear all leads from database (for resetting bad data)."""
    try:
        # Delete all leads from the database
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM leads")
            deleted_count = cursor.rowcount
            conn.commit()
        
        return jsonify({
            'message': f"Cleared {deleted_count} leads from database",
            'count': deleted_count
        })
    except Exception as e:
        import traceback
        print(f"Clear Leads Error: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


@app.route('/api/leads/duplicates', methods=['GET'])
@api_login_required
def api_get_duplicates():
    """Find and return list of duplicate lead IDs."""
    try:
        duplicate_ids = db.find_duplicate_ids(limit=5000)
        return jsonify({
            'success': True,
            'duplicate_ids': duplicate_ids,
            'count': len(duplicate_ids)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/leads/duplicates/delete', methods=['POST'])
@api_login_required
def api_delete_duplicates():
    """Permanently delete duplicate leads."""
    try:
        duplicate_ids = db.find_duplicate_ids(limit=5000)
        if not duplicate_ids:
            return jsonify({'success': True, 'deleted_count': 0, 'message': 'No duplicates found'})
            
        deleted_count = db.delete_leads(duplicate_ids)
        return jsonify({
            'success': True, 
            'deleted_count': deleted_count,
            'message': f'Successfully deleted {deleted_count} duplicates'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500



@app.route('/api/clear-cache', methods=['POST', 'GET'])
@login_required_custom
def force_clear_cache():
    """Manually force a refresh of the internal lead and stats records."""
    try:
        invalidate_cache()
        # Force refresh now
        success = _refresh_lead_cache(force=True)
        
        # Smart redirect back to where user came from
        next_page = request.referrer if request.referrer else url_for('dashboard')
        
        if success:
            if request.is_json:
                return jsonify({'success': True, 'message': 'Cache cleared'})
            flash('Internal lead cache successfully refreshed from Firestore!', 'success')
            return redirect(next_page)
        else:
            if request.is_json:
                return jsonify({'success': False, 'error': 'Refresh failed'}), 500
            flash('Cache refresh failed - keeping stale data.', 'error')
            return redirect(next_page)
    except Exception as e:
        if request.is_json:
            return jsonify({'success': False, 'error': str(e)}), 500
        flash(f'Error clearing cache: {str(e)}', 'error')
        
        next_page = request.referrer if request.referrer else url_for('dashboard')
        return redirect(next_page)


@app.route('/api/fetch-leads-stream')
@api_login_required
def api_fetch_leads_stream():
    """
    Real-time SSE streaming endpoint for fetching leads with live updates.
    Sends updates as each lead is scraped, domain found, owner found, and enriched.
    """
    
    # Get parameters BEFORE the generator (to avoid request context issues)
    limit = min(int(request.args.get('limit', 50)), 100)
    find_domains = request.args.get('find_domains', 'true').lower() == 'true'
    find_owners = request.args.get('find_owners', 'true').lower() == 'true'
    enrich_apify = request.args.get('enrich_apify', 'false').lower() == 'true'
    
    def generate_stream(limit, find_domains, find_owners, enrich_apify):
        import time
        try:
            # Send initial status
            yield f"data: {json.dumps({'type': 'status', 'message': 'Starting pipeline...', 'step': 'init'})}\n\n"
            
            all_records = []
            state_results = {}  # Track counts per state
            lead_index = 0
            
            # Step 1: Scrape from SEC EDGAR (PRIMARY - most reliable source)
            yield f"data: {json.dumps({'type': 'status', 'message': 'Scraping from SEC EDGAR (real company filings)...', 'step': 'scrape'})}\n\n"
            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            
            # SEC EDGAR - use multiple filing types for diverse data
            filing_types = ['10-K', 'S-1', '10-Q', '8-K', 'S-11', '10-K/A', '20-F', 'N-1A']
            records_per_type = max(5, limit // len(filing_types) + 1)
            
            try:
                sec = SECEdgarScraper()
                for f_idx, f_type in enumerate(filing_types):
                    if len(all_records) >= limit:
                        break
                    # Send keepalive before each filing type to prevent timeout
                    yield f"data: {json.dumps({'type': 'log', 'level': 'info', 'message': f'Fetching {f_type} filings ({f_idx+1}/{len(filing_types)})...'})}\n\n"
                    yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                    try:
                        # fast_mode=False to get full address details
                        records = sec.fetch_new_businesses(limit=records_per_type, filing_type=f_type, fast_mode=False)
                        for r in records or []:
                            if len(all_records) >= limit:
                                break
                            # Build SEC EDGAR source URL
                            source_url = r.url or f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={r.business_name}&type={f_type}"
                            # Use phone from SEC if available
                            phone_val = r.phone or r.business_phone or None
                            # Use business_address directly (already includes full address from SEC)
                            full_address = r.business_address or r.address or None
                            
                            lead_data = {
                                'id': lead_index,
                                'business_name': r.business_name,
                                'state': r.state or r.state_of_incorporation or 'US',
                                'filing_date': r.filing_date,
                                'status': r.status or 'SEC Filing',
                                'entity_type': f_type,
                                'url': r.url,
                                'domain': None,
                                'owner_name': None,
                                'owner_first_name': None,
                                'owner_last_name': None,
                                'owner_phone_1': phone_val,
                                'owner_phone_2': None,
                                'owner_email_1': None,
                                'owner_email_2': None,
                                'email': None,
                                'phone': phone_val,
                                'source': f'SEC_{f_type}',
                                'source_url': source_url,
                                'industry': r.industry_category,
                                'business_category': r.industry_category,
                                'industry_category': r.industry_category,
                                'address': full_address
                            }
                            all_records.append(lead_data)
                            
                            # Track state results
                            s_code = lead_data['state'].upper()
                            if s_code not in state_results:
                                state_results[s_code] = 0
                            state_results[s_code] += 1
                            
                            yield f"data: {json.dumps({'type': 'lead', 'action': 'add', 'lead': lead_data})}\n\n"
                            # Send keepalive after each lead to prevent timeout
                            if lead_index % 3 == 0:
                                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                            lead_index += 1
                    except Exception as e:
                        yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'SEC {f_type}: {str(e)}'})}\n\n"
                    # Send keepalive after each filing type
                    yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': f'SEC EDGAR Error: {str(e)}'})}\n\n"
            
            # If no records from SEC, show message
            if not all_records:
                yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': 'SEC EDGAR returned no results. Try again in a moment.'})}\n\n"
            
            yield f"data: {json.dumps({'type': 'status', 'message': f'Scraped {len(all_records)} leads from SEC EDGAR', 'step': 'scrape_done', 'count': len(all_records)})}\n\n"
            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            
            # Step 2: Find Domains
            if find_domains and all_records:
                yield f"data: {json.dumps({'type': 'status', 'message': 'Finding business domains for discovery...', 'step': 'domains'})}\n\n"
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                domain_count = 0
                domain_limit = min(30, len(all_records))  # Limit to conserve API
                try:
                    gemini = get_gemini_service()
                    # Check if Gemini is configured
                    if not gemini or not gemini.model:
                        yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': '⚠️ Gemini AI API key not configured! Set GEMINI_API_KEY in .env or Cloud Run.'})}\n\n"
                    else:
                        for i, record in enumerate(all_records[:domain_limit]):
                            # Send keepalive BEFORE each search to prevent timeout
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                            if record.get('business_name'):
                                try:
                                    # Use business name, state AND address for better accuracy
                                    domain = gemini.find_business_domain(
                                        record['business_name'], 
                                        state=record.get('state', ''),
                                        address=record.get('address')
                                    )
                                    if domain and domain != "Not Found":
                                        record['domain'] = domain
                                        domain_count += 1
                                        yield f"data: {json.dumps({'type': 'lead', 'action': 'update', 'id': record['id'], 'field': 'domain', 'value': domain})}\n\n"
                                        yield f"data: {json.dumps({'type': 'log', 'level': 'info', 'message': f'AI Found Domain {domain_count}/{domain_limit}: {domain}'})}\n\n"
                                except Exception as de:
                                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'AI Domain search failed: {str(de)[:40]}'})}\n\n"
                            # Send keepalive AFTER each search
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'AI Domain search error: {str(e)}'})}\n\n"
                yield f"data: {json.dumps({'type': 'status', 'message': f'Found {domain_count} domains', 'step': 'domains_done', 'count': domain_count})}\n\n"
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            
            # Step 3: Find Owners
            if find_owners and all_records:
                yield f"data: {json.dumps({'type': 'status', 'message': 'Finding business owners...', 'step': 'owners'})}\n\n"
                owner_count = 0
                owner_limit = min(25, len(all_records))  # Limit to conserve API
                try:
                    serper = SerperService()
                    # Check if Serper API is configured
                    if not serper.is_configured():
                        yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': '⚠️ Discovery API key not configured!'})}\n\n"
                    else:
                        for i, record in enumerate(all_records[:owner_limit]):
                            # Send keepalive BEFORE each search to prevent timeout
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                            if record.get('business_name'):
                                try:
                                    result = serper.search_business_owner(record['business_name'], state=record.get('state', ''))
                                    if result and result.owner_name:
                                        record['owner_name'] = result.owner_name
                                        # Split name into first/last
                                        name_parts = result.owner_name.split()
                                        record['owner_first_name'] = name_parts[0] if name_parts else None
                                        record['owner_last_name'] = ' '.join(name_parts[1:]) if len(name_parts) > 1 else None
                                        owner_count += 1
                                        yield f"data: {json.dumps({'type': 'lead', 'action': 'update_multi', 'id': record['id'], 'updates': {'owner_name': result.owner_name, 'owner_first_name': record['owner_first_name'], 'owner_last_name': record['owner_last_name']}})}\n\n"
                                        yield f"data: {json.dumps({'type': 'log', 'level': 'info', 'message': f'Owner {owner_count}/{owner_limit}: {result.owner_name}'})}\n\n"
                                except Exception as oe:
                                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'Owner search failed: {str(oe)[:40]}'})}\n\n"
                            # Send keepalive AFTER each search
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'Owner search error: {str(e)}'})}\n\n"
                yield f"data: {json.dumps({'type': 'status', 'message': f'Found {owner_count} owners', 'step': 'owners_done', 'count': owner_count})}\n\n"
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            
            # Step 4: Apify Enrichment
            if enrich_apify and all_records:
                yield f"data: {json.dumps({'type': 'status', 'message': 'Running deep enrichment to find emails and phones...', 'step': 'enrich'})}\n\n"
                enriched_count = 0
                try:
                    from enrichment import EnrichmentService
                    import os
                    apify_key = os.environ.get('APIFY_API_KEY', '')
                    if not apify_key:
                        yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': '⚠️ Enrichment API key not configured!'})}\n\n"
                    else:
                        enricher = EnrichmentService()
                        for record in all_records[:20]:  # Limit for cost
                            if record.get('owner_name'):
                                first_name = record['owner_name'].split()[0] if record['owner_name'] else None
                                last_name = ' '.join(record['owner_name'].split()[1:]) if record['owner_name'] and ' ' in record['owner_name'] else ''
                                if first_name:
                                    try:
                                        enriched = enricher.skip_trace({
                                            'first_name': first_name,
                                            'last_name': last_name,
                                            'state': record.get('state', ''),
                                            'business_name': record.get('business_name', '')
                                        })
                                        if enriched:
                                            updates = {}
                                            if enriched.get('email') or enriched.get('email_1'):
                                                record['email'] = enriched.get('email_1') or enriched.get('email')
                                                updates['email'] = record['email']
                                            if enriched.get('phone') or enriched.get('phone_1'):
                                                record['phone'] = enriched.get('phone_1') or enriched.get('phone')
                                                updates['phone'] = record['phone']
                                            if updates:
                                                enriched_count += 1
                                                yield f"data: {json.dumps({'type': 'lead', 'action': 'update_multi', 'id': record['id'], 'updates': updates})}\n\n"
                                    except Exception:
                                        pass
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'Enrichment error: {str(e)}'})}\n\n"
                yield f"data: {json.dumps({'type': 'status', 'message': f'Enriched {enriched_count} records', 'step': 'enrich_done', 'count': enriched_count})}\n\n"
            
            # Step 5: Save to database
            yield f"data: {json.dumps({'type': 'status', 'message': 'Saving to database...', 'step': 'save'})}\n\n"
            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            
            from scrapers.base_scraper import BusinessRecord
            records_to_save = []
            for r in all_records:
                rec = BusinessRecord(
                    business_name=r['business_name'],
                    filing_date=r.get('filing_date'),
                    state=r.get('state', ''),
                    status=r.get('status', ''),
                    url=r.get('source_url') or r.get('url', ''),
                    entity_type=r.get('entity_type', ''),
                    filing_number=r.get('filing_number'),
                    domain=r.get('domain'),
                    owner_name=r.get('owner_name'),
                    email=r.get('email'),
                    phone=r.get('phone'),
                    address=r.get('address'),
                    ein=r.get('ein') or r.get('tin'),
                    cik=r.get('cik'),
                    sic_code=r.get('sic_code'),
                    industry_category=r.get('industry_category'),
                    fiscal_year_end=r.get('fiscal_year_end'),
                    state_of_incorporation=r.get('state_of_incorporation'),
                    sec_file_number=r.get('sec_file_number'),
                    film_number=r.get('film_number'),
                    sec_act=r.get('sec_act'),
                    cf_office=r.get('cf_office'),
                    business_address=r.get('business_address'),
                    business_phone=r.get('business_phone'),
                    mailing_address=r.get('mailing_address')
                )
                records_to_save.append(rec)
            
            saved, duplicates, new_ids = db.save_records(records_to_save)
            
            yield f"data: {json.dumps({'type': 'status', 'message': f'Saved {saved} leads ({duplicates} duplicates)', 'step': 'save_done', 'saved': saved, 'duplicates': duplicates})}\n\n"
            yield f"data: {json.dumps({'type': 'complete', 'total': len(all_records), 'saved': saved, 'duplicates': duplicates, 'state_results': state_results})}\n\n"
            
        except Exception as e:
            import traceback
            print(f"Stream Error: {e}")
            print(traceback.format_exc())
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return Response(generate_stream(limit, find_domains, find_owners, enrich_apify), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    })


@app.route('/api/get-leads-table', methods=['GET'])
@api_login_required 
def api_get_leads_table():
    """Get recent leads for displaying in data table."""
    try:
        limit = int(request.args.get('limit', 100))
        if limit < 1:
            limit = 100
        if limit > 1000:
            limit = 1000

        # 1) Primary path: recent leads for fast UI load.
        df = db.get_recent_leads(days=30)

        # 2) Fallback: pull broader dataset when filing_date data is sparse/old.
        if df.empty:
            df = db.get_all_leads(limit=max(500, limit))

        # 3) Fallback: in-memory cache, if available.
        if df.empty:
            cached = get_cached_leads(refresh_if_empty=True)
            if cached:
                df = pd.DataFrame(cached)

        # 4) Hard fallback: direct Firestore stream bypassing DB wrappers.
        if df.empty:
            direct_records = _load_leads_direct_from_firestore(limit=max(500, limit))
            if direct_records:
                df = pd.DataFrame(direct_records)

        # 5) Emergency fallback: bundled backup JSON.
        if df.empty:
            backup_records = _load_leads_from_backup_json(limit=max(500, limit))
            if backup_records:
                df = pd.DataFrame(backup_records)
        
        if df.empty:
            return jsonify({'leads': [], 'count': 0})
        
        # Limit results
        df = df.head(limit)
        
        # Convert to list of dicts with proper null handling
        leads = []
        for _, row in df.iterrows():
            lead = {}
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    lead[col] = None
                else:
                    lead[col] = str(val) if not isinstance(val, (int, float, bool)) else val
            leads.append(lead)
        
        return jsonify({'leads': leads, 'count': len(leads)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# OLD SCRAPPER (Classic Mode)
# ============================================================================

@app.route('/old-scrapper')
@login_required_custom
def old_scrapper():
    """Redirect legacy scraper to the unified Fetch Leads hub."""
    return redirect(url_for('fetch_leads'))


@app.route('/old-scrapper/scrape', methods=['POST'])
@login_required_custom
def old_scrapper_scrape():
    """Redirect legacy scraper POST to the unified Fetch Leads hub."""
    flash('Please use the Fetch Leads page to run scraping jobs.', 'info')
    return redirect(url_for('fetch_leads'))



# ============================================================================
# SCRAPER TEST LAB
# ============================================================================

@app.route('/test')
@login_required_custom
def test_scraper_page():
    """Scraper Test Lab - test individual scrapers."""
    return render_template('test.html')


@app.route('/api/test-scraper', methods=['POST'])
@api_login_required
def api_test_scraper():
    """
    Test a specific scraper and return results.
    
    Request body:
        scraper: string - 'florida', 'sec_edgar', or 'opencorporates'
        search_term: string - search term for the scraper
        limit: int - max results to return
    
    Returns:
        JSON with 'success' boolean and 'results' array
    """
    try:
        vals = request.get_json(force=True, silent=True) or {}
        scraper_name = vals.get('scraper', 'florida')
        search_term = vals.get('search_term', 'NEW')
        limit = int(vals.get('limit', 10))
        
        results = []
        
        if scraper_name == 'florida':
            # Use Florida Playwright scraper
            try:
                from scrapers.florida_playwright_scraper import FloridaPlaywrightScraper
                scraper = FloridaPlaywrightScraper()
                
                if not scraper.is_available():
                    return jsonify({
                        'success': False,
                        'error': 'Playwright not installed. Run: pip install playwright playwright-stealth && playwright install chromium'
                    }), 400
                
                records = scraper.fetch_new_businesses(limit=limit, search_term=search_term)
                results = [
                    {
                        'business_name': r.business_name,
                        'filing_number': r.filing_number,
                        'status': r.status,
                        'entity_type': r.entity_type,
                        'state': r.state,
                        'filing_date': r.filing_date,
                        'url': r.url
                    }
                    for r in records
                ]
            except ImportError as e:
                return jsonify({
                    'success': False,
                    'error': f'Florida scraper not available: {str(e)}'
                }), 400
                
        elif scraper_name == 'sec_edgar':
            # Use SEC EDGAR scraper
            try:
                scraper = SECEdgarScraper()
                records = scraper.fetch_new_businesses(limit=limit, fast_mode=True)
                results = [
                    {
                        'business_name': r.business_name,
                        'filing_number': r.filing_number or '',
                        'status': r.status or 'Filed',
                        'entity_type': r.entity_type or '',
                        'state': r.state or 'US',
                        'filing_date': r.filing_date,
                        'url': r.url or ''
                    }
                    for r in records
                ]
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': f'SEC EDGAR error: {str(e)}'
                }), 400
            
        elif scraper_name == 'opencorporates':
            # Use OpenCorporates scraper
            try:
                scraper = OpenCorporatesScraper()
                records = scraper.fetch_new_businesses(limit=limit, jurisdiction='us_fl')
                results = [
                    {
                        'business_name': r.business_name,
                        'filing_number': r.filing_number or '',
                        'status': r.status or 'Active',
                        'entity_type': r.entity_type or '',
                        'state': r.state or '',
                        'filing_date': r.filing_date,
                        'url': r.url or ''
                    }
                    for r in records
                ]
            except ValueError as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 400
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': f'OpenCorporates error: {str(e)}'
                }), 400
        
        # Option to save results to database
        save_to_db = vals.get('save_to_db', False)
        if save_to_db and results:
            # Convert results back to BusinessRecord for saving
            from scrapers.base_scraper import BusinessRecord
            records_to_save = [
                BusinessRecord(
                    business_name=r['business_name'],
                    filing_date=r['filing_date'],
                    state=r['state'],
                    status=r['status'],
                    url=r.get('url', ''),
                    entity_type=r.get('entity_type', ''),
                    filing_number=r.get('filing_number', '')
                )
                for r in results
            ]
            inserted, duplicates, _ = db.save_records(records_to_save)
            return jsonify({
                'success': True,
                'results': results,
                'count': len(results),
                'scraper': scraper_name,
                'saved': True,
                'inserted': inserted,
                'duplicates': duplicates
            })
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'scraper': scraper_name
        })
        
    except Exception as e:
        import traceback
        print(f"Test Scraper Error: {e}")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/test-scraper-pipeline', methods=['POST'])
@api_login_required
def api_test_scraper_pipeline():
    """
    Full pipeline test: Scrape → Find Domain → Find Owner → Apify Enrich → Auto-Save
    
    Request body:
        scraper: string - 'florida', 'sec_edgar', or 'opencorporates'
        search_term: string - search term for the scraper
        limit: int - max results to return (max 20)
        find_domain: bool - whether to find domains via Serper
        find_owner: bool - whether to find owners via Serper
        enrich_apify: bool - whether to enrich via Apify skip trace
    
    Returns:
        JSON with enriched results and counts
    """
    try:
        vals = request.get_json(force=True, silent=True) or {}
        scraper_name = vals.get('scraper', 'florida')
        search_term = vals.get('search_term', 'NEW')
        limit = min(int(vals.get('limit', 5)), 20)  # Max 20 to conserve API credits
        find_domain = vals.get('find_domain', True)
        find_owner = vals.get('find_owner', True)
        enrich_apify = vals.get('enrich_apify', False)
        
        scrape_success = False
        domain_count = 0
        owner_count = 0
        enriched_count = 0
        results = []
        
        # Step 1: Scrape data
        if scraper_name == 'florida':
            try:
                from scrapers.florida_playwright_scraper import FloridaPlaywrightScraper
                scraper = FloridaPlaywrightScraper()
                
                if not scraper.is_available():
                    return jsonify({
                        'success': False,
                        'error': 'Playwright not installed. Run: pip install playwright && playwright install chromium'
                    }), 400
                
                records = scraper.fetch_new_businesses(limit=limit, search_term=search_term)
                for r in records:
                    results.append({
                        'business_name': r.business_name,
                        'filing_number': r.filing_number,
                        'status': r.status,
                        'entity_type': r.entity_type,
                        'state': r.state or 'FL',
                        'filing_date': r.filing_date,
                        'url': r.url,
                        'domain': None,
                        'owner_name': None,
                        'first_name': None,
                        'last_name': None,
                        'email': None,
                        'email_1': None,
                        'phone': None,
                        'phone_1': None
                    })
                scrape_success = True
            except Exception as e:
                print(f"Florida scraper error: {e}")
                return jsonify({'success': False, 'error': f'Scraper error: {str(e)}'}), 400
                
        elif scraper_name == 'sec_edgar':
            try:
                scraper = SECEdgarScraper()
                records = scraper.fetch_new_businesses(limit=limit, fast_mode=True)
                for r in records:
                    results.append({
                        'business_name': r.business_name,
                        'filing_number': r.filing_number or '',
                        'status': r.status or 'Filed',
                        'entity_type': r.entity_type or '',
                        'state': r.state or 'US',
                        'filing_date': r.filing_date,
                        'url': r.url or '',
                        'domain': None,
                        'owner_name': None,
                        'first_name': None,
                        'last_name': None,
                        'email': None,
                        'email_1': None,
                        'phone': None,
                        'phone_1': None
                    })
                scrape_success = True
            except Exception as e:
                print(f"SEC EDGAR scraper error: {e}")
                return jsonify({'success': False, 'error': f'SEC EDGAR error: {str(e)}'}), 400
            
        elif scraper_name == 'opencorporates':
            try:
                scraper = OpenCorporatesScraper()
                records = scraper.fetch_new_businesses(limit=limit, jurisdiction='us_fl')
                for r in records:
                    results.append({
                        'business_name': r.business_name,
                        'filing_number': r.filing_number or '',
                        'status': r.status or 'Active',
                        'entity_type': r.entity_type or '',
                        'state': r.state or '',
                        'filing_date': r.filing_date,
                        'url': r.url or '',
                        'domain': None,
                        'owner_name': None,
                        'first_name': None,
                        'last_name': None,
                        'email': None,
                        'email_1': None,
                        'phone': None,
                        'phone_1': None
                    })
                scrape_success = True
            except Exception as e:
                print(f"OpenCorporates scraper error: {e}")
                return jsonify({'success': False, 'error': f'OpenCorporates error: {str(e)}'}), 400
        
        if not results:
            return jsonify({
                'success': False,
                'error': 'No results from scraper',
                'scrape_success': False
            })
        
        # Step 2: Find domains using Serper
        if find_domain and results:
            try:
                serper = SerperService()
                for record in results:
                    if record.get('business_name'):
                        domain_result = serper.search_business_domain(
                            record['business_name'], 
                            state=record.get('state', '')
                        )
                        if domain_result and domain_result.domain:
                            record['domain'] = domain_result.domain
                            domain_count += 1
            except Exception as e:
                print(f"Domain search error: {e}")
        
        # Step 3: Find owners using Serper
        if find_owner and results:
            try:
                serper = SerperService()
                for record in results:
                    if record.get('business_name'):
                        owner_result = serper.search_business_owner(
                            record['business_name'], 
                            state=record.get('state', '')
                        )
                        if owner_result and owner_result.owner_name:
                            record['owner_name'] = owner_result.owner_name
                            owner_count += 1
                            # Split name if we have full name
                            name_parts = owner_result.owner_name.split(' ', 1)
                            if len(name_parts) >= 1:
                                record['first_name'] = name_parts[0]
                            if len(name_parts) >= 2:
                                record['last_name'] = name_parts[1]
            except Exception as e:
                print(f"Owner search error: {e}")
        
        # Step 4: Apify enrichment (skip trace)
        if enrich_apify and results:
            try:
                from enrichment import EnrichmentService
                enricher = EnrichmentService()
                
                for record in results:
                    # Need at least a name or business for skip trace
                    first_name = record.get('first_name') or record.get('owner_name', '').split(' ')[0] if record.get('owner_name') else None
                    last_name = record.get('last_name') or (record.get('owner_name', '').split(' ', 1)[1] if record.get('owner_name') and ' ' in record.get('owner_name', '') else None)
                    
                    if first_name:
                        try:
                            enriched = enricher.skip_trace({
                                'first_name': first_name,
                                'last_name': last_name or '',
                                'state': record.get('state', ''),
                                'business_name': record.get('business_name', '')
                            })
                            if enriched:
                                record['email'] = enriched.get('email')
                                record['email_1'] = enriched.get('email_1') or enriched.get('email')
                                record['phone'] = enriched.get('phone')
                                record['phone_1'] = enriched.get('phone_1') or enriched.get('phone')
                                if enriched.get('email') or enriched.get('phone'):
                                    enriched_count += 1
                        except Exception as e:
                            print(f"Skip trace error for {first_name}: {e}")
            except Exception as e:
                print(f"Apify enrichment error: {e}")
        
        # Step 5: Always save to database
        inserted = 0
        duplicates = 0
        
        if results:
            from scrapers.base_scraper import BusinessRecord
            records_to_save = []
            
            for r in results:
                record = BusinessRecord(
                    business_name=r['business_name'],
                    filing_date=r.get('filing_date'),
                    state=r.get('state', ''),
                    status=r.get('status', ''),
                    url=r.get('url', ''),
                    entity_type=r.get('entity_type', ''),
                    filing_number=r.get('filing_number', ''),
                    domain=r.get('domain'),
                    owner_name=r.get('owner_name'),
                    first_name=r.get('first_name'),
                    last_name=r.get('last_name'),
                    email=r.get('email_1') or r.get('email'),
                    phone=r.get('phone_1') or r.get('phone')
                )
                records_to_save.append(record)
            
            try:
                inserted, duplicates, _ = db.save_records(records_to_save)
            except Exception as e:
                print(f"Database save error: {e}")
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'scraper': scraper_name,
            'scrape_success': scrape_success,
            'domain_count': domain_count,
            'owner_count': owner_count,
            'enriched_count': enriched_count,
            'saved': True,
            'inserted': inserted,
            'duplicates': duplicates
        })
        
    except Exception as e:
        import traceback
        print(f"Pipeline Error: {e}")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/fetch-owners', methods=['POST'])
@api_login_required
def api_fetch_owners():
    """Find business owners using Serper with parallel processing."""
    try:
        vals = request.get_json(force=True, silent=True) or {}
        lead_ids = vals.get('ids')
        limit = int(vals.get('limit', 40))  # Reduced limit for faster response
        
        leads_to_process = []
        if not lead_ids:
            # Get leads without owners (prioritize those with domains)
            leads_df = db.get_all_leads()
            if not leads_df.empty:
                if 'owner_name' in leads_df:
                    leads_df = leads_df[leads_df['owner_name'].isna() | leads_df['owner_name'].eq('')]
                if 'domain' in leads_df.columns:
                    leads_df['has_domain'] = leads_df['domain'].notna() & (leads_df['domain'] != '')
                else:
                    leads_df['has_domain'] = False
                if 'created_at' in leads_df.columns:
                    leads_df = leads_df.sort_values(by=['has_domain', 'created_at'], ascending=[False, False])
                leads_to_process = leads_df.head(limit).to_dict('records')
        else:
             leads_df = db.get_leads_by_ids(lead_ids)
             if not leads_df.empty:
                leads_to_process = leads_df.to_dict('records')
        
        # Handle no leads case
        if not leads_to_process:
            return jsonify({
                'message': 'No leads without owners found.',
                'count': 0,
                'total': 0
            })
             
        serper = get_serper_service()
        if not serper.is_configured():
             return jsonify({'error': 'Serper API not configured'}), 400
        
        processed_count = 0
        
        def process_lead(lead):
            """Process a single lead for owner lookup."""
            try:
                result = serper.search_business_owner(
                    lead.get('business_name', ''),
                    lead.get('state', ''),
                    lead.get('address', '')
                )
                if result and (result.owner_name or result.website):
                    return {
                        'id': lead['id'],
                        'owner_name': result.owner_name,
                        'website': result.website
                    }
            except Exception as e:
                print(f"Owner lookup error for {lead.get('business_name')}: {e}")
            return None
        
        # Process leads in parallel for speed
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_lead, lead) for lead in leads_to_process]
            
            try:
                for future in concurrent.futures.as_completed(futures, timeout=22):
                    try:
                        result = future.result(timeout=4)
                        if result:
                            data = {
                                'serper_owner_name': result['owner_name'],
                                'owner_name': result['owner_name'], 
                                'serper_website': result['website'],
                                'website': result['website'],
                                'enrichment_source': 'serper',
                                'enriched_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            db.update_lead_enrichment(result['id'], data)
                            processed_count += 1
                    except Exception as e:
                        print(f"Owner thread error: {e}")
            except concurrent.futures.TimeoutError:
                print(f"Owner lookup timed out, processed {processed_count} leads")
            
        return jsonify({
            'message': f"Found owners for {processed_count} out of {len(leads_to_process)} leads",
            'count': processed_count,
            'total': len(leads_to_process)
        })
    except Exception as e:
        import traceback
        print(f"API Fetch Owners Error: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/api/enrich-leads', methods=['POST'])
@api_login_required
def api_enrich_leads():
    try:
        vals = request.get_json(force=True, silent=True) or {}
        lead_ids = vals.get('ids')
        limit = int(vals.get('limit', 50))  # Accept limit parameter
        
        leads_to_process = []
        if not lead_ids:
            # Process leads with owners but missing linkedin/email
            leads_df = db.get_all_leads()
            if not leads_df.empty:
                if 'owner_name' in leads_df.columns:
                    # Filter for leads with owner names
                    leads_df = leads_df[leads_df['owner_name'].notna() & (leads_df['owner_name'] != '')]
                    # Also exclude leads that have already been enriched by Apify
                    if 'enrichment_source' in leads_df.columns:
                        leads_df = leads_df[~leads_df['enrichment_source'].isin(['apify_skip_trace', 'apify_skip_trace_no_results'])]
                if 'created_at' in leads_df.columns:
                    leads_df = leads_df.sort_values('created_at', ascending=False)
                leads_to_process = leads_df.head(limit).to_dict('records')
        else:
             leads_df = db.get_leads_by_ids(lead_ids)
             if not leads_df.empty:
                leads_to_process = leads_df.to_dict('records')

        # Handle no leads case
        if not leads_to_process:
            return jsonify({
                'message': 'No leads to enrich. Run Find Owners first.',
                'count': 0
            })

        processed_count = 0
        use_apify = vals.get('use_apify', True)  # default to true, or use value from request
        enricher = get_enricher(use_apify=use_apify)
        
        for lead in leads_to_process:
            # Only enrich if we have an owner name
            if not lead.get('owner_name'):
                continue
            
            try:
                data = enricher.enrich_business(lead)
                if data:
                    db.update_lead_enrichment(lead['id'], data)
                    processed_count += 1
            except Exception as e:
                print(f"Enrichment error for lead {lead.get('id')}: {e}")
                
        return jsonify({
            'message': f"Enriched {processed_count} out of {len(leads_to_process)} leads",
            'count': processed_count,
            'total': len(leads_to_process)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/enrich-data', methods=['POST'])
def api_enrich_data():
    """
    Batch owner enrichment endpoint for Enrich Data tab and scheduled jobs.

    Processes leads where enrichment_status is pending or failed and updates:
    owner_first_name, owner_last_name, owner_emails, owner_phone_numbers,
    owner_age, owner_date_of_birth, enrichment_status, enriched_at.
    """
    # Allow either logged-in users OR a cron token for scheduled execution.
    cron_token = os.environ.get('ENRICH_CRON_TOKEN', '').strip()
    request_token = (request.headers.get('X-Enrich-Token') or '').strip()
    authorized = current_user.is_authenticated or (cron_token and request_token and request_token == cron_token)
    if not authorized:
        return jsonify({'success': False, 'error': 'Authentication required'}), 401

    payload = request.get_json(silent=True) or {}
    batch_size = min(max(int(payload.get('batch_size', 25)), 1), 100)
    statuses = payload.get('statuses') or ['pending', 'failed']
    use_apify = bool(payload.get('use_apify', True))

    leads_df = db.get_leads_for_enrichment(limit=batch_size, statuses=statuses)
    if leads_df.empty:
        return jsonify({
            'success': True,
            'message': 'No pending or failed records to enrich.',
            'processed': 0,
            'completed': 0,
            'failed': 0
        })

    serper = get_serper_service()
    gemini = get_gemini_service()
    apify = get_enricher(use_mock=False, use_apify=True) if use_apify else None

    serper_ready = bool(serper and serper.is_configured())
    apify_ready = bool(apify and getattr(apify, 'api_token', None))

    leads = leads_df.to_dict('records')
    processed = 0
    completed = 0
    failed = 0
    results = []

    for lead in leads:
        lead_id = str(lead.get('id', '')).strip()
        if not lead_id:
            continue

        processed += 1
        db.update_lead_enrichment(lead_id, {
            'enrichment_status': 'processing',
            'enrichment_error': None
        })

        try:
            business_name = lead.get('business_name', '')
            state = (lead.get('state') or '').upper()
            address = lead.get('business_address') or lead.get('address') or ''
            phone = lead.get('business_phone') or lead.get('phone') or ''

            owner_name = (lead.get('owner_name') or '').strip() or None
            owner_first_name = (lead.get('owner_first_name') or lead.get('first_name') or '').strip() or None
            owner_last_name = (lead.get('owner_last_name') or lead.get('last_name') or '').strip() or None

            owner_emails = []
            owner_phones = []
            owner_age = None
            owner_dob = None

            update_data = {}

            # Step 1: Serper owner/website discovery
            if serper_ready:
                serper_result = serper.search_business_owner(
                    business_name=business_name,
                    state=state,
                    address=address,
                    phone=phone
                )
                if serper_result:
                    if serper_result.owner_name and not owner_name:
                        owner_name = serper_result.owner_name
                    if serper_result.website:
                        update_data['website'] = serper_result.website
                        update_data['serper_website'] = serper_result.website
                    if serper_result.domain:
                        update_data['serper_domain'] = serper_result.domain
                    if serper_result.business_category and not lead.get('industry_category'):
                        update_data['industry_category'] = serper_result.business_category

            # Step 2: Gemini fallback to parse owner name from Serper snippets
            if not owner_name and serper_ready and gemini and getattr(gemini, 'model', None):
                try:
                    raw = serper.raw_search(f'"{business_name}" {state} owner OR founder OR CEO')
                    if raw:
                        prompt = (
                            'Extract the most likely owner/founder full name for this business from these search results. '
                            'Return ONLY JSON in the form {"owner_name": "..."}. '
                            'If unknown return {"owner_name": null}.\n\n'
                            f'Business: {business_name}\nState: {state}\nResults: {str(raw)[:2800]}'
                        )
                        gemini_text = gemini.generate_text(prompt) or ''
                        parsed_name = None
                        try:
                            response_text = gemini_text.strip()
                            if '```' in response_text:
                                response_text = response_text.split('```')[1]
                                if response_text.startswith('json'):
                                    response_text = response_text[4:]
                            parsed = json.loads(response_text.strip())
                            parsed_name = parsed.get('owner_name')
                        except Exception:
                            parsed_name = None

                        if parsed_name:
                            owner_name = str(parsed_name).strip()
                except Exception:
                    pass

            # Split owner name if needed
            if owner_name and (not owner_first_name or not owner_last_name):
                split_first, split_last = _split_owner_name(owner_name)
                owner_first_name = owner_first_name or split_first
                owner_last_name = owner_last_name or split_last

            # Step 3: Apify deep enrichment for emails/phones/age/dob
            if apify_ready:
                city = _extract_city_from_address(address)
                query_name = owner_name or business_name
                apify_rows = apify.skip_trace_by_name(query_name, city=city, state=state, max_results=1)
                if apify_rows:
                    row = apify_rows[0]
                    apify_first = (row.get('First Name') or '').strip() or None
                    apify_last = (row.get('Last Name') or '').strip() or None

                    if not owner_name and (apify_first or apify_last):
                        owner_name = f"{apify_first or ''} {apify_last or ''}".strip()
                    owner_first_name = owner_first_name or apify_first
                    owner_last_name = owner_last_name or apify_last

                    owner_emails = _dedupe_non_empty([
                        row.get('Email-1'), row.get('Email-2'), row.get('Email-3'),
                        row.get('Email-4'), row.get('Email-5'), lead.get('email')
                    ])
                    owner_phones = _dedupe_non_empty([
                        row.get('Phone-1'), row.get('Phone-2'), lead.get('phone'), lead.get('business_phone')
                    ])

                    owner_age = _safe_int(row.get('Age'))
                    owner_dob = _normalize_date(row.get('DOB') or row.get('Date of Birth'))

            # Build canonical update payload
            if owner_name:
                update_data['owner_name'] = owner_name
            update_data['owner_first_name'] = owner_first_name
            update_data['owner_last_name'] = owner_last_name
            update_data['owner_emails'] = owner_emails
            update_data['owner_phone_numbers'] = owner_phones
            update_data['owner_age'] = owner_age
            update_data['owner_date_of_birth'] = owner_dob

            # Keep existing primary fields synced for current UI/export compatibility
            if owner_emails and not lead.get('email'):
                update_data['email'] = owner_emails[0]
            if owner_phones and not lead.get('phone'):
                update_data['phone'] = owner_phones[0]

            meaningful = any([
                update_data.get('owner_name'),
                bool(update_data.get('owner_emails')),
                bool(update_data.get('owner_phone_numbers')),
                update_data.get('owner_date_of_birth'),
                update_data.get('owner_age') is not None,
                update_data.get('website')
            ])

            if meaningful:
                update_data['enrichment_status'] = 'completed'
                update_data['enriched_at'] = datetime.datetime.now().isoformat()
                update_data['enrichment_error'] = None
                db.update_lead_enrichment(lead_id, update_data)
                completed += 1
                results.append({'id': lead_id, 'status': 'completed'})
            else:
                db.update_lead_enrichment(lead_id, {
                    'enrichment_status': 'failed',
                    'enrichment_error': 'No owner/contact signals found',
                    'enriched_at': None
                })
                failed += 1
                results.append({'id': lead_id, 'status': 'failed'})

        except Exception as e:
            db.update_lead_enrichment(lead_id, {
                'enrichment_status': 'failed',
                'enrichment_error': str(e),
                'enriched_at': None
            })
            failed += 1
            results.append({'id': lead_id, 'status': 'failed', 'error': str(e)})

    invalidate_cache()

    return jsonify({
        'success': True,
        'processed': processed,
        'completed': completed,
        'failed': failed,
        'batch_size': batch_size,
        'serper_configured': serper_ready,
        'apify_configured': apify_ready,
        'results': results
    })


@app.route('/api/find-domains', methods=['POST'])
@api_login_required
def api_find_domains():
    """Find business domains/websites using Serper Google search with retry logic."""
    try:
        vals = request.get_json(force=True, silent=True) or {}
        lead_ids = vals.get('ids')
        limit = int(vals.get('limit', 75))  # Process more leads
        
        leads_to_process = []
        if not lead_ids:
            # Get recent leads without website/domain - EXPANDED query
            leads_df = db.get_all_leads()
            if not leads_df.empty:
                website_empty = leads_df.get('website', pd.Series(dtype=str)).fillna('').astype(str).eq('')
                domain_empty = leads_df.get('domain', pd.Series(dtype=str)).fillna('').astype(str).eq('')
                leads_df = leads_df[website_empty | domain_empty]
                if 'created_at' in leads_df.columns:
                    leads_df = leads_df.sort_values('created_at', ascending=False)
                leads_to_process = leads_df.head(limit).to_dict('records')
        else:
            leads_df = db.get_leads_by_ids(lead_ids)
            if not leads_df.empty:
                leads_to_process = leads_df.to_dict('records')
        
        # Handle case when no leads to process
        if not leads_to_process:
            return jsonify({
                'message': "No leads without domains found. Fetch more leads first.",
                'count': 0,
                'total': 0
            })
        
        serper = get_serper_service()
        
        if not serper.is_configured():
            # Return helpful error with instructions
            return jsonify({
                'error': 'Serper API not configured. Set SERPER_API_KEY environment variable.',
                'count': 0,
                'configured': False
            }), 400
        
        processed_count = 0
        errors = 0
        
        def process_lead(lead):
            """Process a single lead with retry logic."""
            business_name = lead.get('business_name', '')
            state = lead.get('state', '')
            city = lead.get('city', '')
            
            if not business_name:
                return None
            
            # Retry up to 2 times
            for attempt in range(2):
                try:
                    result = serper.search_business_domain(business_name, state, city)
                    if result and (result.website or result.domain):
                        return {
                            'id': lead['id'],
                            'website': result.website,
                            'domain': result.domain
                        }
                    break  # No result but no error, don't retry
                except Exception as e:
                    print(f"Domain search attempt {attempt+1} failed for {business_name}: {e}")
                    if attempt < 1:
                        time.sleep(0.5)  # Brief wait before retry
            return None
        
        # Process leads with threading for speed
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_lead, lead) for lead in leads_to_process]
            
            try:
                for future in concurrent.futures.as_completed(futures, timeout=25):
                    try:
                        result = future.result(timeout=5)
                        if result:
                            data = {
                                'website': result['website'],
                                'domain': result['domain'],
                                'enrichment_source': 'serper_domain',
                                'enriched_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            db.update_lead_enrichment(result['id'], data)
                            processed_count += 1
                    except Exception as e:
                        errors += 1
                        print(f"Domain thread error: {e}")
            except concurrent.futures.TimeoutError:
                print(f"Domain discovery timed out, processed {processed_count} leads")
        
        return jsonify({
            'message': f"Found domains for {processed_count} out of {len(leads_to_process)} leads",
            'count': processed_count,
            'total': len(leads_to_process),
            'errors': errors
        })
    except Exception as e:
        import traceback
        print(f"API Find Domains Error: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e), 'count': 0}), 500


@app.route('/api/ghl-test-webhook', methods=['POST'])
@api_login_required
def api_ghl_test_webhook():
    """Send sample lead data to test GHL webhook integration."""
    try:
        # Load GHL settings
        ghl_settings = load_ghl_settings()
        webhook_url = ghl_settings.get('webhook_url', '')
        tag = ghl_settings.get('tag', 'lead_scraper')
        
        if not webhook_url:
            return jsonify({'success': False, 'message': 'Webhook URL not configured'})
        
        # Sample lead data
        sample_data = {
            'firstName': 'John',
            'lastName': 'Sample',
            'email': 'john.sample@example.com',
            'phone': '+1 (555) 123-4567',
            'companyName': 'Sample Business LLC',
            'website': 'https://example.com',
            'address1': '123 Main Street',
            'city': 'Wilmington',
            'state': 'DE',
            'postalCode': '19801',
            'tags': [tag, 'sample_test'],
            'source': 'LeadGen Pro - Test',
            'customField': {
                'filing_date': '2024-01-15',
                'industry': 'Technology',
                'sic_code': '7372',
                'entity_type': 'LLC'
            }
        }
        
        # Send to GHL webhook
        import requests
        response = requests.post(
            webhook_url,
            json=sample_data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code in [200, 201, 202]:
            return jsonify({'success': True, 'message': 'Sample data sent successfully'})
        else:
            return jsonify({
                'success': False, 
                'message': f'GHL returned status {response.status_code}: {response.text[:200]}'
            })
            
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'message': 'Request timed out. Check webhook URL.'})
    except requests.exceptions.RequestException as e:
        return jsonify({'success': False, 'message': f'Connection error: {str(e)}'})
    except Exception as e:
        print(f"GHL Test Webhook Error: {e}")
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/export-ghl', methods=['POST'])
@api_login_required
def api_export_ghl():
    """Export leads to GoHighLevel via webhook."""
    try:
        vals = request.get_json(force=True, silent=True) or {}
        
        # Check for webhook URL in order: request body > saved settings > env var
        webhook_url = vals.get('webhookUrl', '')
        tag = vals.get('tag', '')
        
        # If not provided in request, load from saved settings
        if not webhook_url:
            saved_settings = load_ghl_settings()
            webhook_url = saved_settings.get('webhook_url', '')
            if not tag:
                tag = saved_settings.get('tag', 'lead_scraper')
        
        # Final fallback to environment variable
        if not webhook_url:
            webhook_url = os.environ.get('GHL_WEBHOOK_URL', '')
        
        if not tag:
            tag = 'lead_scraper'
            
        lead_ids = vals.get('ids')
        
        if not webhook_url:
            return jsonify({'error': 'GoHighLevel webhook URL not configured. Please set it in Settings.'}), 400
        
        # Get leads to export - more lenient query, include leads even without owner_name
        leads_to_export = []
        if not lead_ids:
            # Get leads that haven't been exported to GHL - don't require owner_name
            leads_df = db.get_all_leads()
            if not leads_df.empty:
                if 'ghl_exported' in leads_df.columns:
                    leads_df = leads_df[leads_df['ghl_exported'].isna() | (leads_df['ghl_exported'] == 0) | (leads_df['ghl_exported'] == '0') | (leads_df['ghl_exported'] == False)]
                if 'created_at' in leads_df.columns:
                    leads_df = leads_df.sort_values('created_at', ascending=False)
                leads_to_export = leads_df.head(50).to_dict('records')
        else:
            leads_df = db.get_leads_by_ids(lead_ids)
            if not leads_df.empty:
                leads_to_export = leads_df.to_dict('records')
        
        if not leads_to_export:
            return jsonify({
                'message': 'No new leads to export. All leads may have been exported already.',
                'count': 0,
                'failed': 0
            })
        
        exported_count = 0
        failed_count = 0
        
        for lead in leads_to_export:
            # Use owner_name if available, otherwise use business_name for firstName
            owner = lead.get('owner_name', '') or ''
            business = lead.get('business_name', '') or 'Unknown Business'
            
            # Prepare GHL contact payload
            ghl_payload = {
                'firstName': owner.split()[0] if owner else business.split()[0] if business else 'Lead',
                'lastName': ' '.join(owner.split()[1:]) if owner and len(owner.split()) > 1 else '',
                'name': owner or business,
                'email': lead.get('email', ''),
                'phone': lead.get('phone', '') or lead.get('contact_phone', ''),
                'companyName': business,
                'website': lead.get('website', ''),
                'address1': lead.get('address', ''),
                'city': lead.get('city', ''),
                'state': lead.get('state', ''),
                'postalCode': lead.get('zip_code', ''),
                'source': f"LeadGen Pro - {lead.get('source', 'scraper')}",
                'tags': [tag, f"state_{lead.get('state', 'unknown')}", lead.get('source', 'scraper')],
                'customField': {
                    'business_type': lead.get('business_type', ''),
                    'filing_date': lead.get('filing_date', ''),
                    'registration_number': lead.get('entity_number', ''),
                    'linkedin': lead.get('linkedin', ''),
                    'scrape_date': datetime.datetime.now().strftime("%Y-%m-%d")
                }
            }
            
            # Remove empty fields
            ghl_payload = {k: v for k, v in ghl_payload.items() if v}
            
            try:
                import requests as req
                response = req.post(
                    webhook_url,
                    json=ghl_payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
                
                if response.status_code in [200, 201, 202]:
                    # Mark as exported
                    db.update_lead_enrichment(lead['id'], {
                        'ghl_exported': 1,
                        'ghl_exported_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    exported_count += 1
                else:
                    print(f"GHL export failed for lead {lead['id']}: {response.status_code} - {response.text}")
                    failed_count += 1
                    
            except Exception as e:
                print(f"GHL export error for lead {lead['id']}: {e}")
                failed_count += 1
        
        return jsonify({
            'message': f"Exported {exported_count} leads to GoHighLevel. {failed_count} failed.",
            'count': exported_count,
            'failed': failed_count
        })
    except Exception as e:
        print(f"API Export GHL Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/export-sheets-stream', methods=['POST'])
@api_login_required
def api_export_sheets_stream():
    """Export leads from streaming data to Google Sheets."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        leads = data.get('leads', [])
        
        if not leads:
            return jsonify({'success': False, 'error': 'No leads provided'}), 400
        
        # Use the API exporter
        exporter = GoogleSheetsAPIExporter()
        
        if not exporter.is_configured():
            return jsonify({
                'success': False, 
                'error': 'Google Sheets not configured. Go to Settings to connect your Google account.'
            }), 400
        
        if not exporter.is_authenticated():
            return jsonify({
                'success': False, 
                'error': 'Please connect to Google Sheets first. Go to Settings to authorize.'
            }), 401
        
        # Load spreadsheet ID from settings
        google_settings = load_google_settings()
        spreadsheet_id = google_settings.get('spreadsheet_id')
        
        # Convert leads to DataFrame
        import pandas as pd
        df = pd.DataFrame(leads)
        
        # Create new spreadsheet or use existing one
        title = f"Scraped Leads - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
        if spreadsheet_id:
            # If we have an ID, we prioritize appending rows to it
            title = f"Appended Leads - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
            result = exporter.create_new_spreadsheet(title=title, spreadsheet_id=spreadsheet_id, df=df, append=True)
            # If the provided ID fails (e.g. invalid permissions), try creating a fresh one as fallback
            if not result.get('success'):
                print(f"Failed to use target sheet {spreadsheet_id}, falling back to new sheet. Error: {result.get('error')}")
                result = exporter.create_new_spreadsheet(title=f"NEW - {title}", df=df, append=False)
        else:
            result = exporter.create_new_spreadsheet(title=title, df=df, append=False)
        
        if result.get('success'):
            return jsonify({
                'success': True,
                'message': f'Successfully exported {len(leads)} leads to Google Sheets!',
                'sheet_url': result.get('spreadsheet_url', ''),
                'spreadsheet_url': result.get('spreadsheet_url', ''),
                'count': len(leads)
            })
        else:
            error_msg = result.get('error', 'Unknown error')
            # If it's a quota error, we already have a detailed FIX message in exporter.py
            return jsonify({
                'success': False,
                'error': error_msg,
                'details': result.get('details'),
                'service_email': result.get('service_email')
            }), 500
            
    except Exception as e:
        error_str = str(e)
        print(f"API Export Sheets Error: {e}")
        return jsonify({'success': False, 'error': error_str}), 500


@app.route('/api/clear-all-leads', methods=['POST'])
@api_login_required
def api_clear_all_leads():
    """Clear all leads from database - use with caution!"""
    try:
        count = db.clear_all_leads()
        return jsonify({
            'message': f'Successfully deleted {count} leads. Database is now empty.',
            'deleted': count
        })
    except Exception as e:
        print(f"API Clear Leads Error: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# CUSTOM SEARCH API ENDPOINTS
# ============================================================================

@app.route('/api/search-scrape')
@api_login_required
def api_search_scrape():
    """
    Live scrape search - search SEC EDGAR with filters.
    Uses a hard 25-second timeout to prevent hanging Cloud Run workers.
    """
    import concurrent.futures

    try:
        name_pattern = request.args.get('name_pattern', '').strip()
        sic_code = request.args.get('sic_code', '').strip()
        date_range = int(request.args.get('date_range', 30))
        limit = min(int(request.args.get('limit', 50)), 100)

        sec = SECEdgarScraper()
        all_results = []

        def fetch_and_build(filing_type=None, company_search=None, fetch_limit=25):
            """Fetch from SEC EDGAR and return list of result dicts."""
            results = []
            try:
                records = sec.fetch_new_businesses(
                    limit=fetch_limit,
                    filing_type=filing_type or '10-K',
                    company_search=company_search,
                    fast_mode=True
                )
                for r in records or []:
                    ai_cat = detect_business_category(r.business_name)
                    result = {
                        'business_name': r.business_name,
                        'state': r.state or r.state_of_incorporation or 'US',
                        'filing_date': r.filing_date,
                        'address': r.business_address or r.address,
                        'industry_category': r.industry_category or ai_cat,
                        'business_category': r.industry_category or ai_cat,
                        'source': f'SEC_{filing_type or "EDGAR"}',
                        'sic_code': r.sic_code,
                        'url': r.url
                    }
                    # Only filter by SIC when record actually has SIC data
                    if sic_code and r.sic_code:
                        if not str(r.sic_code).startswith(sic_code[:2]):
                            continue
                    # Name filter for multi-type mode
                    if company_search is None and name_pattern:
                        if name_pattern.lower() not in r.business_name.lower():
                            continue
                    results.append(result)
            except Exception as e:
                print(f"SEC fetch error ({filing_type}): {e}")
            return results

        # Hard 25-second deadline for the entire scrape operation
        TIMEOUT_SECONDS = 25

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            if name_pattern:
                # Targeted company name search - single call
                future = executor.submit(fetch_and_build, None, name_pattern, limit)
                try:
                    all_results = future.result(timeout=TIMEOUT_SECONDS)
                except concurrent.futures.TimeoutError:
                    print("SEC name search timed out - returning partial results")
                    all_results = []
            else:
                # General search: run 2 filing types in parallel (10-K + S-1)
                # Reduced from 4 sequential to 2 parallel for speed
                per_type = max(8, limit // 2)
                futures = {
                    executor.submit(fetch_and_build, '10-K', None, per_type): '10-K',
                    executor.submit(fetch_and_build, 'S-1', None, per_type): 'S-1',
                }
                for future in concurrent.futures.as_completed(futures, timeout=TIMEOUT_SECONDS):
                    try:
                        all_results.extend(future.result(timeout=5))
                    except Exception as e:
                        print(f"Filing type fetch error: {e}")

        # Date filtering - use a generous lookback since SEC filings are always behind
        total_raw = len(all_results)
        if date_range >= 365:
            filtered_results = all_results
        else:
            lookback_days = max(date_range * 12, 365)
            cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            filtered_results = [r for r in all_results if (r.get('filing_date') or '2020-01-01') >= cutoff_date]
            if not filtered_results:
                filtered_results = all_results  # Fallback - never return empty due to date alone

        return jsonify({
            'success': True,
            'results': filtered_results[:limit],
            'count': len(filtered_results[:limit]),
            'total_raw': total_raw
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500



@app.route('/api/search-database')
@api_login_required
def api_search_database():
    """
    Search existing leads in the database with filters.
    Supports name pattern, state, date range filtering.
    """
    try:
        name_pattern = request.args.get('name_pattern', '').strip()
        state = request.args.get('state', '').strip()
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        limit = min(int(request.args.get('limit', 100)), 500)
        
        # Get all leads and filter in Python (for simplicity)
        df = db.get_all_leads()
        
        if df.empty:
            return jsonify({
                'success': True,
                'results': [],
                'count': 0
            })
        
        # KEY FIX: Replace NaN/inf with None BEFORE converting to dict
        # pandas NaN is not valid JSON - must use None (becomes null in JSON)
        import math
        df = df.where(pd.notnull(df), None)
        results = df.to_dict('records')
        
        # Extra sanitization: convert any remaining float NaN/inf to None
        def sanitize_value(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v
        
        results = [{k: sanitize_value(v) for k, v in r.items()} for r in results]
        
        # Apply filters
        if name_pattern:
            pattern_lower = name_pattern.lower()
            results = [r for r in results if pattern_lower in (r.get('business_name') or '').lower()]
        
        if state:
            results = [r for r in results if (r.get('state') or '').upper() == state.upper()]
        
        if date_from:
            results = [r for r in results if (r.get('filing_date') or '') >= date_from]
        
        if date_to:
            results = [r for r in results if (r.get('filing_date') or '') <= date_to]
        
        # Limit results
        results = results[:limit]
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })

        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/save-search-results', methods=['POST'])
@api_login_required
def api_save_search_results():
    """Save search results to database."""
    try:
        data = request.get_json()
        leads = data.get('leads', [])
        
        if not leads:
            return jsonify({'success': False, 'error': 'No leads to save'})
        
        from scrapers.base_scraper import BusinessRecord
        
        saved = 0
        duplicates = 0
        
        for lead in leads:
            record = BusinessRecord(
                business_name=lead.get('business_name', ''),
                filing_date=lead.get('filing_date', ''),
                state=lead.get('state', ''),
                status=lead.get('status', 'Active'),
                url=lead.get('url', ''),
                entity_type=lead.get('entity_type', ''),
                filing_number=lead.get('filing_number', ''),
                address=lead.get('address', ''),
                industry_category=lead.get('industry_category') or lead.get('business_category', ''),
                sic_code=lead.get('sic_code', '')
            )
            
            try:
                db.save_lead(record)
                saved += 1
            except Exception as e:
                if 'UNIQUE constraint' in str(e) or 'Duplicate' in str(e):
                    duplicates += 1
                else:
                    print(f"Error saving lead: {e}")
        
        return jsonify({
            'success': True,
            'saved': saved,
            'duplicates': duplicates,
            'message': f'Saved {saved} leads ({duplicates} duplicates skipped)'
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    if request.path.startswith('/api/') or request.path.startswith('/auth/'):
        return jsonify({'error': 'Not Found', 'message': 'The requested URL was not found on the server.'}), 404
    return render_template('404.html'), 404


@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    import traceback
    print(f"Internal Server Error at {request.path}: {e}")
    print(traceback.format_exc())
    
    if request.path.startswith('/api/') or request.path.startswith('/auth/'):
        return jsonify({
            'error': 'Internal Server Error', 
            'message': str(e),
            'path': request.path
        }), 500
    return render_template('500.html'), 500


# ============================================================================
# MULTI-STATE SOS SCRAPER (Florida + more states)
# ============================================================================
import sys as _sys

# Make Florida package importable
_florida_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Florida')
if _florida_dir not in _sys.path:
    _sys.path.insert(0, _florida_dir)

from sunbiz_scraper_fixed import FixedSunbizScraper

# Import multi-state scraper
try:
    from scrapers.multistate_scraper import get_scraper_for_states, STATE_CONFIGS as MULTISTATE_CONFIGS
    MULTISTATE_AVAILABLE = True
except ImportError:
    MULTISTATE_AVAILABLE = False
    MULTISTATE_CONFIGS = {}

# Supported states for scraping
# Standard HTML states that don't require JS or have CAPTCHAs
ACTIVE_SCRAPER_STATES = {'FL'}
if MULTISTATE_AVAILABLE:
    # Standard HTML states (24 total)
    ACTIVE_SCRAPER_STATES.update({
        # Working
        'FL',
        # Standard HTML - Batch 3
        'OK', 'MO', 'SC', 'UT', 'VT', 'WI', 'NE', 'NH', 'KS', 'KY',
        'LA', 'AR', 'SD', 'OR', 'RI', 'MS', 'NM', 'ME', 'DE', 'HI',
        'AL', 'AK', 'IA', 'CO', 'TN', 'MA', 'NC', 'IL',
    })

# In-memory state for the SOS scraper
_florida_scrape_state = {
    "status": "idle",       # idle | running | done | error
    "businesses": [],
    "message": "",
    "progress": "",
    "logs": [],
    "selected_states": ["FL"],  # Track which states are being scraped
}


def _florida_add_log(msg: str):
    _florida_scrape_state["logs"].append(msg)


def _florida_save_to_db(businesses: list):
    """Save Florida businesses to Firestore for persistence."""
    if not businesses:
        return 0
    
    try:
        db = get_database()
        florida_ref = db.db.collection('florida_leads')
        
        batch = db.db.batch()
        batch_count = 0
        saved_count = 0
        
        for biz in businesses:
            # Create unique ID based on document number
            doc_number = biz.get("document_number", "").strip()
            if not doc_number:
                continue
            
            doc_id = f"fl_{doc_number}"
            
            # Prepare data for storage
            data = {
                "name": biz.get("name", ""),
                "document_number": doc_number,
                "status": biz.get("status", ""),
                "filing_date": biz.get("filing_date", ""),
                "fei_ein": biz.get("fei_ein", ""),
                "principal_address": biz.get("principal_address", ""),
                "mailing_address": biz.get("mailing_address", ""),
                "registered_agent": biz.get("registered_agent", ""),
                "state": biz.get("state", "FL"),
                "last_event": biz.get("last_event", ""),
                "event_date_filed": biz.get("event_date_filed", ""),
                "officer_title": biz.get("officer_title", ""),
                "officer_name": biz.get("officer_name", ""),
                "category": biz.get("category", ""),
                "detail_url": biz.get("detail_url", ""),
                "scraped_date": biz.get("scraped_date", datetime.now().isoformat()),
            }
            
            batch.set(florida_ref.document(doc_id), data, merge=True)
            batch_count += 1
            saved_count += 1
            
            # Commit batch every 450 documents (Firestore limit is 500)
            if batch_count >= 450:
                batch.commit()
                batch = db.db.batch()
                batch_count = 0
        
        # Commit remaining documents
        if batch_count > 0:
            batch.commit()
        
        _florida_add_log(f"Saved {saved_count} businesses to database.")
        return saved_count
        
    except Exception as e:
        _florida_add_log(f"ERROR saving to database: {e}")
        logger.error(f"Error saving Florida businesses to DB: {e}")
        return 0


def _florida_load_from_db(limit: int = 500) -> list:
    """Load Florida businesses from Firestore."""
    try:
        from firebase_admin import firestore
        db = get_database()
        florida_ref = db.db.collection('florida_leads')
        
        # Query with ordering by scraped_date descending
        query = florida_ref.order_by("scraped_date", direction=firestore.Query.DESCENDING).limit(limit)
        docs = query.stream(timeout=10.0)
        
        businesses = []
        for doc in docs:
            data = doc.to_dict()
            if data:
                businesses.append(data)
        
        return businesses
        
    except Exception as e:
        logger.error(f"Error loading Florida businesses from DB: {e}")
        return []


def _florida_run_scrape(keywords: list, max_per_category: int, states: list = None):
    """Run scrape for one or more states."""
    states = states or ["FL"]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_florida_async_scrape(keywords, max_per_category, states))
    except Exception as exc:
        _florida_scrape_state["status"] = "error"
        _florida_scrape_state["message"] = f"Scrape failed: {exc}"
        _florida_add_log(f"FATAL ERROR: {exc}")
    finally:
        loop.close()


async def _florida_async_scrape(keywords: list, max_per_category: int, states: list = None):
    """Async scraping for one or more states."""
    states = states or ["FL"]
    _florida_scrape_state["selected_states"] = states
    
    # If only Florida, use the specialized Sunbiz scraper
    if states == ["FL"]:
        scraper = FixedSunbizScraper(headless=True, on_log=_florida_add_log)
        try:
            await scraper.start_browser()
            page = await scraper.context.new_page()
            page.set_default_timeout(scraper.timeout)

            all_businesses = []
            total = len(keywords)
            for i, keyword in enumerate(keywords, 1):
                _florida_scrape_state["progress"] = f"FL: Scraping keyword {i}/{total}: {keyword}"
                results = await scraper.scrape_keyword(page, keyword, max_per_category)
                all_businesses.extend(results)
                await asyncio.sleep(2)

            await page.close()

            scraper.businesses = all_businesses
            sorted_biz = scraper.sort_by_date(ascending=False)
            _florida_scrape_state["businesses"] = sorted_biz
            
            # Save to database for persistence
            _florida_scrape_state["progress"] = "Saving to database..."
            saved_count = _florida_save_to_db(sorted_biz)
            
            _florida_scrape_state["status"] = "done"
            _florida_scrape_state["message"] = f"Done! Found {len(sorted_biz)} businesses across {total} keywords. Saved {saved_count} to database."
            _florida_add_log(_florida_scrape_state["message"])
        finally:
            await scraper.stop_browser()
    else:
        # Multi-state scraping using MultiStateScraper
        if not MULTISTATE_AVAILABLE:
            _florida_scrape_state["status"] = "error"
            _florida_scrape_state["message"] = "Multi-state scraper not available"
            return
        
        try:
            scraper = get_scraper_for_states(states, headless=True, on_log=_florida_add_log)
            
            _florida_add_log(f"Starting multi-state scrape for: {', '.join(states)}")
            _florida_scrape_state["progress"] = f"Scraping {len(states)} state(s)..."
            
            all_businesses = await scraper.scrape(keywords=keywords, max_per_keyword=max_per_category)
            
            # Add state to each business if not present
            for biz in all_businesses:
                if 'state' not in biz:
                    biz['state'] = states[0] if len(states) == 1 else 'MULTI'
            
            _florida_scrape_state["businesses"] = all_businesses
            
            # Save to database
            _florida_scrape_state["progress"] = "Saving to database..."
            saved_count = _florida_save_to_db(all_businesses)
            
            _florida_scrape_state["status"] = "done"
            state_names = ', '.join(states)
            _florida_scrape_state["message"] = f"Done! Found {len(all_businesses)} businesses from {state_names}. Saved {saved_count} to database."
            _florida_add_log(_florida_scrape_state["message"])
            
        except Exception as e:
            _florida_scrape_state["status"] = "error"
            _florida_scrape_state["message"] = f"Multi-state scrape failed: {str(e)}"
            _florida_add_log(f"ERROR: {str(e)}")


@app.route('/florida-scraper')
@login_required_custom
def florida_scraper():
    return render_template('florida_scraper.html', keywords=FixedSunbizScraper.HOME_SERVICE_KEYWORDS)


@app.route('/api/florida/scrape', methods=['POST'])
@login_required_custom
def florida_api_scrape():
    if _florida_scrape_state["status"] == "running":
        return jsonify({"status": "running", "message": "A scrape is already in progress."})

    data = request.get_json(silent=True) or {}
    keywords = data.get("keywords", FixedSunbizScraper.HOME_SERVICE_KEYWORDS[:3])
    max_per_category = min(int(data.get("max_per_category", 20)), 500)
    
    # Get selected states (default to Florida)
    states = data.get("states", ["FL"])
    if isinstance(states, str):
        states = [states]
    # Filter to only active scraper states
    states = [s.upper() for s in states if s.upper() in ACTIVE_SCRAPER_STATES]
    if not states:
        states = ["FL"]

    keywords = [str(k).strip() for k in keywords if str(k).strip()]
    if not keywords:
        return jsonify({"status": "error", "message": "No keywords provided."}), 400

    _florida_scrape_state["status"] = "running"
    _florida_scrape_state["businesses"] = []
    _florida_scrape_state["message"] = ""
    _florida_scrape_state["progress"] = f"Starting scrape for {', '.join(states)}..."
    _florida_scrape_state["logs"] = []
    _florida_scrape_state["selected_states"] = states

    thread = threading.Thread(target=_florida_run_scrape, args=(keywords, max_per_category, states), daemon=True)
    thread.start()

    return jsonify({"status": "started", "states": states})


@app.route('/api/florida/active-states')
@login_required_custom
def florida_api_active_states():
    """Return list of states with active scrapers."""
    return jsonify({"states": list(ACTIVE_SCRAPER_STATES)})


@app.route('/api/florida/status')
@login_required_custom
def florida_api_status():
    return jsonify({
        "status": _florida_scrape_state["status"],
        "message": _florida_scrape_state["message"],
        "progress": _florida_scrape_state["progress"],
        "count": len(_florida_scrape_state["businesses"]),
    })


@app.route('/api/florida/logs')
@login_required_custom
def florida_api_logs():
    after = int(request.args.get("after", 0))
    logs = _florida_scrape_state["logs"]
    new_lines = logs[after:]
    return jsonify({"lines": new_lines, "cursor": len(logs)})


@app.route('/api/florida/results')
@login_required_custom
def florida_api_results():
    return jsonify(_florida_scrape_state["businesses"])


@app.route('/api/florida/persisted')
@login_required_custom
def florida_api_persisted():
    """Load persisted Florida businesses from database."""
    limit = int(request.args.get("limit", 500))
    businesses = _florida_load_from_db(limit)
    return jsonify(businesses)


@app.route('/api/florida/delete-all', methods=['DELETE'])
@login_required_custom
def florida_api_delete_all():
    """Delete all Florida businesses from database and clear in-memory state."""
    try:
        # Clear in-memory state
        _florida_scrape_state["businesses"] = []
        _florida_scrape_state["status"] = "idle"
        _florida_scrape_state["message"] = ""
        _florida_scrape_state["logs"] = []
        
        # Delete from Firestore
        db = get_database()
        florida_ref = db.db.collection('florida_leads')
        
        # Get all documents and delete in batches
        docs = florida_ref.stream(timeout=30.0)
        batch = db.db.batch()
        count = 0
        deleted = 0
        
        for doc in docs:
            batch.delete(florida_ref.document(doc.id))
            count += 1
            deleted += 1
            
            # Commit every 450 docs (Firestore batch limit is 500)
            if count >= 450:
                batch.commit()
                batch = db.db.batch()
                count = 0
        
        # Commit remaining
        if count > 0:
            batch.commit()
        
        return jsonify({"status": "success", "message": f"Deleted {deleted} businesses."})
        
    except Exception as e:
        logger.error(f"Error deleting Florida businesses: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/florida/download/csv')
@login_required_custom
def florida_download_csv():
    businesses = _florida_scrape_state["businesses"]
    # If no in-memory data, try loading from database
    if not businesses:
        businesses = _florida_load_from_db(500)
    if not businesses:
        return "No data to download", 404

    output = io.StringIO()
    fieldnames = [
        "name", "document_number", "status", "filing_date", "fei_ein",
        "principal_address", "mailing_address", "registered_agent",
        "state", "last_event", "event_date_filed",
        "officer_title", "officer_name", "category", "detail_url", "scraped_date",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for biz in businesses:
        writer.writerow(biz)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=florida_sunbiz_businesses.csv"},
    )


@app.route('/api/florida/download/json')
@login_required_custom
def florida_download_json():
    businesses = _florida_scrape_state["businesses"]
    # If no in-memory data, try loading from database
    if not businesses:
        businesses = _florida_load_from_db(500)
    if not businesses:
        return "No data to download", 404

    return Response(
        json.dumps(businesses, indent=2),
        mimetype="application/json",
        headers={"Content-Disposition": "attachment; filename=florida_sunbiz_businesses.json"},
    )


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)

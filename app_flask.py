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
import threading
import pandas as pd
from functools import wraps
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, Response, session, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

# Import application modules
from scraper_manager import ScraperManager
from database import Database, get_database
from google_sheets import GoogleSheetsExporter, MockGoogleSheetsExporter, GoogleSheetsAPIExporter
from scrapers.real_scrapers import get_real_scraper, get_available_states, OpenCorporatesScraper, SECEdgarScraper
from enrichment import get_enricher, BusinessEnricher, ApifySkipTraceEnricher
from serper_service import SerperService, get_serper_service, detect_business_category
from auth import init_oauth, User, admin_required, login_required_custom, oauth, ADMIN_EMAIL, verify_and_login_firebase
from state_urls import STATE_URLS


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


# ============================================================================
# BACKGROUND TASK MANAGEMENT
# ============================================================================

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
            'progress_percent': int((self.processed / self.total * 100)) if self.total > 0 else 0,
            'elapsed_seconds': (datetime.now() - self.started_at).total_seconds()
        }

# ============================================================================
# APP CONFIGURATION
# ============================================================================

app = Flask(__name__)
CORS(app)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['GOOGLE_CLIENT_ID'] = os.environ.get('GOOGLE_CLIENT_ID', '')
app.config['GOOGLE_CLIENT_SECRET'] = os.environ.get('GOOGLE_CLIENT_SECRET', '')
app.config['ADMIN_EMAIL'] = os.environ.get('ADMIN_EMAIL', 'samadly728@gmail.com')

# Force HTTPS for URL generation on Cloud Run (detected by K_SERVICE env var)
if os.environ.get('K_SERVICE'):
    app.config['PREFERRED_URL_SCHEME'] = 'https'
    # Trust X-Forwarded-Proto header from Cloud Run's load balancer
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Initialize OAuth
init_oauth(app)

# Security Headers for Firebase popup auth
@app.after_request
def add_security_headers(response):
    """Add security headers to support Firebase popup authentication."""
    # Allow Firebase popup auth by using COOP: same-origin-allow-popups
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin-allow-popups'
    # COEP: require-corp can block external CDN scripts like Tailwind/Google Fonts 
    # if they don't explicitly send the CORP header.
    # response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    return response

# Database instance
db = get_database("leads.db")

# Clean up any placeholder data at startup
try:
    cleaned = db.cleanup_placeholder_leads()
    if cleaned > 0:
        print(f"Startup: Cleaned {cleaned} placeholder/invalid leads from database")
except Exception as e:
    print(f"Startup cleanup error (non-critical): {e}")

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


def auto_export_to_sheet(leads, sheet_id=None):
    """
    Automatically export leads to Google Sheet if configured.
    """
    target_sheet_id = sheet_id or os.environ.get('GOOGLE_SHEET_ID')
    if not target_sheet_id:
        return {'success': False, 'error': 'GOOGLE_SHEET_ID not set'}
        
    try:
        # Try Service Account First (Best for Automation)
        exporter_api = GoogleSheetsAPIExporter()
        if exporter_api.is_configured():
            # Convert to DataFrame if list of dicts
            if isinstance(leads, list):
                df = pd.DataFrame(leads)
            else:
                df = leads
            
            if df.empty:
                return {'success': False, 'error': 'No data to export'}
                
            result = exporter_api.export_dataframe(df, target_sheet_id, worksheet_name='Leads', append=True)
            print(f"Auto-export result (API): {result}")
            return result

        # Fallback to OAuth (Client ID/Secret) if authenticated
        exporter_oauth = GoogleSheetsExporter()
        if exporter_oauth.is_configured() and exporter_oauth.is_authenticated():
            # Convert to DataFrame if list of dicts
            if isinstance(leads, list):
                df = pd.DataFrame(leads)
            else:
                df = leads
            
            if df.empty:
                return {'success': False, 'error': 'No data to export'}
                
            # Use 'Leads' worksheet for consistency
            result = exporter_oauth.export_dataframe(df, target_sheet_id, worksheet_name='Leads', append=True)
            print(f"Auto-export result (OAuth): {result}")
            return result
            
        return {'success': False, 'error': 'No Google Sheets configuration found (Service Account or OAuth)'}

    except Exception as e:
        print(f"Auto export error: {e}")
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
    return {'webhook_url': os.environ.get('GHL_WEBHOOK_URL', ''), 'tag': 'lead_scraper'}


def save_ghl_settings_to_file(webhook_url, tag='lead_scraper'):
    """Save GoHighLevel settings to file."""
    try:
        settings = {
            'webhook_url': webhook_url,
            'tag': tag
        }
        with open(GHL_SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving GHL settings: {e}")
        return False


def get_dashboard_stats():
    """Get statistics for the dashboard."""
    try:
        df = db.get_all_leads()
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        total = len(df)
        new_today = len(df[df['filing_date'] == today]) if not df.empty else 0
        this_week = len(df[df['filing_date'] >= week_ago]) if not df.empty else 0
        
        last_fetch = session.get('last_fetch_time', 'Never')
        
        return {
            'total_leads': total,
            'new_today': new_today,
            'this_week': this_week,
            'states_count': 5,
            'last_fetch': last_fetch
        }
    except Exception as e:
        print(f"Error getting stats: {e}")
        return {
            'total_leads': 0,
            'new_today': 0,
            'this_week': 0,
            'states_count': 5,
            'last_fetch': 'Never'
        }


def get_state_stats():
    """Get leads count by state."""
    try:
        df = db.get_all_leads()
        if df.empty:
            return []
        
        state_counts = df['state'].value_counts().to_dict()
        total = sum(state_counts.values()) or 1
        
        state_names = {
            'DE': 'Delaware',
            'CA': 'California',
            'TX': 'Texas',
            'NY': 'New York',
            'FL': 'Florida',
            'US': 'United States'
        }
        
        stats = []
        for code, count in sorted(state_counts.items(), key=lambda x: x[1], reverse=True):
            stats.append({
                'code': code,
                'name': state_names.get(code, code),
                'count': count,
                'percentage': round((count / total) * 100, 1)
            })
        
        return stats
    except Exception:
        return []


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
    # Allow ?preview=1 to see landing page even when logged in
    if request.args.get('preview') != '1' and current_user.is_authenticated:
        if current_user.is_admin:
            return redirect(url_for('admin_dashboard'))
        return redirect(url_for('client_dashboard'))
    
    return render_template('landing.html', current_year=datetime.now().year)


@app.route('/login')
def login():
    """Login is now handled on the landing page via Firebase."""
    return redirect(url_for('landing'))


@app.route('/auth/callback')
def auth_callback():
    """Removed in favor of Firebase Auth."""
    return redirect(url_for('landing'))


@app.route('/logout')
@login_required
def logout():
    """Log out the user."""
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('landing'))


@app.route('/api/auth/firebase', methods=['POST'])
def firebase_login():
    """Handle Firebase login from frontend."""
    try:
        data = request.json
        id_token = data.get('idToken')
        
        if not id_token:
            error_msg = 'No ID token provided'
            print(f"[Firebase Auth] {error_msg}")
            return jsonify({'success': False, 'error': error_msg}), 400
            
        user = verify_and_login_firebase(id_token)
        if user:
            # Determine redirect
            redirect_url = url_for('admin_dashboard') if user.is_admin else url_for('client_dashboard')
            print(f"[Firebase Auth] ✅ User logged in: {user.email} (admin={user.is_admin})")
            return jsonify({
                'success': True,
                'redirect': redirect_url,
                'user': {
                    'id': user.id,
                    'email': user.email,
                    'name': user.name,
                    'is_admin': user.is_admin
                }
            })
        else:
            error_msg = 'Failed to verify Firebase token or create user - check logs'
            print(f"[Firebase Auth] ❌ {error_msg}")
            return jsonify({'success': False, 'error': error_msg}), 401
    except Exception as e:
        error_msg = f"Firebase Login API Error: {str(e)}"
        print(f"[Firebase Auth] ❌ {error_msg}")
        import traceback
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': error_msg}), 500


@app.route('/auth/google-one-tap', methods=['POST'])
def google_one_tap_callback():
    """Removed in favor of Firebase Auth."""
    return redirect(url_for('landing'))


# ============================================================================
# ADMIN ROUTES
# ============================================================================

@app.route('/admin')
@admin_required
def admin_dashboard():
    """Admin dashboard - admin only."""
    stats = get_dashboard_stats()
    users = User.get_all_users()
    
    exporter = GoogleSheetsExporter()
    sheets_configured = exporter.is_configured()
    
    # Mock activity logs
    logs = [
        {'time': datetime.now().strftime('%Y-%m-%d %H:%M'), 'level': 'info', 'message': 'Admin dashboard accessed'},
        {'time': (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M'), 'level': 'info', 'message': 'System startup complete'},
    ]
    
    return render_template('admin_dashboard.html',
                          stats=stats,
                          users=users,
                          logs=logs,
                          sheets_configured=sheets_configured,
                          config=app.config)


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
        df = db.get_all_leads()
        recent_leads = df_to_records(df.head(10))
    except Exception:
        recent_leads = []
    
    return render_template('dashboard.html', 
                          stats=stats, 
                          state_stats=state_stats,
                          recent_leads=recent_leads)


@app.route('/leads')
@login_required_custom
def leads():
    """All leads listing page with pagination."""
    state_filter = request.args.get('state', '')
    status_filter = request.args.get('status', '')
    search_query = request.args.get('search', '')
    page = request.args.get('page', 1, type=int)
    per_page = 50
    
    try:
        page = request.args.get('page', 1, type=int)
        per_page = 50
        
        # Check if we have filters
        has_filters = bool(state_filter or status_filter or search_query)
        
        if not has_filters:
            # Efficient database pagination
            offset = (page - 1) * per_page
            df = db.get_all_leads(limit=per_page, offset=offset)
            total_leads = db.get_leads_count()
            has_next = (offset + len(df)) < total_leads
            has_prev = page > 1
        else:
            # Filtered results - standardized limit to prevent OOM
            # TODO: Move filtering to SQL for full dataset access
            df = db.get_all_leads(limit=2000)
            
            # Apply filters
            if state_filter and not df.empty:
                df = df[df['state'] == state_filter]
            if status_filter and not df.empty:
                df = df[df['status'].str.contains(status_filter, case=False, na=False)]
            if search_query and not df.empty:
                df = df[df['business_name'].str.contains(search_query, case=False, na=False)]
            
            # Manual pagination
            total_filtered = len(df)
            start = (page - 1) * per_page
            end = start + per_page
            
            # Slice the dataframe for current page
            # Note: df is already filtered, so we slice relative to 0
            df_page = df.iloc[start:end]
            
            # Recalculate has_next/prev
            has_next = end < total_filtered
            has_prev = page > 1
            total_leads = total_filtered
            df = df_page

        all_leads = df_to_records(df)
        
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
    """Fetch leads - React Pipeline page."""
    return render_template('react_dashboard.html')


@app.route('/fetch-manual')
@login_required_custom
def fetch_leads_manual():
    """Old manual fetch leads configuration page."""
    # Check if API keys are configured
    serper_configured = bool(os.environ.get('SERPER_API_KEY', ''))
    apify_configured = bool(os.environ.get('APIFY_TOKEN', ''))
    
    return render_template('fetch.html', 
                          serper_configured=serper_configured,
                          apify_configured=apify_configured)


@app.route('/fetch', methods=['POST'])
@login_required_custom
def do_fetch():
    """Execute the lead fetching. Enrichment runs in background."""
    try:
        states = request.form.getlist('states')
        limit = int(request.form.get('limit', 20))
        use_sec = request.form.get('use_sec') == 'on'
        use_serper = request.form.get('use_serper') == 'on'
        use_apify = request.form.get('use_apify') == 'on'
        
        if not states:
            states = ['FL']  # Default to Florida
        
        all_records = []
        scraper_results = []
        
        # ====================================================================
        # STEP 1: SCRAPE BUSINESS DATA (synchronous - fast)
        # ====================================================================
        
        # 1a. Try SEC EDGAR (free, no API key required)
        if use_sec:
            try:
                sec = SECEdgarScraper()
                records = sec.fetch_new_businesses(limit=limit, filing_type='10-K')
                all_records.extend(records)
                scraper_results.append(f"SEC EDGAR: {len(records)} records")
            except Exception as e:
                scraper_results.append(f"SEC EDGAR: Error - {str(e)}")
        
        # 1b. Try real state scrapers (skip if SEC worked, to save time)
        if not all_records:
            for state in states:
                scraper = get_real_scraper(state)
                if scraper:
                    try:
                        if scraper.is_available():
                            records = scraper.fetch_new_businesses(limit=limit)
                            # Detect business category for each record
                            for record in records:
                                if not record.industry_category:
                                    record.industry_category = detect_business_category(record.business_name)
                            all_records.extend(records)
                            scraper_results.append(f"{state} State: {len(records)} records")
                        else:
                            scraper_results.append(f"{state} State: Blocked")
                    except Exception as e:
                        scraper_results.append(f"{state} State: Error")
        
        # Save initial records to database
        saved, duplicates, new_lead_ids = db.save_records(all_records)
        
        # Update session
        session['last_fetch_time'] = datetime.now().strftime("%Y-%m-%d %H:%M")
        session['last_scraper_results'] = scraper_results
        
        # ====================================================================
        # STEP 2 & 3: ENRICHMENT (background thread - slow)
        # ====================================================================
        if saved > 0 and (use_serper or use_apify):
            # Use actual inserted IDs instead of guessing
            if not new_lead_ids:
                # Fallback if IDs empty but saved > 0 (shouldn't happen with updated db code)
                df = db.get_all_leads()
                new_lead_ids = df.head(saved)['id'].tolist()
            
            # Start background enrichment task
            import uuid
            task_id = str(uuid.uuid4())[:8]
            task = EnrichmentTask(task_id, saved)
            enrichment_tasks[task_id] = task
            
            def run_background_enrichment():
                try:
                    # Fetch fresh data for these leads
                    leads_df = db.get_leads_by_ids(new_lead_ids)
                    if leads_df.empty:
                        print(f"Error: Could not find leads with IDs {new_lead_ids}")
                        task.status = 'failed'
                        return

                    leads_to_enrich = leads_df.to_dict('records')
                    
                    # Serper enrichment
                    if use_serper:
                        serper = get_serper_service()
                        if serper.is_configured():
                            for lead in leads_to_enrich:
                                task.processed += 1
                                try:
                                    result = serper.search_business_owner(
                                        lead.get('business_name', ''),
                                        lead.get('state', ''),
                                        lead.get('address', ''),
                                        lead.get('business_phone', '') or lead.get('phone', '')
                                    )
                                    if result.owner_name or result.website or result.business_category:
                                        update_data = {
                                            'serper_owner_name': result.owner_name,
                                            'owner_name': result.owner_name,
                                            'serper_website': result.website,
                                            'serper_domain': result.domain,
                                            'website': result.website
                                        }
                                        
                                        # Add business category if detected
                                        if result.business_category:
                                            update_data['industry_category'] = result.business_category
                                        
                                        # Split owner_name into first/last name if valid
                                        if result.owner_name and result.confidence >= 0.7:
                                            name_parts = result.owner_name.split()
                                            if len(name_parts) >= 2:
                                                update_data['first_name'] = name_parts[0]
                                                update_data['last_name'] = ' '.join(name_parts[1:])
                                        
                                        db.update_lead_enrichment(lead['id'], update_data)
                                        task.enriched += 1
                                except Exception as e:
                                    task.failed += 1
                                    print(f"Serper error: {e}")
                    
                    # Apify enrichment
                    if use_apify:
                        try:
                            apify = get_enricher(use_mock=False, use_apify=True)
                            enriched_results = apify.enrich_batch(leads_to_enrich, max_count=len(leads_to_enrich))
                            
                            for i, enriched_biz in enumerate(enriched_results):
                                lead_id = leads_to_enrich[i].get('id')
                                if enriched_biz.get('email') or enriched_biz.get('email_1') or enriched_biz.get('phone'):
                                    db.update_lead_enrichment(lead_id, {
                                        'email': enriched_biz.get('email'),
                                        'phone': enriched_biz.get('phone'),
                                        'first_name': enriched_biz.get('first_name'),
                                        'last_name': enriched_biz.get('last_name'),
                                        'owner_name': enriched_biz.get('owner_name'),
                                        'phone_1': enriched_biz.get('phone_1'),
                                        'phone_2': enriched_biz.get('phone_2'),
                                        'email_1': enriched_biz.get('email_1'),
                                        'email_2': enriched_biz.get('email_2'),
                                        'email_3': enriched_biz.get('email_3'),
                                        'email_4': enriched_biz.get('email_4'),
                                        'email_5': enriched_biz.get('email_5'),
                                        'age': enriched_biz.get('age'),
                                        'enrichment_source': enriched_biz.get('enrichment_source'),
                                        'confidence_score': enriched_biz.get('confidence_score', 0)
                                    })
                        except Exception as e:
                            print(f"Apify error: {e}")
                    
                    task.status = 'completed'
                    task.completed_at = datetime.now()
                    
                    # Auto-export enriched leads
                    try:
                        enriched_df = db.get_leads_by_ids([l['id'] for l in leads_to_enrich])
                        if not enriched_df.empty:
                            auto_export_to_sheet(enriched_df)
                    except Exception as e:
                        print(f"Auto-export error during enrichment: {e}")
                        
                except Exception as e:
                    task.status = 'failed'
                    task.error = str(e)
                    print(f"Background enrichment error: {e}")
            
            # Start background thread
            thread = threading.Thread(target=run_background_enrichment)
            thread.daemon = True
            thread.start()
            
            flash(f'Fetched {saved} leads! Enrichment running in background (Task: {task_id}). Refresh to see updates.', 'success')
        elif saved > 0:
            flash(f'Fetched {saved} new leads ({duplicates} duplicates). Sources: {", ".join(scraper_results)}', 'success')
            
            # Auto-export new leads (raw)
            try:
                auto_export_to_sheet(leads)
            except Exception as e:
                print(f"Auto-export error: {e}")
        elif all_records:
            flash(f'All {duplicates} records already existed in database', 'info')
        else:
            flash('No records found. Enable SEC EDGAR or check state scrapers.', 'warning')
            
    except Exception as e:
        flash(f'Error fetching leads: {str(e)}', 'error')
    
    return redirect(url_for('leads'))


@app.route('/search')
@login_required_custom
def search_page():
    """Custom search page for live scraping and database queries."""
    return render_template('search.html')


@app.route('/export')
@login_required_custom
def export_page():
    """Export configuration page."""
    # Use API exporter which supports OAuth tokens
    exporter = GoogleSheetsAPIExporter()
    sheets_configured = exporter.is_configured()
    sheets_authenticated = exporter.is_authenticated()
    
    try:
        total_leads = len(db.get_all_leads())
    except Exception:
        total_leads = 0
    
    export_history = session.get('export_history', [])
    
    return render_template('export.html', 
                          sheets_configured=sheets_configured,
                          sheets_authenticated=sheets_authenticated,
                          total_leads=total_leads,
                          export_history=export_history)


@app.route('/export/csv')
@login_required_custom
def export_csv():
    """Export leads as CSV file."""
    try:
        df = db.get_all_leads()
        
        if df.empty:
            flash('No leads to export', 'warning')
            return redirect(url_for('export_page'))
        
        all_leads = df.to_dict('records')
        
        # Create CSV in memory with all available columns
        output = io.StringIO()
        fieldnames = ['business_name', 'filing_date', 'state', 'status', 'url', 'entity_type', 
                     'filing_number', 'phone', 'email', 'address', 'sic_code', 'industry_category',
                     'business_address', 'business_phone', 'mailing_address', 'cik', 'ein',
                     'owner_name', 'first_name', 'last_name', 'phone_1', 'phone_2',
                     'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'age', 'website']
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        
        writer.writeheader()
        for lead in all_leads:
            writer.writerow(lead)
        
        # Update export history
        history = session.get('export_history', [])
        history.insert(0, {
            'type': 'csv',
            'records': len(all_leads),
            'date': datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        session['export_history'] = history[:10]  # Keep last 10
        
        # Return CSV file
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
    except Exception as e:
        flash(f'Error exporting CSV: {str(e)}', 'error')
        return redirect(url_for('export_page'))


@app.route('/export/sheets/direct')
@login_required_custom
def export_sheets_direct():
    """Export leads directly to a new Google Sheet using current user's Google account."""
    try:
        df = db.get_all_leads()
        
        if df.empty:
            flash('No leads to export', 'warning')
            return redirect(url_for('leads'))
        
        # Get user's token from session
        user_token = session.get('google_sheets_token')
        
        # Use the API exporter with user's token
        exporter = GoogleSheetsAPIExporter(user_token_json=user_token)
        
        if not exporter.is_configured():
            flash('Google Sheets not configured. Go to Settings to connect your Google account.', 'warning')
            return redirect(url_for('settings'))
        
        if not exporter.is_authenticated(user_token):
            flash('Please connect to Google Sheets first. Go to Settings to authorize with your Google account.', 'warning')
            return redirect(url_for('settings'))
        
        # Create new spreadsheet with data
        title = f"Business Leads Export - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        result = exporter.create_new_spreadsheet(title, df)
        
        if result.get('success'):
            # Update export history
            history = session.get('export_history', [])
            history.insert(0, {
                'type': 'sheets',
                'records': len(df),
                'date': datetime.now().strftime("%Y-%m-%d %H:%M"),
                'url': result.get('spreadsheet_url', '')
            })
            session['export_history'] = history[:10]
            
            flash(f'Successfully exported {len(df)} leads to Google Sheets! <a href="{result.get("spreadsheet_url", "")}" target="_blank" class="underline">Open Sheet</a>', 'success')
            return redirect(result.get('spreadsheet_url', ''))
        else:
            flash(f'Export failed: {result.get("error", "Unknown error")}', 'error')
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
        worksheet_name = request.form.get('worksheet_name', 'Leads')
        
        if not spreadsheet_id:
            flash('Please enter a Spreadsheet ID', 'error')
            return redirect(url_for('export_page'))
        
        # Use API exporter for exports
        api_exporter = GoogleSheetsAPIExporter()
        oauth_exporter = GoogleSheetsExporter()
        
        if not oauth_exporter.is_configured():
            flash('Google Sheets not configured. Go to Settings to set up.', 'warning')
            return redirect(url_for('settings'))
        
        # Check if authenticated (token exists)
        if not api_exporter.is_authenticated():
            session['pending_export'] = {
                'spreadsheet_id': spreadsheet_id,
                'worksheet_name': worksheet_name
            }
            # Use OAuth exporter to start auth flow
            redirect_uri = request.host_url.rstrip('/') + url_for('oauth2callback')
            auth_url = oauth_exporter.get_authorization_url(redirect_uri)
            return redirect(auth_url)
        
        df = db.get_all_leads()
        
        result = api_exporter.export_dataframe(df, spreadsheet_id, worksheet_name)
        
        if result.get('success'):
            # Update export history
            history = session.get('export_history', [])
            history.insert(0, {
                'type': 'sheets',
                'records': len(df),
                'date': datetime.now().strftime("%Y-%m-%d %H:%M")
            })
            session['export_history'] = history[:10]
            
            flash(f'Successfully exported {len(df)} leads to Google Sheets', 'success')
        else:
            flash(f'Export failed: {result.get("error", "Unknown error")}', 'error')
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
        result = exporter.handle_oauth_callback(authorization_response, redirect_uri)
        
        if result.get('success'):
            # Store the user's token in their session for per-user Sheets access
            session['google_sheets_token'] = result.get('token_json')
            
            flash('Successfully connected to Google Sheets!', 'success')
            pending = session.pop('pending_export', None)
            if pending:
                # Use API exporter with user's token for the actual export
                user_token = session.get('google_sheets_token')
                api_exporter = GoogleSheetsAPIExporter(user_token_json=user_token)
                df = db.get_all_leads()
                export_result = api_exporter.export_dataframe(
                    df, 
                    pending['spreadsheet_id'], 
                    pending['worksheet_name']
                )
                if export_result.get('success'):
                    flash(f'Exported {len(df)} leads to Google Sheets', 'success')
                else:
                    flash(f'Export failed: {export_result.get("error", "Unknown error")}', 'error')
        else:
            flash(f'Failed to complete Google authentication: {result.get("error", "Unknown")}', 'error')
    except Exception as e:
        flash(f'OAuth error: {str(e)}', 'error')
    
    return redirect(url_for('settings'))


@app.route('/settings/google/connect')
@login_required_custom
def google_connect():
    """Start Google Sheets OAuth flow."""
    try:
        exporter = GoogleSheetsExporter()
        if not exporter.is_configured():
            flash('Google Sheets not configured. Please contact administrator.', 'warning')
            return redirect(url_for('settings'))
        
        # Get the redirect URI
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


@app.route('/settings')
@login_required_custom
def settings():
    """Settings page."""
    # Use the API exporter which supports OAuth tokens
    exporter = GoogleSheetsAPIExporter()
    sheets_configured = exporter.is_configured()
    sheets_authenticated = exporter.is_authenticated()
    
    try:
        total_records = len(db.get_all_leads())
    except Exception:
        total_records = 0
    
    # Get database type and info for display
    db_type = db.db_type if hasattr(db, 'db_type') else 'sqlite'
    db_url = db.db_url if hasattr(db, 'db_url') else None
    
    # Detect Google Cloud SQL
    is_cloud_sql = db_url and '/cloudsql/' in db_url
    
    if is_cloud_sql:
        db_type_display = 'Google Cloud SQL (PostgreSQL)'
    else:
        db_type_display = {
            'postgres': 'PostgreSQL (Persistent)',
            'sqlite': 'SQLite (Local/Ephemeral)'
        }.get(db_type, db_type)
    
    db_is_persistent = db_type == 'postgres'
    
    db_stats = {
        'total_records': total_records,
        'db_type': db_type,
        'db_type_display': db_type_display,
        'is_persistent': db_is_persistent,
        'is_cloud_sql': is_cloud_sql
    }
    
    # Load GHL settings
    ghl_settings = load_ghl_settings()
    
    return render_template('settings.html', 
                          sheets_configured=sheets_configured,
                          sheets_authenticated=sheets_authenticated,
                          db_stats=db_stats,
                          ghl_settings=ghl_settings)


@app.route('/settings/ghl', methods=['POST'])
@login_required_custom
def save_ghl_settings():
    """Save GoHighLevel settings."""
    webhook_url = request.form.get('ghl_webhook_url', '').strip()
    tag = request.form.get('ghl_tag', 'lead_scraper').strip()
    
    if save_ghl_settings_to_file(webhook_url, tag):
        if webhook_url:
            flash('GoHighLevel settings saved successfully!', 'success')
        else:
            flash('GoHighLevel settings cleared.', 'info')
    else:
        flash('Failed to save GoHighLevel settings.', 'error')
    
    return redirect(url_for('settings'))


@app.route('/api/ghl-settings', methods=['GET'])
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
    """Enrichment configuration page."""
    try:
        df = db.get_all_leads()
        total_leads = len(df)
        
        if not df.empty and 'email' in df.columns:
            enriched_count = len(df[df['email'].notna() & (df['email'] != '')])
        else:
            enriched_count = 0
        
        unenriched_count = total_leads - enriched_count
        
        unenriched_df = db.get_unenriched_leads(limit=10)
        sample_leads = unenriched_df.to_dict('records') if not unenriched_df.empty else []
        
    except Exception as e:
        print(f"Error in enrich_page: {e}")
        total_leads = 0
        enriched_count = 0
        unenriched_count = 0
        sample_leads = []
    
    return render_template('enrich.html',
                          total_leads=total_leads,
                          enriched_count=enriched_count,
                          unenriched_count=unenriched_count,
                          sample_leads=sample_leads)


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
                        
                        if enriched_biz.get('email') or enriched_biz.get('phone'):
                            try:
                                db.update_lead_enrichment(lead_id, {
                                    'email': enriched_biz.get('email'),
                                    'phone': enriched_biz.get('phone'),
                                    'owner_name': enriched_biz.get('owner_name'),
                                    'address': enriched_biz.get('address'),
                                    'enrichment_source': enriched_biz.get('enrichment_source'),
                                    'confidence_score': enriched_biz.get('confidence_score', 0)
                                })
                                task.enriched += 1
                            except Exception as e:
                                print(f"Error updating lead {lead_id}: {e}")
                                task.failed += 1
                    
                    task.status = 'completed'
                    task.completed_at = datetime.now()
                    
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


@app.route('/api/leads/enriched')
@login_required_custom
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


# ============================================================================
# NEW API ROUTES (REACT FRONTEND)
# ============================================================================

import concurrent.futures

def api_login_required(f):
    """Decorator for API routes that returns JSON error instead of redirect."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required', 'redirect': '/'}), 401
        return f(*args, **kwargs)
    return decorated_function

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
                records = sec.fetch_new_businesses(limit=records_per_type, filing_type=filing_type)
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


@app.route('/api/fetch-leads-stream')
def api_fetch_leads_stream():
    """
    Real-time SSE streaming endpoint for fetching leads with live updates.
    Sends updates as each lead is scraped, domain found, owner found, and enriched.
    """
    # Check auth via session
    if not current_user.is_authenticated:
        def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Authentication required'})}\n\n"
        return Response(error_stream(), mimetype='text/event-stream')
    
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
                yield f"data: {json.dumps({'type': 'status', 'message': 'Finding business domains via Serper...', 'step': 'domains'})}\n\n"
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                domain_count = 0
                domain_limit = min(30, len(all_records))  # Limit to conserve API
                try:
                    serper = SerperService()
                    # Check if Serper API is configured
                    if not serper.is_configured():
                        yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': '⚠️ SERPER_API_KEY not configured! Set it in Cloud Run environment variables.'})}\n\n"
                    else:
                        for i, record in enumerate(all_records[:domain_limit]):
                            # Send keepalive BEFORE each search to prevent timeout
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                            if record.get('business_name'):
                                try:
                                    result = serper.search_business_domain(record['business_name'], state=record.get('state', ''))
                                    if result and result.domain:
                                        record['domain'] = result.domain
                                        domain_count += 1
                                        yield f"data: {json.dumps({'type': 'lead', 'action': 'update', 'id': record['id'], 'field': 'domain', 'value': result.domain})}\n\n"
                                        yield f"data: {json.dumps({'type': 'log', 'level': 'info', 'message': f'Domain {domain_count}/{domain_limit}: {result.domain}'})}\n\n"
                                except Exception as de:
                                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'Domain search failed: {str(de)[:40]}'})}\n\n"
                            # Send keepalive AFTER each search
                            yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'log', 'level': 'warning', 'message': f'Domain search error: {str(e)}'})}\n\n"
                yield f"data: {json.dumps({'type': 'status', 'message': f'Found {domain_count} domains', 'step': 'domains_done', 'count': domain_count})}\n\n"
                yield f"data: {json.dumps({'type': 'keepalive'})}\n\n"
            
            # Step 3: Find Owners
            if find_owners and all_records:
                yield f"data: {json.dumps({'type': 'status', 'message': 'Finding business owners via Serper...', 'step': 'owners'})}\n\n"
                owner_count = 0
                owner_limit = min(25, len(all_records))  # Limit to conserve API
                try:
                    serper = SerperService()
                    # Check if Serper API is configured
                    if not serper.is_configured():
                        yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': '⚠️ SERPER_API_KEY not configured! Set it in Cloud Run environment variables.'})}\n\n"
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
                yield f"data: {json.dumps({'type': 'status', 'message': 'Enriching with Apify skip trace...', 'step': 'enrich'})}\n\n"
                enriched_count = 0
                try:
                    from enrichment import EnrichmentService
                    import os
                    apify_key = os.environ.get('APIFY_API_KEY', '')
                    if not apify_key:
                        yield f"data: {json.dumps({'type': 'log', 'level': 'error', 'message': '⚠️ APIFY_API_KEY not configured! Set it in Cloud Run environment variables.'})}\n\n"
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
                    domain=r.get('domain'),
                    owner_name=r.get('owner_name'),
                    email=r.get('email'),
                    phone=r.get('phone'),
                    address=r.get('address')
                )
                records_to_save.append(rec)
            
            saved, duplicates, new_ids = db.save_records(records_to_save)
            
            yield f"data: {json.dumps({'type': 'status', 'message': f'Saved {saved} leads ({duplicates} duplicates)', 'step': 'save_done', 'saved': saved, 'duplicates': duplicates})}\n\n"
            yield f"data: {json.dumps({'type': 'complete', 'total': len(all_records), 'saved': saved, 'duplicates': duplicates})}\n\n"
            
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
        df = db.get_recent_leads(days=30)
        
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
def old_scrapper_page():
    """Old Scrapper - Classic Flask form-based scraping."""
    return render_template('old_scrapper.html')


@app.route('/old-scrapper/scrape', methods=['POST'])
@login_required_custom
def do_old_scrape():
    """
    Classic form-based scraping endpoint.
    This is the original Flask-only approach before React/Vite.
    """
    try:
        source = request.form.get('source', 'sec_edgar')
        search_term = request.form.get('search_term', '').strip()
        limit = int(request.form.get('limit', 25))
        save_to_db = request.form.get('save_to_db') == 'on'
        find_owners = request.form.get('find_owners') == 'on'
        
        results = []
        
        if source == 'sec_edgar':
            # Use SEC EDGAR scraper (free, no API key needed)
            try:
                scraper = SECEdgarScraper()
                records = scraper.fetch_new_businesses(limit=limit)
                results = [r.to_dict() if hasattr(r, 'to_dict') else {
                    'business_name': r.business_name,
                    'filing_date': r.filing_date,
                    'state': r.state,
                    'status': r.status,
                    'entity_type': r.entity_type,
                    'filing_number': r.filing_number,
                    'url': r.url,
                    'ein': getattr(r, 'ein', None),
                    'cik': getattr(r, 'cik', None),
                    'phone': getattr(r, 'phone', None),
                    'address': getattr(r, 'address', None)
                } for r in records]
            except Exception as e:
                flash(f'SEC EDGAR Error: {str(e)}', 'error')
                return redirect(url_for('old_scrapper_page'))
                
        elif source == 'florida':
            # Use Florida Playwright scraper
            try:
                from scrapers.florida_playwright_scraper import FloridaPlaywrightScraper
                scraper = FloridaPlaywrightScraper()
                
                if not scraper.is_available():
                    flash('Playwright not installed. Install with: pip install playwright && playwright install chromium', 'error')
                    return redirect(url_for('old_scrapper_page'))
                
                records = scraper.fetch_new_businesses(limit=limit, search_term=search_term or 'NEW')
                results = [{
                    'business_name': r.business_name,
                    'filing_date': r.filing_date,
                    'state': r.state or 'FL',
                    'status': r.status,
                    'entity_type': r.entity_type,
                    'filing_number': r.filing_number,
                    'url': r.url
                } for r in records]
            except ImportError:
                flash('Florida Playwright scraper not available', 'error')
                return redirect(url_for('old_scrapper_page'))
            except Exception as e:
                flash(f'Florida Scraper Error: {str(e)}', 'error')
                return redirect(url_for('old_scrapper_page'))
        
        if not results:
            flash('No results found. Try a different search or source.', 'warning')
            return redirect(url_for('old_scrapper_page'))
        
        # Save to database
        inserted = 0
        duplicates = 0
        if save_to_db and results:
            from scrapers.base_scraper import BusinessRecord
            records_to_save = []
            for r in results:
                records_to_save.append(BusinessRecord(
                    business_name=r.get('business_name', ''),
                    filing_date=r.get('filing_date', ''),
                    state=r.get('state', ''),
                    status=r.get('status', ''),
                    url=r.get('url', ''),
                    entity_type=r.get('entity_type', ''),
                    filing_number=r.get('filing_number', ''),
                    phone=r.get('phone'),
                    address=r.get('address'),
                    ein=r.get('ein'),
                    cik=r.get('cik')
                ))
            inserted, duplicates, _ = db.save_records(records_to_save)
        
        # Find owners with Serper if requested
        owner_count = 0
        if find_owners and results:
            try:
                serper = SerperService()
                for r in results[:min(len(results), 10)]:  # Limit to 10 for API credits
                    if r.get('business_name'):
                        owner_result = serper.search_business_owner(
                            r['business_name'],
                            state=r.get('state', '')
                        )
                        if owner_result and owner_result.owner_name:
                            owner_count += 1
            except Exception as e:
                print(f"Serper owner lookup error: {e}")
        
        # Success message
        msg = f'Successfully scraped {len(results)} businesses from {source.upper()}'
        if save_to_db:
            msg += f' | Saved: {inserted} new, {duplicates} duplicates'
        if find_owners:
            msg += f' | Found {owner_count} owners'
        
        flash(msg, 'success')
        return redirect(url_for('leads'))
        
    except Exception as e:
        import traceback
        print(f"Old Scrapper Error: {e}")
        print(traceback.format_exc())
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('old_scrapper_page'))


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
                records = scraper.fetch_new_businesses(limit=limit)
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
                records = scraper.fetch_new_businesses(limit=limit)
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
            query = """SELECT id, business_name, state, address, domain, website 
                      FROM leads 
                      WHERE owner_name IS NULL 
                      ORDER BY CASE WHEN domain IS NOT NULL THEN 0 ELSE 1 END, created_at DESC 
                      LIMIT %s"""
            with db.get_connection() as conn:
                leads_df = pd.read_sql(query, conn, params=[limit])
                if not leads_df.empty:
                    leads_to_process = leads_df.to_dict('records')
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
                                'enriched_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
             query = """SELECT id, business_name, state, owner_name, website, domain 
                       FROM leads 
                       WHERE owner_name IS NOT NULL AND linkedin IS NULL 
                       ORDER BY created_at DESC LIMIT %s"""
             with db.get_connection() as conn:
                leads_df = pd.read_sql(query, conn, params=[limit])
                if not leads_df.empty:
                    leads_to_process = leads_df.to_dict('records')
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
        enricher = get_enricher('apify') # Prefer apify
        
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
            query = """SELECT id, business_name, state, address, city 
                      FROM leads 
                      WHERE (website IS NULL OR website = '' OR domain IS NULL OR domain = '') 
                      ORDER BY created_at DESC LIMIT %s"""
            with db.get_connection() as conn:
                leads_df = pd.read_sql(query, conn, params=[limit])
                if not leads_df.empty:
                    leads_to_process = leads_df.to_dict('records')
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
                                'enriched_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            query = """SELECT * FROM leads 
                      WHERE (ghl_exported IS NULL OR ghl_exported = 0)
                      ORDER BY created_at DESC LIMIT 50"""
            with db.get_connection() as conn:
                leads_df = pd.read_sql(query, conn)
                if not leads_df.empty:
                    leads_to_export = leads_df.to_dict('records')
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
                    'scrape_date': datetime.now().strftime("%Y-%m-%d")
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
                        'ghl_exported_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    """Export leads from streaming data to Google Sheets using user's own Google account."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        leads = data.get('leads', [])
        
        if not leads:
            return jsonify({'success': False, 'error': 'No leads provided'}), 400
        
        # Get user's OAuth token from session - this creates files in THEIR Drive, not service account
        user_token = session.get('google_sheets_token')
        
        if not user_token:
            # User needs to auth with their Google account first
            return jsonify({
                'success': False, 
                'error': 'Please connect to Google Sheets first. Go to Settings > Connect to Google Sheets to authorize with your Google account.',
                'need_auth': True
            }), 401
        
        # Use the API exporter with USER's token (not service account)
        exporter = GoogleSheetsAPIExporter(user_token_json=user_token)
        
        # Convert leads to DataFrame
        import pandas as pd
        df = pd.DataFrame(leads)
        
        # Create new spreadsheet with data in USER's Drive
        title = f"Scraped Leads - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        result = exporter.create_new_spreadsheet(title, df)
        
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
            # Check for quota exceeded error and provide helpful message
            if 'quota' in error_msg.lower() or 'storage' in error_msg.lower():
                error_msg = 'Google Drive storage quota exceeded. Please free up space in your Google Drive (delete old files/sheets) or use a different Google account. You can also download as CSV instead.'
            return jsonify({
                'success': False,
                'error': error_msg
            }), 500
            
    except Exception as e:
        error_str = str(e)
        print(f"API Export Sheets Error: {e}")
        # Check for quota exceeded error
        if 'quota' in error_str.lower() or 'storage' in error_str.lower():
            error_str = 'Google Drive storage quota exceeded. Please free up space in your Google Drive (delete old files/sheets) or use a different Google account. You can also download as CSV instead.'
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
    Supports name pattern, date range, SIC code filtering.
    """
    try:
        name_pattern = request.args.get('name_pattern', '').strip()
        date_range = int(request.args.get('date_range', 30))
        sic_code = request.args.get('sic_code', '').strip()
        limit = min(int(request.args.get('limit', 50)), 100)
        
        # Use SEC EDGAR scraper
        sec = SECEdgarScraper()
        all_results = []
        
        # Search with company name if provided, otherwise search common filing types
        if name_pattern:
            # Search for specific company name pattern
            try:
                records = sec.fetch_new_businesses(limit=limit, company_search=name_pattern, fast_mode=True)
                for r in records or []:
                    result = {
                        'business_name': r.business_name,
                        'state': r.state or r.state_of_incorporation or 'US',
                        'filing_date': r.filing_date,
                        'address': r.business_address or r.address,
                        'industry_category': r.industry_category,
                        'business_category': r.industry_category,
                        'source': 'SEC_EDGAR',
                        'sic_code': r.sic_code,
                        'url': r.url
                    }
                    
                    # Filter by SIC code if specified
                    if sic_code and r.sic_code:
                        if not str(r.sic_code).startswith(sic_code[:2]):
                            continue
                    
                    all_results.append(result)
            except Exception as e:
                print(f"SEC search error: {e}")
        else:
            # Search multiple filing types
            filing_types = ['10-K', 'S-1', '10-Q', '8-K']
            records_per_type = max(5, limit // len(filing_types))
            
            for f_type in filing_types:
                if len(all_results) >= limit:
                    break
                try:
                    records = sec.fetch_new_businesses(limit=records_per_type, filing_type=f_type, fast_mode=True)
                    for r in records or []:
                        if len(all_results) >= limit:
                            break
                        
                        result = {
                            'business_name': r.business_name,
                            'state': r.state or r.state_of_incorporation or 'US',
                            'filing_date': r.filing_date,
                            'address': r.business_address or r.address,
                            'industry_category': r.industry_category,
                            'business_category': r.industry_category,
                            'source': f'SEC_{f_type}',
                            'sic_code': r.sic_code,
                            'url': r.url
                        }
                        
                        # Filter by SIC code if specified
                        if sic_code and r.sic_code:
                            if not str(r.sic_code).startswith(sic_code[:2]):
                                continue
                        
                        # Filter by name pattern if specified
                        if name_pattern:
                            if name_pattern.lower() not in r.business_name.lower():
                                continue
                        
                        all_results.append(result)
                except Exception as e:
                    print(f"SEC {f_type} search error: {e}")
        
        # Filter by date range (approximate based on filing date)
        from datetime import datetime, timedelta
        cutoff_date = (datetime.now() - timedelta(days=date_range)).strftime('%Y-%m-%d')
        filtered_results = [r for r in all_results if (r.get('filing_date') or '2020-01-01') >= cutoff_date]
        
        return jsonify({
            'success': True,
            'results': filtered_results[:limit],
            'count': len(filtered_results[:limit])
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
        
        results = df.to_dict('records')
        
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
    return render_template('404.html'), 404


@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    return render_template('500.html'), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)

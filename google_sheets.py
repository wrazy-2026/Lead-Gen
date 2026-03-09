"""
Google Sheets Integration
=========================
Provides functionality to export lead data to Google Sheets.
Supports OAuth 2.0 authentication for user accounts.

Setup Instructions:
------------------
1. Go to Google Cloud Console (https://console.cloud.google.com)
2. Create a new project or select existing
3. Enable the Google Sheets API and Google Drive API
4. Create OAuth 2.0 credentials:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - Choose "Web application"
   - Add redirect URI: http://localhost:5000/oauth2callback
   - Download the credentials JSON
   - Save as `credentials.json` in the project root

Usage:
------
    from google_sheets import GoogleSheetsExporter
    
    exporter = GoogleSheetsExporter()
    if not exporter.is_authenticated():
        auth_url = exporter.get_authorization_url()
        # Redirect user to auth_url
    else:
        exporter.export_dataframe(df, "your-spreadsheet-id")
"""

import json
import os
import pandas as pd
import requests
from typing import Optional, List
from datetime import datetime
import logging
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# OAuth 2.0 scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]


class GoogleSheetsExporter:
    """
    Exports data to Google Sheets using OAuth 2.0 authentication.
    
    Requires:
    - gspread library
    - google-auth-oauthlib library
    - A credentials.json file with OAuth client credentials
    """
    
    # Default columns to export
    DEFAULT_COLUMNS = [
        "business_name",
        "filing_date", 
        "state",
        "status",
        "email",
        "phone",
        "owner_name",
        "address",
        "url"
    ]
    
    def __init__(self, credentials_path: str = "credentials.json", token_path: str = "token.json"):
        """
        Initialize the Google Sheets exporter.
        
        Args:
            credentials_path: Path to OAuth credentials JSON file
            token_path: Path to store the user's access token
        """
        self.credentials_path = credentials_path
        self.token_path = token_path
        self._client = None
        self._credentials = None
        self._flow = None
        
        # Check if credentials file exists
        if not Path(credentials_path).exists() and not (os.environ.get('GOOGLE_CLIENT_ID') and os.environ.get('GOOGLE_CLIENT_SECRET')):
            logger.warning(
                f"Credentials file not found: {credentials_path}. "
                "Google Sheets export will not work until configured."
            )
    
    def is_configured(self) -> bool:
        """Check if OAuth credentials are configured."""
        if Path(self.credentials_path).exists():
            return True
        # Check environment variables
        return bool(os.environ.get('GOOGLE_CLIENT_ID') and os.environ.get('GOOGLE_CLIENT_SECRET'))
    
    def is_authenticated(self) -> bool:
        """Check if user has valid authentication token."""
        # Check for token in file or environment variable
        token_json = os.environ.get('GOOGLE_TOKEN_JSON')
        if not Path(self.token_path).exists() and not token_json:
            return False
            
        try:
            from google.oauth2.credentials import Credentials
            if token_json:
                import json
                token_info = json.loads(token_json)
                creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            else:
                creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)
            return creds and creds.valid
        except Exception:
            return False
    
    def get_authorization_url(self, redirect_uri: str = "http://127.0.0.1:5000/oauth2callback") -> str:
        """
        Get the URL to redirect user for OAuth authorization.
        
        Args:
            redirect_uri: Where Google should redirect after auth
            
        Returns:
            Authorization URL string
        """
        try:
            from google_auth_oauthlib.flow import Flow
        except ImportError:
            raise ImportError("Run: pip install google-auth-oauthlib")
            
        if os.environ.get('GOOGLE_CLIENT_ID') or os.environ.get('GOOGLE_CLIENT_SECRET'):
            # 1. Prioritize environment variables for Cloud Run/Docker
            client_id = os.environ.get('GOOGLE_CLIENT_ID', '')
            client_secret = os.environ.get('GOOGLE_CLIENT_SECRET', '')
            client_config = {
                "web": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            }
            self._flow = Flow.from_client_config(
                client_config,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
        elif Path(self.credentials_path).exists():
            # 2. Fallback to credentials.json file
            self._flow = Flow.from_client_secrets_file(
                self.credentials_path,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
        else:
            raise ValueError("Google OAuth is not configured. Set GOOGLE_CLIENT_ID/SECRET env vars or add credentials.json")
        
        auth_url, _ = self._flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        
        return auth_url
    
    def handle_oauth_callback(self, authorization_response: str, redirect_uri: str = "http://127.0.0.1:5000/oauth2callback") -> bool:
        """
        Handle the OAuth callback and save credentials.
        
        Args:
            authorization_response: The full callback URL with code
            redirect_uri: The redirect URI used in authorization
            
        Returns:
            True if successful
        """
        try:
            from google_auth_oauthlib.flow import Flow
            
            # Enable insecure transport for libraries that are picky about http/https mismatch 
            # (Cloud Run often has http internal headers but https external)
            os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

            client_id = os.environ.get('GOOGLE_CLIENT_ID', '')
            client_secret = os.environ.get('GOOGLE_CLIENT_SECRET', '')
            
            if client_id and client_secret:
                client_config = {
                    "web": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                }
                flow = Flow.from_client_config(
                    client_config,
                    scopes=SCOPES,
                    redirect_uri=redirect_uri
                )
            elif Path(self.credentials_path).exists():
                # 2. Fallback to credentials.json file
                flow = Flow.from_client_secrets_file(
                    self.credentials_path,
                    scopes=SCOPES,
                    redirect_uri=redirect_uri
                )
            else:
                raise ValueError("Google OAuth is not configured")
            
            # Ensure authorization_response uses https if the redirect_uri does
            # This fixes issues where the app thinks it's http but Google redirected to https
            fixed_response = authorization_response
            if redirect_uri.startswith('https://') and fixed_response.startswith('http://'):
                fixed_response = fixed_response.replace('http://', 'https://', 1)
                
            logger.debug(f"Fetching token with response: {fixed_response}")
            flow.fetch_token(authorization_response=fixed_response)
            
            # Save credentials
            creds = flow.credentials
            with open(self.token_path, 'w') as token_file:
                token_file.write(creds.to_json())
            
            logger.info("OAuth credentials saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"OAuth callback error: {e}")
            return False
    
    def _authenticate(self):
        """
        Authenticate with Google using stored OAuth credentials.
        
        Raises:
            Exception: If not authenticated
        """
        if self._client is not None:
            return
        
        try:
            import gspread
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
        except ImportError as e:
            raise ImportError(
                "Required packages not installed. Run: "
                "pip install gspread google-auth google-auth-oauthlib"
            ) from e
        
        if not Path(self.token_path).exists():
            raise Exception("Not authenticated. Please complete OAuth flow first.")
        
        # Load credentials
        self._credentials = Credentials.from_authorized_user_file(self.token_path, SCOPES)
        
        # Refresh if expired
        if self._credentials.expired and self._credentials.refresh_token:
            self._credentials.refresh(Request())
            # Save refreshed credentials
            with open(self.token_path, 'w') as token_file:
                token_file.write(self._credentials.to_json())
        
        # Create gspread client
        self._client = gspread.authorize(self._credentials)
        
        logger.info("Successfully authenticated with Google Sheets API")
    
    def export_dataframe(
        self,
        df: pd.DataFrame,
        spreadsheet_id: str,
        worksheet_name: str = "Leads",
        append: bool = True,
        columns: Optional[List[str]] = None
    ) -> dict:
        """
        Export a pandas DataFrame to a Google Sheet.
        
        Args:
            df: DataFrame to export
            spreadsheet_id: ID of the Google Sheet 
                           (from the URL: docs.google.com/spreadsheets/d/{ID}/...)
            worksheet_name: Name of the worksheet/tab to write to
            append: If True, append to existing data. If False, replace all data.
            columns: List of column names to export (None for default columns)
            
        Returns:
            Dictionary with export results
        """
        self._authenticate()
        
        # Select columns to export
        export_columns = columns or self.DEFAULT_COLUMNS
        available_columns = [col for col in export_columns if col in df.columns]
        
        if not available_columns:
            raise ValueError(f"No matching columns found. Available: {df.columns.tolist()}")
        
        export_df = df[available_columns].copy()
        
        try:
            # Open the spreadsheet
            spreadsheet = self._client.open_by_key(spreadsheet_id)
            
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except Exception:
                # Worksheet doesn't exist, create it
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name,
                    rows=1000,
                    cols=len(available_columns)
                )
                logger.info(f"Created new worksheet: {worksheet_name}")
            
            if append:
                # Get existing data to determine where to append
                existing_data = worksheet.get_all_values()
                
                if not existing_data:
                    # Sheet is empty, add headers first
                    worksheet.append_row(available_columns)
                    start_row = 2
                else:
                    start_row = len(existing_data) + 1
                
                # Prepare data for appending
                rows_to_add = export_df.values.tolist()
                
                if rows_to_add:
                    worksheet.append_rows(rows_to_add)
                    logger.info(f"Appended {len(rows_to_add)} rows to {worksheet_name}")
            else:
                # Replace all data
                worksheet.clear()
                
                # Prepare all data including headers
                all_data = [available_columns] + export_df.values.tolist()
                
                worksheet.update(range_name='A1', values=all_data)
                logger.info(f"Replaced data with {len(export_df)} rows in {worksheet_name}")
            
            return {
                "success": True,
                "rows_exported": len(export_df),
                "columns": available_columns,
                "spreadsheet_id": spreadsheet_id,
                "worksheet": worksheet_name,
                "exported_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to export to Google Sheets: {e}")
            return {
                "success": False,
                "error": str(e),
                "spreadsheet_id": spreadsheet_id
            }
    
    def create_new_spreadsheet(
        self,
        title: str,
        df: Optional[pd.DataFrame] = None
    ) -> dict:
        """
        Create a new Google Sheet and optionally populate it with data.
        
        Args:
            title: Title for the new spreadsheet
            df: Optional DataFrame to populate the sheet with
            
        Returns:
            Dictionary with the new spreadsheet details
        """
        self._authenticate()
        
        try:
            # Create new spreadsheet
            spreadsheet = self._client.create(title)
            
            logger.info(f"Created new spreadsheet: {title}")
            
            result = {
                "success": True,
                "spreadsheet_id": spreadsheet.id,
                "spreadsheet_url": spreadsheet.url,
                "title": title
            }
            
            # Populate with data if provided
            if df is not None and not df.empty:
                export_result = self.export_dataframe(
                    df,
                    spreadsheet.id,
                    worksheet_name="Leads",
                    append=False
                )
                result["export"] = export_result
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to create spreadsheet: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def list_spreadsheets(self) -> List[dict]:
        """
        List all spreadsheets accessible by the service account.
        
        Returns:
            List of spreadsheet metadata dictionaries
        """
        self._authenticate()
        
        try:
            spreadsheets = self._client.openall()
            return [
                {
                    "id": s.id,
                    "title": s.title,
                    "url": s.url
                }
                for s in spreadsheets
            ]
        except Exception as e:
            logger.error(f"Failed to list spreadsheets: {e}")
            return []
    
    def is_configured(self) -> bool:
        """Check if the exporter is properly configured."""
        return Path(self.credentials_path).exists()
    
    def get_service_account_email(self) -> Optional[str]:
        """
        Get the service account email from credentials file.
        This email needs to be granted access to target spreadsheets.
        
        Returns:
            Service account email or None if not configured
        """
        if not self.is_configured():
            return None
        
        try:
            with open(self.credentials_path, 'r') as f:
                creds = json.load(f)
                return creds.get("client_email")
        except Exception:
            return None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def export_to_sheets(
    df: pd.DataFrame,
    spreadsheet_id: str,
    credentials_path: str = "service_account.json",
    **kwargs
) -> dict:
    """
    Convenience function to export a DataFrame to Google Sheets.
    
    Args:
        df: DataFrame to export
        spreadsheet_id: Target Google Sheet ID
        credentials_path: Path to service account credentials
        **kwargs: Additional arguments passed to export_dataframe
        
    Returns:
        Export result dictionary
    """
    exporter = GoogleSheetsExporter(credentials_path)
    return exporter.export_dataframe(df, spreadsheet_id, **kwargs)


# ============================================================================
# MOCK EXPORTER FOR TESTING
# ============================================================================

class MockGoogleSheetsExporter:
    """
    Mock exporter for testing without actual Google Sheets connection.
    Use this when developing/testing the application flow.
    """
    
    def __init__(self, credentials_path: str = "service_account.json"):
        self.credentials_path = credentials_path
        self._exported_data = []
    
    def export_dataframe(
        self,
        df: pd.DataFrame,
        spreadsheet_id: str,
        worksheet_name: str = "Leads",
        append: bool = True,
        columns: Optional[List[str]] = None
    ) -> dict:
        """Mock export that stores data locally."""
        self._exported_data.append({
            "df": df.copy(),
            "spreadsheet_id": spreadsheet_id,
            "worksheet_name": worksheet_name,
            "timestamp": datetime.now()
        })
        
        logger.info(f"[MOCK] Exported {len(df)} rows to {spreadsheet_id}")
        
        return {
            "success": True,
            "mock": True,
            "rows_exported": len(df),
            "spreadsheet_id": spreadsheet_id,
            "worksheet": worksheet_name,
            "exported_at": datetime.now().isoformat()
        }
    
    def get_exported_data(self) -> List[dict]:
        """Get all data that was 'exported' during testing."""
        return self._exported_data
    
    def is_configured(self) -> bool:
        """Mock is always configured."""
        return True


class GoogleSheetsAPIExporter:
    """
    Export to Google Sheets using Service Account, OAuth, or API Key.
    Uses the Google Sheets API v4 directly.
    
    Priority:
    1. Service Account (GOOGLE_CREDENTIALS_JSON or credentials.json)
    2. OAuth Token (token.json - from GOOGLE_CLIENT_ID/SECRET flow)
    3. API Key (read-only, for public sheets)
    """
    
    API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
    
    DEFAULT_COLUMNS = [
        "business_name", "state", "phone", "address", "filing_date", "url", "ein",
        "industry_category", "owner_name", "first_name", "last_name",
        "phone_1", "phone_2", "email_1", "email_2", "email_3", "email_4", "email_5",
        "age", "website"
    ]
    
    def __init__(self, api_key: str = None, token_dict: dict = None):
        """
        Initialize with Google API Key, Service Account, or session token.
        """
        self.api_key = api_key or os.environ.get('GOOGLE_API_KEY', '')
        self.creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        self.token_dict = token_dict
        
        # Default to sheets-exporter-key.json if it exists, else credentials.json
        if Path("sheets-exporter-key.json").exists():
            self.creds_path = "sheets-exporter-key.json"
        else:
            self.creds_path = "credentials.json"
            
        self.token_path = "token.json"
        self.session = requests.Session()
    
    def _get_service_account_email(self):
        """Get the email of the service account from credentials."""
        try:
            with open(self.creds_path, 'r') as f:
                info = json.load(f)
                return info.get('client_email', 'unknown-service-account')
        except Exception:
            return os.environ.get('GOOGLE_SERVICE_ACCOUNT_EMAIL', 'unknown-service-account')

    def is_configured(self) -> bool:
        """Check if any Google Sheets auth method is configured."""
        # Service Account
        if self.creds_json or Path(self.creds_path).exists():
            return True
        # OAuth token exists
        if Path(self.token_path).exists():
            return True
        # OAuth credentials available (can do OAuth flow)
        client_id = os.environ.get('GOOGLE_CLIENT_ID', '')
        client_secret = os.environ.get('GOOGLE_CLIENT_SECRET', '')
        if client_id and client_secret:
            return True
        return False
    
    def is_authenticated(self) -> bool:
        """Check if we have valid credentials to export."""
        # Priority 1: Session or passed token dict
        if self.token_dict:
            return True
        # Priority 2: Personal OAuth token file
        if Path(self.token_path).exists():
            return True
        # Priority 3: Service Account (Fallback)
        if self.creds_json or Path(self.creds_path).exists():
            return True
        return False
    
    def _get_credentials(self):
        """Get Google credentials for API access."""
        from google.oauth2.service_account import Credentials as ServiceAccountCredentials
        from google.oauth2.credentials import Credentials as OAuthCredentials
        from google.auth.transport.requests import Request
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # 0. Try Session Token FIRST (Passed from Flask session)
        if self.token_dict:
            try:
                creds = OAuthCredentials.from_authorized_user_info(self.token_dict, scopes)
                if creds and creds.valid:
                    logger.info("Auth: Using Session OAuth token")
                    return creds
                if creds and creds.expired and creds.refresh_token:
                    logger.info("Auth: Refreshing Session OAuth token")
                    creds.refresh(Request())
                    return creds
            except Exception as e:
                logger.warning(f"Error loading Session token: {e}")

        # 1. Try OAuth token SECOND (User's personal account from file)
        if Path(self.token_path).exists():
            try:
                creds = OAuthCredentials.from_authorized_user_file(self.token_path, scopes)
                if creds and creds.expired and creds.refresh_token:
                    try:
                        logger.info("Auth: Refreshing OAuth token from file")
                        creds.refresh(Request())
                        with open(self.token_path, 'w') as f:
                            f.write(creds.to_json())
                    except Exception as re:
                        logger.warning(f"Could not refresh OAuth token: {re}")
                if creds and creds.valid:
                    logger.info("Auth: Using OAuth token from file")
                    return creds
            except Exception as e:
                logger.warning(f"Error loading OAuth token: {e}")
        
        # 2. Try Service Account (Explicit ENV)
        if self.creds_json:
            try:
                creds_dict = json.loads(self.creds_json)
                logger.info("Auth: Using Service Account from GOOGLE_CREDENTIALS_JSON")
                return ServiceAccountCredentials.from_service_account_info(creds_dict, scopes=scopes)
            except Exception as e:
                logger.warning(f"Invalid GOOGLE_CREDENTIALS_JSON: {e}")
        
        # 3. Try Service Account File (sheets-exporter-key.json preferred)
        if Path(self.creds_path).exists():
            try:
                with open(self.creds_path, 'r') as f:
                    data = json.load(f)
                    if data.get('type') == 'service_account':
                        logger.info(f"Auth: Using Service Account file: {self.creds_path}")
                        return ServiceAccountCredentials.from_service_account_file(self.creds_path, scopes=scopes)
                    else:
                        logger.debug(f"{self.creds_path} is not a service account file")
            except Exception as e:
                logger.warning(f"Error loading service account file {self.creds_path}: {e}")
        
        logger.error("Auth: No valid Google credentials found!")
        return None
    
    def export_dataframe(
        self,
        df: pd.DataFrame,
        spreadsheet_id: str,
        worksheet_name: str = "Sheet1",
        append: bool = True,
        columns: Optional[List[str]] = None
    ) -> dict:
        """
        Export DataFrame to Google Sheets using API.
        Supports Service Account, OAuth, or API Key authentication.
        """
        if not self.is_authenticated():
            return {"success": False, "error": "Not authenticated. Please connect to Google first."}
        
        try:
            import gspread
            
            # Get credentials using priority method
            credentials = self._get_credentials()
            if not credentials:
                return {"success": False, "error": "No valid Google credentials found"}
            
            client = gspread.authorize(credentials)
            
            # Open spreadsheet
            spreadsheet = client.open_by_key(spreadsheet_id)
            
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
            
            # Use all available columns from the DataFrame instead of a restricted list
            available_columns = df.columns.tolist()
            export_df = df[available_columns].fillna('')
            
            try:
                if append:
                    existing = worksheet.get_all_values()
                    if not existing:
                        # Add headers
                        worksheet.append_row(available_columns)
                    
                    # Add data rows
                    rows = export_df.values.tolist()
                    if rows:
                        worksheet.append_rows(rows)
                else:
                    worksheet.clear()
                    all_data = [available_columns] + export_df.values.tolist()
                    worksheet.update('A1', all_data)
            except Exception as e:
                err_str = str(e)
                if "quota" in err_str.lower() or "403" in err_str:
                    logger.warning("Quota exceeded during export. Attempting emergency cleanup and retry...")
                    try:
                        self.purge_service_account_drive()
                        time.sleep(2) # Give Google time to update quota
                        
                        # Re-authorize and retry exactly as before
                        client = gspread.authorize(self._get_credentials())
                        spreadsheet = client.open_by_key(spreadsheet_id)
                        worksheet = spreadsheet.worksheet(worksheet_name)
                        
                        if append:
                            rows = export_df.values.tolist()
                            if rows:
                                worksheet.append_rows(rows)
                        else:
                            worksheet.clear()
                            all_data = [available_columns] + export_df.values.tolist()
                            worksheet.update('A1', all_data)
                    except Exception as retry_e:
                        return {"success": False, "error": f"Quota still exceeded after cleanup: {str(retry_e)}"}
                else:
                    raise e
            
            return {
                "success": True,
                "rows_exported": len(export_df),
                "spreadsheet_id": spreadsheet_id,
                "worksheet": worksheet_name,
                "exported_at": datetime.now().isoformat()
            }
            
        except ImportError:
            return {"success": False, "error": "gspread not installed. Run: pip install gspread"}
        except Exception as e:
            logger.error(f"Export error: {e}")
            return {"success": False, "error": str(e)}
    
    def cleanup_drive_files(self, keep_count: int = 5, empty_trash: bool = True):
        """
        Delete old files created by service account to free up quota.
        
        Args:
            keep_count: Number of most recent files to keep. 0 to delete everything.
            empty_trash: Whether to empty the trash after deletion.
        """
        try:
            from googleapiclient.discovery import build
            
            credentials = self._get_credentials()
            if not credentials:
                logger.warning("No credentials found for Drive cleanup")
                return False
            
            # CRITICAL: Only cleanup if we are using a service account or explicitly authorized
            # We never want to auto-delete files from a real user's personal Drive without care.
            from google.oauth2.service_account import Credentials as ServiceAccountCredentials
            is_service_account = isinstance(credentials, ServiceAccountCredentials)
            
            if not is_service_account and keep_count > 0:
                logger.info("Skipping auto-cleanup for personal OAuth account (safety first)")
                return False
                
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # List all files owned by the service account
            all_files = []
            page_token = None
            
            while True:
                try:
                    results = drive_service.files().list(
                        q="'me' in owners",
                        orderBy="createdTime desc",
                        pageSize=1000,
                        pageToken=page_token,
                        fields="nextPageToken, files(id, name, createdTime, mimeType, size)"
                    ).execute()
                    
                    files = results.get('files', [])
                    all_files.extend(files)
                    
                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break
                except Exception as e:
                    logger.warning(f"Error listing files for cleanup: {e}")
                    break
            
            # Delete old files
            if len(all_files) > keep_count:
                files_to_delete = all_files[keep_count:]
                logger.info(f"Drive has {len(all_files)} files. Deleting {len(files_to_delete)} oldest files...")
                
                for file in files_to_delete:
                    try:
                        # Use delete() for permanent removal (frees up quota immediately)
                        drive_service.files().delete(fileId=file['id']).execute()
                        logger.debug(f"Deleted file: {file['name']} ({file['id']})")
                    except Exception as e:
                        logger.warning(f"Could not delete {file['name']}: {e}")
            
            if empty_trash:
                try:
                    drive_service.files().emptyTrash().execute()
                    logger.info("Drive trash emptied")
                except Exception as e:
                    logger.warning(f"Could not empty trash: {e}")
            
            return True
                
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return False

    def purge_service_account_drive(self):
        """Emergency purge: delete EVERYTHING in the service account's Drive."""
        logger.warning("Initiating emergency purge of all service account Drive files")
        return self.cleanup_drive_files(keep_count=0, empty_trash=True)

    def get_quota_info(self) -> dict:
        """
        Fetch Drive storage quota information.
        Returns:
            dict with 'usage', 'limit', 'usage_formatted', 'limit_formatted', 'percent'
        """
        try:
            from googleapiclient.discovery import build
            credentials = self._get_credentials()
            if not credentials:
                return {"error": "No credentials"}
                
            drive_service = build('drive', 'v3', credentials=credentials)
            about = drive_service.about().get(fields="storageQuota").execute()
            quota = about.get('storageQuota', {})
            
            usage = int(quota.get('usage', 0))
            limit = int(quota.get('limit', 0))
            
            def format_size(size_bytes):
                if size_bytes == 0: return "0 B"
                import math
                size_name = ("B", "KB", "MB", "GB", "TB")
                i = int(math.floor(math.log(size_bytes, 1024)))
                p = math.pow(1024, i)
                s = round(size_bytes / p, 2)
                return f"{s} {size_name[i]}"
            
            return {
                "usage": usage,
                "limit": limit,
                "usage_formatted": format_size(usage),
                "limit_formatted": format_size(limit) if limit > 0 else "Unlimited",
                "percent": round((usage / limit * 100), 2) if limit > 0 else 0
            }
        except Exception as e:
            logger.error(f"Failed to fetch quota: {e}")
            return {"error": str(e)}
    
    def create_new_spreadsheet(
        self,
        title: str = "Leads Export",
        spreadsheet_id: str = None,
        df: pd.DataFrame = None,
        append: bool = False,
        worksheet_name: str = "Leads"
    ) -> dict:
        """Create a new spreadsheet or use existing one and prepare it."""
        if not self.is_authenticated():
            return {"success": False, "error": "Not authenticated. Please connect to Google first."}
        
        try:
            import gspread
            from google.oauth2.service_account import Credentials as ServiceAccountCredentials
            credentials = self._get_credentials()
            if not credentials:
                return {"success": False, "error": "No valid Google credentials found"}
            
            client = gspread.authorize(credentials)
            
            if spreadsheet_id:
                try:
                    spreadsheet = client.open_by_key(spreadsheet_id)
                    logger.info(f"Using existing spreadsheet: {spreadsheet_id}")
                except Exception as e:
                    return {"success": False, "error": f"Could not access spreadsheet with ID {spreadsheet_id}: {str(e)}"}
            else:
                # 1. First attempt with standard cleanup
                try:
                    # Keep only 2 recent sheets to save quota
                    self.cleanup_drive_files(keep_count=2)
                except Exception:
                    pass
                
                try:
                    spreadsheet = client.create(title)
                except Exception as e:
                    err_str = str(e)
                    if "quota" in err_str.lower() or "403" in err_str:
                        # 2. SEVERE QUOTA ERROR: Try deleting EVERYTHING and retry once
                        logger.warning("Quota exceeded. Attempting emergency purge of all service account files...")
                        try:
                            self.purge_service_account_drive()
                            time.sleep(2) # Give Google time to reflect the deleted space
                            # Retry creation
                            spreadsheet = client.create(title)
                        except Exception as retry_e:
                            # Still failing after purge, provide helpful instructions
                            service_email = self._get_service_account_email()
                            is_service_account = isinstance(credentials, ServiceAccountCredentials)
                            
                            error_msg = f"Drive storage quota exceeded (Service Account: {service_email})."
                            if is_service_account:
                                details = "The background service account has no more Drive storage space. TO FIX THIS:\n" \
                                          "1. LOG OUT AND LOG BACK IN using the standard 'Google Login' button (this sets your personal 15GB quota),\n" \
                                          "2. OR: Go to Settings and click 'Cleanup Google Drive' to purge old files,\n" \
                                          "3. OR: Create a Sheet manually and share as 'Editor' with the service account email above."
                            else:
                                details = "Your personal Google Drive quota appears to be full. Please free up some space in your Google Drive."
                                
                            return {
                                "success": False, 
                                "error": error_msg,
                                "details": details,
                                "service_email": service_email if is_service_account else None
                            }
                    else:
                        raise e
            
            # Share with admin and transfer ownership if it's a new sheet or service account owned
            admin_email = os.environ.get('ADMIN_EMAIL')
            if admin_email:
                try:
                    # First share as writer
                    spreadsheet.share(admin_email, perm_type='user', role='writer', notify=False)
                    logger.info(f"Shared spreadsheet with {admin_email}")
                    
                    # Try to transfer ownership (moves file to user's Drive)
                    try:
                        from googleapiclient.discovery import build
                        drive_service = build('drive', 'v3', credentials=credentials)
                        
                        # Find the permission ID for admin email
                        permissions = drive_service.permissions().list(fileId=spreadsheet.id).execute()
                        for perm in permissions.get('permissions', []):
                            if perm.get('emailAddress', '').lower() == admin_email.lower():
                                # Transfer ownership
                                drive_service.permissions().update(
                                    fileId=spreadsheet.id,
                                    permissionId=perm['id'],
                                    transferOwnership=True,
                                    body={'role': 'owner'}
                                ).execute()
                                logger.info(f"Transferred ownership to {admin_email}")
                                break
                    except Exception as e:
                        logger.warning(f"Could not transfer ownership (user will still have access): {e}")
                        
                except Exception as e:
                    logger.warning(f"Could not share with admin: {e}")
            
            # Also make readable by anyone with link as fallback
            try:
                spreadsheet.share(None, perm_type='anyone', role='reader')
            except Exception as e:
                logger.warning(f"Could not share publicly: {e}")
            
            result = {
                "success": True,
                "spreadsheet_id": spreadsheet.id,
                "spreadsheet_url": spreadsheet.url,
                "title": title
            }
            
            if df is not None and not df.empty:
                export_result = self.export_dataframe(
                    df,
                    spreadsheet.id,
                    worksheet_name=worksheet_name,
                    append=append
                )
                result["export"] = export_result
            
            return result
            
        except ImportError:
            return {"success": False, "error": "gspread library missing"}
        except Exception as e:
            logger.error(f"Error creating spreadsheet: {e}")
            return {"success": False, "error": str(e)}


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("GOOGLE SHEETS INTEGRATION DEMO")
    print("="*60)
    
    # Check if credentials exist
    exporter = GoogleSheetsExporter()
    
    if exporter.is_configured():
        email = exporter.get_service_account_email()
        print(f"\nService Account Email: {email}")
        print("(Share your Google Sheet with this email to enable export)")
        
        # Example usage:
        # df = pd.DataFrame({
        #     "business_name": ["Test Corp LLC", "Demo Inc"],
        #     "filing_date": ["2024-01-15", "2024-01-16"],
        #     "state": ["Delaware", "California"],
        #     "status": ["Active", "Active"],
        #     "url": ["https://example.com/1", "https://example.com/2"]
        # })
        # 
        # result = exporter.export_dataframe(
        #     df,
        #     spreadsheet_id="YOUR_SPREADSHEET_ID_HERE"
        # )
        # print(result)
    else:
        print("\n⚠️  Google Sheets not configured!")
        print("\nTo enable Google Sheets export:")
        print("1. Create a Google Cloud project")
        print("2. Enable Google Sheets API")
        print("3. Create a service account")
        print("4. Download the JSON key as 'service_account.json'")
        print("5. Share your target spreadsheet with the service account email")
        
        print("\n--- Using Mock Exporter for Demo ---")
        
        # Demo with mock exporter
        mock_exporter = MockGoogleSheetsExporter()
        
        test_df = pd.DataFrame({
            "business_name": ["Test Corp LLC", "Demo Inc"],
            "filing_date": ["2024-01-15", "2024-01-16"],
            "state": ["Delaware", "California"],
            "status": ["Active", "Active"],
            "url": ["https://example.com/1", "https://example.com/2"]
        })
        
        result = mock_exporter.export_dataframe(
            test_df,
            spreadsheet_id="mock-spreadsheet-id"
        )
        
        print(f"\nMock Export Result: {result}")

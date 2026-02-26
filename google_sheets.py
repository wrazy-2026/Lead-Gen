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
            
        if Path(self.credentials_path).exists():
            self._flow = Flow.from_client_secrets_file(
                self.credentials_path,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
        else:
            # Construct client config from environment variables
            client_config = {
                "web": {
                    "client_id": os.environ.get('GOOGLE_CLIENT_ID'),
                    "client_secret": os.environ.get('GOOGLE_CLIENT_SECRET'),
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            }
            self._flow = Flow.from_client_config(
                client_config,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
        
        auth_url, _ = self._flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        
        return auth_url
    
    def handle_oauth_callback(self, authorization_response: str, redirect_uri: str = "http://127.0.0.1:5000/oauth2callback") -> dict:
        """
        Handle the OAuth callback and return credentials.
        
        Args:
            authorization_response: The full callback URL with code
            redirect_uri: The redirect URI used in authorization
            
        Returns:
            Dictionary with 'success' and 'token_json' (if successful)
        """
        try:
            from google_auth_oauthlib.flow import Flow
            
            if Path(self.credentials_path).exists():
                flow = Flow.from_client_secrets_file(
                    self.credentials_path,
                    scopes=SCOPES,
                    redirect_uri=redirect_uri
                )
            else:
                client_config = {
                    "web": {
                        "client_id": os.environ.get('GOOGLE_CLIENT_ID'),
                        "client_secret": os.environ.get('GOOGLE_CLIENT_SECRET'),
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                }
                flow = Flow.from_client_config(
                    client_config,
                    scopes=SCOPES,
                    redirect_uri=redirect_uri
                )
            
            flow.fetch_token(authorization_response=authorization_response)
            
            # Get credentials as JSON for session storage
            creds = flow.credentials
            token_json = creds.to_json()
            
            # Also save to file for backward compatibility
            with open(self.token_path, 'w') as token_file:
                token_file.write(token_json)
            
            logger.info("OAuth credentials obtained successfully")
            return {'success': True, 'token_json': token_json}
            
        except Exception as e:
            logger.error(f"OAuth callback error: {e}")
            return {'success': False, 'error': str(e)}
    
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
    1. User token (passed directly - for per-user OAuth)
    2. Service Account (GOOGLE_CREDENTIALS_JSON or credentials.json)
    3. OAuth Token (token.json - from GOOGLE_CLIENT_ID/SECRET flow)
    4. API Key (read-only, for public sheets)
    """
    
    API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
    
    DEFAULT_COLUMNS = [
        "business_name", "filing_date", "state", "status", "address",
        "phone", "owner_name", "first_name", "last_name",
        "phone_1", "phone_2", "email_1", "email_2", "email_3", "email_4", "email_5",
        "age", "website", "url"
    ]
    
    def __init__(self, api_key: str = None, user_token_json: str = None):
        """
        Initialize with Google API Key, Service Account, or User Token.
        
        Args:
            api_key: Optional Google API key
            user_token_json: Optional JSON string of user's OAuth token (from session)
        """
        self.api_key = api_key or os.environ.get('GOOGLE_API_KEY', '')
        self.creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        self.creds_path = "credentials.json"
        self.token_path = "token.json"
        self.user_token_json = user_token_json  # User-specific OAuth token
        self.session = requests.Session()
    
    def is_configured(self) -> bool:
        """Check if any Google Sheets auth method is configured."""
        # User token passed directly
        if self.user_token_json:
            return True
        # Service Account
        if self.creds_json or Path(self.creds_path).exists():
            return True
        # OAuth token exists
        if Path(self.token_path).exists():
            return True
        # OAuth credentials available (can do OAuth flow)
        if os.environ.get('GOOGLE_CLIENT_ID') and os.environ.get('GOOGLE_CLIENT_SECRET'):
            return True
        return False
    
    def is_authenticated(self, user_token_json: str = None) -> bool:
        """Check if we have valid credentials to export.
        
        Args:
            user_token_json: Optional user-specific token to check
        """
        # Check passed user token
        if user_token_json or self.user_token_json:
            return True
        # Service Account is always ready
        if self.creds_json or Path(self.creds_path).exists():
            return True
        # Check for OAuth token
        if Path(self.token_path).exists():
            return True
        return False
    
    def _get_credentials(self, user_token_json: str = None):
        """Get Google credentials for API access.
        
        Priority:
        1. User token (passed or from constructor)
        2. Service Account (env var or file)
        3. OAuth token file
        
        Args:
            user_token_json: Optional user-specific OAuth token JSON
        """
        from google.oauth2.service_account import Credentials as ServiceAccountCredentials
        from google.oauth2.credentials import Credentials as OAuthCredentials
        from google.auth.transport.requests import Request
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # 1. Try user-specific token first (from session/passed)
        token_to_use = user_token_json or self.user_token_json
        if token_to_use:
            try:
                token_info = json.loads(token_to_use)
                creds = OAuthCredentials.from_authorized_user_info(token_info, scopes)
                # Refresh if expired
                if creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                logger.info("Using user-specific OAuth credentials")
                return creds
            except Exception as e:
                logger.warning(f"Could not use user token: {e}")
        
        # 2. Try Service Account (GOOGLE_CREDENTIALS_JSON)
        if self.creds_json:
            try:
                creds_dict = json.loads(self.creds_json)
                return ServiceAccountCredentials.from_service_account_info(creds_dict, scopes=scopes)
            except json.JSONDecodeError:
                logger.warning("Invalid GOOGLE_CREDENTIALS_JSON format")
        
        # 3. Try credentials.json file (Service Account)
        if Path(self.creds_path).exists():
            return ServiceAccountCredentials.from_service_account_file(self.creds_path, scopes=scopes)
        
        # 4. Try OAuth token file (from Client ID/Secret flow)
        if Path(self.token_path).exists():
            creds = OAuthCredentials.from_authorized_user_file(self.token_path, scopes)
            # Refresh if expired
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Save refreshed token
                with open(self.token_path, 'w') as f:
                    f.write(creds.to_json())
            return creds
        
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
            
            # Select columns
            export_columns = columns or self.DEFAULT_COLUMNS
            available_columns = [col for col in export_columns if col in df.columns]
            
            if not available_columns:
                available_columns = df.columns.tolist()
            
            export_df = df[available_columns].fillna('')
            
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
                worksheet.update(range_name='A1', values=all_data)
            
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
    
    def _cleanup_old_spreadsheets(self, client, keep_count: int = 5):
        """Delete old spreadsheets created by service account to free up quota."""
        try:
            from googleapiclient.discovery import build
            credentials = self._get_credentials()
            if not credentials:
                return
            
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # List all spreadsheets owned by service account
            results = drive_service.files().list(
                q="mimeType='application/vnd.google-apps.spreadsheet'",
                orderBy="createdTime desc",
                pageSize=100,
                fields="files(id, name, createdTime)"
            ).execute()
            
            files = results.get('files', [])
            
            # Delete old files, keeping only recent ones
            if len(files) > keep_count:
                for file in files[keep_count:]:
                    try:
                        drive_service.files().delete(fileId=file['id']).execute()
                        logger.info(f"Deleted old spreadsheet: {file['name']}")
                    except Exception as e:
                        logger.warning(f"Could not delete {file['name']}: {e}")
        except Exception as e:
            logger.warning(f"Cleanup failed (non-critical): {e}")
    
    def create_new_spreadsheet(self, title: str, df: pd.DataFrame = None) -> dict:
        """
        Create a new spreadsheet using direct Sheets API v4.
        
        When using user_token_json, this creates the sheet in the USER's own Google Drive,
        avoiding service account quota issues.
        """
        try:
            # Get credentials - will use user token if provided
            credentials = self._get_credentials()
            if not credentials:
                return {"success": False, "error": "Not authenticated. Please connect to Google Sheets in Settings."}
            
            # Use direct HTTP API like the reference Chrome extension
            # This is simpler and more reliable than gspread for user tokens
            import requests
            from google.auth.transport.requests import Request as GoogleRequest
            
            # Refresh credentials if needed
            if hasattr(credentials, 'expired') and credentials.expired:
                if hasattr(credentials, 'refresh_token') and credentials.refresh_token:
                    credentials.refresh(GoogleRequest())
            
            # Get access token
            if hasattr(credentials, 'token'):
                token = credentials.token
            else:
                token = credentials.get_access_token().access_token
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Step 1: Create spreadsheet (in user's Drive)
            create_response = requests.post(
                'https://sheets.googleapis.com/v4/spreadsheets',
                headers=headers,
                json={
                    'properties': {
                        'title': title
                    }
                },
                timeout=30
            )
            
            if not create_response.ok:
                error_detail = create_response.text
                logger.error(f"Failed to create spreadsheet: {create_response.status_code} - {error_detail}")
                return {"success": False, "error": f"Failed to create spreadsheet: {error_detail}"}
            
            sheet_data = create_response.json()
            spreadsheet_id = sheet_data['spreadsheetId']
            spreadsheet_url = sheet_data['spreadsheetUrl']
            
            logger.info(f"Created spreadsheet: {title} ({spreadsheet_id})")
            
            result = {
                "success": True,
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_url": spreadsheet_url,
                "title": title
            }
            
            # Step 2: Add data if provided
            if df is not None and not df.empty:
                # Prepare headers and data
                headers_row = list(df.columns)
                values = [headers_row]
                
                for _, row in df.iterrows():
                    values.append([str(v) if pd.notna(v) else '' for v in row.values])
                
                # Append data to sheet
                append_response = requests.post(
                    f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/A1:append',
                    headers={
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    },
                    params={'valueInputOption': 'RAW'},
                    json={'values': values},
                    timeout=60
                )
                
                if append_response.ok:
                    result["rows_exported"] = len(df)
                    logger.info(f"Exported {len(df)} rows to {title}")
                else:
                    logger.warning(f"Data append had issues: {append_response.text}")
                    result["export_warning"] = append_response.text
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating spreadsheet: {e}")
            import traceback
            logger.error(traceback.format_exc())
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

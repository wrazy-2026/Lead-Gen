import os
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from database import get_database
from google_sheets import GoogleSheetsAPIExporter

def debug():
    print("="*60)
    print("GOOGLE QUOTA & AUTH DIAGNOSTIC")
    print("="*60)
    
    db = get_database()
    
    # 1. Check Firestore Admin Token
    print("\n1. CHECKING FIRESTORE TOKEN:")
    token = db.get_setting('google_admin_token')
    if token:
        print(f"✅ Found google_admin_token in Firestore")
        print(f"   Email: {token.get('email', 'N/A')}")
        print(f"   Has refresh_token: {'refresh_token' in token}")
        print(f"   Scopes: {token.get('scopes', [])}")
    else:
        print(f"❌ No google_admin_token found in Firestore. USER NEEDS TO LOG IN.")

    # 2. Check Service Account Quota
    print("\n2. CHECKING SERVICE ACCOUNT QUOTA:")
    exporter = GoogleSheetsAPIExporter()
    quota = exporter.get_quota_info()
    if 'error' in quota:
        print(f"❌ Error fetching service account quota: {quota['error']}")
    else:
        print(f"✅ Service Account Usage: {quota.get('usage_formatted')} / {quota.get('limit_formatted')} ({quota.get('percent')}%)")
        if quota.get('percent', 0) > 95:
            print("⚠️  Warning: Service account is almost full!")
            
    # 3. Check if clean-up is working
    if token:
        print("\n3. CHECKING ADMIN ACCOUNT QUOTA:")
        admin_exporter = GoogleSheetsAPIExporter(token_dict=token)
        admin_quota = admin_exporter.get_quota_info()
        if 'error' in admin_quota:
            print(f"❌ Error fetching admin account quota: {admin_quota['error']}")
        else:
            print(f"✅ Admin Account Usage: {admin_quota.get('usage_formatted')} / {admin_quota.get('limit_formatted')} ({admin_quota.get('percent')}%)")

if __name__ == "__main__":
    debug()

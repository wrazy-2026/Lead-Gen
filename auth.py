"""
Authentication Module - Google OAuth
=====================================
Handles Google OAuth authentication and user management.

Admin email: samadly728@gmail.com
"""

import os
from datetime import datetime
from functools import wraps
from flask import redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, current_user
from authlib.integrations.flask_client import OAuth
from database import get_database

# ============================================================================
# CONFIGURATION
# ============================================================================

ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'samadly728@gmail.com')
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')

# ============================================================================
# USER MODEL
# ============================================================================

class User(UserMixin):
    """User model for Flask-Login."""
    
    def __init__(self, id, email, name, picture=None, is_admin=False, created_at=None, last_login=None):
        self.id = str(id)
        self.email = email
        self.name = name
        self.picture = picture
        self.is_admin = is_admin
        self.created_at = created_at
        self.last_login = last_login
    
    @staticmethod
    def _get_users_ref():
        from firebase_admin import firestore
        from firebase_setup import initialize_firebase
        try:
            # Ensure initialization happens first
            initialize_firebase() 
            firestore_db_id = (os.environ.get('FIRESTORE_DATABASE_ID') or 'leadgen').strip() or 'leadgen'
            return firestore.client(database_id=firestore_db_id).collection('users')
        except Exception as e:
            print(f"[Auth] Firestore client error: {e}")
            return None

    @staticmethod
    def get(user_id):
        """Get user by ID from database."""
        if not user_id or user_id == "guest":
            return None
            
        import time
        t0 = time.time()
        try:
            print(f"[Auth] Attempting to fetch user {user_id}...")
            
            ref = User._get_users_ref()
            if not ref:
                print("[Auth] Firestore not initialized")
                return None
                
            doc = ref.document(str(user_id)).get(timeout=3.0)
                
            if doc.exists:
                row = doc.to_dict()
                print(f"[Auth] User {user_id} fetched successfully in {time.time()-t0:.2f}s")
                return User(
                    id=doc.id,
                    email=row.get('email'),
                    name=row.get('name'),
                    picture=row.get('picture'),
                    is_admin=row.get('is_admin', False),
                    created_at=row.get('created_at'),
                    last_login=row.get('last_login')
                )
        except Exception as e:
            print(f"[Auth] Auth Error (get) for {user_id} after {time.time()-t0:.2f}s: {e}")
            from flask import session
            user_data = session.get(f'user_data_{user_id}')
            if user_data:
                print(f"[Auth] Returning cached user data for {user_id}")
                return User(
                    id=user_id,
                    email=user_data.get('email'),
                    name=user_data.get('name'),
                    picture=user_data.get('picture'),
                    is_admin=user_data.get('is_admin', False)
                )
        
        return None

    @staticmethod
    def get_by_email(email):
        """Get user by email from database."""
        import time
        t0 = time.time()
        try:
            print(f"[Auth] Fetching user by email {email}...")
            from google.cloud.firestore_v1.base_query import FieldFilter
            
            ref = User._get_users_ref()
            if not ref:
                return None
                
            docs = list(ref.where(filter=FieldFilter('email', '==', email)).limit(1).stream(timeout=3.0))
            
            for doc in docs:
                row = doc.to_dict()
                print(f"[Auth] User {email} found in {time.time()-t0:.2f}s")
                return User(
                    id=doc.id,
                    email=row.get('email'),
                    name=row.get('name'),
                    picture=row.get('picture'),
                    is_admin=True,
                    created_at=row.get('created_at'),
                    last_login=row.get('last_login')
                )
        except Exception as e:
            print(f"[Auth] Auth Error (get_by_email) for {email} after {time.time()-t0:.2f}s: {e}")
        return None
    
    @staticmethod
    def create_or_update(email, name, picture=None):
        """Create or update user in database."""
        print(f"[Auth] Entering create_or_update for {email}")
        # Only make admin if email matches the configured admin email
        is_admin = email.lower() == ADMIN_EMAIL.lower()
        from datetime import datetime
        import time
        import re
        
        now = datetime.now().isoformat()
        doc_id = re.sub(r'[^a-zA-Z0-9_-]', '', email.replace('@', '_').replace('.', '_'))
        if not doc_id:
            doc_id = f"user_{int(datetime.now().timestamp() * 1000)}"

        # Check existing role from Firestore (don't overwrite if already set by admin)
        try:
            ref = User._get_users_ref()
            if ref:
                existing_doc = ref.document(doc_id).get(timeout=3.0)
                if existing_doc.exists:
                    existing_data = existing_doc.to_dict()
                    # Preserve existing role unless this is admin email
                    if not is_admin:
                        is_admin = existing_data.get('is_admin', False)
        except Exception as e:
            print(f"[Auth] Could not read existing role: {e}")

        # UPDATE DB SYNCHRONOUSLY
        try:
            print(f"[Auth] Saving user {doc_id} to Firestore...")
            ref = User._get_users_ref()
            if ref:
                ref.document(doc_id).set({
                    'email': email,
                    'name': name,
                    'picture': picture,
                    'is_admin': is_admin,
                    'last_login': now
                }, merge=True)
                # Set created_at only on first create (merge won't overwrite if already set)
                # We use update with server sentinel to avoid overwriting created_at
                try:
                    ref.document(doc_id).update({
                        'created_at': existing_doc.to_dict().get('created_at', now) if existing_doc.exists else now
                    })
                except:
                    pass
                print(f"[Auth] User {doc_id} saved successfully")
            else:
                print("[Auth] Cannot save user: Firestore ref is None")
        except Exception as e:
            print(f"[Auth] DB update failed for {email}: {e}")
        
        return User(
            id=doc_id,
            email=email,
            name=name,
            picture=picture,
            is_admin=is_admin
        )
    
    @staticmethod
    def get_all_users():
        """Get all users from database."""
        try:
            print("[Auth] Fetching all users...")
            ref = User._get_users_ref()
            if not ref:
                return []
                
            docs = list(ref.stream(timeout=5.0))
                
            users = []
            for doc in docs:
                row = doc.to_dict()
                users.append(User(
                    id=doc.id,
                    email=row.get('email'),
                    name=row.get('name'),
                    picture=row.get('picture'),
                    is_admin=row.get('is_admin', False),
                    created_at=row.get('created_at'),
                    last_login=row.get('last_login')
                ))
            # Sort by created_at descending
            users.sort(key=lambda u: u.created_at or '', reverse=True)
            return users
        except Exception as e:
            print(f"Auth Error (get_all_users): {e}")
            return []

def admin_required(f):
    """Decorator to require admin access."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth_login'))
        if not current_user.is_admin:
            flash('Admin access required.', 'error')
            return redirect(url_for('landing'))
        return f(*args, **kwargs)
    return decorated_function


def login_required_custom(f):
    """Decorator to require login."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'info')
            return redirect(url_for('auth_login'))
        return f(*args, **kwargs)
    return decorated_function


# ============================================================================
# OAUTH SETUP
# ============================================================================

from flask_login import LoginManager, UserMixin, AnonymousUserMixin, current_user

login_manager = LoginManager()

class Anonymous(AnonymousUserMixin):
    """Custom class for anonymous users - not authenticated."""
    def __init__(self):
        self.id = "guest"
        self.email = "guest@example.com"
        self.name = "Guest User"
        self.picture = None
        self.is_admin = False 

login_manager.anonymous_user = Anonymous
oauth = OAuth()

def init_oauth(app):
    """Initialize OAuth with the Flask app."""
    login_manager.init_app(app)
    login_manager.login_view = 'landing'
    login_manager.login_message = None
    login_manager.login_message_category = None
    
    oauth.init_app(app)
    
    # Register Google OAuth - LOGIN ONLY (fast, no Sheet scopes at login time)
    oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )
    
    # Register Google OAuth with Sheets scope - only used for export authorization
    oauth.register(
        name='google_sheets',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive.file'
        }
    )
    
    @login_manager.user_loader
    def load_user(user_id):
        # 1. First check session to save DB quota
        user_data = session.get(f'user_data_{user_id}')
        if user_data:
            return User(
                id=user_id, 
                email=user_data.get('email'), 
                name=user_data.get('name'), 
                picture=user_data.get('picture'),
                is_admin=user_data.get('is_admin', True)
            )
            
        # 2. Fallback to DB
        user = User.get(user_id)
        
        # Cache to session so next request doesn't hit DB
        if user:
            session[f'user_data_{user.id}'] = {
                'email': user.email,
                'name': user.name,
                'picture': user.picture,
                'is_admin': user.is_admin
            }
        return user
    
    return oauth

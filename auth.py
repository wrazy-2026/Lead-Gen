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
from flask_login import LoginManager, UserMixin, current_user, login_user
from authlib.integrations.flask_client import OAuth
from database import get_database
import firebase_admin
from firebase_admin import credentials, auth as firebase_auth

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
        self.id = id
        self.email = email
        self.name = name
        self.picture = picture
        self.is_admin = is_admin
        self.created_at = created_at
        self.last_login = last_login
    
    @staticmethod
    def get(user_id):
        """Get user by ID."""
        db = get_database()
        
        # Check if Firestore
        if hasattr(db, 'get_user_by_id'):
            row = db.get_user_by_id(user_id)
        else:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                sql = "SELECT * FROM users WHERE id = ?"
                cursor.execute(db.prepare_sql(sql), (user_id,))
                row = cursor.fetchone()
                
        if row:
            return User(
                id=row['id'],
                email=row['email'],
                name=row['name'],
                picture=row['picture'],
                is_admin=bool(row['is_admin']),
                created_at=row['created_at'],
                last_login=row['last_login']
            )
        return None

    @staticmethod
    def get_by_email(email):
        """Get user by email."""
        db = get_database()
        
        if hasattr(db, 'get_user_by_email'):
            row = db.get_user_by_email(email)
        else:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                sql = "SELECT * FROM users WHERE email = ?"
                cursor.execute(db.prepare_sql(sql), (email,))
                row = cursor.fetchone()
        
        if row:
            return User(
                id=row['id'],
                email=row['email'],
                name=row['name'],
                picture=row['picture'],
                is_admin=bool(row['is_admin']),
                created_at=row['created_at'],
                last_login=row['last_login']
            )
        return None
    
    @staticmethod
    def create_or_update(email, name, picture=None):
        """Create or update user."""
        is_admin = (email.lower() == ADMIN_EMAIL.lower())
        db = get_database()
        
        print(f"[User] Creating/updating user: {email} (admin={is_admin})")
        print(f"[User] Database type: {getattr(db, 'db_type', 'unknown')}")
        
        try:
            if hasattr(db, 'create_or_update_user'):
                print(f"[User] Using Firestore backend")
                user_id = db.create_or_update_user(email, name, picture, is_admin)
                print(f"[User] Firestore result: user_id={user_id}")
            else:
                print(f"[User] Using SQL backend (type={db.db_type})")
                now = datetime.now().isoformat()
                with db.get_connection() as conn:
                    cursor = conn.cursor()
                    sql = "SELECT id FROM users WHERE email = ?"
                    prepared_sql = db.prepare_sql(sql)
                    print(f"[User] Checking existing user: {email}")
                    cursor.execute(prepared_sql, (email,))
                    existing = cursor.fetchone()
                    
                    if existing:
                        print(f"[User] User exists, updating (id={existing['id']})")
                        update_sql = "UPDATE users SET name = ?, picture = ?, is_admin = ?, last_login = ? WHERE email = ?"
                        cursor.execute(db.prepare_sql(update_sql), (name, picture, is_admin, now, email))
                        user_id = existing['id']
                    else:
                        print(f"[User] New user, inserting")
                        insert_sql = "INSERT INTO users (email, name, picture, is_admin, created_at, last_login) VALUES (?, ?, ?, ?, ?, ?)"
                        prepared_sql = db.prepare_sql(insert_sql)
                        if db.db_type == 'postgres':
                            cursor.execute(prepared_sql + " RETURNING id", (email, name, picture, is_admin, now, now))
                            row = cursor.fetchone()
                            user_id = row['id'] if row else None
                        else:
                            cursor.execute(prepared_sql, (email, name, picture, is_admin, now, now))
                            user_id = cursor.lastrowid
                    print(f"[User] SQL result: user_id={user_id}")
        except Exception as e:
            print(f"[User] ❌ Error in create_or_update: {type(e).__name__}: {e}")
            import traceback
            print(traceback.format_exc())
            return None
            
        if user_id:
            return User.get(user_id)
        return None
    
    @staticmethod
    def get_all_users():
        """Get all users from database."""
        db = get_database()
        with db.get_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM users ORDER BY created_at DESC"
            cursor.execute(db.prepare_sql(sql))
            rows = cursor.fetchall()
        
        return [User(
            id=row['id'],
            email=row['email'],
            name=row['name'],
            picture=row['picture'],
            is_admin=bool(row['is_admin']),
            created_at=row['created_at'],
            last_login=row['last_login']
        ) for row in rows]


# ============================================================================
# AUTH DECORATORS
# ============================================================================

def admin_required(f):
    """Decorator to require admin access."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('landing'))
        if not current_user.is_admin:
            flash('Admin access required.', 'error')
            return redirect(url_for('client_dashboard'))
        return f(*args, **kwargs)
    return decorated_function


def login_required_custom(f):
    """Decorator to require login."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('landing'))
        return f(*args, **kwargs)
    return decorated_function


# ============================================================================
# OAUTH SETUP
# ============================================================================

login_manager = LoginManager()
oauth = OAuth()

def init_oauth(app):
    """Initialize OAuth with the Flask app."""
    login_manager.init_app(app)
    login_manager.login_view = 'landing'
    login_manager.login_message = None
    login_manager.login_message_category = None
    
    oauth.init_app(app)
    
    # Standard Google OAuth registration removed in favor of Firebase Auth

    # Initialize Firebase Admin
    if not firebase_admin._apps:
        # Look for the specific JSON file provided by the user
        cred_path = 'lively-paratext-487716-r8-firebase-adminsdk-fbsvc-8406fdde9d.json'
        if os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
        else:
            # Fallback (maybe env var)
            try:
                firebase_admin.initialize_app()
            except Exception as e:
                print(f"Firebase Init Error: {e}")

    @login_manager.user_loader
    def load_user(user_id):
        return User.get(user_id)
    
    return oauth

def verify_and_login_firebase(id_token):
    """Verify Firebase ID token and login user if valid."""
    try:
        # Step 1: Verify the token
        print(f"[Firebase] Verifying ID token...")
        decoded_token = firebase_auth.verify_id_token(id_token)
        print(f"[Firebase] ✅ Token verified, payload: {decoded_token.get('email')}")
        
        # Extract user info
        email = decoded_token.get('email')
        name = decoded_token.get('name', 'Firebase User')
        picture = decoded_token.get('picture')
        
        if not email:
            print(f"[Firebase] ❌ No email in token")
            return None
        
        # Step 2: Create or update user in database
        print(f"[Firebase] Creating/updating user in database: {email}")
        user = User.create_or_update(email, name, picture)
        
        if user:
            print(f"[Firebase] ✅ User created/updated: {user.email} (id={user.id}, admin={user.is_admin})")
            login_user(user)
            return user
        else:
            print(f"[Firebase] ❌ Failed to create/update user in database")
            return None
            
    except Exception as e:
        print(f"[Firebase] ❌ Token Verification Error: {type(e).__name__}: {e}")
        import traceback
        print(traceback.format_exc())
        return None

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
        self.id = id
        self.email = email
        self.name = name
        self.picture = picture
        self.is_admin = is_admin
        self.created_at = created_at
        self.last_login = last_login
    
    @staticmethod
    def get(user_id):
        """Get user by ID from database."""
        db = get_database()
        with db.get_connection() as conn:
            cursor = conn.cursor()
            # Prepare SQL for safe execution
            sql = "SELECT * FROM users WHERE id = ?"
            cursor.execute(db.prepare_sql(sql), (user_id,))
            if db.db_type == 'sqlite':
                row = cursor.fetchone()
            else:
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
        """Get user by email from database."""
        db = get_database()
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
        """Create or update user in database."""
        # Use Python boolean for Postgres compatibility (psycopg2 adapts True -> true/1, False -> false/0)
        is_admin = (email.lower() == ADMIN_EMAIL.lower())
        now = datetime.now().isoformat()
        
        db = get_database()
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if user exists
                sql = "SELECT id FROM users WHERE email = ?"
                cursor.execute(db.prepare_sql(sql), (email,))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing user
                    update_sql = """
                        UPDATE users 
                        SET name = ?, picture = ?, is_admin = ?, last_login = ?
                        WHERE email = ?
                    """
                    cursor.execute(db.prepare_sql(update_sql), (name, picture, is_admin, now, email))
                    # Handle return value based on db type/cursor
                    if isinstance(existing, dict): # MySQL/Postgres (RealDictCursor)
                        user_id = existing['id']
                    else: # SQLite
                        user_id = existing['id'] 
                else:
                    # Create new user
                    insert_sql = """
                        INSERT INTO users (email, name, picture, is_admin, created_at, last_login)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """
                    prepared_sql = db.prepare_sql(insert_sql)
                    
                    if db.db_type == 'postgres':
                        cursor.execute(prepared_sql + " RETURNING id", (email, name, picture, is_admin, now, now))
                        row = cursor.fetchone()
                        user_id = row['id'] if row else None
                    else:
                        cursor.execute(prepared_sql, (email, name, picture, is_admin, now, now))
                        user_id = cursor.lastrowid
            
            if user_id:
                return User.get(user_id)
            return None
            
        except Exception as e:
            print(f"Auth Error (create_or_update): {e}")
            import traceback
            traceback.print_exc()
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
    
    # Register Google OAuth
    oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )
    
    @login_manager.user_loader
    def load_user(user_id):
        return User.get(user_id)
    
    return oauth

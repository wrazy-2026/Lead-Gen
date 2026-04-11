import re

with open('auth.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_user_model = """class User(UserMixin):
    \"\"\"User model for Flask-Login.\"\"\"
    
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
        return firestore.client().collection('users')

    @staticmethod
    def get(user_id):
        \"\"\"Get user by ID from database.\"\"\"
        try:
            doc = User._get_users_ref().document(str(user_id)).get()
            if doc.exists:
                row = doc.to_dict()
                return User(
                    id=doc.id,
                    email=row.get('email'),
                    name=row.get('name'),
                    picture=row.get('picture'),
                    is_admin=bool(row.get('is_admin')),
                    created_at=row.get('created_at'),
                    last_login=row.get('last_login')
                )
        except Exception as e:
            print(f\"Auth Error (get): {e}\")
        return None

    @staticmethod
    def get_by_email(email):
        \"\"\"Get user by email from database.\"\"\"
        try:
            docs = User._get_users_ref().where('email', '==', email).limit(1).stream()
            for doc in docs:
                row = doc.to_dict()
                return User(
                    id=doc.id,
                    email=row.get('email'),
                    name=row.get('name'),
                    picture=row.get('picture'),
                    is_admin=bool(row.get('is_admin')),
                    created_at=row.get('created_at'),
                    last_login=row.get('last_login')
                )
        except Exception as e:
            print(f\"Auth Error (get_by_email): {e}\")
        return None
    
    @staticmethod
    def create_or_update(email, name, picture=None):
        \"\"\"Create or update user in database.\"\"\"
        is_admin = (email.lower() == ADMIN_EMAIL.lower())
        from datetime import datetime
        now = datetime.now().isoformat()
        
        try:
            ref = User._get_users_ref()
            docs = list(ref.where('email', '==', email).limit(1).stream())
            
            if docs:
                doc_id = docs[0].id
                ref.document(doc_id).set({
                    'name': name,
                    'picture': picture,
                    'is_admin': is_admin,
                    'last_login': now
                }, merge=True)
                return User.get(doc_id)
            else:
                import re
                doc_id = re.sub(r'[^a-zA-Z0-9_-]', '', email.replace('@', '_').replace('.', '_'))
                if not doc_id:
                    doc_id = f\"user_{int(datetime.now().timestamp() * 1000)}\"
                    
                ref.document(doc_id).set({
                    'email': email,
                    'name': name,
                    'picture': picture,
                    'is_admin': is_admin,
                    'created_at': now,
                    'last_login': now
                })
                return User.get(doc_id)
                
        except Exception as e:
            print(f\"Auth Error (create_or_update): {e}\")
            return None
    
    @staticmethod
    def get_all_users():
        \"\"\"Get all users from database.\"\"\"
        try:
            docs = User._get_users_ref().order_by('created_at', direction=\"DESCENDING\").stream()
            users = []
            for doc in docs:
                row = doc.to_dict()
                users.append(User(
                    id=doc.id,
                    email=row.get('email'),
                    name=row.get('name'),
                    picture=row.get('picture'),
                    is_admin=bool(row.get('is_admin')),
                    created_at=row.get('created_at'),
                    last_login=row.get('last_login')
                ))
            return users
        except Exception as e:
            print(f\"Auth Error (get_all_users): {e}\")
            return []
"""

text = re.sub(r'class User\(UserMixin\):.*?(?=\n# ============================================================================\n# AUTH DECORATORS)', new_user_model, text, flags=re.DOTALL)

with open('auth.py', 'w', encoding='utf-8') as f:
    f.write(text)

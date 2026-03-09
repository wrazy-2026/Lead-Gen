import firebase_admin
from firebase_admin import credentials
import os
import logging

logger = logging.getLogger(__name__)

def initialize_firebase():
    """Centralized Firebase initialization."""
    if firebase_admin._apps:
        # Already initialized
        return firebase_admin.get_app()
        
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    target_project_id = os.environ.get("FIREBASE_PROJECT_ID", "lively-paratext-487716-r8").strip() or "lively-paratext-487716-r8"
    
    # Improved discovery
    cred_path = None
    for f in os.listdir(base_dir):
        if f.endswith(".json") and ("firebase-adminsdk" in f or "lively-paratext" in f):
            cred_path = os.path.join(base_dir, f)
            break
            
    if cred_path:
        logger.info("Initializing Firebase with service account: %s", cred_path)
        cred = credentials.Certificate(cred_path)
        return firebase_admin.initialize_app(cred, options={'projectId': target_project_id})
    else:
        logger.warning(f"Firebase service account file not found; using ADC fallback for project {target_project_id}")
        # Default ADC
        return firebase_admin.initialize_app(options={'projectId': target_project_id})

if __name__ == "__main__":
    # Test
    app = initialize_firebase()
    from firebase_admin import firestore
    db = firestore.client()
    print(f"Connected to Firestore project: {db.project}")

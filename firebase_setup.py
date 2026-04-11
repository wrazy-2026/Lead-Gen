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
    
    target_project_id = os.environ.get("FIREBASE_PROJECT_ID", "lively-paratext-487716-r8").strip()
    is_cloud_runtime = bool(os.environ.get("K_SERVICE") or os.environ.get("GOOGLE_CLOUD_PROJECT"))

    # In Cloud Run/GCP, prefer ADC (Application Default Credentials).
    # Only provide projectId if explicitly set via env var, otherwise let ADC auto-detect it.
    if is_cloud_runtime:
        if target_project_id:
            logger.info("Initializing Firebase with ADC for project: %s", target_project_id)
            return firebase_admin.initialize_app(options={'projectId': target_project_id})
        else:
            logger.info("Initializing Firebase with ADC (auto-detect project)")
            return firebase_admin.initialize_app()
    
    # Improved discovery for local development
    cred_path = None
    for f in os.listdir(base_dir):
        if f.endswith(".json") and ("firebase-adminsdk" in f or "lively-paratext" in f):
            cred_path = os.path.join(base_dir, f)
            break
            
    if cred_path:
        logger.info("Initializing Firebase with service account: %s", cred_path)
        cred = credentials.Certificate(cred_path)
        options = {'projectId': target_project_id} if target_project_id else {}
        return firebase_admin.initialize_app(cred, options=options)
    else:
        # Default ADC fallback for local
        if target_project_id:
            logger.warning(f"Firebase service account file not found; using ADC fallback for project {target_project_id}")
            return firebase_admin.initialize_app(options={'projectId': target_project_id})
        else:
            logger.warning("Firebase service account file not found; using pure ADC fallback")
            return firebase_admin.initialize_app()

if __name__ == "__main__":
    # Test
    app = initialize_firebase()
    from firebase_admin import firestore
    db = firestore.client()
    print(f"Connected to Firestore project: {db.project}")

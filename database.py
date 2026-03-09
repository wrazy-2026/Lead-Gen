"""
Firebase Database Module for Business Leads
Replaces former SQLite/PostgreSQL database implementation.
"""
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
import pandas as pd
from datetime import datetime, timedelta
import os
import logging
from typing import List, Optional, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from functools import wraps

from scrapers.base_scraper import BusinessRecord

logger = logging.getLogger(__name__)

# Global executor for database tasks to prevent thread leakage
_db_executor = ThreadPoolExecutor(max_workers=20)

def prevent_hang(timeout_sec=5.0, default_return=None):
    """Decorator to prevent database methods from hanging forever."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                # Use global executor to avoid 'with' block hanging on exit
                future = _db_executor.submit(func, self, *args, **kwargs)
                return future.result(timeout=timeout_sec)
            except TimeoutError:
                logger.error(f"Timeout ({timeout_sec}s) executing {func.__name__} (likely Quota Exceeded)")
                return default_return
            except Exception as e:
                logger.error(f"Error executing {func.__name__}: {e}")
                return default_return
        return wrapper
    return decorator

class Database:

    def __init__(self, db_path: str = None):
        self.db_path = db_path
        self.db_type = 'firestore'
        self.db_url = 'Google Cloud Firestore'
        self._init_firebase()

    def _init_firebase(self):
        from firebase_setup import initialize_firebase
        initialize_firebase()
        self.db = firestore.client()
        logger.info(f"Firestore client initialized for project: {self.db.project}")
        self.leads_ref = self.db.collection('leads')

    def save_records(self, records: List[BusinessRecord]) -> Tuple[int, int, List[str]]:
        import re
        if not records:
            return 0, 0, []
            
        inserted = 0
        duplicates = 0
        inserted_ids = []
        
        INVALID_VALUES = {
            'business_name', 'filing_date', 'state', 'status', 'url',
            'entity_type', 'filing_number', 'registered_agent', 'address',
            'phone', 'email', 'owner_name', 'name', 'company_name',
            'placeholder', 'test', 'demo', 'sample', 'example',
            'unknown', 'undefined', 'null', 'none', 'n/a',
        }
        
        # 1. Pre-calculate doc IDs and filter invalid records
        to_process = []
        for record in records:
            record_dict = record.to_dict()
            business_name = (record_dict.get("business_name") or "").strip().lower()
            state_val = (record_dict.get("state") or "").strip().lower()
            
            if business_name in INVALID_VALUES or state_val in INVALID_VALUES or not business_name or len(business_name) < 2:
                duplicates += 1
                continue
            
            # Generate ID (stable)
            cik = record_dict.get("cik")
            if cik:
                doc_id = f"cik_{cik}"
            else:
                name_clean = re.sub(r'[^a-z0-9]', '', business_name)
                doc_id = f"{state_val}_{name_clean}"
            
            to_process.append((doc_id, record_dict))
            
        if not to_process:
            return 0, duplicates, []

        # 2. Check for existence in batches (Firestore 'in' limit is 30)
        existing_ids = set()
        for i in range(0, len(to_process), 30):
            chunk = [x[0] for x in to_process[i:i+30]]
            try:
                # Optimized way: get just the snapshots to see if they exist
                docs = self.db.get_all([self.leads_ref.document(did) for did in chunk], timeout=5.0)
                for doc in docs:
                    if doc.exists:
                        existing_ids.add(doc.id)
            except Exception as e:
                logger.error(f"Batch check error: {e}")
        
        # 3. Batch insert new records (Firestore batch limit is 500)
        batch = self.db.batch()
        batch_count = 0
        
        for doc_id, record_dict in to_process:
            if doc_id in existing_ids:
                # Keep existing lead but merge newly discovered metadata (SEC/TIN/SIC/etc.).
                try:
                    merge_data = {
                        "ein": record_dict.get("ein"),
                        "tin": record_dict.get("ein") or record_dict.get("tin"),
                        "cik": record_dict.get("cik"),
                        "sic_code": record_dict.get("sic_code"),
                        "industry_category": record_dict.get("industry_category"),
                        "fiscal_year_end": record_dict.get("fiscal_year_end"),
                        "state_of_incorporation": record_dict.get("state_of_incorporation"),
                        "sec_file_number": record_dict.get("sec_file_number"),
                        "film_number": record_dict.get("film_number"),
                        "sec_act": record_dict.get("sec_act"),
                        "cf_office": record_dict.get("cf_office"),
                        "business_address": record_dict.get("business_address"),
                        "business_phone": record_dict.get("business_phone"),
                        "mailing_address": record_dict.get("mailing_address"),
                    }
                    merge_data = {k: v for k, v in merge_data.items() if v not in (None, "")}
                    if merge_data:
                        self.leads_ref.document(doc_id).set(merge_data, merge=True)
                except Exception as merge_error:
                    logger.error(f"Error merging duplicate metadata for {doc_id}: {merge_error}")
                duplicates += 1
                continue
                
            fetched_at = record_dict.get("fetched_at") or datetime.now().isoformat()
            
            data = {
                "id": doc_id,
                "business_name": record_dict.get("business_name"),
                "filing_date": record_dict.get("filing_date"),
                "state": record_dict.get("state"),
                "status": record_dict.get("status"),
                "url": record_dict.get("url"),
                "entity_type": record_dict.get("entity_type"),
                "filing_number": record_dict.get("filing_number"),
                "registered_agent": record_dict.get("registered_agent"),
                "address": record_dict.get("address"),
                "phone": record_dict.get("phone"),
                "email": record_dict.get("email"),
                "owner_name": record_dict.get("owner_name"),
                "ein": record_dict.get("ein"),
                "tin": record_dict.get("ein") or record_dict.get("tin"),
                "cik": record_dict.get("cik"),
                "sic_code": record_dict.get("sic_code"),
                "industry_category": record_dict.get("industry_category"),
                "fiscal_year_end": record_dict.get("fiscal_year_end"),
                "state_of_incorporation": record_dict.get("state_of_incorporation"),
                "sec_file_number": record_dict.get("sec_file_number"),
                "film_number": record_dict.get("film_number"),
                "sec_act": record_dict.get("sec_act"),
                "cf_office": record_dict.get("cf_office"),
                "business_address": record_dict.get("business_address"),
                "business_phone": record_dict.get("business_phone"),
                "mailing_address": record_dict.get("mailing_address"),
                "fetched_at": fetched_at
            }
            
            batch.set(self.leads_ref.document(doc_id), data)
            inserted_ids.append(doc_id)
            batch_count += 1
            inserted += 1
            
            if batch_count >= 450:
                batch.commit()
                batch = self.db.batch()
                batch_count = 0
        
        if batch_count > 0:
            batch.commit()
            
        return inserted, duplicates, inserted_ids

    @prevent_hang(timeout_sec=10.0, default_return=pd.DataFrame())
    def get_all_leads(self, limit: int = 1000, offset: int = 0) -> pd.DataFrame:
        try:
            query = self.leads_ref.order_by("filing_date", direction=firestore.Query.DESCENDING).limit(limit)
            if offset > 0:
                query = query.offset(offset)
            df = self._docs_to_df(query.stream(timeout=10.0))
            if not df.empty:
                return df

            # Fallback for datasets where filing_date is missing/inconsistent.
            # This prevents UI pages from appearing empty when leads exist.
            logger.warning("get_all_leads ordered query returned no rows; trying unordered fallback")
            fallback_query = self.leads_ref.limit(limit)
            if offset > 0:
                fallback_query = fallback_query.offset(offset)
            fallback_df = self._docs_to_df(fallback_query.stream(timeout=10.0))
            if not fallback_df.empty and 'filing_date' in fallback_df.columns:
                fallback_df = fallback_df.sort_values(by='filing_date', ascending=False, kind='stable')
            return fallback_df
        except Exception as e:
            logger.error(f"Error getting all leads: {e}")
            try:
                # Secondary fallback with no ordering constraints.
                fallback_query = self.leads_ref.limit(limit)
                if offset > 0:
                    fallback_query = fallback_query.offset(offset)
                fallback_df = self._docs_to_df(fallback_query.stream(timeout=10.0))
                if not fallback_df.empty and 'filing_date' in fallback_df.columns:
                    fallback_df = fallback_df.sort_values(by='filing_date', ascending=False, kind='stable')
                return fallback_df
            except Exception as inner_e:
                logger.error(f"Fallback get_all_leads failed: {inner_e}")
                return pd.DataFrame()

    def _docs_to_df(self, docs):
        """Converts Firestore documents to a DataFrame."""
        data = []
        for doc in docs:
            doc_dict = doc.to_dict()
            if 'id' not in doc_dict:
                doc_dict['id'] = doc.id
            data.append(doc_dict)
        return pd.DataFrame(data)

    @prevent_hang(timeout_sec=5.0, default_return=0)
    def get_leads_count(self) -> int:
        try:
            aggregation_query = self.leads_ref.count()
            results = aggregation_query.get(timeout=5.0)
            return results[0][0].value
        except Exception as e:
            logger.error(f"Error getting leads count: {e}")
            return 0

    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())
    def get_leads_by_state(self, state: str) -> pd.DataFrame:
        try:
            query = self.leads_ref.where(filter=FieldFilter("state", "==", state.upper())) \
                                  .order_by("filing_date", direction=firestore.Query.DESCENDING).limit(500)
            return self._docs_to_df(query.stream(timeout=8.0))
        except Exception as e:
            logger.error(f"Error getting leads by state: {e}")
            return pd.DataFrame()

    def get_leads_by_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            query = self.leads_ref.where(filter=FieldFilter("filing_date", ">=", start_date)) \
                                  .where(filter=FieldFilter("filing_date", "<=", end_date)) \
                                  .order_by("filing_date", direction=firestore.Query.DESCENDING)
            return self._docs_to_df(query.stream(timeout=8.0))
        except Exception as e:
            logger.error(f"Error getting leads by date range: {e}")
            return pd.DataFrame()

    def get_leads_by_ids(self, lead_ids: list) -> pd.DataFrame:
        if not lead_ids:
            return pd.DataFrame()
        try:
            all_docs = []
            for i in range(0, len(lead_ids), 30):
                chunk = [str(x) for x in lead_ids[i:i+30]]
                if chunk:
                    # Optimized: use get_all instead of where(in) for direct ID lookup
                    doc_refs = [self.leads_ref.document(did) for did in chunk]
                    all_docs.extend([d for d in self.db.get_all(doc_refs, timeout=5.0) if d.exists])
            return self._docs_to_df(all_docs)
        except Exception as e:
            logger.error(f"Error getting leads by ids: {e}")
            return pd.DataFrame()

    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())
    def get_recent_leads(self, days: int = 7) -> pd.DataFrame:
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            query = self.leads_ref.where(filter=FieldFilter("filing_date", ">=", cutoff_date)) \
                                  .order_by("filing_date", direction=firestore.Query.DESCENDING)
            return self._docs_to_df(query.stream(timeout=8.0))
        except Exception as e:
            logger.error(f"Error getting recent leads: {e}")
            return pd.DataFrame()

    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())
    def search_leads(self, search_query: str) -> pd.DataFrame:
        try:
            query = self.leads_ref.order_by("filing_date", direction=firestore.Query.DESCENDING).limit(500)
            df = self._docs_to_df(query.stream(timeout=8.0))
            if df.empty:
                return df
            search_query = search_query.lower()
            return df[df['business_name'].astype(str).str.lower().str.contains(search_query)]
        except Exception as e:
            logger.error(f"Error searching leads: {e}")
            return pd.DataFrame()

    @prevent_hang(timeout_sec=12.0, default_return={'total_leads': 0, 'new_today': 0, 'this_week': 0, 'leads_by_state': [], 'states_count': 0, 'last_fetch': 'Never'})
    def get_stats(self) -> dict:
        """
        Get high-level statistics using optimized Firestore count queries.
        """
        try:
            logger.info("Starting optimized get_stats calculation...")
            total = self.get_leads_count()
            
            # Today's leads (optimized count)
            today_str = datetime.now().strftime("%Y-%m-%d")
            today_query = self.leads_ref.where(filter=FieldFilter("filing_date", "==", today_str)).count()
            new_today = today_query.get(timeout=3.0)[0][0].value
            
            # This week's leads (optimized count)
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            week_query = self.leads_ref.where(filter=FieldFilter("filing_date", ">=", week_ago)).count()
            this_week = week_query.get(timeout=3.0)[0][0].value
            
            # Latest filing and fetch dates
            latest_query = self.leads_ref.order_by("filing_date", direction=firestore.Query.DESCENDING).limit(1)
            latest_docs = list(latest_query.stream(timeout=5.0))
            
            newest_filing = None
            last_fetch = None
            if latest_docs:
                doc_data = latest_docs[0].to_dict()
                newest_filing = doc_data.get('filing_date')
                last_fetch = doc_data.get('fetched_at')

            # Dynamic States Stats (Optimized fetch)
            # Fetch just the 'state' field for up to 10,000 records (very fast)
            sample_query = self.leads_ref.select(['state']).limit(10000)
            sample_df = self._docs_to_df(sample_query.stream(timeout=10.0))
            
            states_stats = []
            if not sample_df.empty and 'state' in sample_df.columns:
                # Group by state to get counts
                counts = sample_df['state'].value_counts()
                total_sample = len(sample_df)
                for state_code, count in counts.items():
                    states_stats.append({
                        'code': str(state_code).upper(),
                        'name': str(state_code).upper(), # Name resolution in app layer
                        'count': int(count),
                        'percentage': round((count / total_sample) * 100, 1)
                    })
            
            unique_states_count = sample_df['state'].nunique() if not sample_df.empty else 0

            return {
                "total_leads": total,
                "new_today": new_today,
                "this_week": this_week,
                "leads_by_state": states_stats,
                "states_count": unique_states_count,
                "newest_filing": newest_filing,
                "last_fetch": last_fetch
            }
        except Exception as e:
            logger.error(f"Error in optimized get_stats: {e}")
            return {
                'total_leads': 0, 
                'new_today': 0, 
                'this_week': 0, 
                'leads_by_state': [], 
                'states_count': 0,
                'newest_filing': None, 
                'last_fetch': 'Never'
            }

    def update_lead_enrichment(self, lead_id: str, data: dict):
        try:
            self.leads_ref.document(str(lead_id)).update(data, timeout=5.0)
            return True
        except Exception as e:
            logger.error(f"Error updating lead enrichment: {e}")
            return False

    def get_enriched_leads(self, limit: int = 1000) -> pd.DataFrame:
        try:
            # We filter for leads that have email or phone
            query = self.leads_ref.where(filter=FieldFilter("email", "!=", None)).limit(limit)
            return self._docs_to_df(query.stream(timeout=10.0))
        except Exception as e:
            logger.error(f"Error getting enriched leads: {e}")
            return pd.DataFrame()

    def get_unenriched_leads(self, limit: int = 100) -> pd.DataFrame:
        """Get leads that are missing website or contact info."""
        try:
            # We sample a larger set to find enough unenriched ones
            query = self.leads_ref.limit(500)
            df = self._docs_to_df(query.stream(timeout=10.0))
            if df.empty: return df
            
            # Check 'website' column, but also 'url' as SEC leads often have 'url' instead of 'website' initially
            website_col = df['website'] if 'website' in df.columns else pd.Series([None]*len(df))
            url_col = df['url'] if 'url' in df.columns else pd.Series([None]*len(df))
            
            # Unenriched if website is missing/generic OR if it's an SEC/generic URL
            # Generic collector sites we want to overwrite:
            generic_sites = ['sunbiz.org', 'sec.gov', 'dos.ny.gov', 'delaware.gov']
            
            mask = website_col.isna() | (website_col == '') | (website_col == 'Not Found')
            
            # Also count it as unenriched if the only URL we have is a generic state registry link
            is_generic_url = url_col.str.lower().apply(lambda x: any(gs in str(x) for gs in generic_sites) if x else True)
            
            # Final mask: (No website) AND (No valid URL or Generic URL)
            final_mask = mask & is_generic_url
            
            return df[final_mask].head(limit)
        except Exception as e:
            logger.error(f"Error getting unenriched leads: {e}")
            return pd.DataFrame()

    def clear_old_leads(self, days: int = 30) -> int:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        query = self.leads_ref.where(filter=FieldFilter("filing_date", "<", cutoff_date))
        batch = self.db.batch()
        deleted = 0
        try:
            for doc in query.stream(timeout=15.0):
                batch.delete(doc.reference)
                deleted += 1
                if deleted % 450 == 0:
                    batch.commit()
                    batch = self.db.batch()
            if deleted % 450 != 0:
                batch.commit()
        except Exception as e:
            logger.error(f"Error deleting old leads: {e}")
        return deleted

    def clear_all_leads(self) -> int:
        deleted = 0
        try:
            while True:
                docs = list(self.leads_ref.limit(500).stream(timeout=15.0))
                if not docs:
                    break
                batch = self.db.batch()
                for doc in docs:
                    batch.delete(doc.reference)
                    deleted += 1
                batch.commit()
        except Exception as e:
            logger.error(f"Error clearing all leads: {e}")
        return deleted

    def cleanup_placeholder_leads(self) -> int:
        return 0

    @prevent_hang(timeout_sec=3.0, default_return=None)
    def get_setting(self, key: str, default=None) -> any:
        try:
            doc = self.db.collection('settings').document(key).get(timeout=3.0)
            if doc.exists:
                return doc.to_dict().get('value', default)
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}")
        return default

    @prevent_hang(timeout_sec=3.0, default_return=False)
    def save_setting(self, key: str, value: any) -> bool:
        try:
            self.db.collection('settings').document(key).set({
                'value': value,
                'updated_at': datetime.now().isoformat()
            }, timeout=3.0)
            return True
        except Exception as e:
            logger.error(f"Error saving setting {key}: {e}")
            return False

    def delete_leads(self, lead_ids: List[str]) -> int:
        if not lead_ids:
            return 0
        count = 0
        try:
            for i in range(0, len(lead_ids), 500):
                batch = self.db.batch()
                chunk = lead_ids[i:i+500]
                for lid in chunk:
                    batch.delete(self.leads_ref.document(str(lid)))
                    count += 1
                batch.commit()
            return count
        except Exception as e:
            logger.error(f"Error deleting leads: {e}")
            return count

    @prevent_hang(timeout_sec=20.0, default_return=[])
    def find_duplicate_ids(self, limit: int = 5000) -> List[str]:
        """Return IDs of duplicate leads based on business_name + state."""
        try:
            docs = self.leads_ref.select(['business_name', 'state']).limit(limit).stream(timeout=20.0)
            rows = []
            for doc in docs:
                rec = doc.to_dict() or {}
                rows.append({
                    'id': doc.id,
                    'business_name': (rec.get('business_name') or '').strip().lower(),
                    'state': (rec.get('state') or '').strip().upper()
                })

            if not rows:
                return []

            df = pd.DataFrame(rows)
            # Ignore rows without enough identity info.
            df = df[(df['business_name'] != '') & (df['state'] != '')]
            if df.empty:
                return []

            dup_df = df[df.duplicated(subset=['business_name', 'state'], keep='first')]
            return dup_df['id'].astype(str).tolist()
        except Exception as e:
            logger.error(f"Error finding duplicate ids: {e}")
            return []

_db_instance: Optional[Database] = None

def get_database(db_path: str = None) -> Database:
    global _db_instance
    if _db_instance is None:
        _db_instance = Database(db_path)
    return _db_instance

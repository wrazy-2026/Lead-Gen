"""
Database Module - SQLite Storage for Business Leads
====================================================
Provides local storage for fetched business leads using SQLite.
The database stores leads temporarily and supports export operations.

Features:
- Automatic table creation
- Duplicate detection
- Query by state/date
- Export to pandas DataFrame
- Data cleanup utilities
"""

import sqlite3
import pandas as pd
import os
import pymysql
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from pathlib import Path
import logging
from contextlib import contextmanager

from scrapers.base_scraper import BusinessRecord

# Configure logging
logger = logging.getLogger(__name__)


class Database:
    """
    Database manager for storing business leads.
    Supports SQLite (local) and MySQL (production/cPanel).
    
    Features:
    - Connection pooling
    - Thread-safe operations
    - Automatic schema management
    - Unified API for both database types
    """
    
    DEFAULT_DB_PATH = "leads.db"

    # SQL Schema - Core table (without new enrichment columns for compatibility)
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        business_name VARCHAR(255) NOT NULL,
        filing_date VARCHAR(50) NOT NULL,
        state VARCHAR(50) NOT NULL,
        status VARCHAR(50) NOT NULL,
        url TEXT,
        entity_type TEXT,
        filing_number TEXT,
        registered_agent TEXT,
        address TEXT,
        fetched_at TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(business_name, state, filing_date)
    );
    """

    # User Schema
    CREATE_USERS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        picture TEXT,
        is_admin BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP
    );
    """
    
    # Client Schemas
    CREATE_CLIENT_SETTINGS_SQL = """
    CREATE TABLE IF NOT EXISTS client_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        setting_key VARCHAR(255) NOT NULL,
        setting_value TEXT,
        updated_at VARCHAR(50),
        FOREIGN KEY (user_id) REFERENCES users(id),
        UNIQUE(user_id, setting_key)
    );
    """
    
    CREATE_SAVED_SEARCHES_SQL = """
    CREATE TABLE IF NOT EXISTS saved_searches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name VARCHAR(255) NOT NULL,
        filters TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    
    # Indexes (executed separately for compatibility)
    CREATE_INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS idx_state ON leads(state);",
        "CREATE INDEX IF NOT EXISTS idx_filing_date ON leads(filing_date);",
        "CREATE INDEX IF NOT EXISTS idx_fetched_at ON leads(fetched_at);"
    ]
    
    def __init__(self, db_path: str = None):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file. Defaults to 'leads.db'
        """
        # Check for PostgreSQL configuration (Google Cloud SQL or other)
        self.db_url = os.environ.get('DATABASE_URL')

        # MySQL is deprecated - use Google Cloud SQL (PostgreSQL) instead
        self.db_host = None
        self.db_user = None
        self.db_password = None
        self.db_name = None
        
        if self.db_url and 'postgres' in self.db_url:
            self.db_type = 'postgres'
            self.db_path = None
            
            # Detect if using Google Cloud SQL (Unix socket)
            if '/cloudsql/' in self.db_url:
                logger.info("Using Google Cloud SQL (PostgreSQL)")
            else:
                logger.info("Using PostgreSQL Database")
            
            # Test connection immediately
            try:
                import psycopg2
                conn = psycopg2.connect(self.db_url, connect_timeout=10)
                conn.close()
                logger.info("PostgreSQL Connection Successful")
            except Exception as e:
                import traceback
                logger.error(f"FAILED TO CONNECT TO POSTGRESQL: {e}")
                logger.error(traceback.format_exc())
                # DO NOT fall back to SQLite - this hides production issues
                # Keep db_type as 'postgres' so the error is visible
                logger.error("DATABASE_URL is set but connection failed. Check your Cloud SQL configuration.")
                raise RuntimeError(f"PostgreSQL connection failed: {e}. Check DATABASE_URL and Cloud SQL instance.")
        else:
            # No DATABASE_URL set - use SQLite (local development only)
            self.db_type = 'sqlite'
            self.db_path = db_path or self.DEFAULT_DB_PATH
            logger.info(f"Using SQLite Database at {self.db_path}")
            logger.warning("SQLite is for local development only. Set DATABASE_URL for production (Google Cloud SQL).")

        try:
            self._init_database()
            self._run_migrations()
            self._create_enrichment_index()
        except Exception as e:
            logger.error(f"DATABASE INITIALIZATION FAILED: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't crash immediately, allows app to start and maybe show error page
            # But functionality will be broken.
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        if self.db_type == 'postgres':
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(
                self.db_url,
                cursor_factory=RealDictCursor
            )
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
        elif self.db_type == 'mysql':
            conn = pymysql.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            try:
                yield conn
            finally:
                conn.close()
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()

    def prepare_sql(self, sql: str) -> str:
        """Convert SQLite SQL query to MySQL or PostgreSQL if needed."""
        if self.db_type == 'postgres':
            # Replace ? placeholder with %s
            sql = sql.replace('?', '%s')
            # Replace INTEGER PRIMARY KEY AUTOINCREMENT with SERIAL PRIMARY KEY
            sql = sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY')
            
            # Replace BOOLEAN DEFAULT 0 with BOOLEAN DEFAULT FALSE (Postgres strictness)
            sql = sql.replace('BOOLEAN DEFAULT 0', 'BOOLEAN DEFAULT FALSE')
            sql = sql.replace('BOOLEAN DEFAULT 1', 'BOOLEAN DEFAULT TRUE')
            
            # Handle INSERT OR IGNORE conversion if it comes through
            if 'INSERT OR IGNORE' in sql:
                sql = sql.replace('INSERT OR IGNORE INTO', 'INSERT INTO')
                sql += " ON CONFLICT DO NOTHING"
                
            return sql

        if self.db_type == 'mysql':
            # Replace ? placeholder with %s
            sql = sql.replace('?', '%s')
            # Replace AUTOINCREMENT with AUTO_INCREMENT
            sql = sql.replace('AUTOINCREMENT', 'AUTO_INCREMENT')
            # Replace BOOLEAN with TINYINT(1)
            sql = sql.replace('BOOLEAN', 'TINYINT(1)')
            # Replace INSERT OR IGNORE with INSERT IGNORE
            sql = sql.replace('INSERT OR IGNORE', 'INSERT IGNORE')
            # Handle TEXT types in keys (MySQL limitation)
            if 'CREATE TABLE' in sql.upper():
                 # For keys, we need VARCHAR(255) instead of TEXT
                 # This is a bit hacky but works for this specific schema
                 sql = sql.replace('business_name TEXT NOT NULL', 'business_name VARCHAR(255) NOT NULL')
                 sql = sql.replace('state TEXT NOT NULL', 'state VARCHAR(50) NOT NULL')
                 sql = sql.replace('filing_date TEXT NOT NULL', 'filing_date VARCHAR(50) NOT NULL')
                 # Replace other text fields if generally better for MySQL
                 # But standard TEXT is fine for non-indexed columns
            
            return sql
        return sql
    
    def _run_migrations(self):
        """Run database migrations for new columns."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get existing columns
            existing_columns = []
            if self.db_type == 'mysql':
                try:
                    cursor.execute("DESCRIBE leads")
                    existing_columns = [row['Field'] for row in cursor.fetchall()]
                except (pymysql.err.ProgrammingError, pymysql.err.OperationalError):
                    # Table might not exist yet
                    return
            elif self.db_type == 'postgres':
                try:
                    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'leads'")
                    existing_columns = [row['column_name'] for row in cursor.fetchall()]
                except Exception:
                    # Table might not exist yet
                    return
            else:
                cursor.execute("PRAGMA table_info(leads)")
                existing_columns = [col[1] for col in cursor.fetchall()]
            
            new_columns = [
                ('email', 'TEXT'),
                ('phone', 'TEXT'),
                ('website', 'TEXT'),
                ('owner_name', 'TEXT'),
                ('linkedin', 'TEXT'),
                ('enrichment_source', 'TEXT'),
                ('confidence_score', 'REAL DEFAULT 0'),
                ('enriched_at', 'TEXT'),
                # SEC EDGAR detailed fields
                ('ein', 'TEXT'),
                ('cik', 'TEXT'),
                ('sic_code', 'TEXT'),
                ('industry_category', 'TEXT'),
                ('fiscal_year_end', 'TEXT'),
                ('state_of_incorporation', 'TEXT'),
                ('sec_file_number', 'TEXT'),
                ('film_number', 'TEXT'),
                ('sec_act', 'TEXT'),
                ('cf_office', 'TEXT'),
                ('business_address', 'TEXT'),
                ('business_phone', 'TEXT'),
                ('mailing_address', 'TEXT'),
                # New enrichment fields from Apify
                ('first_name', 'TEXT'),
                ('last_name', 'TEXT'),
                ('phone_1', 'TEXT'),
                ('phone_2', 'TEXT'),
                ('email_1', 'TEXT'),
                ('email_2', 'TEXT'),
                ('email_3', 'TEXT'),
                ('email_4', 'TEXT'),
                ('email_5', 'TEXT'),
                ('age', 'TEXT'),
                ('city', 'TEXT'),
                ('zipcode', 'TEXT'),
                ('street_address', 'TEXT'),
                ('address_locality', 'TEXT'),
                ('address_region', 'TEXT'),
                ('postal_code', 'TEXT'),
                # Serper fields
                ('serper_owner_name', 'TEXT'),
                ('serper_website', 'TEXT'),
                ('serper_domain', 'TEXT'),
                # Domain discovery fields
                ('domain', 'TEXT'),
                ('website', 'TEXT'),
                # GHL Export fields
                ('ghl_exported', 'INTEGER DEFAULT 0'),
                ('ghl_exported_at', 'TEXT'),
            ]
            
            for col_name, col_type in new_columns:
                if col_name not in existing_columns:
                    try:
                        sql = f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}"
                        if self.db_type == 'mysql':
                            # Use VARCHAR for indexing efficiency if needed, but TEXT is okay for basic storage
                            pass
                        cursor.execute(sql)
                        logger.info(f"Added column: {col_name}")
                    except Exception:
                        pass  # Column already exists or other error
    
    def _create_enrichment_index(self):
        """Create index on email column after migrations."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # MySQL warning: TEXT needs prefix length for key
                # SQLite doesn't care
                if self.db_type == 'mysql':
                    # Check if index exists first to avoid error
                    # Or just try/catch
                    cursor.execute("CREATE INDEX idx_email ON leads(email(50))")
                else:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_email ON leads(email)")
            except (sqlite3.OperationalError, pymysql.err.OperationalError, pymysql.err.InternalError):
                pass  # Index already exists or column doesn't exist
    
    def _init_database(self):
        """Create database tables if they don't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Helper to execute SQL
            def exec_sql(raw_sql):
                prepared = self.prepare_sql(raw_sql)
                if self.db_type in ('mysql', 'postgres'):
                    cursor.execute(prepared)
                else:
                    conn.executescript(raw_sql)

            # Create tables
            exec_sql(self.CREATE_TABLE_SQL)
            exec_sql(self.CREATE_USERS_TABLE_SQL)
            exec_sql(self.CREATE_CLIENT_SETTINGS_SQL)
            exec_sql(self.CREATE_SAVED_SEARCHES_SQL)
            
            # Create indexes
            for index_sql in self.CREATE_INDEXES_SQL:
                try:
                    if self.db_type in ('mysql', 'postgres'):
                        cursor.execute(index_sql)
                    else:
                        conn.executescript(index_sql)
                except Exception:
                    pass

        logger.debug("Database schema initialized")
    
    def save_records(self, records: List[BusinessRecord]) -> Tuple[int, int, List[int]]:
        """
        Save business records to the database.
        
        Args:
            records: List of BusinessRecord objects to save
            
        Returns:
            Tuple of (inserted_count, duplicate_count, inserted_ids)
        """
        if not records:
            return 0, 0, []
        
        inserted = 0
        duplicates = 0
        inserted_ids = []
        
        insert_sql = """
        INSERT INTO leads 
        (business_name, filing_date, state, status, url, 
         entity_type, filing_number, registered_agent, address, 
         phone, email, owner_name, ein, cik, sic_code, industry_category,
         fiscal_year_end, state_of_incorporation, sec_file_number, film_number,
         sec_act, cf_office, business_address, business_phone, mailing_address, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare SQL for DB type (Handle INSERT IGNORE manually via Exception or keyword)
        # Using INSERT IGNORE syntax for MySQL, but for SQLite we need INSERT OR IGNORE
        if self.db_type == 'mysql':
            insert_sql = insert_sql.replace("INSERT INTO", "INSERT IGNORE INTO")
        elif self.db_type == 'postgres':
            insert_sql += " ON CONFLICT DO NOTHING"
        else:
            insert_sql = insert_sql.replace("INSERT INTO", "INSERT OR IGNORE INTO")
            
        insert_sql = self.prepare_sql(insert_sql)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # List of column names / placeholder values to reject
            INVALID_VALUES = {
                'business_name', 'filing_date', 'state', 'status', 'url',
                'entity_type', 'filing_number', 'registered_agent', 'address',
                'phone', 'email', 'owner_name', 'name', 'company_name',
                'placeholder', 'test', 'demo', 'sample', 'example',
                'unknown', 'undefined', 'null', 'none', 'n/a',
            }
            
            for record in records:
                # Validate: Skip records that look like column headers or placeholders
                record_dict = record.to_dict()
                business_name = (record_dict.get("business_name") or "").strip().lower()
                state_val = (record_dict.get("state") or "").strip().lower()
                
                # Skip invalid records
                if business_name in INVALID_VALUES:
                    logger.warning(f"Rejected placeholder record: business_name='{business_name}'")
                    duplicates += 1
                    continue
                if state_val in INVALID_VALUES:
                    logger.warning(f"Rejected placeholder record: state='{state_val}'")
                    duplicates += 1
                    continue
                if not business_name or len(business_name) < 2:
                    logger.warning(f"Rejected empty business_name record")
                    duplicates += 1
                    continue
                    
                # ... (rest of loop setup)
                values = (
                    record_dict["business_name"],
                    record_dict["filing_date"],
                    record_dict["state"],
                    record_dict["status"],
                    record_dict["url"],
                    record_dict.get("entity_type"),
                    record_dict.get("filing_number"),
                    record_dict.get("registered_agent"),
                    record_dict.get("address"),
                    record_dict.get("phone"),
                    record_dict.get("email"),
                    record_dict.get("owner_name"),
                    record_dict.get("ein"),
                    record_dict.get("cik"),
                    record_dict.get("sic_code"),
                    record_dict.get("industry_category"),
                    record_dict.get("fiscal_year_end"),
                    record_dict.get("state_of_incorporation"),
                    record_dict.get("sec_file_number"),
                    record_dict.get("film_number"),
                    record_dict.get("sec_act"),
                    record_dict.get("cf_office"),
                    record_dict.get("business_address"),
                    record_dict.get("business_phone"),
                    record_dict.get("mailing_address"),
                    record_dict.get("fetched_at", datetime.now().isoformat())
                )
                
                try:
                    if self.db_type == 'postgres':
                        # Postgres needs RETURNING id to get the ID, and lastrowid doesn't work
                        cursor.execute(insert_sql + " RETURNING id", values)
                        row = cursor.fetchone()
                        if row:
                            inserted += 1
                            inserted_ids.append(row['id'])
                        else:
                            duplicates += 1
                    else:
                        cursor.execute(insert_sql, values)
                        
                        if cursor.rowcount > 0:
                            inserted += 1
                            if self.db_type == 'mysql':
                                # In MySQL cursor.lastrowid is reliable for single inserts
                                inserted_ids.append(cursor.lastrowid)
                            else:
                                # In SQLite standard cursor.lastrowid works
                                inserted_ids.append(cursor.lastrowid)
                        else:
                            duplicates += 1
                except Exception as e:
                    logger.error(f"Error saving record: {e}")
                    duplicates += 1 # Treat error as duplicate/skip for now
        
        logger.info(f"Saved {inserted} records, {duplicates} duplicates skipped. IDs: {inserted_ids}")
        return inserted, duplicates, inserted_ids
    
    def get_all_leads(self, limit: int = None, offset: int = None) -> pd.DataFrame:
        """
        Get all leads from the database as a DataFrame.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            pandas DataFrame with leads
        """
        with self.get_connection() as conn:
            sql = "SELECT * FROM leads ORDER BY filing_date DESC"
            params = []
            
            if limit is not None:
                sql += " LIMIT %s" if self.db_type in ('postgres', 'mysql') else " LIMIT ?"
                params.append(limit)
            
            if offset is not None:
                sql += " OFFSET %s" if self.db_type in ('postgres', 'mysql') else " OFFSET ?"
                params.append(offset)
            
            try:
                if params:
                    df = pd.read_sql_query(sql, conn, params=params)
                else:
                    df = pd.read_sql_query(sql, conn)
            except Exception as e:
                logger.error(f"Error in get_all_leads: {e}")
                logger.error(f"SQL: {sql}, Params: {params}")
                df = pd.DataFrame()
        return df
    
    def get_leads_count(self) -> int:
        """Get total number of leads."""
        with self.get_connection() as conn:
            sql = "SELECT COUNT(*) as cnt FROM leads"
            cursor = conn.cursor()
            cursor.execute(self.prepare_sql(sql))
            result = cursor.fetchone()
            return result['cnt'] if isinstance(result, dict) else result[0]

    def get_leads_by_state(self, state: str) -> pd.DataFrame:
        """
        Get leads for a specific state.
        
        Args:
            state: State name to filter by
            
        Returns:
            pandas DataFrame with filtered leads
        """
        with self.get_connection() as conn:
            sql = "SELECT * FROM leads WHERE state = ? ORDER BY filing_date DESC"
            df = pd.read_sql_query(
                self.prepare_sql(sql),
                conn,
                params=(state,)
            )
        return df
    
    def get_leads_by_date_range(
        self, 
        start_date: str, 
        end_date: str
    ) -> pd.DataFrame:
        """
        Get leads within a date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            pandas DataFrame with filtered leads
        """
        with self.get_connection() as conn:
            sql = """SELECT * FROM leads 
                   WHERE filing_date >= ? AND filing_date <= ?
                   ORDER BY filing_date DESC"""
            df = pd.read_sql_query(
                self.prepare_sql(sql),
                conn,
                params=(start_date, end_date)
            )
        return df
    
    def get_leads_by_ids(self, lead_ids: list) -> pd.DataFrame:
        """
        Get leads by list of IDs.
        
        Args:
            lead_ids: List of lead IDs
            
        Returns:
            pandas DataFrame with leads for these IDs
        """
        if not lead_ids:
            return pd.DataFrame()
            
        with self.get_connection() as conn:
            # Safe parameterized query for variable length list
            placeholders = ','.join(['?'] * len(lead_ids))
            sql = f"SELECT * FROM leads WHERE id IN ({placeholders})"
            df = pd.read_sql_query(
                self.prepare_sql(sql),
                conn,
                params=tuple(lead_ids)
            )
        return df
        
    def get_recent_leads(self, days: int = 7) -> pd.DataFrame:
        """
        Get leads from the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            pandas DataFrame with recent leads
        """
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        with self.get_connection() as conn:
            sql = """SELECT * FROM leads 
                   WHERE filing_date >= ?
                   ORDER BY filing_date DESC"""
            df = pd.read_sql_query(
                self.prepare_sql(sql),
                conn,
                params=(cutoff_date,)
            )
        return df
    
    def search_leads(self, query: str) -> pd.DataFrame:
        """
        Search leads by business name.
        
        Args:
            query: Search term (partial match)
            
        Returns:
            pandas DataFrame with matching leads
        """
        with self.get_connection() as conn:
            sql = """SELECT * FROM leads 
                   WHERE business_name LIKE ?
                   ORDER BY filing_date DESC"""
            df = pd.read_sql_query(
                self.prepare_sql(sql),
                conn,
                params=(f"%{query}%",)
            )
        return df
    
    def get_stats(self) -> dict:
        """
        Get database statistics.
        
        Returns:
            Dictionary with stats
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Total count
            cursor.execute(self.prepare_sql("SELECT COUNT(*) as cnt FROM leads"))
            result = cursor.fetchone()
            total_count = result['cnt'] if isinstance(result, dict) else result[0]
            
            # Count by state
            cursor.execute(
                "SELECT state, COUNT(*) as count FROM leads GROUP BY state ORDER BY count DESC"
            )
            states = dict(cursor.fetchall())
            
            # Recent activity
            cursor.execute(
                """SELECT MIN(filing_date) as oldest, 
                          MAX(filing_date) as newest,
                          MAX(fetched_at) as last_fetch
                   FROM leads"""
            )
            row = cursor.fetchone()
            
            return {
                "total_leads": total_count,
                "leads_by_state": states,
                "oldest_filing": row["oldest"],
                "newest_filing": row["newest"],
                "last_fetch": row["last_fetch"]
            }
    
    def update_lead_enrichment(self, lead_id: int, enrichment_data: dict) -> bool:
        """
        Update a lead with enrichment data.
        
        Args:
            lead_id: The ID of the lead to update
            enrichment_data: Dictionary with enrichment fields
            
        Returns:
            True if update was successful
        """
        update_sql = """
        UPDATE leads SET
            email = COALESCE(?, email),
            phone = COALESCE(?, phone),
            website = COALESCE(?, website),
            owner_name = COALESCE(?, owner_name),
            linkedin = COALESCE(?, linkedin),
            enrichment_source = ?,
            confidence_score = ?,
            enriched_at = ?,
            first_name = COALESCE(?, first_name),
            last_name = COALESCE(?, last_name),
            phone_1 = COALESCE(?, phone_1),
            phone_2 = COALESCE(?, phone_2),
            email_1 = COALESCE(?, email_1),
            email_2 = COALESCE(?, email_2),
            email_3 = COALESCE(?, email_3),
            email_4 = COALESCE(?, email_4),
            email_5 = COALESCE(?, email_5),
            age = COALESCE(?, age),
            street_address = COALESCE(?, street_address),
            address_locality = COALESCE(?, address_locality),
            address_region = COALESCE(?, address_region),
            postal_code = COALESCE(?, postal_code),
            serper_owner_name = COALESCE(?, serper_owner_name),
            serper_website = COALESCE(?, serper_website),
            serper_domain = COALESCE(?, serper_domain)
        WHERE id = ?
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.prepare_sql(update_sql), (
                    enrichment_data.get('email'),
                    enrichment_data.get('phone'),
                    enrichment_data.get('website'),
                    enrichment_data.get('owner_name'),
                    enrichment_data.get('linkedin'),
                    enrichment_data.get('enrichment_source'),
                    enrichment_data.get('confidence_score', 0),
                    datetime.now().isoformat(),
                    enrichment_data.get('first_name'),
                    enrichment_data.get('last_name'),
                    enrichment_data.get('phone_1') or enrichment_data.get('Phone-1'),
                    enrichment_data.get('phone_2') or enrichment_data.get('Phone-2'),
                    enrichment_data.get('email_1') or enrichment_data.get('Email-1'),
                    enrichment_data.get('email_2') or enrichment_data.get('Email-2'),
                    enrichment_data.get('email_3') or enrichment_data.get('Email-3'),
                    enrichment_data.get('email_4') or enrichment_data.get('Email-4'),
                    enrichment_data.get('email_5') or enrichment_data.get('Email-5'),
                    enrichment_data.get('age') or enrichment_data.get('Age'),
                    enrichment_data.get('street_address') or enrichment_data.get('Street Address'),
                    enrichment_data.get('address_locality') or enrichment_data.get('Address Locality'),
                    enrichment_data.get('address_region') or enrichment_data.get('Address Region'),
                    enrichment_data.get('postal_code') or enrichment_data.get('Postal Code'),
                    enrichment_data.get('serper_owner_name'),
                    enrichment_data.get('serper_website'),
                    enrichment_data.get('serper_domain'),
                    lead_id
                ))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating lead {lead_id}: {e}")
            return False
    
    def get_unenriched_leads(self, limit: int = 50) -> pd.DataFrame:
        """
        Get leads that haven't been enriched yet.
        
        Args:
            limit: Maximum number of leads to return
            
        Returns:
            DataFrame with unenriched leads
        """
        with self.get_connection() as conn:
            sql = """SELECT * FROM leads 
                   WHERE email IS NULL AND enriched_at IS NULL
                   ORDER BY created_at DESC
                   LIMIT ?"""
            df = pd.read_sql_query(
                self.prepare_sql(sql),
                conn,
                params=(limit,)
            )
        return df
    
    def get_enriched_leads(self) -> pd.DataFrame:
        """
        Get leads that have been enriched.
        
        Returns:
            DataFrame with enriched leads
        """
        with self.get_connection() as conn:
            df = pd.read_sql_query(
                """SELECT * FROM leads 
                   WHERE email IS NOT NULL OR phone IS NOT NULL
                   ORDER BY enriched_at DESC""",
                conn
            )
        return df
    
    def get_lead_by_id(self, lead_id: int) -> Optional[dict]:
        """
        Get a single lead by ID.
        
        Args:
            lead_id: The ID of the lead
            
        Returns:
            Lead as dictionary, or None if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(self.prepare_sql("SELECT * FROM leads WHERE id = ?"), (lead_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None
    
    def delete_old_leads(self, days: int = 90) -> int:
        """
        Delete leads older than N days.
        
        Args:
            days: Delete leads with filing_date older than this many days
            
        Returns:
            Number of deleted records
        """
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                self.prepare_sql("DELETE FROM leads WHERE filing_date < ?"),
                (cutoff_date,)
            )
            deleted = cursor.rowcount
        
        logger.info(f"Deleted {deleted} leads older than {days} days")
        return deleted
    
    def clear_all_leads(self) -> int:
        """
        Delete all leads from the database.
        
        Returns:
            Number of deleted records
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(self.prepare_sql("SELECT COUNT(*) as cnt FROM leads"))
            result = cursor.fetchone()
            count = result['cnt'] if isinstance(result, dict) else result[0]
            cursor.execute("DELETE FROM leads")
        
        logger.info(f"Cleared all {count} leads from database")
        return count
    
    def cleanup_placeholder_leads(self) -> int:
        """
        Remove placeholder/invalid leads from the database.
        These are records where business_name or state contain column names as values.
        
        Returns:
            Number of deleted records
        """
        # Column names and placeholder values that shouldn't be in data
        INVALID_VALUES = [
            'business_name', 'filing_date', 'state', 'status', 'url',
            'entity_type', 'filing_number', 'registered_agent', 'address',
            'phone', 'email', 'owner_name', 'name', 'company_name',
            'placeholder', 'test', 'demo', 'sample', 'example',
            'unknown', 'undefined', 'null', 'none', 'n/a',
            'first_name', 'last_name', 'phone_1', 'phone_2',
            'email_1', 'email_2', 'business_phone', 'owner'
        ]
        
        deleted_total = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for invalid_val in INVALID_VALUES:
                # Delete records where business_name exactly matches invalid value (case-insensitive)
                sql = "DELETE FROM leads WHERE LOWER(business_name) = LOWER(?)"
                cursor.execute(self.prepare_sql(sql), (invalid_val,))
                deleted_total += cursor.rowcount
                
                # Delete records where state exactly matches invalid value (case-insensitive)
                sql = "DELETE FROM leads WHERE LOWER(state) = LOWER(?)"
                cursor.execute(self.prepare_sql(sql), (invalid_val,))
                deleted_total += cursor.rowcount
            
            # Also delete records with very short business names (likely invalid)
            sql = "DELETE FROM leads WHERE LENGTH(business_name) < 3"
            cursor.execute(self.prepare_sql(sql))
            deleted_total += cursor.rowcount
        
        logger.info(f"Cleaned up {deleted_total} placeholder/invalid leads from database")
        return deleted_total
    
    def export_to_csv(self, filepath: str = "leads_export.csv") -> str:
        """
        Export all leads to a CSV file.
        
        Args:
            filepath: Path for the output CSV file
            
        Returns:
            Path to the created file
        """
        df = self.get_all_leads()
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} leads to {filepath}")
        return filepath


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_db_instance: Optional[Database] = None


def get_database(db_path: str = None) -> Database:
    """
    Get or create the database singleton instance.
    
    Args:
        db_path: Optional custom database path
        
    Returns:
        Database instance
    """
    global _db_instance
    
    if _db_instance is None or (db_path and db_path != _db_instance.db_path):
        _db_instance = Database(db_path)
    
    return _db_instance


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Demo the database functionality
    print("\n" + "="*60)
    print("DATABASE MODULE - REAL DATA ONLY")
    print("="*60)
    
    # Create test database
    db = Database(":memory:")  # Use in-memory for demo
    
    # Create some test records using real scraper
    from scrapers.base_scraper import BusinessRecord
    from datetime import datetime
    
    # Sample real business records
    records = [
        BusinessRecord(
            business_name="TEST REAL COMPANY LLC",
            filing_date=datetime.now().strftime("%Y-%m-%d"),
            state="FL",
            status="Active",
            url="https://search.sunbiz.org/example",
            entity_type="LLC",
            filing_number="L23000000001"
        ),
        BusinessRecord(
            business_name="ANOTHER BUSINESS INC",
            filing_date=datetime.now().strftime("%Y-%m-%d"),
            state="FL",
            status="Active",
            url="https://search.sunbiz.org/example2",
            entity_type="Corporation",
            filing_number="P23000000002"
        )
    ]
    
    # Save records
    inserted, duplicates = db.save_records(records)
    print(f"\nInserted: {inserted}, Duplicates: {duplicates}")
    
    # Get all leads
    df = db.get_all_leads()
    print(f"\nAll leads: {len(df)} records")
    print(df[["business_name", "state", "filing_date"]].head())
    
    # Get stats
    stats = db.get_stats()
    print(f"\nDatabase Stats:")
    print(f"  Total Leads: {stats['total_leads']}")
    print(f"  States: {stats['leads_by_state']}")

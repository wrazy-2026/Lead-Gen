"""
Reconstruct Firestore collections for the LeadGen app in the target Firebase project.

Usage:
    python reconstruct_firestore.py
    python reconstruct_firestore.py --skip-import
    python reconstruct_firestore.py --json-file all_states_leads.json
"""

import argparse
import json
import os
from datetime import datetime

from database import get_database

DEFAULT_JSON_FILE = "all_states_leads.json"


def _doc_exists(ref):
    snap = ref.get(timeout=5.0)
    return snap.exists


def ensure_base_documents(db_client):
    """Create baseline documents used by application settings/auth flows."""
    settings_ref = db_client.collection("settings")

    defaults = {
        "google_admin_token": None,
        "ghl_api_key": "",
        "ghl_location_id": "",
    }

    created = 0
    for key, value in defaults.items():
        ref = settings_ref.document(key)
        if _doc_exists(ref):
            continue
        ref.set(
            {
                "value": value,
                "updated_at": datetime.now().isoformat(),
            },
            timeout=5.0,
        )
        created += 1

    return created


def import_leads(json_path):
    """Import leads via existing database.save_records logic."""
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    class SimpleRecord:
        def __init__(self, row):
            self.row = row

        def to_dict(self):
            payload = self.row.copy()
            if "tin_number" in payload and "ein" not in payload:
                payload["ein"] = payload["tin_number"]
            return payload

    records = [SimpleRecord(row) for row in data]
    db = get_database()
    inserted, duplicates, _ = db.save_records(records)
    return len(records), inserted, duplicates


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-import", action="store_true", help="Only bootstrap base Firestore documents.")
    parser.add_argument("--json-file", default=DEFAULT_JSON_FILE, help="Path to source leads JSON file.")
    return parser.parse_args()


def main():
    args = parse_args()

    db = get_database()
    db_client = db.db
    print(f"Connected project: {db_client.project}")

    created_docs = ensure_base_documents(db_client)
    print(f"Bootstrap complete. New settings docs created: {created_docs}")

    if args.skip_import:
        print("Skipping leads import (--skip-import).")
        return

    total, inserted, duplicates = import_leads(args.json_file)
    print(f"Import complete from {args.json_file}")
    print(f"Total input rows: {total}")
    print(f"Inserted: {inserted}")
    print(f"Duplicates: {duplicates}")


if __name__ == "__main__":
    main()

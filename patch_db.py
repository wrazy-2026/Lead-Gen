import re

with open('database.py', 'r') as f:
    code = f.read()

# ADD DECORATOR
decorator_code = """
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from functools import wraps
import pandas as pd

def prevent_hang(timeout_sec=5.0, default_return=None):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    return executor.submit(func, self, *args, **kwargs).result(timeout=timeout_sec)
            except TimeoutError:
                logger.error(f"Timeout ({timeout_sec}s) executing {func.__name__} (likely Quota Exceeded)")
                return default_return
            except Exception as e:
                logger.error(f"Error executing {func.__name__}: {e}")
                return default_return
        return wrapper
    return decorator

class Database:
"""

if 'def prevent_hang(' not in code:
    code = code.replace('class Database:', decorator_code)

# APPLY TO METHODS
methods = [
    (r"    def get_all_leads\(self, limit: int = None, offset: int = None\) -> pd\.DataFrame:", 
     "    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())\n    def get_all_leads(self, limit: int = None, offset: int = None) -> pd.DataFrame:"),
    (r"    def get_leads_count\(self\) -> int:", 
     "    @prevent_hang(timeout_sec=5.0, default_return=0)\n    def get_leads_count(self) -> int:"),
    (r"    def get_leads_by_state\(self, state: str\) -> pd\.DataFrame:", 
     "    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())\n    def get_leads_by_state(self, state: str) -> pd.DataFrame:"),
    (r"    def get_recent_leads\(self, days: int = 7\) -> pd\.DataFrame:", 
     "    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())\n    def get_recent_leads(self, days: int = 7) -> pd.DataFrame:"),
    (r"    def search_leads\(self, search_query: str\) -> pd\.DataFrame:", 
     "    @prevent_hang(timeout_sec=8.0, default_return=pd.DataFrame())\n    def search_leads(self, search_query: str) -> pd.DataFrame:"),
    (r"    def get_stats\(self\) -> dict:", 
     "    @prevent_hang(timeout_sec=8.0, default_return={'total_leads': 0, 'leads_by_state': {}, 'oldest_filing': None, 'newest_filing': None, 'last_fetch': None})\n    def get_stats(self) -> dict:"),
    (r"    def get_setting\(self, key: str, default=None\) -> any:", 
     "    @prevent_hang(timeout_sec=3.0, default_return=None)\n    def get_setting(self, key: str, default=None) -> any:"),
    (r"    def save_setting\(self, key: str, value: any\) -> bool:", 
     "    @prevent_hang(timeout_sec=3.0, default_return=False)\n    def save_setting(self, key: str, value: any) -> bool:")
]

for pattern, replacement in methods:
    if '@prevent_hang' not in code.split(pattern.replace('\\', ''), 1)[0][-50:]:
        code = re.sub(pattern, replacement, code, count=1)

with open('database.py', 'w') as f:
    f.write(code)
print('Database patched successfully.')

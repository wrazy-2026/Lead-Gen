"""
Anti-Bot Infrastructure
=======================
Provides UA rotation, proxy support, CAPTCHA handling, and retry logic
with exponential backoff for all scrapers.

Features:
- 25+ rotating User-Agent strings (desktop + mobile)
- Residential proxy support (env-configurable)
- CAPTCHA detection and optional solver integration (2Captcha / Anti-Captcha)
- Exponential backoff with jitter for 403/429 responses
- Standard browser-like headers
"""

import logging
import os
import random
import time
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ============================================================================
# USER-AGENT POOL  (25 real-world strings rotated per request)
# ============================================================================

USER_AGENTS: List[str] = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Mobile - Chrome on Android
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    # Mobile - Safari on iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    # Opera
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 OPR/110.0.0.0",
    # Brave (looks like Chrome)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.60 Safari/537.36",
]


def get_random_ua() -> str:
    """Return a random User-Agent string from the pool."""
    return random.choice(USER_AGENTS)


def get_browser_headers(ua: Optional[str] = None, referer: Optional[str] = None) -> Dict[str, str]:
    """
    Build a realistic browser-like header dict.
    
    Args:
        ua: Specific User-Agent to use (random if None)
        referer: Referer URL (omitted if None)
    """
    ua = ua or get_random_ua()
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        headers["Referer"] = referer
    return headers


# ============================================================================
# PROXY SUPPORT
# ============================================================================

def get_proxy_config() -> Optional[Dict[str, str]]:
    """
    Read proxy settings from environment variables.
    
    Supported env vars (in priority order):
      SCRAPER_PROXY_URL   – full URL  e.g. http://user:pass@proxy.example.com:8080
      HTTP_PROXY / HTTPS_PROXY – standard proxy env vars
      
    Returns dict like {"http": url, "https": url} or None.
    """
    proxy_url = os.environ.get("SCRAPER_PROXY_URL")
    if proxy_url:
        return {"http": proxy_url, "https": proxy_url}

    http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if http_proxy or https_proxy:
        return {"http": http_proxy or https_proxy, "https": https_proxy or http_proxy}

    return None


# ============================================================================
# CAPTCHA DETECTION & SOLVING
# ============================================================================

CAPTCHA_INDICATORS = [
    "captcha", "recaptcha", "hcaptcha", "g-recaptcha",
    "cf-challenge", "challenge-platform", "ray id",
    "please verify you are a human", "access denied",
    "unusual traffic", "bot detection",
    "are you a robot", "verify you are human",
    "one more step", "checking your browser",
    "security check", "cf-browser-verification",
]

# Indicators that the page returned a genuine "no results" response
NO_RESULTS_INDICATORS = [
    "no results found", "no records found", "no matches",
    "0 results", "no entities found", "no businesses found",
    "your search returned no results", "no filings found",
    "did not match any", "nothing found",
]


def detect_captcha(response_text: str) -> bool:
    """Return True if the response body appears to contain a CAPTCHA challenge."""
    lower = response_text.lower()
    return any(indicator in lower for indicator in CAPTCHA_INDICATORS)


def classify_response(response_text: str) -> str:
    """
    Classify a search response into one of:
      'captcha'    – CAPTCHA / bot detection page
      'no_results' – genuine "no results" page
      'results'    – likely contains real data

    Used for error-handling & logging per spec section 4.
    """
    lower = response_text.lower()
    if any(ind in lower for ind in CAPTCHA_INDICATORS):
        return "captcha"
    if any(ind in lower for ind in NO_RESULTS_INDICATORS):
        return "no_results"
    return "results"


def solve_captcha(site_key: str, page_url: str) -> Optional[str]:
    """
    Attempt CAPTCHA solving via 2Captcha or Anti-Captcha.
    
    Requires env var CAPTCHA_API_KEY and optionally CAPTCHA_SERVICE
    (defaults to '2captcha').  Returns the solved token or None.
    """
    api_key = os.environ.get("CAPTCHA_API_KEY")
    if not api_key:
        logger.warning("No CAPTCHA_API_KEY set – cannot solve CAPTCHA")
        return None

    service = os.environ.get("CAPTCHA_SERVICE", "2captcha").lower()

    try:
        if service == "2captcha":
            return _solve_2captcha(api_key, site_key, page_url)
        elif service == "anticaptcha":
            return _solve_anticaptcha(api_key, site_key, page_url)
        else:
            logger.error(f"Unknown CAPTCHA service: {service}")
            return None
    except Exception as e:
        logger.error(f"CAPTCHA solve failed: {e}")
        return None


def _solve_2captcha(api_key: str, site_key: str, page_url: str) -> Optional[str]:
    """Solve reCAPTCHA v2 via 2Captcha API."""
    # Submit task
    resp = requests.post(
        "https://2captcha.com/in.php",
        data={
            "key": api_key,
            "method": "userrecaptcha",
            "googlekey": site_key,
            "pageurl": page_url,
            "json": 1,
        },
        timeout=30,
    )
    data = resp.json()
    if data.get("status") != 1:
        logger.error(f"2Captcha submit error: {data}")
        return None

    task_id = data["request"]

    # Poll for result (max ~120s)
    for _ in range(24):
        time.sleep(5)
        resp = requests.get(
            "https://2captcha.com/res.php",
            params={"key": api_key, "action": "get", "id": task_id, "json": 1},
            timeout=15,
        )
        result = resp.json()
        if result.get("status") == 1:
            return result["request"]
        if result.get("request") == "ERROR_CAPTCHA_UNSOLVABLE":
            logger.error("2Captcha: CAPTCHA unsolvable")
            return None

    logger.error("2Captcha: timeout waiting for solution")
    return None


def _solve_anticaptcha(api_key: str, site_key: str, page_url: str) -> Optional[str]:
    """Solve reCAPTCHA v2 via Anti-Captcha API."""
    resp = requests.post(
        "https://api.anti-captcha.com/createTask",
        json={
            "clientKey": api_key,
            "task": {
                "type": "RecaptchaV2TaskProxyless",
                "websiteURL": page_url,
                "websiteKey": site_key,
            },
        },
        timeout=30,
    )
    data = resp.json()
    if data.get("errorId") != 0:
        logger.error(f"Anti-Captcha error: {data}")
        return None

    task_id = data["taskId"]

    for _ in range(24):
        time.sleep(5)
        resp = requests.post(
            "https://api.anti-captcha.com/getTaskResult",
            json={"clientKey": api_key, "taskId": task_id},
            timeout=15,
        )
        result = resp.json()
        if result.get("status") == "ready":
            return result["solution"]["gRecaptchaResponse"]
        if result.get("errorId") != 0:
            logger.error(f"Anti-Captcha error: {result}")
            return None

    logger.error("Anti-Captcha: timeout waiting for solution")
    return None


# ============================================================================
# RETRY WITH EXPONENTIAL BACKOFF
# ============================================================================

# Status codes that trigger automatic retry
RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}


def make_request_with_retry(
    session: requests.Session,
    url: str,
    method: str = "GET",
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    timeout: int = 30,
    **kwargs,
) -> requests.Response:
    """
    Make an HTTP request with exponential backoff on retryable failures.
    
    Rotates UA on each retry. Detects CAPTCHA responses.
    
    Args:
        session: requests.Session to use
        url: Target URL
        method: HTTP method (GET/POST)
        max_retries: Maximum number of retries
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap in seconds
        timeout: Request timeout in seconds
        **kwargs: Passed through to session.request()
        
    Returns:
        requests.Response object
        
    Raises:
        CaptchaDetectedError: If CAPTCHA is detected after all retries
        requests.RequestException: If all retries are exhausted
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            # Rotate UA on each attempt
            ua = get_random_ua()
            if "headers" in kwargs:
                if "User-Agent" not in kwargs["headers"]:
                    kwargs["headers"]["User-Agent"] = ua
            else:
                # Let session handle headers, just update session UA
                session.headers.update({"User-Agent": ua})

            # Apply proxy if configured
            if "proxies" not in kwargs:
                proxy = get_proxy_config()
                if proxy:
                    kwargs["proxies"] = proxy

            kwargs.setdefault("timeout", timeout)

            response = session.request(method, url, **kwargs)

            # Check for CAPTCHA
            if response.status_code == 200 and detect_captcha(response.text):
                logger.warning(f"CAPTCHA detected on {url} (attempt {attempt + 1})")
                if attempt < max_retries:
                    delay = _backoff_delay(attempt, base_delay, max_delay)
                    logger.info(f"Backing off {delay:.1f}s before retry...")
                    time.sleep(delay)
                    continue
                raise CaptchaDetectedError(f"CAPTCHA detected on {url} after {max_retries + 1} attempts")

            # Check for retryable status codes
            if response.status_code in RETRYABLE_STATUS_CODES:
                logger.warning(
                    f"HTTP {response.status_code} from {url} (attempt {attempt + 1})"
                )
                if attempt < max_retries:
                    # Honor Retry-After header if present
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = min(float(retry_after), max_delay)
                        except ValueError:
                            delay = _backoff_delay(attempt, base_delay, max_delay)
                    else:
                        delay = _backoff_delay(attempt, base_delay, max_delay)
                    logger.info(f"Backing off {delay:.1f}s before retry...")
                    time.sleep(delay)
                    continue
                response.raise_for_status()

            return response

        except requests.ConnectionError as e:
            last_exception = e
            if attempt < max_retries:
                delay = _backoff_delay(attempt, base_delay, max_delay)
                logger.warning(f"Connection error on {url}, retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                raise
        except requests.Timeout as e:
            last_exception = e
            if attempt < max_retries:
                delay = _backoff_delay(attempt, base_delay, max_delay)
                logger.warning(f"Timeout on {url}, retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                raise

    raise last_exception or requests.RequestException(f"All retries exhausted for {url}")


def _backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter: delay = random(0, min(cap, base * 2^attempt))."""
    exp = min(cap, base * (2 ** attempt))
    return random.uniform(0, exp)


class CaptchaDetectedError(Exception):
    """Raised when a CAPTCHA challenge is detected."""
    pass


# ============================================================================
# SESSION FACTORY
# ============================================================================

def create_scraper_session(
    max_pool_retries: int = 3,
    backoff_factor: float = 0.5,
    pool_connections: int = 10,
    pool_maxsize: int = 10,
) -> requests.Session:
    """
    Create a requests.Session pre-configured with:
    - Connection-level retry via urllib3
    - Random UA
    - Proxy if configured
    - Browser-like headers
    """
    session = requests.Session()

    # urllib3 low-level retries (connection errors, not HTTP status retries)
    retry_strategy = Retry(
        total=max_pool_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[],  # We handle HTTP retries in make_request_with_retry
        allowed_methods=["GET", "POST", "HEAD"],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set default headers
    session.headers.update(get_browser_headers())

    # Apply proxy
    proxy = get_proxy_config()
    if proxy:
        session.proxies.update(proxy)
        logger.info("Proxy configured for scraper session")

    return session


# ============================================================================
# PROXY ROTATION
# ============================================================================

def get_proxy_pool() -> List[str]:
    """
    Read a newline-delimited list of proxy URLs from SCRAPER_PROXY_POOL env var.
    Format: http://user:pass@host:port (one per line, or comma-separated).

    Falls back to the single SCRAPER_PROXY_URL if pool is not set.
    """
    pool_raw = os.environ.get("SCRAPER_PROXY_POOL", "")
    if pool_raw:
        # Support both comma-separated and newline-separated
        urls = [u.strip() for u in pool_raw.replace(",", "\n").split("\n") if u.strip()]
        if urls:
            return urls

    single = os.environ.get("SCRAPER_PROXY_URL")
    return [single] if single else []


def rotate_proxy(session: requests.Session) -> Optional[str]:
    """
    Pick a random proxy from the pool and apply to the session.
    Returns the chosen proxy URL (or None if no pool available).
    """
    pool = get_proxy_pool()
    if not pool:
        return None

    proxy_url = random.choice(pool)
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    logger.debug(f"Rotated proxy to: {proxy_url[:30]}...")
    return proxy_url


# ============================================================================
# STEALTH HEADERS HELPER
# ============================================================================

def get_stealth_headers(referer: Optional[str] = None) -> Dict[str, str]:
    """
    Build headers that mimic a real browser as closely as possible.
    Ensures the Referer matches the state portal URL when provided.
    """
    ua = get_random_ua()
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if referer else "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "DNT": "1",
    }
    if referer:
        headers["Referer"] = referer
    return headers

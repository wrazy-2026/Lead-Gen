#!/usr/bin/env python3
"""
Test all state scrapers to identify which ones are working.
"""

import asyncio
import sys
from datetime import datetime

# Add the scrapers directory to the path
sys.path.insert(0, '.')

from scrapers.multistate_scraper import STATE_CONFIGS, get_scraper_for_states

# States to test (current active states)
STATES_TO_TEST = [
    'FL', 'OK', 'MO', 'SC', 'VT', 'WI', 'NE', 'NH', 'KS', 'KY',
    'LA', 'AR', 'SD', 'OR', 'RI', 'MS', 'NM', 'ME', 'DE', 'HI',
    'AL', 'AK', 'IA', 'CO', 'TN', 'MA', 'NC'
]

# Test keyword
TEST_KEYWORD = "plumber"

async def test_single_state(state_code: str) -> dict:
    """Test a single state's scraper."""
    result = {
        "state": state_code,
        "name": STATE_CONFIGS.get(state_code, {}).name if state_code in STATE_CONFIGS else "Unknown",
        "status": "unknown",
        "count": 0,
        "error": None,
        "url": STATE_CONFIGS.get(state_code).search_url if state_code in STATE_CONFIGS else "N/A"
    }
    
    try:
        print(f"\n{'='*60}")
        print(f"Testing {state_code} - {result['name']}...")
        print(f"URL: {result['url']}")
        
        scraper = get_scraper_for_states([state_code], headless=True)
        
        # Run with timeout
        businesses = await asyncio.wait_for(
            scraper.scrape(keywords=[TEST_KEYWORD], max_per_keyword=3),
            timeout=60  # 60 second timeout per state
        )
        
        result["count"] = len(businesses)
        
        if len(businesses) > 0:
            result["status"] = "WORKING"
            print(f"✅ {state_code}: WORKING - Found {len(businesses)} businesses")
            for b in businesses[:3]:
                print(f"   - {b.get('name', 'N/A')}")
        else:
            result["status"] = "NO_RESULTS"
            print(f"⚠️ {state_code}: NO RESULTS - Scraper ran but found nothing")
            
    except asyncio.TimeoutError:
        result["status"] = "TIMEOUT"
        result["error"] = "Timed out after 60 seconds"
        print(f"⏱️ {state_code}: TIMEOUT - Took too long")
        
    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)[:200]
        print(f"❌ {state_code}: ERROR - {str(e)[:100]}")
    
    return result


async def main():
    print("="*60)
    print("STATE SCRAPER TEST - Testing all active states")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Test keyword: '{TEST_KEYWORD}'")
    print(f"States to test: {len(STATES_TO_TEST)}")
    print("="*60)
    
    results = []
    
    for state in STATES_TO_TEST:
        result = await test_single_state(state)
        results.append(result)
        
        # Small delay between states
        await asyncio.sleep(2)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    working = [r for r in results if r["status"] == "WORKING"]
    no_results = [r for r in results if r["status"] == "NO_RESULTS"]
    timeouts = [r for r in results if r["status"] == "TIMEOUT"]
    errors = [r for r in results if r["status"] == "ERROR"]
    
    print(f"\n✅ WORKING ({len(working)}):")
    for r in working:
        print(f"   {r['state']} - {r['name']} ({r['count']} results)")
    
    print(f"\n⚠️ NO RESULTS ({len(no_results)}):")
    for r in no_results:
        print(f"   {r['state']} - {r['name']}")
    
    print(f"\n⏱️ TIMEOUT ({len(timeouts)}):")
    for r in timeouts:
        print(f"   {r['state']} - {r['name']}")
    
    print(f"\n❌ ERRORS ({len(errors)}):")
    for r in errors:
        print(f"   {r['state']} - {r['name']}: {r['error'][:80]}")
    
    # Write results to file
    with open("state_test_results.txt", "w") as f:
        f.write(f"STATE SCRAPER TEST RESULTS\n")
        f.write(f"Tested: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Keyword: {TEST_KEYWORD}\n\n")
        
        f.write(f"WORKING ({len(working)}):\n")
        for r in working:
            f.write(f"  {r['state']}\n")
        
        f.write(f"\nNO_RESULTS ({len(no_results)}):\n")
        for r in no_results:
            f.write(f"  {r['state']}\n")
        
        f.write(f"\nTIMEOUT ({len(timeouts)}):\n")
        for r in timeouts:
            f.write(f"  {r['state']}\n")
        
        f.write(f"\nERRORS ({len(errors)}):\n")
        for r in errors:
            f.write(f"  {r['state']}: {r['error']}\n")
    
    print(f"\nResults saved to state_test_results.txt")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())

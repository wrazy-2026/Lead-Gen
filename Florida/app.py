#!/usr/bin/env python3
"""
Florida Sunbiz Home Services Scraper - Web Interface

A simple Flask web app that wraps the FixedSunbizScraper to provide
a browser-based interface for scraping Florida Sunbiz business data,
with a real-time log panel.
"""

import asyncio
import io
import csv
import json
import threading
from datetime import datetime

from flask import Flask, render_template_string, request, jsonify, Response

from sunbiz_scraper_fixed import FixedSunbizScraper

app = Flask(__name__)

# In-memory store for the latest scrape results, status, and logs
scrape_state = {
    "status": "idle",       # idle | running | done | error
    "businesses": [],
    "message": "",
    "progress": "",
    "logs": [],             # list of log-line strings
    "log_index": 0,         # client-side cursor for incremental fetching
}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Florida Sunbiz Scraper</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #f0f2f5; color: #333; }
  .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
  h1 { text-align: center; margin: 20px 0 10px; color: #1a3a5c; }
  .subtitle { text-align: center; color: #666; margin-bottom: 30px; font-size: 0.95rem; }

  /* Controls */
  .controls { background: #fff; border-radius: 10px; padding: 24px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 24px; }
  .controls h2 { margin-bottom: 16px; font-size: 1.1rem; color: #1a3a5c; }
  .form-row { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-end; margin-bottom: 16px; }
  .form-group { display: flex; flex-direction: column; }
  .form-group label { font-size: 0.85rem; font-weight: 600; margin-bottom: 4px; color: #555; }
  .form-group input, .form-group select { padding: 8px 12px; border: 1px solid #ccc;
             border-radius: 6px; font-size: 0.9rem; }
  .form-group input:focus, .form-group select:focus { outline: none; border-color: #2c7be5; }

  .keyword-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
  .keyword-tag { background: #e8f0fe; color: #1a3a5c; padding: 4px 10px; border-radius: 14px;
                 font-size: 0.8rem; cursor: pointer; border: 1px solid transparent;
                 user-select: none; transition: all .15s; }
  .keyword-tag.selected { background: #2c7be5; color: #fff; border-color: #2c7be5; }
  .keyword-tag:hover { border-color: #2c7be5; }

  .btn { padding: 10px 24px; border: none; border-radius: 6px; cursor: pointer;
         font-size: 0.9rem; font-weight: 600; transition: background .2s; }
  .btn-primary { background: #2c7be5; color: #fff; }
  .btn-primary:hover { background: #1a5fc4; }
  .btn-primary:disabled { background: #a0c4f1; cursor: not-allowed; }
  .btn-secondary { background: #e2e8f0; color: #333; }
  .btn-secondary:hover { background: #cbd5e0; }
  .btn-sm { padding: 6px 14px; font-size: 0.82rem; }
  .btn-danger { background: #dc3545; color: #fff; }
  .btn-danger:hover { background: #c82333; }

  .actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }

  /* Status */
  .status-bar { padding: 10px 16px; border-radius: 6px; margin-bottom: 16px;
                font-size: 0.9rem; display: none; }
  .status-bar.show { display: block; }
  .status-idle { background: #e2e8f0; }
  .status-running { background: #fef3cd; color: #856404; }
  .status-done { background: #d4edda; color: #155724; }
  .status-error { background: #f8d7da; color: #721c24; }
  .spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #856404;
             border-top-color: transparent; border-radius: 50%;
             animation: spin .7s linear infinite; vertical-align: middle; margin-right: 6px; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Log panel */
  .log-section { background: #1e1e2e; border-radius: 10px; padding: 16px; margin-bottom: 24px;
                 box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
  .log-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
  .log-header h2 { color: #cdd6f4; font-size: 1rem; }
  .log-header .log-actions { display: flex; gap: 8px; }
  .log-box { background: #11111b; border-radius: 6px; padding: 12px; height: 300px;
             overflow-y: auto; font-family: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
             font-size: 0.78rem; line-height: 1.6; color: #a6adc8; }
  .log-box .log-line { white-space: pre-wrap; word-break: break-all; }
  .log-box .log-line.error { color: #f38ba8; }
  .log-box .log-line.success { color: #a6e3a1; }
  .log-box .log-line.keyword { color: #89b4fa; font-weight: 700; }
  .log-box .log-line.info { color: #cdd6f4; }
  .log-box .log-line.dim { color: #585b70; }

  /* Results table */
  .results-section { background: #fff; border-radius: 10px; padding: 24px;
                     box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .results-section h2 { margin-bottom: 12px; font-size: 1.1rem; color: #1a3a5c; }
  .result-count { color: #666; font-size: 0.85rem; margin-bottom: 12px; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th { background: #f7fafc; text-align: left; padding: 10px 12px; border-bottom: 2px solid #e2e8f0;
       color: #555; font-weight: 600; white-space: nowrap; position: sticky; top: 0; }
  td { padding: 9px 12px; border-bottom: 1px solid #edf2f7; }
  tr:hover td { background: #f7fafc; }
  .status-active { color: #28a745; font-weight: 600; }
  .status-inactive { color: #dc3545; }
  .empty-state { text-align: center; padding: 40px; color: #999; }
</style>
</head>
<body>
<div class="container">
  <h1>&#127774; Florida Sunbiz Scraper</h1>
  <p class="subtitle">Search Florida home-service businesses from the Sunbiz registry</p>

  <div class="controls">
    <h2>Search Settings</h2>
    <p style="font-size:0.85rem;color:#666;margin-bottom:10px;">Click keywords to select/deselect, or type a custom keyword.</p>
    <div class="keyword-tags" id="keywordTags"></div>
    <div class="form-row">
      <div class="form-group">
        <label for="customKeyword">Custom keyword</label>
        <input type="text" id="customKeyword" placeholder="e.g. fencing">
      </div>
      <div class="form-group">
        <button class="btn btn-secondary btn-sm" onclick="addCustomKeyword()">+ Add</button>
      </div>
      <div class="form-group">
        <label for="maxResults">Max per keyword</label>
        <input type="number" id="maxResults" value="20" min="1" max="500" style="width:90px;">
      </div>
    </div>
    <div class="actions">
      <button class="btn btn-primary" id="startBtn" onclick="startScrape()">Start Scraping</button>
      <button class="btn btn-secondary btn-sm" onclick="selectAll()">Select All</button>
      <button class="btn btn-secondary btn-sm" onclick="selectNone()">Clear All</button>
    </div>
  </div>

  <div class="status-bar" id="statusBar"></div>

  <!-- LOG PANEL -->
  <div class="log-section">
    <div class="log-header">
      <h2>&#128221; Scraper Log</h2>
      <div class="log-actions">
        <button class="btn btn-secondary btn-sm" onclick="clearLog()">Clear</button>
        <label style="color:#a6adc8;font-size:0.8rem;display:flex;align-items:center;gap:4px;">
          <input type="checkbox" id="autoScroll" checked> Auto-scroll
        </label>
      </div>
    </div>
    <div class="log-box" id="logBox">
      <div class="log-line dim">Waiting for scrape to start...</div>
    </div>
  </div>

  <div class="results-section">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;">
      <div>
        <h2>Results</h2>
        <p class="result-count" id="resultCount">No results yet.</p>
      </div>
      <div class="actions" id="downloadBtns" style="display:none;">
        <button class="btn btn-secondary btn-sm" onclick="downloadCSV()">Download CSV</button>
        <button class="btn btn-secondary btn-sm" onclick="downloadJSON()">Download JSON</button>
      </div>
    </div>
    <div class="table-wrap" style="max-height:500px;overflow-y:auto;">
      <table id="resultsTable">
        <thead>
          <tr>
            <th>#</th><th>Name</th><th>Doc #</th><th>Status</th><th>Filing Date</th>
            <th>Principal Address</th><th>Category</th>
          </tr>
        </thead>
        <tbody id="resultsBody">
          <tr><td colspan="7" class="empty-state">Run a scrape to see results here.</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
const ALL_KEYWORDS = {{ keywords | tojson }};
let selectedKeywords = new Set(ALL_KEYWORDS.slice(0, 3));
let pollTimer = null;
let logCursor = 0;

function renderTags() {
  const el = document.getElementById('keywordTags');
  el.innerHTML = '';
  ALL_KEYWORDS.forEach(kw => {
    const tag = document.createElement('span');
    tag.className = 'keyword-tag' + (selectedKeywords.has(kw) ? ' selected' : '');
    tag.textContent = kw;
    tag.onclick = () => { selectedKeywords.has(kw) ? selectedKeywords.delete(kw) : selectedKeywords.add(kw); renderTags(); };
    el.appendChild(tag);
  });
}
function selectAll() { ALL_KEYWORDS.forEach(k => selectedKeywords.add(k)); renderTags(); }
function selectNone() { selectedKeywords.clear(); renderTags(); }
function addCustomKeyword() {
  const inp = document.getElementById('customKeyword');
  const kw = inp.value.trim();
  if (kw && !ALL_KEYWORDS.includes(kw)) ALL_KEYWORDS.push(kw);
  if (kw) selectedKeywords.add(kw);
  inp.value = '';
  renderTags();
}

function setStatus(type, msg) {
  const bar = document.getElementById('statusBar');
  bar.className = 'status-bar show status-' + type;
  bar.innerHTML = (type === 'running' ? '<span class="spinner"></span>' : '') + msg;
}

function classifyLine(text) {
  if (/ERROR|FAIL|exception/i.test(text)) return 'error';
  if (/^={3,}|^KEYWORD:/i.test(text)) return 'keyword';
  if (/OK\s*[-–]|complete:|Done!/i.test(text)) return 'success';
  if (/Navigat|Typed|Clicked|Pressed|loaded|Found|Parsed/i.test(text)) return 'info';
  return '';
}

function appendLogLines(lines) {
  const box = document.getElementById('logBox');
  const auto = document.getElementById('autoScroll').checked;
  lines.forEach(text => {
    const div = document.createElement('div');
    div.className = 'log-line ' + classifyLine(text);
    div.textContent = text;
    box.appendChild(div);
  });
  if (auto) box.scrollTop = box.scrollHeight;
}
function clearLog() {
  document.getElementById('logBox').innerHTML = '<div class="log-line dim">Log cleared.</div>';
}

async function startScrape() {
  const keywords = Array.from(selectedKeywords);
  if (!keywords.length) { alert('Select at least one keyword.'); return; }
  const maxResults = parseInt(document.getElementById('maxResults').value) || 20;

  document.getElementById('startBtn').disabled = true;
  setStatus('running', 'Scraping in progress\\u2026');
  document.getElementById('logBox').innerHTML = '';
  logCursor = 0;

  try {
    const resp = await fetch('/api/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keywords, max_per_category: maxResults })
    });
    const data = await resp.json();
    if (data.status === 'started' || data.status === 'running') {
      pollStatus();
    }
  } catch (e) {
    setStatus('error', 'Failed to start scrape: ' + e.message);
    document.getElementById('startBtn').disabled = false;
  }
}

function pollStatus() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    try {
      // Fetch status + new log lines in parallel
      const [statusResp, logResp] = await Promise.all([
        fetch('/api/status'),
        fetch('/api/logs?after=' + logCursor)
      ]);
      const sData = await statusResp.json();
      const lData = await logResp.json();

      // Append new log lines
      if (lData.lines && lData.lines.length) {
        appendLogLines(lData.lines);
        logCursor = lData.cursor;
      }

      if (sData.status === 'running') {
        setStatus('running', sData.progress || 'Scraping\\u2026');
      } else if (sData.status === 'done') {
        clearInterval(pollTimer);
        setStatus('done', sData.message);
        document.getElementById('startBtn').disabled = false;
        loadResults();
      } else if (sData.status === 'error') {
        clearInterval(pollTimer);
        setStatus('error', sData.message);
        document.getElementById('startBtn').disabled = false;
      }
    } catch (_) {}
  }, 800);
}

async function loadResults() {
  const resp = await fetch('/api/results');
  const data = await resp.json();
  const tbody = document.getElementById('resultsBody');
  const count = document.getElementById('resultCount');
  const dl = document.getElementById('downloadBtns');
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No results found.</td></tr>';
    count.textContent = '0 results.';
    dl.style.display = 'none';
    return;
  }
  dl.style.display = 'flex';
  count.textContent = data.length + ' businesses found.';
  tbody.innerHTML = data.map((b, i) => {
    const sc = (b.status||'').toLowerCase().includes('active') ? 'status-active' :
               (b.status||'').toLowerCase().includes('inact') ? 'status-inactive' : '';
    return '<tr><td>'+(i+1)+'</td><td>'+esc(b.name)+'</td><td>'+esc(b.document_number)+
           '</td><td class="'+sc+'">'+esc(b.status)+'</td><td>'+esc(b.filing_date)+
           '</td><td>'+esc(b.principal_address)+'</td><td>'+esc(b.category)+'</td></tr>';
  }).join('');
}

function esc(s) { const d = document.createElement('div'); d.textContent = s || ''; return d.innerHTML; }
function downloadCSV() { window.location = '/api/download/csv'; }
function downloadJSON() { window.location = '/api/download/json'; }

renderTags();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE, keywords=FixedSunbizScraper.HOME_SERVICE_KEYWORDS)


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    if scrape_state["status"] == "running":
        return jsonify({"status": "running", "message": "A scrape is already in progress."})

    data = request.get_json(silent=True) or {}
    keywords = data.get("keywords", FixedSunbizScraper.HOME_SERVICE_KEYWORDS[:3])
    max_per_category = min(int(data.get("max_per_category", 20)), 500)

    # Validate keywords are strings
    keywords = [str(k).strip() for k in keywords if str(k).strip()]
    if not keywords:
        return jsonify({"status": "error", "message": "No keywords provided."}), 400

    scrape_state["status"] = "running"
    scrape_state["businesses"] = []
    scrape_state["message"] = ""
    scrape_state["progress"] = "Starting..."
    scrape_state["logs"] = []

    thread = threading.Thread(
        target=_run_scrape, args=(keywords, max_per_category), daemon=True
    )
    thread.start()

    return jsonify({"status": "started"})


def _add_log(msg: str):
    """Thread-safe log appender."""
    scrape_state["logs"].append(msg)


def _run_scrape(keywords: list, max_per_category: int):
    """Run the scraper in a background thread with its own event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_async_scrape(keywords, max_per_category))
    except Exception as exc:
        scrape_state["status"] = "error"
        scrape_state["message"] = f"Scrape failed: {exc}"
        _add_log(f"FATAL ERROR: {exc}")
    finally:
        loop.close()


async def _async_scrape(keywords: list, max_per_category: int):
    scraper = FixedSunbizScraper(headless=True, on_log=_add_log)
    try:
        await scraper.start_browser()
        page = await scraper.context.new_page()
        page.set_default_timeout(scraper.timeout)

        all_businesses = []
        total = len(keywords)
        for i, keyword in enumerate(keywords, 1):
            scrape_state["progress"] = f"Scraping keyword {i}/{total}: {keyword}"
            results = await scraper.scrape_keyword(page, keyword, max_per_category)
            all_businesses.extend(results)
            await asyncio.sleep(2)

        await page.close()

        scraper.businesses = all_businesses
        sorted_biz = scraper.sort_by_date(ascending=False)
        scrape_state["businesses"] = sorted_biz
        scrape_state["status"] = "done"
        scrape_state["message"] = f"Done! Found {len(sorted_biz)} businesses across {total} keywords."
        _add_log(scrape_state["message"])
    finally:
        await scraper.stop_browser()


@app.route("/api/status")
def api_status():
    return jsonify({
        "status": scrape_state["status"],
        "message": scrape_state["message"],
        "progress": scrape_state["progress"],
        "count": len(scrape_state["businesses"]),
    })


@app.route("/api/logs")
def api_logs():
    """Return new log lines since the cursor position."""
    after = int(request.args.get("after", 0))
    logs = scrape_state["logs"]
    new_lines = logs[after:]
    return jsonify({"lines": new_lines, "cursor": len(logs)})


@app.route("/api/results")
def api_results():
    return jsonify(scrape_state["businesses"])


@app.route("/api/download/csv")
def download_csv():
    businesses = scrape_state["businesses"]
    if not businesses:
        return "No data to download", 404

    output = io.StringIO()
    fieldnames = [
        "name", "document_number", "status", "filing_date", "fei_ein",
        "principal_address", "mailing_address", "registered_agent",
        "state", "last_event", "event_date_filed",
        "officer_title", "officer_name", "category", "detail_url", "scraped_date",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for biz in businesses:
        writer.writerow(biz)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=sunbiz_businesses.csv"},
    )


@app.route("/api/download/json")
def download_json():
    businesses = scrape_state["businesses"]
    if not businesses:
        return "No data to download", 404

    return Response(
        json.dumps(businesses, indent=2),
        mimetype="application/json",
        headers={"Content-Disposition": "attachment; filename=sunbiz_businesses.json"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)

"""
RegPulse NG - Automated Nigerian Regulatory Intelligence Platform
Fully automated monitoring of CBN, SEC, NDPC, NERC and more.
"""

import os
import sqlite3
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, List, Tuple
from contextlib import contextmanager

from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
import feedparser
import requests
from bs4 import BeautifulSoup

# ============== CONFIG ==============
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "regpulse.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Official sources
FEEDS = {
    "CBN": {
        "name": "Central Bank of Nigeria",
        "api": "https://www.cbn.gov.ng/api/GetAllCirculars",
        "color": "#006400"
    },
    "SEC": {
        "name": "Securities and Exchange Commission",
        "circulars": "https://sec.gov.ng/feeds/circulars.rss",
        "color": "#1a365d"
    }
}

SECTOR_KEYWORDS = {
    "Fintech / Payments": ["payment", "fintech", "psp", "mobile money", "instant payment", "nip", "atm", "pos", "bv n", "wallet"],
    "Virtual Assets / Crypto": ["virtual asset", "crypto", "digital asset", "vasp", "blockchain", "token"],
    "Data Protection": ["data protection", "ndpr", "ndpa", "privacy", "personal data", "dpo"],
    "Banking / AML": ["aml", "cft", "kyc", "customer due diligence", "sanctions", "pep", "suspicious"],
    "Capital Markets": ["capital market", "securities", "broker", "fund manager", "ipo", "listing", "cmo"],
    "Energy / Power": ["electricity", "nerc", "disco", "mini-grid", "meter", "power"],
    "Corporate / Governance": ["corporate governance", "board", "director", "cama", "company"]
}

app = FastAPI(
    title="RegPulse NG",
    description="Automated Nigerian Regulatory Intelligence Platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== DATABASE ==============
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                regulator TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                link TEXT,
                published TEXT,
                published_ts REAL,
                sector TEXT,
                doc_type TEXT,
                raw_content TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_published ON items(published_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_regulator ON items(regulator)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sector ON items(sector)")
        conn.commit()

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

# ============== HELPERS ==============
def make_id(regulator: str, title: str, link: str) -> str:
    raw = f"{regulator}|{title}|{link}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

def classify_sector(title: str, summary: str = "") -> str:
    text = (title + " " + (summary or "")).lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return sector
    return "General / Other"

def classify_doc_type(title: str) -> str:
    t = title.lower()
    if "exposure draft" in t or "draft guideline" in t:
        return "Exposure Draft"
    if "circular" in t:
        return "Circular"
    if "guideline" in t or "framework" in t:
        return "Guideline / Framework"
    if "press" in t or "statement" in t:
        return "Press Release"
    if "notice" in t:
        return "Notice"
    return "Regulatory Update"

def parse_date(entry) -> Tuple[str, float]:
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat(), dt.timestamp()
        if hasattr(entry, "updated_parsed") and entry.updated_parsed:
            dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat(), dt.timestamp()
    except Exception:
        pass
    now = datetime.now(timezone.utc)
    return now.isoformat(), now.timestamp()

# ============== INGESTION ==============
def fetch_rss(url: str) -> list:
    try:
        headers = {"User-Agent": "RegPulseNG/1.0 (Regulatory Intelligence Bot)"}
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        return feed.entries
    except Exception as e:
        print(f"[INGEST] Error fetching RSS {url}: {e}")
        return []

def ingest_sec():
    """Ingest SEC circulars via official RSS."""
    entries = fetch_rss(FEEDS["SEC"]["circulars"])
    new_count = 0
    with get_db() as conn:
        for entry in entries:
            title = getattr(entry, "title", "").strip()
            if not title:
                continue
            link = getattr(entry, "link", "") or getattr(entry, "id", "")
            # Fix relative links
            if link and link.startswith("http://localhost"):
                link = link.replace("http://localhost", "https://sec.gov.ng")
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
            if summary:
                summary = BeautifulSoup(summary, "lxml").get_text(separator=" ", strip=True)[:600]
            published, published_ts = parse_date(entry)
            item_id = make_id("SEC", title, link)
            if conn.execute("SELECT id FROM items WHERE id = ?", (item_id,)).fetchone():
                continue
            sector = classify_sector(title, summary)
            doc_type = classify_doc_type(title)
            conn.execute("""
                INSERT INTO items (id, regulator, title, summary, link, published, published_ts, sector, doc_type, raw_content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (item_id, "SEC", title, summary, link, published, published_ts, sector, doc_type, summary))
            new_count += 1
        conn.commit()
    if new_count:
        print(f"[INGEST] SEC: {new_count} new items")
    return new_count

def ingest_cbn():
    """Ingest CBN circulars via official JSON API."""
    try:
        headers = {"User-Agent": "RegPulseNG/1.0 (Regulatory Intelligence Bot)"}
        resp = requests.get(FEEDS["CBN"]["api"], headers=headers, timeout=25)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[INGEST] CBN API error: {e}")
        return 0

    new_count = 0
    with get_db() as conn:
        for item in data:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            link = item.get("link") or ""
            if link and not link.startswith("http"):
                link = "https://www.cbn.gov.ng" + link
            summary = (item.get("description") or item.get("keywords") or "").strip()[:600]
            ref = (item.get("refNo") or "").strip()
            if ref:
                summary = f"[{ref}] {summary}".strip()

            # Parse date dd/mm/yyyy
            pub_str = item.get("documentDate") or ""
            published_ts = datetime.now(timezone.utc).timestamp()
            published = datetime.now(timezone.utc).isoformat()
            try:
                if pub_str:
                    dt = datetime.strptime(pub_str.strip(), "%d/%m/%Y").replace(tzinfo=timezone.utc)
                    published = dt.isoformat()
                    published_ts = dt.timestamp()
            except Exception:
                pass

            item_id = make_id("CBN", title, link)
            if conn.execute("SELECT id FROM items WHERE id = ?", (item_id,)).fetchone():
                continue
            sector = classify_sector(title, summary)
            doc_type = classify_doc_type(title)
            conn.execute("""
                INSERT INTO items (id, regulator, title, summary, link, published, published_ts, sector, doc_type, raw_content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (item_id, "CBN", title, summary, link, published, published_ts, sector, doc_type, summary))
            new_count += 1
        conn.commit()
    if new_count:
        print(f"[INGEST] CBN: {new_count} new items")
    return new_count

def run_ingestion():
    print(f"[INGEST] Starting scheduled ingestion at {datetime.now().isoformat()}")
    total = 0
    total += ingest_cbn()
    total += ingest_sec()
    print(f"[INGEST] Done. {total} new items total.")
    return total

# ============== API MODELS ==============
class ItemOut(BaseModel):
    id: str
    regulator: str
    title: str
    summary: Optional[str]
    link: Optional[str]
    published: Optional[str]
    sector: Optional[str]
    doc_type: Optional[str]

# ============== ROUTES ==============
@app.on_event("startup")
def startup():
    init_db()
    # Run initial ingestion
    run_ingestion()
    # Schedule every 30 minutes
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_ingestion, "interval", minutes=30, id="ingest")
    scheduler.start()
    print("[STARTUP] RegPulse NG ready. Scheduler running every 30 min.")

@app.get("/api/items", response_model=List[ItemOut])
def list_items(
    regulator: Optional[str] = None,
    sector: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    sql = "SELECT * FROM items WHERE 1=1"
    params = []
    if regulator:
        sql += " AND regulator = ?"
        params.append(regulator)
    if sector:
        sql += " AND sector = ?"
        params.append(sector)
    if q:
        sql += " AND (title LIKE ? OR summary LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%"])
    sql += " ORDER BY published_ts DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

@app.get("/api/stats")
def stats():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        by_reg = conn.execute("SELECT regulator, COUNT(*) as c FROM items GROUP BY regulator").fetchall()
        by_sector = conn.execute("SELECT sector, COUNT(*) as c FROM items GROUP BY sector ORDER BY c DESC").fetchall()
        latest = conn.execute("SELECT published FROM items ORDER BY published_ts DESC LIMIT 1").fetchone()
    return {
        "total_items": total,
        "by_regulator": {r["regulator"]: r["c"] for r in by_reg},
        "by_sector": {r["sector"]: r["c"] for r in by_sector},
        "latest_update": latest["published"] if latest else None
    }

@app.post("/api/ingest")
def trigger_ingest(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_ingestion)
    return {"status": "ingestion started"}

@app.get("/api/regulators")
def regulators():
    return [{"code": k, "name": v["name"], "color": v.get("color", "#333")} for k, v in FEEDS.items()]

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))

# Mount static files
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)

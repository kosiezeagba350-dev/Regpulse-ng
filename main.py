"""
RegPulse NG - Automated Nigerian Regulatory Intelligence Platform
Fully automated monitoring of CBN, SEC, NDPC, NERC and more.
"""

import os
import re
import sqlite3
import hashlib
import secrets
import threading
from datetime import datetime, timezone
from typing import Optional, List, Tuple
from contextlib import contextmanager, asynccontextmanager

from fastapi import FastAPI, Query, BackgroundTasks, Depends, Header, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
import feedparser
import requests
from bs4 import BeautifulSoup
INGEST_KEY = os.environ.get("INGEST_KEY", "")

def verify_key(x_api_key: str = Header(default="")):
    if not INGEST_KEY or x_api_key != INGEST_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
# ============== CONFIG ==============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

# On Render with a Persistent Disk, set DB_PATH=/var/data/regpulse.db
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "regpulse.db"))
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Secret needed to call POST /api/ingest (set INGEST_KEY in Render > Environment)
INGEST_KEY = os.environ.get("INGEST_KEY", "")
INGEST_INTERVAL_MINUTES = int(os.environ.get("INGEST_INTERVAL_MINUTES", "30"))
USER_AGENT = "RegPulseNG/1.0 (Regulatory Intelligence Bot)"

# Official sources
FEEDS = {
    "CBN": {
        "name": "Central Bank of Nigeria",
        "api": "https://www.cbn.gov.ng/api/GetAllCirculars",
        "color": "#006400",
    },
    "SEC": {
        "name": "Securities and Exchange Commission",
        "circulars": "https://sec.gov.ng/feeds/circulars.rss",
        "color": "#1a365d",
    },
}

SECTOR_KEYWORDS = {
    "Fintech / Payments": ["payment", "fintech", "psp", "mobile money", "instant payment", "nip", "atm", "pos", "bvn", "wallet"],
    "Virtual Assets / Crypto": ["virtual asset", "crypto", "digital asset", "vasp", "blockchain", "token"],
    "Data Protection": ["data protection", "ndpr", "ndpa", "privacy", "personal data", "dpo"],
    "Banking / AML": ["aml", "cft", "kyc", "customer due diligence", "sanctions", "pep", "suspicious"],
    "Capital Markets": ["capital market", "securities", "broker", "brokerage", "fund manager", "ipo", "listing", "cmo"],
    "Energy / Power": ["electricity", "nerc", "disco", "mini-grid", "meter", "metering", "power"],
    "Corporate / Governance": ["corporate governance", "board", "director", "cama", "company", "companies"],
}


def _compile_keywords(keywords: List[str]):
    # Whole-word match (allows a plural "s"), so "pos" does not match "position"
    body = "|".join(re.escape(k) for k in keywords)
    return re.compile(r"\b(?:" + body + r")(?:s|es)?\b", re.IGNORECASE)


SECTOR_PATTERNS = [(sector, _compile_keywords(kws)) for sector, kws in SECTOR_KEYWORDS.items()]

# ============== INGESTION STATUS ==============
_ingest_lock = threading.Lock()
LAST_INGEST = {"started": None, "finished": None, "new_items": 0, "by_source": {}, "errors": {}}


# ============== DATABASE ==============
def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = _connect()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
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
    finally:
        conn.close()


@contextmanager
def get_db():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============== HELPERS ==============
def make_id(regulator: str, title: str, link: str) -> str:
    raw = f"{regulator}|{title}|{link}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def classify_sector(title: str, summary: str = "") -> str:
    text = f"{title} {summary or ''}"
    for sector, pattern in SECTOR_PATTERNS:
        if pattern.search(text):
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
        if getattr(entry, "published_parsed", None):
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat(), dt.timestamp()
        if getattr(entry, "updated_parsed", None):
            dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat(), dt.timestamp()
    except Exception:
        pass
    now = datetime.now(timezone.utc)
    return now.isoformat(), now.timestamp()


def save_item(conn, regulator: str, title: str, summary: str, link: str,
              published: str, published_ts: float) -> bool:
    """Insert an item if it is new. Returns True when a new row was added."""
    item_id = make_id(regulator, title, link)
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO items
            (id, regulator, title, summary, link, published, published_ts, sector, doc_type, raw_content)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_id, regulator, title, summary, link, published, published_ts,
            classify_sector(title, summary), classify_doc_type(title), summary,
        ),
    )
    return cur.rowcount > 0


# ============== INGESTION ==============
def fetch_rss(url: str) -> list:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    return feedparser.parse(resp.content).entries


def ingest_sec() -> int:
    """Ingest SEC circulars via official RSS."""
    try:
        entries = fetch_rss(FEEDS["SEC"]["circulars"])
    except Exception as e:
        print(f"[INGEST] SEC error: {e}")
        LAST_INGEST["errors"]["SEC"] = str(e)[:200]
        return 0

    new_count = 0
    with get_db() as conn:
        for entry in entries:
            title = (getattr(entry, "title", "") or "").strip()
            if not title:
                continue
            link = getattr(entry, "link", "") or getattr(entry, "id", "") or ""
            if link.startswith("http://localhost"):
                link = link.replace("http://localhost", "https://sec.gov.ng")
            elif link.startswith("/"):
                link = "https://sec.gov.ng" + link
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
            if summary:
                summary = BeautifulSoup(summary, "html.parser").get_text(separator=" ", strip=True)[:600]
            published, published_ts = parse_date(entry)
            if save_item(conn, "SEC", title, summary, link, published, published_ts):
                new_count += 1
    if new_count:
        print(f"[INGEST] SEC: {new_count} new items")
    return new_count


def _extract_list(data) -> list:
    """The CBN API normally returns a list, but tolerate a wrapped object."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                return value
    return []


def ingest_cbn() -> int:
    """Ingest CBN circulars via official JSON API."""
    try:
        resp = requests.get(FEEDS["CBN"]["api"], headers={"User-Agent": USER_AGENT}, timeout=25)
        resp.raise_for_status()
        data = _extract_list(resp.json())
    except Exception as e:
        print(f"[INGEST] CBN API error: {e}")
        LAST_INGEST["errors"]["CBN"] = str(e)[:200]
        return 0

    new_count = 0
    with get_db() as conn:
        for item in data:
            if not isinstance(item, dict):
                continue
            title = (item.get("title") or "").strip()
            if not title:
                continue
            link = item.get("link") or ""
            if link and not link.startswith("http"):
                link = "https://www.cbn.gov.ng" + (link if link.startswith("/") else "/" + link)
            summary = (item.get("description") or item.get("keywords") or "").strip()[:600]
            ref = (item.get("refNo") or "").strip()
            if ref:
                summary = f"[{ref}] {summary}".strip()

            # Parse date dd/mm/yyyy
            now = datetime.now(timezone.utc)
            published, published_ts = now.isoformat(), now.timestamp()
            pub_str = (item.get("documentDate") or "").strip()
            if pub_str:
                try:
                    dt = datetime.strptime(pub_str, "%d/%m/%Y").replace(tzinfo=timezone.utc)
                    published, published_ts = dt.isoformat(), dt.timestamp()
                except ValueError:
                    pass

            if save_item(conn, "CBN", title, summary, link, published, published_ts):
                new_count += 1
    if new_count:
        print(f"[INGEST] CBN: {new_count} new items")
    return new_count


def run_ingestion() -> int:
    # Skip if another ingestion is already running
    if not _ingest_lock.acquire(blocking=False):
        print("[INGEST] Already running, skipping.")
        return 0
    try:
        print(f"[INGEST] Starting ingestion at {datetime.now(timezone.utc).isoformat()}")
        LAST_INGEST["started"] = datetime.now(timezone.utc).isoformat()
        LAST_INGEST["errors"] = {}
        cbn = ingest_cbn()
        sec = ingest_sec()
        total = cbn + sec
        LAST_INGEST.update({
            "finished": datetime.now(timezone.utc).isoformat(),
            "new_items": total,
            "by_source": {"CBN": cbn, "SEC": sec},
        })
        print(f"[INGEST] Done. {total} new items total.")
        return total
    finally:
        _ingest_lock.release()


# ============== APP ==============
scheduler = BackgroundScheduler(timezone="UTC")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # First run starts immediately in the background, so startup is not blocked
    scheduler.add_job(
        run_ingestion,
        "interval",
        minutes=INGEST_INTERVAL_MINUTES,
        id="ingest",
        next_run_time=datetime.now(timezone.utc),
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    print(f"[STARTUP] RegPulse NG ready. Scheduler running every {INGEST_INTERVAL_MINUTES} min.")
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(
    title="RegPulse NG",
    description="Automated Nigerian Regulatory Intelligence Platform",
    version="1.0.1",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def verify_key(x_api_key: str = Header(default="")):
    if not INGEST_KEY:
        raise HTTPException(status_code=503, detail="Ingest is disabled: INGEST_KEY is not configured")
    if not secrets.compare_digest(x_api_key.encode(), INGEST_KEY.encode()):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ============== API MODELS ==============
class ItemOut(BaseModel):
    id: str
    regulator: str
    title: str
    summary: Optional[str] = None
    link: Optional[str] = None
    published: Optional[str] = None
    sector: Optional[str] = None
    doc_type: Optional[str] = None


# ============== ROUTES ==============
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/items", response_model=List[ItemOut])
def list_items(
    regulator: Optional[str] = None,
    sector: Optional[str] = None,
    q: Optional[str] = Query(None, max_length=100),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
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
        by_reg = conn.execute("SELECT regulator, COUNT(*) AS c FROM items GROUP BY regulator").fetchall()
        by_sector = conn.execute("SELECT sector, COUNT(*) AS c FROM items GROUP BY sector ORDER BY c DESC").fetchall()
        latest = conn.execute("SELECT published FROM items ORDER BY published_ts DESC LIMIT 1").fetchone()
    return {
        "total_items": total,
        "by_regulator": {r["regulator"]: r["c"] for r in by_reg},
        "by_sector": {r["sector"]: r["c"] for r in by_sector},
        "latest_update": latest["published"] if latest else None,
        "last_ingest": LAST_INGEST,
    }


", @app.post("/api/ingestdependencies=[Depends(verify_key)])
def trigger_ingest(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_ingestion)
    return {"status": "ingestion started"}


@app.get("/api/regulators")
def regulators():
    return [{"code": k, "name": v["name"], "color": v.get("color", "#333")} for k, v in FEEDS.items()]


@app.get("/", response_class=HTMLResponse)
def dashboard():
    index = os.path.join(STATIC_DIR, "index.html")
    if not os.path.isfile(index):
        raise HTTPException(status_code=404, detail="Dashboard file not found")
    return FileResponse(index)


# Mount static files
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
"""
RegPulse NG - Automated Nigerian Regulatory Intelligence Platform
Fully automated monitoring of CBN, SEC, NDPC, NERC and more.
"""
app = FastAPI(
    title="RegPulse NG",
    description="Automated Nigerian Regulatory Intelligence Platform",
    version="1.0.2",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

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
        return [dict(row) for row in rows]


@app.get("/api/stats")
def stats():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        by_regulator = conn.execute(
            "SELECT regulator, COUNT(*) AS c FROM items GROUP BY regulator"
        ).fetchall()
        by_sector = conn.execute(
            "SELECT sector, COUNT(*) AS c FROM items GROUP BY sector ORDER BY c DESC"
        ).fetchall()
        latest = conn.execute(
            "SELECT published FROM items ORDER BY published_ts DESC LIMIT 1"
        ).fetchone()

    return {
        "total_items": total,
        "by_regulator": {row["regulator"]: row["c"] for row in by_regulator},
        "by_sector": {row["sector"]: row["c"] for row in by_sector},
        "latest_update": latest["published"] if latest else None,
        "last_ingest": LAST_INGEST,
    }


@app.post("/api/ingest", dependencies=[Depends(verify_key)])
def trigger_ingest(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_ingestion)
    return {"status": "ingestion started"}


@app.get("/api/regulators")
def regulators():
    return [
        {"code": key, "name": value["name"], "color": value.get("color", "#333")}
        for key, value in FEEDS.items()
    ]


@app.get("/", response_class=HTMLResponse)
def dashboard():
    index = os.path.join(STATIC_DIR, "index.html")
    if not os.path.isfile(index):
        raise HTTPException(status_code=404, detail="Dashboard file not found")
    return FileResponse(index)


# Mount static files if the folder exists.
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        reload=False,
    )
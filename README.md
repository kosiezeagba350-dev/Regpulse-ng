# RegPulse NG

**Automated Nigerian Regulatory Intelligence Platform**

Fully automated monitoring of official regulatory feeds from the Central Bank of Nigeria (CBN) and the Securities and Exchange Commission (SEC). Built for partners and teams at top-tier Nigerian law firms.

## Features

- Automatically pulls new circulars, guidelines and notices from official sources
- Classifies each item by sector (Fintech, Virtual Assets, Data Protection, Banking/AML, Capital Markets, etc.)
- Clean dashboard with search, filters and one-click access to original official documents
- Runs on a schedule (every 30 minutes) with a manual refresh button
- Zero document upload required — pure automated intelligence

## Quick Start (Mac / Linux)

```bash
# 1. Unzip and enter the folder
cd regpulse-ng

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start the app
python3 main.py
```

Open your browser: **http://localhost:8000**

## Windows

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Then open http://localhost:8000

## First Run

On first launch the app will automatically download the latest circulars from CBN and SEC. This can take 30–90 seconds depending on your connection. Subsequent refreshes are much faster.

## API Endpoints

- `GET /api/items` – list updates (supports `regulator`, `sector`, `q`, `limit`, `offset`)
- `GET /api/stats` – dashboard statistics
- `POST /api/ingest` – trigger immediate ingestion
- `GET /api/regulators` – list monitored regulators

## Project Structure

```
regpulse-ng/
├── main.py              # Backend + automated ingestion engine
├── static/
│   └── index.html       # Dashboard UI
├── requirements.txt
└── README.md
```

A `data/` folder with the SQLite database is created automatically on first run.

## Notes

- The app only uses public official sources (CBN API + SEC RSS).
- No user documents are uploaded or stored.
- Designed to impress partners at firms like Olaniwun Ajayi LP.

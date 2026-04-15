"""
Loads VN30 and HNX30 index constituents from the SSI FC Data API
into data.indexcomponent in MySQL.

Usage:
    python dataSSI/load_index_components.py
    python dataSSI/load_index_components.py --index VN30
"""
import argparse
import ast
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────────

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")   
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "5455"))  
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "stream_flow")
MYSQL_DB = "data"

SSI_URL = os.getenv("url", "https://fc-data.ssi.com.vn/").rstrip("/")
SSI_STREAM_URL = os.getenv("stream_url", "https://fc-datahub.ssi.com.vn/").rstrip("/")
SSI_API_BASE = f"{SSI_URL}/api/v2/Market"
CONSUMER_ID = os.getenv("consumerID", "")
CONSUMER_SECRET = os.getenv("consumerSecret", "")

# ── Auth ────────────────────────────────────────────────────────────────────────────

def get_token() -> str:
    resp = requests.post(
        f"{SSI_API_BASE}/AccessToken",
        json={"consumerID": CONSUMER_ID, "consumerSecret": CONSUMER_SECRET},
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("data", {}).get("accessToken")
    if not token:
        raise RuntimeError(f"No access token in response: {resp.json()}")
    return token


def api_get(path: str, params: dict, token: str) -> dict:
    resp = requests.get(
        f"{SSI_API_BASE}/{path}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    if resp.status_code == 401:
        logger.warning("Token expired, refreshing...")
        token = get_token()
        resp = requests.get(
            f"{SSI_API_BASE}/{path}",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
    resp.raise_for_status()
    return resp.json()


# ── DB helpers ────────────────────────────────────────────────────────────────────

def connect_db():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_exchanges(conn) -> dict[str, int]:
    """
    Ensures HOSE, HNX, UPCOM exchange records exist in data.exchange.
    Returns a dict mapping exchange_name → exchange_key.
    """
    exchanges = [
        ("HOSE", "Sở Giao dịch Chứng khoán TP.HCM"),
        ("HNX", "Sở Giao dịch Chứng khoán Hà Nội"),
        ("UPCOM", "UPCOM"),
    ]
    result = {}
    with conn.cursor() as cur:
        for name, full_name in exchanges:
            cur.execute(
                "INSERT INTO data.exchange (exchange_name) VALUES (%s) "
                "ON DUPLICATE KEY UPDATE exchange_name = VALUES(exchange_name)",
                (name,),
            )
            cur.execute(
                "SELECT exchange_key FROM data.exchange WHERE exchange_name = %s",
                (name,),
            )
            row = cur.fetchone()
            result[name] = row["exchange_key"] if row else None
    return result


def get_exchange_key(exchange_map: dict[str, int], exchange_name: str) -> int | None:
    return exchange_map.get(exchange_name)


def upsert_components(
    conn, index_id: str, exchange_key: int, effective_date: date, components: list[dict]
):
    """
    Upserts a list of {StockSymbol, Isin, Weight} objects into data.indexcomponent.
    Weight is optional.
    """
    if not components:
        return

    rows = []
    for comp in components:
        symbol = (comp.get("StockSymbol") or comp.get("stockSymbol") or "").strip()
        weight_str = comp.get("Weight") or comp.get("weight")
        weight = float(weight_str) if weight_str is not None else None
        if not symbol:
            continue
        rows.append((index_id, symbol, exchange_key, weight, effective_date))

    if not rows:
        return

    sql = """
        INSERT INTO data.indexcomponent
            (index_id, symbol, exchange_key, weight, effective_date)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            exchange_key = VALUES(exchange_key),
            weight = COALESCE(VALUES(weight), weight)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    logger.info("  Upserted %d rows for index_id=%s", len(rows), index_id)


# ── Fetch & parse ──────────────────────────────────────────────────────────────

def fetch_index_components(index_id: str, token: str) -> list[dict]:
    """
    Fetches index components from SSI API.
    Returns a list of {StockSymbol, Isin, Weight?} dicts.
    """
    params = {"indexCode": index_id, "pageIndex": 1, "pageSize": 100}
    data = api_get("IndexComponents", params, token)

    # SSI returns components in top-level "data" array, not "dataList"
    raw_items = data.get("data") or data.get("dataList") or []
    if isinstance(raw_items, dict):
        raw_items = raw_items.get("data") or raw_items.get("dataList") or []

    rows: list[dict] = []
    for item in raw_items:
        index_code = item.get("IndexCode", index_id)
        components_raw = item.get("IndexComponent")

        # IndexComponent can be a native JSON array or a stringified Python list
        components: list
        if isinstance(components_raw, list):
            components = components_raw
        elif isinstance(components_raw, str):
            try:
                components = json.loads(components_raw)
            except Exception:
                try:
                    components = ast.literal_eval(components_raw)
                except Exception as e:
                    logger.warning("  Could not parse IndexComponent for %s: %s", index_code, e)
                    continue
        else:
            continue

        if not isinstance(components, list):
            continue

        for comp in components:
            rows.append({
                "StockSymbol": comp.get("StockSymbol") or "",
                "Isin": comp.get("Isin") or "",
                "Weight": comp.get("Weight"),
            })

        logger.info(
            "  IndexCode=%s TotalSymbolNo=%s Got %d symbols",
            index_code,
            item.get("TotalSymbolNo"),
            len(components),
        )

    return rows


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load VN30 / HNX30 index components from SSI API")
    parser.add_argument(
        "--index",
        default="VN30,HNX30",
        help="Comma-separated index IDs (default: VN30,HNX30)",
    )
    parser.add_argument(
        "--effective-date",
        default=date.today().isoformat(),
        help="Effective date in YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--mysql-host",
        default=MYSQL_HOST,
        help=f"MySQL host (default: {MYSQL_HOST})",
    )
    parser.add_argument(
        "--mysql-port",
        type=int,
        default=MYSQL_PORT,
        help=f"MySQL port (default: {MYSQL_PORT})",
    )
    args = parser.parse_args()

    indices = [i.strip().upper() for i in args.index.split(",") if i.strip()]
    effective_date = datetime.strptime(args.effective_date, "%Y-%m-%d").date()

    # Allow CLI args to override env defaults
    global MYSQL_HOST, MYSQL_PORT
    MYSQL_HOST = args.mysql_host
    MYSQL_PORT = args.mysql_port

    token = get_token()
    logger.info("Authenticated. Fetching %s", indices)

    conn = connect_db()
    exchange_map = ensure_exchanges(conn)
    loaded = 0

    for index_id in indices:
        logger.info("Processing index: %s", index_id)

        try:
            components = fetch_index_components(index_id, token)
        except Exception as e:
            logger.error("  Failed to fetch %s: %s — retrying once...", index_id, e)
            time.sleep(2)
            try:
                token = get_token()
                components = fetch_index_components(index_id, token)
            except Exception as e2:
                logger.error("  Failed again for %s: %s — skipping.", index_id, e2)
                continue

        if not components:
            logger.warning("  No components returned for %s", index_id)
            continue

        if index_id.startswith("VN"):
            exchange_name = "HOSE"
        elif index_id.startswith("HNX"):
            exchange_name = "HNX"
        else:
            exchange_name = "HOSE"

        exchange_key = get_exchange_key(exchange_map, exchange_name)
        if not exchange_key:
            logger.error("  exchange_key not found for %s", exchange_name)
            continue

        upsert_components(conn, index_id, exchange_key, effective_date, components)
        loaded += len(components)

    conn.close()
    logger.info("Done. Loaded %d total component rows.", loaded)


if __name__ == "__main__":
    main()

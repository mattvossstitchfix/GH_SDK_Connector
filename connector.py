"""Fivetran connector for Greenhouse Harvest API v3.
Syncs recruiting data: candidates, applications, jobs, offers, and more.
"""

import json
import re
import time

import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

BASE_URL = "https://harvest.greenhouse.io/v3"
TOKEN_URL = "https://auth.greenhouse.io/token"
PER_PAGE = 500

# Incremental tables — synced with updated_at[gte] high-watermark
INCREMENTAL_TABLES = [
    ("candidates",        "/candidates"),
    ("applications",      "/applications"),
    ("jobs",              "/jobs"),
    ("offers",            "/offers"),
    ("job_posts",         "/job_posts"),
    ("departments",       "/departments"),
    ("offices",           "/offices"),
    ("users",             "/users"),
    ("openings",          "/openings"),
    ("scorecards",        "/scorecards"),
    ("interviews",        "/interviews"),
]

# Lookup tables — small, always full-refreshed each sync
LOOKUP_TABLES = [
    ("sources",           "/sources"),
    ("rejection_reasons", "/rejection_reasons"),
    ("close_reasons",     "/close_reasons"),
    ("candidate_tags",    "/candidate_tags"),
]


def _safe_str(v: str) -> str:
    """Replace invalid UTF-8 sequences so DuckDB can store the string."""
    return v.encode("utf-8", errors="replace").decode("utf-8")


def normalize_record(record: dict) -> dict:
    """Serialize all nested dicts/lists to JSON strings and sanitize strings.

    The Greenhouse API returns highly variable nested structures (custom_fields,
    phone_numbers, addresses, tags, etc.) whose value types can differ between
    records. Serializing them prevents DuckDB type-inference conflicts at scale.
    Old records can also contain invalid UTF-8 sequences that DuckDB rejects.
    """
    out = {}
    for k, v in record.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=True)
        elif isinstance(v, str):
            out[k] = _safe_str(v)
        else:
            out[k] = v
    return out


def validate_configuration(configuration: dict):
    for key in ("client_id", "client_secret"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration: {key}")


def get_access_token(configuration: dict) -> str:
    resp = requests.post(
        TOKEN_URL,
        auth=(configuration["client_id"], configuration["client_secret"]),
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def parse_next_link(link_header: str) -> str | None:
    """Extract the next-page URL from a Link response header."""
    for part in link_header.split(","):
        m = re.match(r'\s*<([^>]+)>;\s*rel="next"', part.strip())
        if m:
            return m.group(1)
    return None


def fetch_page(
    url: str,
    token: str,
    configuration: dict,
    params: dict | None = None,
) -> tuple[list, str | None, str]:
    """GET one page. Handles token refresh (401), rate limits (429), server errors (5xx),
    and transient network errors (connection reset, timeout)."""
    for attempt in range(5):
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=60,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = 2 ** attempt
            log.warning(f"Network error ({e.__class__.__name__}) — retrying in {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            log.warning("Access token expired — refreshing")
            token = get_access_token(configuration)
            continue

        if resp.status_code == 404:
            log.warning(f"Endpoint not found (404): {url} — skipping table")
            return [], None, token

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 30))
            log.warning(f"Rate limited — waiting {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code >= 500:
            wait = 2 ** attempt
            log.warning(f"Server error {resp.status_code} — retrying in {wait}s")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        body = resp.json()
        records = body if isinstance(body, list) else body.get("data", [])
        next_url = parse_next_link(resp.headers.get("Link", ""))
        return records, next_url, token

    raise RuntimeError(f"Failed to fetch {url} after 5 attempts")


def sync_incremental(
    table: str,
    endpoint: str,
    token: str,
    configuration: dict,
    last_updated_at: str | None,
    test_mode: bool = False,
) -> tuple[str | None, str]:
    """Sync one incremental table. Returns (new high-watermark, token)."""
    first_page_params = {"per_page": PER_PAGE}
    if last_updated_at:
        first_page_params["updated_at[gte]"] = last_updated_at

    url = f"{BASE_URL}{endpoint}"
    new_high_watermark = last_updated_at
    page = count = 0

    while url:
        params = first_page_params if page == 0 else None
        records, next_url, token = fetch_page(url, token, configuration, params)

        for record in records:
            op.upsert(table=table, data=normalize_record(record))
            ts = record.get("updated_at")
            if ts and (new_high_watermark is None or ts > new_high_watermark):
                new_high_watermark = ts
            count += 1

        page += 1
        # In test mode, stop after the first page
        url = None if test_mode else next_url

    log.info(f"{table}: synced {count} record(s)")
    return new_high_watermark, token


def sync_full_refresh(
    table: str,
    endpoint: str,
    token: str,
    configuration: dict,
    test_mode: bool = False,
) -> str:
    """Full-refresh a lookup table. Returns updated token."""
    url = f"{BASE_URL}{endpoint}"
    page = count = 0

    while url:
        params = {"per_page": PER_PAGE} if page == 0 else None
        records, next_url, token = fetch_page(url, token, configuration, params)
        for record in records:
            op.upsert(table=table, data=normalize_record(record))
            count += 1
        page += 1
        url = None if test_mode else next_url

    log.info(f"{table}: synced {count} record(s) (full refresh)")
    return token


def schema(configuration: dict):
    tables = [name for name, _ in INCREMENTAL_TABLES + LOOKUP_TABLES]
    return [{"table": t, "primary_key": ["id"]} for t in tables]


def update(configuration: dict, state: dict):
    validate_configuration(configuration)
    token = get_access_token(configuration)
    log.info("Authenticated with Greenhouse Harvest API v3")

    test_mode = configuration.get("test_mode", "").lower() == "true"
    if test_mode:
        log.warning("Running in test mode — only first page per table will be synced")

    start_date = configuration.get("start_date")  # e.g. "2020-01-01T00:00:00Z"
    if start_date:
        log.info(f"start_date set — historical sync limited to records updated after {start_date}")

    new_state = dict(state)

    for table, endpoint in INCREMENTAL_TABLES:
        # Use saved high-watermark, falling back to start_date for first sync
        last_updated_at = state.get(f"{table}_updated_at") or start_date
        new_ts, token = sync_incremental(table, endpoint, token, configuration, last_updated_at, test_mode)
        if new_ts:
            new_state[f"{table}_updated_at"] = new_ts
        # Checkpoint after each table so a mid-sync restart doesn't re-fetch completed tables
        op.checkpoint(state=new_state)

    for table, endpoint in LOOKUP_TABLES:
        token = sync_full_refresh(table, endpoint, token, configuration, test_mode)

    op.checkpoint(state=new_state)
    log.info("Sync complete")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)

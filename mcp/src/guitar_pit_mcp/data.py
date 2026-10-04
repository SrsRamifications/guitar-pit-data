"""Load the Guitar Pit data feed, strip private fields, and serve it from a
locked-down, in-memory DuckDB database.

Safety model
------------
* Allowlist, not blocklist: only the columns named in the schemas below are
  ever loaded. Anything new the feed grows (and every community/Discord
  identifier it has today) stays out until someone adds it here on purpose.
* URLs lose their query string and fragment (Reverb share links carry
  ``bk=`` session tokens and utm tags).
* Scam ("fake") and junk ("ignored") rows are dropped, as are the URLs the
  moderators removed for being scams.
* After loading, the database has external access disabled and its
  configuration locked, so SQL from a client cannot read files, hit the
  network, install extensions or change settings. Only single SELECT
  statements are accepted, with a timeout and a row cap.
"""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import duckdb

DEFAULT_BASE_URL = "https://raw.githubusercontent.com/SrsRamifications/guitar-pit-data/main/"
CACHE_TTL_SECONDS = int(os.environ.get("GUITAR_PIT_CACHE_TTL", "900"))
QUERY_TIMEOUT_SECONDS = float(os.environ.get("GUITAR_PIT_QUERY_TIMEOUT", "10"))
MAX_ROWS_HARD_LIMIT = 2000

# --------------------------------------------------------------------------
# Schemas: (column, duckdb type, description). This is the allowlist.
# --------------------------------------------------------------------------

LISTINGS_SCHEMA: list[tuple[str, str, str]] = [
    ("url", "VARCHAR", "Listing URL (query string stripped). Unique key."),
    ("title", "VARCHAR", "Listing title as written by the seller (third-party text)."),
    ("status", "VARCHAR", "live | sold | removed (delisted without a confirmed sale)."),
    ("status_date", "TIMESTAMP", "When the current status was recorded (e.g. when it sold)."),
    ("posted_at", "TIMESTAMP", "When the listing was first found and posted to The Guitar Pit."),
    ("published_at", "TIMESTAMP", "When the seller originally published the listing (Reverb only)."),
    ("category", "VARCHAR", "Where it was found: reverb | marketplace | other | news | youtube | deals."),
    ("domain", "VARCHAR", "Host name of the listing URL, e.g. reverb.com."),
    ("brand", "VARCHAR", "Normalised brand, e.g. Gibson."),
    ("model", "VARCHAR", "Normalised model, e.g. Les Paul."),
    ("variant", "VARCHAR", "Model variant, e.g. Custom Shop."),
    ("model_key", "VARCHAR", "Slug grouping brand+model+variant; joins to price_sightings.model_key."),
    ("brand_line", "VARCHAR", "Product line, e.g. Ibanez Prestige."),
    ("brand_line_tier", "VARCHAR", "budget | budget-mid | mid | mid-high | high | premium | ultra."),
    ("instrument_type", "VARCHAR", "guitar, bass, amp, ..."),
    ("reverb_category", "VARCHAR", "Reverb's category, e.g. 'Electric Guitars / Solid Body'."),
    ("reverb_make", "VARCHAR", "Make as listed on Reverb."),
    ("reverb_model", "VARCHAR", "Model as listed on Reverb."),
    ("condition", "VARCHAR", "Brand New | Mint | Excellent | Very Good | Good | Fair | Poor | ..."),
    ("finish", "VARCHAR", "Finish/colour."),
    ("origin_country", "VARCHAR", "Country of manufacture (ISO-2)."),
    ("year", "VARCHAR", "Year text as listed (may be a range)."),
    ("model_year", "INTEGER", "Parsed model year, when known."),
    ("is_reissue", "BOOLEAN", "Listing is a reissue model."),
    ("handmade", "BOOLEAN", "Reverb 'handmade' flag."),
    ("price", "DOUBLE", "Current (or last) asking price, USD."),
    ("original_price", "DOUBLE", "Asking price before the seller's discount, USD."),
    ("previous_price", "DOUBLE", "Asking price before the most recent price change, USD."),
    ("sold_price", "DOUBLE", "Final price when sold, USD (only for status = sold)."),
    ("price_drop_percent", "DOUBLE", "Percent drop at the most recent price change."),
    ("price_drop_date", "TIMESTAMP", "When the most recent price drop happened."),
    ("seller_discount_pct", "DOUBLE", "Seller's advertised discount, percent."),
    ("shipping_us", "DOUBLE", "US shipping cost, USD."),
    ("offers_enabled", "BOOLEAN", "Seller accepts offers."),
    ("offer_count", "INTEGER", "Number of offers received (Reverb)."),
    ("views", "INTEGER", "Listing views (Reverb, at last check)."),
    ("watchers", "INTEGER", "Listing watchers (Reverb, at last check)."),
    ("seller_region", "VARCHAR", "Seller state/region code, e.g. CA."),
    ("seller_country", "VARCHAR", "Seller country (ISO-2)."),
    ("seller_rating", "DOUBLE", "Seller rating, 0-1."),
    ("seller_feedback_count", "INTEGER", "Seller's feedback count."),
    ("preferred_seller", "BOOLEAN", "Reverb Preferred Seller."),
    ("price_guide_url", "VARCHAR", "Reverb Price Guide link for this model, when known."),
    ("source", "VARCHAR", "'feed' for the current feed, or the archive quarter, e.g. '2026-Q2'."),
]

EVENTS_SCHEMA: list[tuple[str, str, str]] = [
    ("event", "VARCHAR", "listed | price_changed | sold | removed | relisted | enriched | brand_categorized | fake_removed."),
    ("ts", "TIMESTAMP", "When the event happened."),
    ("url", "VARCHAR", "Listing URL (query string stripped); joins to listings.url."),
    ("brand", "VARCHAR", "Brand at the time of the event."),
    ("brand_line", "VARCHAR", "Product line."),
    ("model", "VARCHAR", "Model."),
    ("variant", "VARCHAR", "Variant."),
    ("year", "VARCHAR", "Year text."),
    ("model_year", "INTEGER", "Parsed model year."),
    ("is_reissue", "BOOLEAN", "Reissue model."),
    ("condition", "VARCHAR", "Condition."),
    ("asking_price", "DOUBLE", "Asking price at the event, USD."),
    ("sold_price", "DOUBLE", "Sale price (sold events), USD."),
    ("previous_price", "DOUBLE", "Price before this change (price_changed events), USD."),
    ("category", "VARCHAR", "reverb | marketplace | news | deals | other | youtube."),
    ("days_listed", "DOUBLE", "Days the listing had been up at the event."),
    ("offer_count", "INTEGER", "Offers received."),
    ("views", "INTEGER", "Views."),
    ("watchers", "INTEGER", "Watchers."),
    ("seller_region", "VARCHAR", "Seller state/region."),
    ("seller_country", "VARCHAR", "Seller country."),
    ("seller_rating", "DOUBLE", "Seller rating, 0-1."),
    ("seller_feedback_count", "INTEGER", "Seller feedback count."),
    ("preferred_seller", "BOOLEAN", "Reverb Preferred Seller."),
    ("handmade", "BOOLEAN", "Handmade flag."),
    ("origin_country", "VARCHAR", "Country of manufacture."),
    ("finish", "VARCHAR", "Finish/colour."),
    ("published_at", "TIMESTAMP", "When the seller originally published the listing."),
]

PRICE_SIGHTINGS_SCHEMA: list[tuple[str, str, str]] = [
    ("model_key", "VARCHAR", "Model slug; joins to listings.model_key."),
    ("display_name", "VARCHAR", "Human-readable model name."),
    ("brand", "VARCHAR", "Brand."),
    ("model", "VARCHAR", "Model."),
    ("variant", "VARCHAR", "Variant."),
    ("price", "DOUBLE", "Observed asking price, USD."),
    ("sighted_at", "TIMESTAMP", "When the price was observed."),
    ("condition", "VARCHAR", "Condition."),
    ("url", "VARCHAR", "Listing URL (query string stripped)."),
    ("title", "VARCHAR", "Listing title (third-party text)."),
]

BRAND_SELL_THROUGH_SCHEMA: list[tuple[str, str, str]] = [
    ("brand", "VARCHAR", "Brand."),
    ("sold", "INTEGER", "All-time listings that sold."),
    ("total", "INTEGER", "All-time listings tracked."),
    ("sell_through_pct", "DOUBLE", "sold / total * 100."),
]

TABLES: dict[str, tuple[list[tuple[str, str, str]], str]] = {
    "listings": (LISTINGS_SCHEMA, "One row per tracked listing: current feed plus quarterly archives."),
    "events": (EVENTS_SCHEMA, "Lifecycle event log (listed, price changes, sold, removed, ...)."),
    "price_sightings": (PRICE_SIGHTINGS_SCHEMA, "Every observed asking price per model over time."),
    "brand_sell_through": (BRAND_SELL_THROUGH_SCHEMA, "All-time sold/total counts per brand, including listings pruned from the feed."),
}

# stats.json keys that are market data. Everything else (leaderboards, top
# posters, ...) is about community members and stays out.
MARKET_SUMMARY_KEYS = [
    "generatedAt", "totalLinks", "totalEver", "totalWithPrice", "filtered", "byMonth",
    "byCategory", "byCondition", "seasonal", "priceDistributionFiltered", "daysOnMarket",
    "byBrandPrice", "soldPrices", "sellThrough", "negotiationRate", "offerActivity",
    "shippingInsights", "watcherDemand", "offersEffect", "sellerType", "geoSellThrough",
]

KNOWN_EVENTS = {"listed", "price_changed", "sold", "removed", "relisted", "enriched", "brand_categorized", "fake_removed"}
KNOWN_SOURCES = {"", "reverb", "marketplace", "news", "deals", "other", "youtube"}
EXCLUDED_STATUSES = {"fake", "ignored"}


# --------------------------------------------------------------------------
# Value cleaning
# --------------------------------------------------------------------------

def clean_url(url: Any) -> str | None:
    if not url or not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if parts.scheme not in ("http", "https"):
        return None
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def domain_of(url: str | None) -> str | None:
    if not url:
        return None
    host = urlsplit(url).hostname or ""
    return host[4:] if host.startswith("www.") else host or None


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("$", "").replace(",", "").replace("%", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_ts(value: Any) -> str | None:
    """ISO timestamp -> naive UTC 'YYYY-MM-DD HH:MM:SS.ffffff' (all times are UTC)."""
    text = to_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat(sep=" ")


# --------------------------------------------------------------------------
# Row builders
# --------------------------------------------------------------------------

def listing_row(raw: dict[str, Any], source: str) -> dict[str, Any] | None:
    status = raw.get("status")
    url = clean_url(raw.get("url"))
    if not url or status in EXCLUDED_STATUSES:
        return None
    return {
        "url": url,
        "title": to_text(raw.get("title")),
        "status": to_text(status),
        "status_date": to_ts(raw.get("statusDate")),
        "posted_at": to_ts(raw.get("postedAt")),
        "published_at": to_ts(raw.get("publishedAt")),
        "category": to_text(raw.get("category")),
        "domain": domain_of(url),
        "brand": to_text(raw.get("brand")),
        "model": to_text(raw.get("model")),
        "variant": to_text(raw.get("variant")),
        "model_key": to_text(raw.get("modelKey")),
        "brand_line": to_text(raw.get("brandLine")),
        "brand_line_tier": to_text(raw.get("brandLineTier")),
        "instrument_type": to_text(raw.get("instrumentType")),
        "reverb_category": to_text(raw.get("reverbCategory")),
        "reverb_make": to_text(raw.get("reverbMake")),
        "reverb_model": to_text(raw.get("reverbModel")),
        "condition": to_text(raw.get("condition")),
        "finish": to_text(raw.get("finish")),
        "origin_country": to_text(raw.get("originCountry")),
        "year": to_text(raw.get("year")),
        "model_year": to_int(raw.get("modelYear")),
        "is_reissue": to_bool(raw.get("isReissue")),
        "handmade": to_bool(raw.get("handmade")),
        "price": to_float(raw.get("price")),
        "original_price": to_float(raw.get("originalPrice")),
        "previous_price": to_float(raw.get("previousPrice")),
        "sold_price": to_float(raw.get("soldPrice")),
        "price_drop_percent": to_float(raw.get("priceDropPercent")),
        "price_drop_date": to_ts(raw.get("priceDropDate")),
        "seller_discount_pct": to_float(raw.get("sellerDiscount")),
        "shipping_us": to_float(raw.get("shippingUS")),
        "offers_enabled": to_bool(raw.get("offersEnabled")),
        "offer_count": to_int(raw.get("offerCount")),
        "views": to_int(raw.get("views")),
        "watchers": to_int(raw.get("watchers")),
        "seller_region": to_text(raw.get("sellerRegion")),
        "seller_country": to_text(raw.get("sellerCountry")),
        "seller_rating": to_float(raw.get("sellerRating")),
        "seller_feedback_count": to_int(raw.get("sellerFeedbackCount")),
        "preferred_seller": to_bool(raw.get("preferredSeller")),
        "price_guide_url": clean_url(raw.get("reverbPriceGuideUrl")),
        "source": source,
    }


def _numeric_ok(value: str) -> bool:
    return value == "" or to_float(value) is not None


def _bool_ok(value: str) -> bool:
    return value in ("", "true", "false")


def repair_csv_fields(fields: list[str], width: int) -> list[str] | None:
    """Re-join values the upstream CSV writer split on commas.

    Seller names/locations containing commas come out as e.g.
    ``\"\"\"Marietta",GA,"United States\"\"\"``, which a CSV parser reads as three
    fields ('"Marietta', 'GA', 'United States"'). Glue such runs back into one
    value and pad the row back to the header width.
    """
    repaired: list[str] = []
    i = 0
    while i < len(fields):
        value = fields[i]
        if value.startswith('"') and not (len(value) > 1 and value.endswith('"')):
            parts = [value]
            while True:
                i += 1
                if i >= len(fields):
                    return None
                parts.append(fields[i])
                if fields[i].endswith('"'):
                    break
            value = ", ".join(parts)
        repaired.append(value.strip().strip('"').strip())
        i += 1
    if len(repaired) > width:
        return None
    return repaired + [""] * (width - len(repaired))


def event_row(raw: dict[str, str]) -> dict[str, Any] | None:
    """Convert one (repaired) analytics.csv row, or return None if it still
    doesn't line up with the header, rather than serve shifted values."""
    if None in raw or raw.get("event") not in KNOWN_EVENTS or raw.get("source") not in KNOWN_SOURCES:
        return None
    if not to_ts(raw.get("timestamp")):
        return None
    numeric = ("asking_price", "sold_price", "previous_price", "days_listed", "offer_count",
               "views", "watchers", "seller_rating", "seller_feedback")
    booleans = ("is_reissue", "preferred_seller", "handmade")
    if not all(_numeric_ok(raw.get(k) or "") for k in numeric):
        return None
    if not all(_bool_ok(raw.get(k) or "") for k in booleans):
        return None
    discord_id = raw.get("discord_id") or ""
    if discord_id and not discord_id.isdigit():
        return None
    return {
        "event": raw["event"],
        "ts": to_ts(raw["timestamp"]),
        # Never point anyone at a listing that was removed as a scam.
        "url": None if raw["event"] == "fake_removed" else clean_url(raw.get("url")),
        "brand": to_text(raw.get("brand")),
        "brand_line": to_text(raw.get("brand_line")),
        "model": to_text(raw.get("model")),
        "variant": to_text(raw.get("variant")),
        "year": to_text(raw.get("year")),
        "model_year": to_int(raw.get("model_year")),
        "is_reissue": to_bool(raw.get("is_reissue")),
        "condition": to_text(raw.get("condition")),
        "asking_price": to_float(raw.get("asking_price")),
        "sold_price": to_float(raw.get("sold_price")),
        "previous_price": to_float(raw.get("previous_price")),
        "category": to_text(raw.get("source")),
        "days_listed": to_float(raw.get("days_listed")),
        "offer_count": to_int(raw.get("offer_count")),
        "views": to_int(raw.get("views")),
        "watchers": to_int(raw.get("watchers")),
        "seller_region": to_text(raw.get("seller_region")),
        "seller_country": to_text(raw.get("seller_country")),
        "seller_rating": to_float(raw.get("seller_rating")),
        "seller_feedback_count": to_int(raw.get("seller_feedback")),
        "preferred_seller": to_bool(raw.get("preferred_seller")),
        "handmade": to_bool(raw.get("handmade")),
        "origin_country": to_text(raw.get("origin_country")),
        "finish": to_text(raw.get("finish")),
        "published_at": to_ts(raw.get("published_at")),
    }


def sighting_rows(model_key: str, model: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for sighting in model.get("sightings") or []:
        rows.append({
            "model_key": model_key,
            "display_name": to_text(model.get("displayName")),
            "brand": to_text(model.get("brand")),
            "model": to_text(model.get("model")),
            "variant": to_text(model.get("variant")),
            "price": to_float(sighting.get("price")),
            "sighted_at": to_ts(sighting.get("date")),
            "condition": to_text(sighting.get("condition")),
            "url": clean_url(sighting.get("url")),
            "title": to_text(sighting.get("title")),
        })
    return rows


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------

class Source:
    """Reads feed files from a local checkout or over HTTPS."""

    def __init__(self, data_dir: str | None = None, base_url: str | None = None):
        self.data_dir = Path(data_dir) if data_dir else None
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/") + "/"

    @classmethod
    def from_env(cls) -> "Source":
        return cls(os.environ.get("GUITAR_PIT_DATA_DIR"), os.environ.get("GUITAR_PIT_DATA_URL"))

    def read_text(self, name: str) -> str:
        if self.data_dir:
            return (self.data_dir / name).read_text(encoding="utf-8")
        request = urllib.request.Request(self.base_url + name, headers={"User-Agent": "guitar-pit-mcp"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")

    def read_json(self, name: str) -> Any:
        return json.loads(self.read_text(name))

    def describe(self) -> str:
        return str(self.data_dir) if self.data_dir else self.base_url


# --------------------------------------------------------------------------
# Snapshot: one immutable, locked database built from one fetch
# --------------------------------------------------------------------------

class QueryError(Exception):
    pass


@dataclass
class Snapshot:
    con: duckdb.DuckDBPyConnection
    loaded_at: float
    data_as_of: str | None
    market_summary: dict[str, Any]
    row_counts: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def _create_table(con: duckdb.DuckDBPyConnection, name: str, schema, rows: list[dict[str, Any]],
                  workdir: str) -> None:
    columns = ", ".join(f'"{col}" {typ}' for col, typ, _ in schema)
    con.execute(f"CREATE TABLE {name} ({columns})")
    if not rows:
        return
    # Bulk-load through a temp NDJSON file: ~100x faster than executemany.
    path = os.path.join(workdir, f"{name}.ndjson")
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps({col: row.get(col) for col, _, _ in schema}))
            fh.write("\n")
    as_text = ", ".join(f"'{col}': 'VARCHAR'" for col, _, _ in schema)
    casts = ", ".join(f'CAST("{col}" AS {typ})' for col, typ, _ in schema)
    con.execute(
        f"INSERT INTO {name} SELECT {casts} FROM read_json(?, format = 'newline_delimited', columns = {{{as_text}}})",
        [path],
    )


def build_snapshot(source: Source) -> Snapshot:
    stats = source.read_json("stats.json")
    notes: list[str] = []

    # Listings: current feed first so it wins over any archived copy.
    scam_urls = {
        clean_url(item.get("url"))
        for item in source.read_json("removed-urls.json")
        if "scam" in str(item.get("reason", "")).lower()
    }
    listings: dict[str, dict[str, Any]] = {}
    sources = [("feed", "links.json")] + [(q, f"archive/{q}.json") for q in stats.get("archiveYears") or []]
    for label, name in sources:
        try:
            raw_rows = source.read_json(name)
        except Exception as exc:  # a missing archive shouldn't take the server down
            notes.append(f"Could not load {name}: {exc}")
            continue
        for raw in raw_rows:
            if raw.get("status") == "fake":
                scam_urls.add(clean_url(raw.get("url")))
                continue
            row = listing_row(raw, label)
            if row and row["url"] not in listings:
                listings[row["url"]] = row
    for url in scam_urls:
        listings.pop(url, None)

    reader = csv.reader(io.StringIO(source.read_text("analytics.csv"), newline=""))
    header = next(reader, [])
    events, dropped = [], 0
    for fields in reader:
        fixed = repair_csv_fields(fields, len(header))
        row = event_row(dict(zip(header, fixed))) if fixed else None
        if row:
            if row["url"] in scam_urls:
                row["url"] = None
            events.append(row)
        else:
            dropped += 1
    if dropped:
        notes.append(
            f"{dropped} of {dropped + len(events)} event rows were dropped because they could "
            "not be parsed reliably; the events table may slightly undercount."
        )

    sightings: list[dict[str, Any]] = []
    for key, model in (source.read_json("price-history.json").get("models") or {}).items():
        sightings.extend(r for r in sighting_rows(key, model) if r["url"] not in scam_urls)

    sell_through = []
    for brand, counts in (source.read_json("sell-through-archive.json").get("brands") or {}).items():
        total = to_int(counts.get("total")) or 0
        sold = to_int(counts.get("sold")) or 0
        sell_through.append({
            "brand": brand, "sold": sold, "total": total,
            "sell_through_pct": round(sold / total * 100, 1) if total else None,
        })

    con = duckdb.connect(":memory:", config={"threads": 2, "memory_limit": "512MB"})
    rows_by_table = {
        "listings": list(listings.values()),
        "events": events,
        "price_sightings": sightings,
        "brand_sell_through": sell_through,
    }
    with tempfile.TemporaryDirectory(prefix="guitar-pit-mcp-") as workdir:
        for table, (schema, _) in TABLES.items():
            _create_table(con, table, schema, rows_by_table[table], workdir)

    # Lock it down. After this, client SQL can't touch files, the network,
    # extensions or settings. lock_configuration must come last.
    con.execute("SET autoinstall_known_extensions = false")
    con.execute("SET autoload_known_extensions = false")
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")

    return Snapshot(
        con=con,
        loaded_at=time.time(),
        data_as_of=stats.get("generatedAt"),
        market_summary={k: stats[k] for k in MARKET_SUMMARY_KEYS if k in stats},
        row_counts={t: len(r) for t, r in rows_by_table.items()},
        notes=notes,
    )


# --------------------------------------------------------------------------
# Query execution
# --------------------------------------------------------------------------

def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def check_read_only(sql: str) -> None:
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as exc:
        raise QueryError(f"SQL parse error: {exc}") from exc
    if len(statements) != 1:
        raise QueryError("Send exactly one SQL statement.")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise QueryError("Only read-only SELECT queries (including WITH ... SELECT) are allowed.")


def run_query(snapshot: Snapshot, sql: str, params: list[Any] | None = None,
              max_rows: int = 500) -> dict[str, Any]:
    check_read_only(sql)
    max_rows = max(1, min(int(max_rows), MAX_ROWS_HARD_LIMIT))
    cursor = snapshot.con.cursor()
    timer = threading.Timer(QUERY_TIMEOUT_SECONDS, cursor.interrupt)
    timer.start()
    try:
        cursor.execute(sql, params or [])
        columns = [d[0] for d in cursor.description or []]
        rows = cursor.fetchmany(max_rows + 1)
    except duckdb.InterruptException as exc:
        raise QueryError(f"Query exceeded the {QUERY_TIMEOUT_SECONDS:g}s time limit.") from exc
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc
    finally:
        timer.cancel()
        cursor.close()
    truncated = len(rows) > max_rows
    rows = rows[:max_rows]
    return {
        "columns": columns,
        "rows": [_jsonable(list(r)) for r in rows],
        "row_count": len(rows),
        "truncated": truncated,
    }


# --------------------------------------------------------------------------
# Store: caches a snapshot and rebuilds it when it goes stale
# --------------------------------------------------------------------------

class Store:
    def __init__(self, source: Source | None = None, ttl: int = CACHE_TTL_SECONDS):
        self.source = source or Source.from_env()
        self.ttl = ttl
        self._snapshot: Snapshot | None = None
        self._lock = threading.Lock()

    def get(self) -> Snapshot:
        snap = self._snapshot
        if snap and time.time() - snap.loaded_at < self.ttl:
            return snap
        with self._lock:
            snap = self._snapshot
            if snap and time.time() - snap.loaded_at < self.ttl:
                return snap
            try:
                self._snapshot = build_snapshot(self.source)
            except Exception:
                if snap is None:
                    raise
                # Keep serving stale data rather than failing; retry next time.
                snap.loaded_at = time.time() - self.ttl + 60
            return self._snapshot  # type: ignore[return-value]

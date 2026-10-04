import json
from pathlib import Path

import pytest

from guitar_pit_mcp.data import (
    QueryError,
    Source,
    build_snapshot,
    clean_url,
    repair_csv_fields,
    run_query,
    to_ts,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

CSV_HEADER = (
    "event,timestamp,url,brand,brand_line,model,variant,year,model_year,is_reissue,condition,"
    "asking_price,sold_price,previous_price,seller,source,posted_by,discord_id,days_listed,"
    "offer_count,views,watchers,seller_location,seller_region,seller_country,seller_rating,"
    "seller_feedback,preferred_seller,handmade,origin_country,finish,published_at"
)


@pytest.fixture
def snapshot(tmp_path):
    listing = {
        "url": "https://reverb.com/item/1-prs?bk=SECRET_TOKEN&utm_source=x",
        "title": "PRS Custom 24", "price": "$2,199", "status": "sold", "soldPrice": "$2,000",
        "brand": "PRS", "model": "Custom 24", "modelKey": "prs-custom-24",
        "postedAt": "2026-09-01T10:00:00+00:00", "sellerName": "Jane Doe",
        "sellerLocation": "Springfield, IL, United States", "sellerRegion": "IL",
        "postedBy": "Some Member", "postedByUsername": "member123", "messageId": "999",
        "discordTags": ["x"],
    }
    scam = {"url": "https://reverb.com/item/2-scam", "status": "live", "price": "$100"}
    fake = {"url": "https://reverb.com/item/3-fake", "status": "fake", "price": "$100"}
    (tmp_path / "archive").mkdir()
    (tmp_path / "links.json").write_text(json.dumps([listing, scam, fake]))
    (tmp_path / "archive" / "2026-Q3.json").write_text(json.dumps([]))
    (tmp_path / "removed-urls.json").write_text(json.dumps([{"url": scam["url"], "reason": "scam"}]))
    (tmp_path / "stats.json").write_text(json.dumps({
        "generatedAt": "2026-10-01T00:00:00Z", "archiveYears": ["2026-Q3"],
        "filtered": {"count": 1}, "leaderboard": [{"username": "member123"}],
        "topPosters": [{"username": "member123"}],
    }))
    (tmp_path / "price-history.json").write_text(json.dumps({"models": {"prs-custom-24": {
        "displayName": "PRS Custom 24", "brand": "PRS", "model": "Custom 24",
        "sightings": [{"price": 2199, "date": "2026-09-01T10:00:00Z", "condition": "Good",
                       "url": "https://reverb.com/item/1-prs?x=1", "title": "PRS"},
                      {"price": 100, "date": "2026-09-01T10:00:00Z", "url": "https://reverb.com/item/3-fake"}],
    }}}))
    (tmp_path / "sell-through-archive.json").write_text(json.dumps(
        {"brands": {"PRS": {"sold": 1, "total": 4}}}))
    rows = [
        CSV_HEADER,
        "sold,2026-09-02T00:00:00+00:00,https://reverb.com/item/1-prs?bk=SECRET,PRS,,Custom 24,,,,,"
        "Good,2199,2000,,Jane Doe,reverb,member123,123456789,1.5,0,10,2,"
        '"""Springfield",IL,"United States""",IL,US,1,10,false,false,US',
        "listed,2026-09-01T00:00:00+00:00,https://x.com/a,,,,,,,,,,,,,news,member123,,,,,,,,,,,,,,,",
        "listed,2026-09-01T00:00:00+00:00,https://reverb.com/item/2-scam,,,,,,,,,,,,,reverb,member123,,,,,,,,,,,,,,,",
    ]
    (tmp_path / "analytics.csv").write_text("\n".join(rows) + "\n")
    return build_snapshot(Source(data_dir=str(tmp_path)))


def all_values(snapshot, table):
    result = run_query(snapshot, f"SELECT * FROM {table}", max_rows=2000)
    return json.dumps(result)


def test_private_fields_never_loaded(snapshot):
    for table in ("listings", "events", "price_sightings"):
        dumped = all_values(snapshot, table)
        for secret in ("SECRET", "member123", "Some Member", "Jane Doe", "Springfield", "123456789", "999"):
            assert secret not in dumped, (table, secret)
    assert "leaderboard" not in json.dumps(snapshot.market_summary)
    assert "topPosters" not in json.dumps(snapshot.market_summary)


def test_scam_and_fake_listings_excluded(snapshot):
    urls = [r[0] for r in run_query(snapshot, "SELECT url FROM listings")["rows"]]
    assert urls == ["https://reverb.com/item/1-prs"]
    for table in ("events", "price_sightings"):
        dumped = all_values(snapshot, table)
        assert "2-scam" not in dumped and "3-fake" not in dumped, table


def test_values_parsed(snapshot):
    row = run_query(snapshot, "SELECT price, sold_price, posted_at FROM listings")["rows"][0]
    assert row == [2199.0, 2000.0, "2026-09-01T10:00:00"]


def test_misquoted_csv_row_repaired(snapshot):
    result = run_query(
        snapshot,
        "SELECT seller_region, seller_country, seller_rating, seller_feedback_count, origin_country "
        "FROM events WHERE event = 'sold'",
    )
    assert result["rows"] == [["IL", "US", 1.0, 10, "US"]]
    assert snapshot.row_counts["events"] == 3


@pytest.mark.parametrize("sql", [
    "CREATE TABLE x AS SELECT 1",
    "INSERT INTO listings (url) VALUES ('x')",
    "DELETE FROM listings",
    "SELECT 1; SELECT 2",
    "COPY listings TO 'out.csv'",
    "ATTACH 'other.db'",
    "SET enable_external_access = true",
    "INSTALL httpfs",
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM read_json('https://example.com/x.json')",
    "SELECT * FROM glob('/*')",
])
def test_unsafe_sql_rejected(snapshot, sql):
    with pytest.raises(QueryError):
        run_query(snapshot, sql)


def test_row_cap(snapshot):
    result = run_query(snapshot, "SELECT * FROM range(50)", max_rows=10)
    assert result["row_count"] == 10 and result["truncated"] is True


def test_helpers():
    assert clean_url("https://reverb.com/item/1?bk=abc#x") == "https://reverb.com/item/1"
    assert clean_url("javascript:alert(1)") is None
    assert to_ts("2026-01-01T05:00:00-05:00") == "2026-01-01 10:00:00"
    assert repair_csv_fields(['"A', "B", 'C"', "x"], 5) == ["A, B, C", "x", "", "", ""]
    assert repair_csv_fields(['"A', "B"], 5) is None


@pytest.mark.skipif(not (REPO_ROOT / "links.json").exists(), reason="needs the data checkout")
def test_real_feed_loads():
    snap = build_snapshot(Source(data_dir=str(REPO_ROOT)))
    assert snap.row_counts["listings"] > 1000
    assert snap.row_counts["events"] > 1000
    dumped = json.dumps(run_query(snap, "SELECT * FROM listings", max_rows=2000))
    assert "bk=" not in dumped and "utm_" not in dumped

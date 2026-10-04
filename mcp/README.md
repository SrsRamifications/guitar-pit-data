# Guitar Pit MCP server

A read-only [MCP](https://modelcontextprotocol.io) server for The Guitar Pit's
used-guitar market data. Connect it to Claude (Claude Desktop, claude.ai,
Claude Code, or any MCP client), then ask questions in plain English. Claude
writes the SQL, runs it here, and builds charts or analysis from the results.

> "What's the median sold price of a PRS Custom 24 by condition?"
> "Chart weekly listings vs. sales for Gibson and Fender since April."
> "Which brands sell fastest, and does enabling offers help?"

## What it exposes

| Tool | What it does |
|---|---|
| `describe_dataset` | Tables, columns, row counts, data freshness, caveats |
| `query` | Any single read-only DuckDB `SELECT` (row-capped, time-limited) |
| `search_listings` | Filter by brand, model, title text, status, condition and price |
| `price_history` | Price sightings and summary stats for a model |
| `market_summary` | The site's pre-computed market stats |

Tables: `listings` (feed plus quarterly archives), `events` (listed /
price_changed / sold / removed log), `price_sightings`, `brand_sell_through`.

Data is pulled from this repo's `main` branch on GitHub and cached for 15
minutes, so it stays current with the feed without any redeploys.

## What keeps it safe

- **Read-only, sandboxed SQL.** The data is loaded into an in-memory DuckDB
  database, then file, network and extension access are switched off and the
  settings are locked. Only single `SELECT` statements are accepted, with a
  10 s timeout and a 2,000-row cap. Nothing a client sends can write
  anywhere or reach outside the dataset.
- **Allowlisted fields.** Only the columns listed in `data.py` are loaded.
  Discord IDs, community usernames, message IDs, poster leaderboards, seller
  names and seller city are never loaded, and neither are the
  site's social and Discord growth metrics. A new field in the feed stays
  hidden until it is added to the allowlist.
- **Clean URLs.** Query strings are stripped, which removes Reverb share
  tokens (`bk=…`) and tracking tags.
- **No scams or junk.** Listings flagged fake or removed as scams, and
  non-listing pages, are excluded.

## Use it

### Claude Desktop / Claude Code (local)

Requires [uv](https://docs.astral.sh/uv/). Add to
`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "guitar-pit": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/SrsRamifications/guitar-pit-data#subdirectory=mcp", "guitar-pit-mcp"]
    }
  }
}
```

or for Claude Code:

```sh
claude mcp add guitar-pit -- uvx --from "git+https://github.com/SrsRamifications/guitar-pit-data#subdirectory=mcp" guitar-pit-mcp
```

The first install can be slow because this repo's history is large; after
that it's cached.

### Hosted (claude.ai custom connector)

Run it as an HTTP server and anyone can add the URL as a custom connector in
claude.ai with nothing to install:

```sh
guitar-pit-mcp --http --host 0.0.0.0 --port 8000   # serves /mcp
```

It is stateless, so it runs on any container host (Fly.io, Render, Cloud
Run, ...). Put it behind HTTPS. The data is public, so no auth is needed,
but rate-limit at the proxy if you expect heavy use.

## Configuration

| Env var | Default | |
|---|---|---|
| `GUITAR_PIT_DATA_DIR` | unset | Read from a local checkout instead of GitHub |
| `GUITAR_PIT_DATA_URL` | raw GitHub `main` | Base URL for the data files |
| `GUITAR_PIT_CACHE_TTL` | `900` | Seconds before data is re-fetched |
| `GUITAR_PIT_QUERY_TIMEOUT` | `10` | Per-query time limit, seconds |

## Development

```sh
cd mcp
uv venv && uv pip install -e '.[test]'
.venv/bin/pytest
GUITAR_PIT_DATA_DIR=.. .venv/bin/guitar-pit-mcp   # run against the local checkout
```

A note on `analytics.csv`: until SrsRamifications/the-guitar-pit#3, the bot
split events on every comma before storing them, so values containing commas
(mostly seller locations) spilled into later columns. The loader re-joins
them (`repair_csv_fields`) and drops the few rows it can't realign. Once that
fix is deployed and its repair script has run, the published CSV is clean and
this becomes a no-op safety net.

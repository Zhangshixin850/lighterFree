# Repository Guidelines

## Project Structure
- `Light_grid.py`: entry point, configuration, `GridTrader`, risk checks, and console output.
- `lighter_exchange.py`: SDK signing, batch submissions, WebSocket state, REST reconciliation, and request budgets.
- `test_light_grid.py`: offline regression tests with simulated exchange responses.
- `.env.example`: public configuration template; `.env` holds local credentials and parameters.
- `README.md` and `USAGE.md`: v5.0 setup, strategy, limits, and operating instructions.

This is a flat Python application without a build or asset directory.

## Development Commands
Target Ubuntu with Python 3.10+:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest -v test_light_grid
python Light_grid.py --check-config
```

Create `.env` from `.env.example` on first setup. Configuration checks are offline. `python Light_grid.py` submits real orders; never use it as a smoke test.

## Coding Style
Use four-space indentation, `snake_case` functions, `PascalCase` classes, and uppercase constants. Preserve established names such as `gridStep`, `orderSize`, and `All_orders`. Use `Decimal` for financial calculations and `asyncio` for I/O. Keep strategy rules in `GridTrader` and transport operations in the adapter. Favor flat logic over tiny helpers; retain concise Chinese logs and comments. No formatter or linter configuration is present.

## v5.0 Strategy Invariants
Edge fills shift the grid around the original edge limit price. Preserve overlapping strategy orders, confirm selective cancellations before adding missing orders, and retain the edge fill's ordinary reverse order. Count pending submissions and queued replacements when avoiding duplicates. Track intentional cancellations until terminal status; never rehang retired slots. Defer new edge events until the current shift finishes. Reserve market-wide cancellation for startup and shutdown. Keep existing risk and request limits.

## Testing
Use `unittest`, `IsolatedAsyncioTestCase`, mocks, and `test_<behavior>` names. Cover both shift directions, preserved order IDs, partial fills, cancellation races, cooldown recovery, uncertain submissions, deduplication, and shutdown. Keep tests offline with fake credentials. No coverage threshold is configured.

## Commits & Pull Requests
Use imperative subjects; prefix version changes with `v5.0:`. Describe behavior, configuration impact, and validation results. Link issues when applicable. Preserve remote history and unrelated changes; never commit credentials, logs, or temporary files. Keep environment-variable precedence and redact secrets.

<!-- codex-dedicated-temp -->
## Dedicated Temporary Directory
Use `C:\CodexTemp` for task temporary files, downloads, scratch scripts, intermediate outputs, and verification artifacts. Create a separate subdirectory per task. Set `TEMP`, `TMP`, and `TMPDIR` to `C:\CodexTemp` for commands and subprocesses. Keep final deliverables in the requested project or output location.
<!-- /codex-dedicated-temp -->

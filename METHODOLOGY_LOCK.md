# DSI Method Lock

This repository's public DSI outputs are locked to the original DSI-ICF construction rule used in
`D:\Codex璁烘枃\澶栦氦鎯呯华璁烘枃\DSI-ICF\code\data_integration.executed.ipynb`.

## Authoritative rules

1. Each official text is scored on the three DSI dimensions: war-related
   (`c1` / `WDSI`), economic (`c2` / `EDSI`), and other diplomatic
   (`c3` / `ODSI`), each on the `-3` to `3` scale.
2. For each branch, the website's daily raw value is the lowest same-day
   score, not a mean.
3. Missing calendar days inherit the previous publication-day raw value via
   forward fill.
4. The public 3-day, 7-day, and 30-day series are rolling means computed on
   that forward-filled daily path.
5. Raw publication-day values must remain discrete integers in exported CSV and
   JSON files.

## Source references

- `DSI-ICF/code/data_integration.executed.ipynb:156-160`
  same-day aggregation uses `groupby(level=0).min()`
- `DSI-ICF/code/data_integration.executed.ipynb:198`
  missing days are `ffill()`ed
- `DSI-ICF/code/data_integration.executed.ipynb:203-204`
  rolling 7-day and 30-day means are computed after forward fill

## Repository guardrails

- `scripts/build_dsi_site_data.py`
  publishes the three-branch daily panels and download files used by the site
- `scripts/check_method_lock.py`
  runs a deterministic sanity check so the build cannot quietly drift away from
  the original same-day-minimum construction
- `.github/workflows/update-data.yml`
  runs the method-lock check during the rebuild job before data are committed

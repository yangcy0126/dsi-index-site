# DSI Index Site

This repository hosts the public DSI website on GitHub Pages and the automation/build pipeline that refreshes its data assets.

## What the site now supports

- Static public site on GitHub Pages
- Repository-local historical records in `records/*.csv`
- Daily site assets in `data/*.json` and `data/*.csv`
- Automated fetching for:
  - China MFA regular press conferences
  - U.S. State Department `Office of the Spokesperson` press releases
  - U.S. State Department `Department Press Briefing`
  - France MEAE spokesperson live Q&A transcripts
  - Russia MFA foreign policy news

The UK, Japan, and South Korea series are still included in the website. The automation code is structured so more source adapters can be added later.

## Local setup

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Bootstrap repository-local historical records from the parent research folder:

```bash
python scripts/bootstrap_records.py
```

Rebuild website data files from the integrated DSI-ICF outputs:

```bash
python scripts/build_dsi_site_data.py
```

Run the method lock check:

```bash
python scripts/check_method_lock.py
```

Preview recent source fetches without scoring or writing:

```bash
python scripts/update_wdsi_records.py --countries CN,US,FR,RU --dry-run
```

Run the full update locally:

```bash
set OPENAI_API_KEY=your_key_here
python scripts/update_wdsi_records.py --countries CN,US,FR,RU
```

If the Russian MFA site is unavailable, use the ministry's official English Telegram channel as the fallback while retaining the original `mid.ru` article URLs:

```bash
set WDSI_RU_OFFICIAL_TELEGRAM_ONLY=1
python scripts/update_three_category_sources.py --countries RU --start-date 2026-04-12 --end-date 2026-08-06 --max-pages 120
```

For the full three-category research refresh, use `update_three_category_sources.py` to update the local corpus, run the sibling DSI-ICF scorer, then use `rebuild_three_category_daily.py`, `sync_three_category_records.py`, and `build_dsi_site_data.py` in that order. The Brazil election-period migration gap has a dedicated reproducible recovery command:

```bash
python scripts/recover_brazil_migration_gap.py --start-date 2026-04-20 --end-date 2026-07-02
```

Serve locally:

```bash
python -m http.server 8000
```

Then open `http://127.0.0.1:8000`.

## Data flow

1. Historical baseline is imported into `records/*.csv`.
2. `scripts/update_wdsi_records.py` refreshes recent official texts for the currently automated sources.
3. `scripts/build_dsi_site_data.py` rebuilds the public site assets from the latest three-branch DSI panels and score metadata.
4. The public DSI series keep the original DSI-ICF construction rule:
   same-day raw score = daily minimum, then forward-fill, then rolling 3-day / 7-day / 30-day means.
5. `scripts/check_method_lock.py` verifies that the exported series still obey that rule.
6. GitHub Actions commits the changed `records/` and `data/` files.
7. The existing Pages workflow deploys the refreshed site.

## GitHub Actions setup

Add these repository settings before relying on daily updates:

- Preferred repository secret: `WDSI_API_KEY`
- Backward-compatible secret: `OPENAI_API_KEY`
- Optional repository variable: `WDSI_API_BASE_URL`
- Optional repository variable: `OPENAI_BASE_URL`
- Optional repository variable: `WDSI_MODEL`
- Backward-compatible variable: `WDSI_OPENAI_MODEL`
- Optional repository variable: `WDSI_REASONING_EFFORT`

The scheduled workflow lives at:

- `.github/workflows/update-data.yml`

It runs daily at `15:20 UTC`.

## Using Qwen instead of OpenAI

Yes. The scorer now supports OpenAI-compatible providers.

For Qwen / Alibaba Cloud Model Studio, set:

- Secret `WDSI_API_KEY`: your DashScope / Model Studio API key
- Variable `WDSI_API_BASE_URL`: one of
  - `https://dashscope-intl.aliyuncs.com/compatible-mode/v1`
  - `https://dashscope-us.aliyuncs.com/compatible-mode/v1`
  - `https://dashscope.aliyuncs.com/compatible-mode/v1`
- Variable `WDSI_MODEL`: for example `qwen-plus` or `qwen-max`

When `WDSI_API_BASE_URL` is set, the pipeline automatically uses the OpenAI-compatible `chat/completions` path, which is the compatibility mode documented by Alibaba Cloud Model Studio.

## Important directories

- `records/`: canonical scored records used by the website build
- `data/`: static assets served by the site
- `METHODOLOGY_LOCK.md`: authoritative DSI construction rule for this repo
- `scripts/bootstrap_records.py`: one-time baseline import
- `scripts/update_wdsi_records.py`: incremental fetch and score pipeline
- `scripts/update_three_category_sources.py`: official-source corpus refresh for all 15 countries
- `scripts/recover_brazil_migration_gap.py`: Brazil 2026 migration-gap recovery from official search entries and web archives
- `scripts/rebuild_three_category_daily.py`: archived-baseline overlay and daily 3/7/30-day rebuild
- `scripts/sync_three_category_records.py`: sync accepted c1 scores to the website record store
- `scripts/build_dsi_site_data.py`: public site asset builder for the three DSI branches
- `scripts/build_wdsi_data.py`: legacy helper module retained for method-lock and visitor utilities
- `scripts/check_method_lock.py`: deterministic guardrail against aggregation drift

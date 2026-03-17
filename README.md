# WDSI Index Site

This repository hosts a static WDSI website on GitHub Pages and a daily automation pipeline that can refresh the data.

## What the site now supports

- Static public site on GitHub Pages
- Repository-local historical records in `records/*.csv`
- Daily site assets in `data/*.json` and `data/*.csv`
- Automated fetching for:
  - China MFA regular press conferences
  - U.S. State Department `Office of the Spokesperson` press releases
  - U.S. State Department `Department Press Briefing`

The UK, Japan, and South Korea series are still included in the website, but they currently update from the historical baseline only. The automation code is structured so more source adapters can be added later.

## Local setup

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Bootstrap repository-local historical records from the parent research folder:

```bash
python scripts/bootstrap_records.py
```

Rebuild website data files from `records/*.csv`:

```bash
python scripts/build_wdsi_data.py
```

Preview recent source fetches without scoring or writing:

```bash
python scripts/update_wdsi_records.py --countries CN,US --dry-run
```

Run the full update locally:

```bash
set OPENAI_API_KEY=your_key_here
python scripts/update_wdsi_records.py --countries CN,US
```

Serve locally:

```bash
python -m http.server 8000
```

Then open `http://127.0.0.1:8000`.

## Data flow

1. Historical baseline is imported into `records/*.csv`.
2. `scripts/update_wdsi_records.py` fetches recent official texts.
3. New or changed texts are scored with the OpenAI API on the `-3` to `3` WDSI scale.
4. `scripts/build_wdsi_data.py` regenerates the frontend JSON and CSV files.
5. GitHub Actions commits the changed `records/` and `data/` files.
6. The existing Pages workflow deploys the refreshed site.

## GitHub Actions setup

Add these repository settings before relying on daily updates:

- Repository secret: `OPENAI_API_KEY`
- Optional repository variable: `WDSI_OPENAI_MODEL`
- Optional repository variable: `WDSI_REASONING_EFFORT`

The scheduled workflow lives at:

- `.github/workflows/update-data.yml`

It runs daily at `15:20 UTC`.

## Important directories

- `records/`: canonical scored records used by the website build
- `data/`: static assets served by the site
- `scripts/bootstrap_records.py`: one-time baseline import
- `scripts/update_wdsi_records.py`: incremental fetch and score pipeline
- `scripts/build_wdsi_data.py`: static asset builder

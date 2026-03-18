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

Rebuild website data files from `records/*.csv`:

```bash
python scripts/build_wdsi_data.py
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

If you are fetching `RU` locally and the Russian MFA site blocks headless Chromium, run it with a visible browser session:

```bash
set WDSI_PLAYWRIGHT_HEADLESS=0
python scripts/update_wdsi_records.py --countries RU --dry-run --max-pages 1
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
- `scripts/bootstrap_records.py`: one-time baseline import
- `scripts/update_wdsi_records.py`: incremental fetch and score pipeline
- `scripts/build_wdsi_data.py`: static asset builder

# Equity Research Pipeline

Phase 2 implementation of the B3 equity research case. The project now has a structured pipeline, SQLite persistence, handled error states, and a Streamlit dashboard for both fresh runs and historical inspection.

## Branches

- `main`: stable Phase 1 baseline
- `codex/phase-2-pipeline` or your current Phase 2 branch: pipeline, database, history dashboard, and operational improvements

## What this branch includes

- pipeline execution for any B3 ticker accepted by the B3 listed-companies feed
- SQLite storage for company identity, per-run snapshots, LLM outputs, and handled errors
- Streamlit dashboard for running the pipeline and reviewing stored history
- CLI entrypoint for recurring execution through Task Scheduler, cron, or another orchestrator
- logging to file and console

## Project structure

```text
project_root/
  app.py
  README.md
  requirements.txt
  .env.example
  pages/
    1_Pipeline_History.py
  src/
    config.py
    errors.py
    logging_utils.py
    collectors/
      company_data.py
      market_data.py
      news_data.py
      public_api.py
    llm/
      client.py
      prompts.py
      schemas.py
    pipeline/
      cli.py
      runner.py
    services/
      briefing_service.py
    storage/
      database.py
      repository.py
    utils/
      formatting.py
      validation.py
```

## Data model

The SQLite schema separates relatively stable company identity from run-varying data:

- `companies`: ticker, CNPJ, CVM code, active flag, first seen, last seen
- `pipeline_runs`: one row per execution with status, trigger, timestamps, summary error text, and stored raw payload
- `company_profile_snapshots`: company description captured for a specific run
- `market_snapshots`: fundamentals, metric provenance, warnings, and stored price history for a specific run
- `news_snapshots`: news items captured for a specific run
- `llm_reports`: generated report, raw response, parse status, and LLM error message
- `pipeline_errors`: handled failures by stage, type, message, and details

This prevents later runs from overwriting earlier market/news/LLM results while still keeping a canonical company table.

## Requirements

- Python 3.10+
- an OpenAI-compatible API key if you want LLM synthesis

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create your environment file:

```bash
copy .env.example .env
```

4. Fill in the variables:

```bash
OPENAI_API_KEY=your_api_key_here
OPENAI_MODEL=gpt-5.4-mini
OPENAI_BASE_URL=https://api.openai.com/v1
DATABASE_PATH=data/briefings.db
LOG_LEVEL=INFO
LOG_FILE_PATH=logs/pipeline.log
```

## Running in under 10 minutes

Start the dashboard:

```bash
streamlit run app.py
```

Open the app, enter a B3 ticker such as `PETR4` or `VALE3`, and run the pipeline. Streamlit will also expose the `Pipeline History` page automatically from `pages/`.

## Running the pipeline from the command line

Single ticker:

```bash
python -m src.pipeline.cli --ticker PETR4
```

Multiple tickers:

```bash
python -m src.pipeline.cli --ticker PETR4 --ticker VALE3 --ticker ITUB4
```

The CLI is designed so recurrence is handled by your scheduler of choice. On Windows, use Task Scheduler. On Linux or macOS, use cron or another job runner.

## Sources

- B3 listed-companies API for ticker validation, company name, CVM code, and segment
- Yahoo Finance for first-pass fundamentals
- CVM open data for fallback accounting data and company description fields
- Status Invest for current price, price history, and dividend fallback inputs
- Google News RSS for recent news
- OpenAI-compatible API for the structured briefing

## Error handling implemented

- API unavailable: source-specific failures are logged and stored in `pipeline_errors`; the run is marked `partial_success` when enough data remains, or `failed_source_unavailable` when it does not
- Invalid or delisted ticker: the B3 listed-companies lookup raises a handled failure and the run is marked `failed_invalid_ticker`
- LLM out-of-format response: the raw response is stored, the error is recorded, and the run continues with collected market/news/company data

## Logging and storage

- SQLite database defaults to `data/briefings.db`
- logs default to `logs/pipeline.log`
- both paths are configurable through `.env`

## Notes for collaborators

- `app.py` is the latest-run execution page
- `pages/1_Pipeline_History.py` is the history and inspection page
- `src/pipeline/runner.py` is the main orchestration layer
- `src/storage/repository.py` is the place to extend queries or persistence rules

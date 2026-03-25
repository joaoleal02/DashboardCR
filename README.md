# Monday Morning Equity Briefing

Phase 1 prototype of a local Streamlit app that generates a Monday morning equity research briefing for one allowed B3 ticker at a time.

## Purpose

The app collects public information about a selected company, combines company and market data with recent news, and sends the result to an LLM to produce a concise buy-side style briefing.

This repository intentionally solves **Phase 1 only**.

## Phase 1 scope

Implemented in this prototype:

- one-ticker briefing flow
- restricted ticker universe
- company overview from public sources
- current quote and selected fundamentals
- simple returns chart with switchable windows
- up to 5 recent news items
- LLM-generated structured analysis
- minimal Streamlit interface

Intentionally not implemented yet:

- databases or persistence
- historical snapshots or execution history
- recurring pipelines
- generalized support for all B3 tickers
- advanced orchestration or fault tolerance
- background jobs
- RAG, embeddings, vector stores, or retrieval systems
- production architecture or infrastructure

## Supported tickers

- ASAI3
- RECV3
- MOVI3
- BRKM5
- HBSA3
- ITUB4
- BBDC4
- OPCT3
- BRSR6
- PRIO3

## Project structure

```text
project_root/
  app.py
  requirements.txt
  README.md
  .env.example
  src/
    __init__.py
    config.py
    ticker_universe.py
    collectors/
      __init__.py
      company_data.py
      market_data.py
      news_data.py
    llm/
      __init__.py
      prompts.py
      client.py
      schemas.py
    services/
      __init__.py
      briefing_service.py
    utils/
      __init__.py
      formatting.py
      validation.py
```

## Setup

1. Use Python 3.10 or newer.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and configure the LLM credentials:

```bash
OPENAI_API_KEY=your_api_key_here
OPENAI_MODEL=gpt-5.4-mini
```

`OPENAI_BASE_URL` is optional if you want to point the app to another OpenAI-compatible endpoint.

## Run

```bash
streamlit run app.py
```

## How it works

- `yfinance` is used for quote, business summary, and first-pass company/news metadata.
- Fundamentus is used as a best-effort public fallback for Brazilian fundamentals and sector/subsector classification.
- Google News RSS is used as a simple public fallback when Yahoo news is unavailable or too sparse.
- The LLM receives a structured payload and returns JSON with:
  - business summary
  - fundamentals interpretation
  - news synthesis
  - three analyst questions

## Known Phase 1 limitations

These limitations are intentional and acceptable for this prototype:

- restricted ticker list
- dependence on public/free sources that may occasionally return missing fields
- best-effort extraction of company metadata
- limited malformed-output handling for the LLM response
- no persistence or historical tracking
- no guarantee of production-grade availability or robustness

## Deferred to Phase 2

Examples of work intentionally left for later:

- persistent storage of briefings
- execution history and historical comparisons
- broader ticker coverage
- stronger resilience and retries
- richer validation and monitoring
- deeper analyst workflows or recurring automation

## Notes for evaluators

- If some data points are unavailable from the public sources, the app shows `Unavailable` instead of failing.
- If the LLM call fails, the app still renders the raw collected data so the pipeline remains explainable.
- The design is intentionally small and pragmatic so it is easy to run locally and easy to explain in an interview.

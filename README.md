# Monday Morning Equity Briefing

Phase 1 prototype of a local Streamlit app that generates a Monday morning equity research briefing for one allowed B3 ticker at a time.

## Purpose

The app collects public information about a selected company, combines company and market data with recent news, and sends the result to an LLM to produce a concise buy-side style briefing.

## Phase 1 scope

- one-ticker briefing flow
- restricted ticker universe
- company overview from public sources
- current quote and selected fundamentals
- simple returns chart with switchable windows
- up to 5 recent news items
- LLM-generated structured analysis
- minimal Streamlit interface

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
      public_api.py
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

- The B3 listed-companies JSON endpoint is used to resolve company name, CVM code, and B3 segment.
- CVM open-data files are used for sector, business description, shares outstanding, and accounting-based fundamentals.
- Status Invest JSON endpoints are used for current price, price history, and provents-based dividend inputs.
- Google News RSS is used to collect up to five recent public news items.
- The LLM receives a structured payload and returns JSON with:
  - business summary
  - fundamentals interpretation
  - news synthesis
  - three analyst questions

## Notes for evaluators

- If some data points are unavailable from the public sources, the app shows `Unavailable` instead of failing.
- If the LLM call fails, the app still renders the raw collected data so the pipeline remains explainable.
- The design is intentionally small and pragmatic so it is easy to run locally and easy to explain in an interview.

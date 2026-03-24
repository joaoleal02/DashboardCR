from __future__ import annotations

import streamlit as st

from src.services.briefing_service import BriefingService
from src.ticker_universe import ALLOWED_TICKERS
from src.utils.formatting import (
    compact_date,
    format_metric_value,
    normalize_title_key,
    safe_text,
    to_pretty_json,
)
from src.utils.validation import normalize_ticker, validate_ticker


st.set_page_config(
    page_title="Monday Morning Equity Briefing",
    page_icon=":bar_chart:",
    layout="centered",
)


def render_overview(profile: dict[str, object], ticker: str) -> None:
    st.subheader("1. Company overview")
    left, right = st.columns(2)
    left.markdown(f"**Ticker**  \n{ticker}")
    left.markdown(f"**Company name**  \n{safe_text(profile.get('company_name'))}")
    left.markdown(f"**Sector**  \n{safe_text(profile.get('sector'))}")
    right.markdown(f"**Segment**  \n{safe_text(profile.get('segment'))}")
    st.markdown("**Business description**")
    st.write(safe_text(profile.get("business_description")))


def render_market_data(market_data: dict[str, object]) -> None:
    st.subheader("2. Market and fundamentals")
    metrics = [
        ("Current price", "current_price"),
        ("P/L", "p_l"),
        ("ROE", "roe"),
        ("Net Debt / EBITDA", "net_debt_ebitda"),
        ("Net Margin", "net_margin"),
        ("Dividend Yield", "dividend_yield"),
    ]
    col_a, col_b, col_c = st.columns(3)
    cols = [col_a, col_b, col_c]
    for index, (label, field) in enumerate(metrics):
        column = cols[index % 3]
        with column:
            st.metric(label=label, value=format_metric_value(field, market_data.get(field)))


def render_news(news_items: list[dict[str, object]], llm_report: object | None) -> None:
    st.subheader("3. Recent news")
    if not news_items:
        st.info("No recent news was found from the available public sources. The briefing below uses company and market data only.")
        return

    sentiment_map: dict[str, str] = {}
    if llm_report is not None and getattr(llm_report, "news_analysis", None) is not None:
        for item in llm_report.news_analysis.items:
            sentiment_map[normalize_title_key(item.title)] = item.sentiment.title()

    for item in news_items:
        title = safe_text(item.get("title"))
        source = safe_text(item.get("source"))
        date_value = compact_date(item.get("date"))
        url = item.get("url")
        sentiment = sentiment_map.get(normalize_title_key(title))
        st.markdown(f"**{title}**")
        meta_parts = [part for part in [source, date_value] if part != "Unavailable"]
        if sentiment:
            meta_parts.append(f"Sentiment: {sentiment}")
        if meta_parts:
            st.caption(" | ".join(meta_parts))
        if url:
            st.markdown(f"[Open article]({url})")
        st.divider()


def render_llm_report(llm_report: object | None, llm_error: str | None) -> None:
    st.subheader("4. LLM report")
    if llm_report is None:
        st.warning(llm_error or "The LLM analysis could not be generated.")
        return

    st.markdown("**Business summary**")
    st.write(llm_report.business_summary)

    st.markdown("**Interpretation of indicators**")
    st.write(llm_report.fundamentals_interpretation)

    st.markdown("**News synthesis**")
    st.write(llm_report.news_analysis.overall)

    st.markdown("**Analyst questions**")
    for question in llm_report.analyst_questions:
        st.write(f"- {question}")


def main() -> None:
    service = BriefingService()

    st.title("Monday Morning Equity Briefing")
    st.write(
        "A Phase 1 Streamlit prototype for generating a compact equity research briefing from public data plus an LLM synthesis."
    )

    with st.form("briefing-form"):
        selector_col, input_col = st.columns([1, 1])
        with selector_col:
            selected_ticker = st.selectbox("Choose an allowed ticker", options=ALLOWED_TICKERS, index=0)
        with input_col:
            typed_ticker = st.text_input(
                "Or type a ticker",
                placeholder="Example: ITUB4",
                help="Phase 1 only supports the 10 tickers listed in the case study.",
            )
        submitted = st.form_submit_button("Generate briefing", use_container_width=True)

    if submitted:
        raw_choice = typed_ticker or selected_ticker
        normalized = normalize_ticker(raw_choice)
        is_valid, error_message = validate_ticker(normalized)
        if not is_valid:
            st.error(error_message)
            return

        with st.spinner(f"Collecting data and generating the briefing for {normalized}..."):
            result = service.generate_briefing(normalized)
        st.session_state["briefing_result"] = result

    result = st.session_state.get("briefing_result")
    if not result:
        st.caption("Choose one of the allowed B3 tickers and click Generate briefing.")
        return

    render_overview(result.company_profile, result.ticker)
    render_market_data(result.market_data)
    render_news(result.news, result.llm_report)
    render_llm_report(result.llm_report, result.llm_error)

    with st.expander("5. Raw collected data"):
        st.code(to_pretty_json(result.raw_payload), language="json")

    with st.expander("Debug information"):
        st.code(to_pretty_json(result.debug_info), language="json")

    if result.llm_error:
        st.info(
            "The app still returned the collected raw data because the LLM step is intentionally best-effort in Phase 1."
        )


if __name__ == "__main__":
    main()

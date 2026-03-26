from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from src.services.briefing_service import BriefingService
from src.utils.formatting import (
    compact_date,
    escape_streamlit_text,
    format_metric_value,
    normalize_title_key,
    safe_text,
)
from src.utils.validation import normalize_ticker, validate_ticker


st.set_page_config(
    page_title="Equity Research Pipeline",
    page_icon=":bar_chart:",
    layout="centered",
)


def render_overview(profile: dict[str, object], ticker: str) -> None:
    st.subheader("Company profile")
    left, right = st.columns(2)
    left.markdown(f"**Ticker**  \n{ticker}")
    left.markdown(f"**Company name**  \n{safe_text(profile.get('company_name'))}")
    left.markdown(f"**Sector**  \n{safe_text(profile.get('sector'))}")
    right.markdown(f"**Segment**  \n{safe_text(profile.get('segment'))}")
    st.markdown("**Business description**")
    st.write(safe_text(profile.get("business_description")))


def render_market_data(market_data: dict[str, object]) -> None:
    st.subheader("Market snapshot and fundamentals")
    metrics = [
        ("Current price", "current_price"),
        ("P/L", "p_l"),
        ("ROE", "roe"),
        ("Net Debt / EBITDA", "net_debt_ebitda"),
        ("Net Margin", "net_margin"),
        ("Dividend Yield", "dividend_yield"),
    ]
    metric_sources = market_data.get("metric_sources") or {}
    metric_warnings = market_data.get("metric_warnings") or []
    col_a, col_b, col_c = st.columns(3)
    cols = [col_a, col_b, col_c]
    for index, (label, field) in enumerate(metrics):
        column = cols[index % 3]
        with column:
            st.metric(label=label, value=format_metric_value(field, market_data.get(field)))
            source_info = metric_sources.get(field) or {}
            source = source_info.get("source")
            detail = source_info.get("detail")
            if source and detail:
                st.caption(f"{source} | {detail}")
            elif source:
                st.caption(source)

    if metric_warnings:
        warning_lines = "\n".join(f"- {warning}" for warning in metric_warnings)
        st.warning(f"Could not find or derive some fundamentals:\n{warning_lines}")


def render_returns_chart(price_history: list[dict[str, object]], ticker: str) -> None:
    st.markdown("**Returns chart**")
    if not price_history:
        st.info("Historical price data is unavailable for this ticker at the moment.")
        return

    history_frame = pd.DataFrame(price_history)
    if history_frame.empty or "date" not in history_frame or "close" not in history_frame:
        st.info("Historical price data is unavailable for this ticker at the moment.")
        return

    history_frame["date"] = pd.to_datetime(history_frame["date"], errors="coerce")
    history_frame["close"] = pd.to_numeric(history_frame["close"], errors="coerce")
    history_frame = history_frame.dropna(subset=["date", "close"]).sort_values("date")
    if history_frame.empty:
        st.info("Historical price data is unavailable for this ticker at the moment.")
        return

    selected_window = st.radio(
        "Window",
        options=["12M", "6M", "YTD", "MTD"],
        horizontal=True,
        key=f"returns-window-{ticker}",
    )
    filtered_frame = filter_price_history(history_frame, selected_window)
    if filtered_frame.empty:
        st.info(f"No historical points are available for the selected {selected_window} window.")
        return

    base_close = filtered_frame["close"].iloc[0]
    if base_close <= 0:
        st.info("Historical price data is unavailable for return normalization.")
        return
    filtered_frame["Return (%)"] = ((filtered_frame["close"] / base_close) - 1) * 100
    chart_frame = filtered_frame.set_index("date")[["Return (%)"]]

    latest_return = chart_frame["Return (%)"].iloc[-1]
    st.caption(f"Cumulative return for {selected_window}: {latest_return:.2f}%")
    st.line_chart(chart_frame, height=260)


def get_result_price_history(result: object) -> list[dict[str, object]]:
    direct_history = getattr(result, "price_history", None)
    if isinstance(direct_history, list):
        return direct_history

    raw_payload = getattr(result, "raw_payload", None)
    if isinstance(raw_payload, dict):
        payload_history = raw_payload.get("price_history")
        if isinstance(payload_history, list):
            return payload_history

    return []


def filter_price_history(history_frame: pd.DataFrame, window: str) -> pd.DataFrame:
    latest_date = history_frame["date"].max()
    if pd.isna(latest_date):
        return history_frame.iloc[0:0]

    if window == "12M":
        start_date = latest_date - pd.DateOffset(months=12)
    elif window == "6M":
        start_date = latest_date - pd.DateOffset(months=6)
    elif window == "YTD":
        start_date = pd.Timestamp(datetime(latest_date.year, 1, 1))
    else:
        start_date = pd.Timestamp(datetime(latest_date.year, latest_date.month, 1))

    filtered = history_frame.loc[history_frame["date"] >= start_date].copy()
    if filtered.empty:
        return history_frame.copy()
    return filtered


def render_news(news_items: list[dict[str, object]], llm_report: object | None) -> None:
    st.subheader("Recent news and impact")
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
            meta_parts.append(f"Impact: {sentiment}")
        if meta_parts:
            st.caption(" | ".join(meta_parts))
        if url:
            st.markdown(f"[Open article]({url})")
        st.divider()


def render_llm_report(llm_report: object | None, llm_error: str | None) -> None:
    st.subheader("Analyst briefing")
    if llm_report is None:
        st.warning(llm_error or "The LLM analysis could not be generated.")
        return

    st.markdown("**Business summary**")
    st.markdown(escape_streamlit_text(llm_report.business_summary))

    st.markdown("**Interpretation of indicators**")
    st.markdown(escape_streamlit_text(llm_report.fundamentals_interpretation))

    st.markdown("**News synthesis**")
    st.markdown(escape_streamlit_text(llm_report.news_analysis.overall))

    st.markdown("**Analyst questions**")
    for question in llm_report.analyst_questions:
        st.markdown(f"- {escape_streamlit_text(question)}")


def render_run_summary(result: object) -> None:
    st.subheader("Pipeline run")
    left, right, third = st.columns(3)
    left.metric("Run ID", getattr(result, "run_id", "Unavailable"))
    right.metric("Run status", safe_text(getattr(result, "run_status", None)))
    third.metric("LLM status", safe_text(getattr(result, "llm_status", None)))


def render_pipeline_errors(error_events: list[dict[str, object]]) -> None:
    if not error_events:
        return
    st.warning("This run completed with handled issues.")
    for event in error_events:
        stage = safe_text(event.get("stage"))
        error_type = safe_text(event.get("error_type"))
        message = safe_text(event.get("message"))
        details = event.get("details")
        st.caption(f"{stage} | {error_type} | {message}")
        if details and len(str(details)) <= 200:
            st.caption(safe_text(details))


def main() -> None:
    service = BriefingService()

    st.title("Equity Research Pipeline Dashboard")
    st.write(
        "Phase 2 dashboard for running the pipeline, storing each execution in SQLite, and reviewing the latest generated briefing."
    )
    st.caption("Each run writes company, market, news, LLM, and error events to the local pipeline database.")

    with st.form("briefing-form"):
        entered_ticker = st.text_input("B3 ticker", placeholder="Examples: PETR4, VALE3, ITUB4")
        submitted = st.form_submit_button("Run pipeline", use_container_width=True)

    if submitted:
        normalized = normalize_ticker(entered_ticker)
        is_valid, error_message = validate_ticker(normalized)
        if not is_valid:
            st.error(error_message)
            return

        with st.spinner(f"Running the pipeline for {normalized}..."):
            result = service.generate_briefing(normalized)
        st.session_state["briefing_result"] = result

    result = st.session_state.get("briefing_result")
    if not result:
        st.caption("Enter any B3 ticker and click Run pipeline to generate and store a briefing.")
        return

    render_run_summary(result)
    render_pipeline_errors(getattr(result, "error_events", []))
    if result.run_status == "failed_invalid_ticker":
        st.error("Ticker was not found in B3. It may be invalid or delisted.")
        return
    if result.run_status == "failed_source_unavailable":
        st.error("The required sources were unavailable for this run, so the pipeline could not build a usable briefing.")
        return

    render_overview(result.company_profile, result.ticker)
    render_market_data(result.market_data)
    render_returns_chart(get_result_price_history(result), result.ticker)
    render_news(result.news, result.llm_report)
    render_llm_report(result.llm_report, result.llm_error)

    if result.llm_error:
        st.info(
            "The company, market, and news sections still rendered even though the LLM step failed."
        )
        raw_response = result.debug_info.get("llm_raw_response")
        if raw_response:
            with st.expander("LLM failure details"):
                st.code(raw_response, language="json")


if __name__ == "__main__":
    main()

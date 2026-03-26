from __future__ import annotations

import pandas as pd
import streamlit as st

from src.storage.repository import BriefingRepository
from src.utils.formatting import compact_date, format_metric_value, safe_text, to_pretty_json


st.set_page_config(
    page_title="Pipeline History",
    page_icon=":card_index_dividers:",
    layout="wide",
)


def build_runs_table(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.rename(
        columns={
            "id": "Run ID",
            "normalized_ticker": "Ticker",
            "status": "Run Status",
            "llm_status": "LLM Status",
            "started_at": "Started At",
            "finished_at": "Finished At",
            "company_name": "Company",
            "current_price": "Current Price",
            "p_l": "P/L",
            "roe": "ROE",
            "net_margin": "Net Margin",
            "error_summary": "Error Summary",
        }
    )


def render_metric_history(history_rows: list[dict[str, object]]) -> None:
    st.subheader("Metric history")
    if not history_rows:
        st.info("No stored market snapshots were found for this ticker yet.")
        return

    history_frame = pd.DataFrame(history_rows)
    history_frame["started_at"] = pd.to_datetime(history_frame["started_at"], errors="coerce")
    history_frame = history_frame.sort_values("started_at")
    chart_frame = history_frame.set_index("started_at")[
        ["current_price", "p_l", "roe", "net_margin", "net_debt_ebitda", "dividend_yield"]
    ]
    st.line_chart(chart_frame, height=280)


def render_run_detail(detail: dict[str, object] | None) -> None:
    st.subheader("Run detail")
    if detail is None:
        st.info("Choose a run to inspect.")
        return

    left, right = st.columns(2)
    left.markdown(f"**Ticker**  \n{safe_text(detail.get('normalized_ticker'))}")
    left.markdown(f"**Company**  \n{safe_text(detail.get('company_name'))}")
    left.markdown(f"**Run status**  \n{safe_text(detail.get('status'))}")
    left.markdown(f"**LLM status**  \n{safe_text(detail.get('llm_status'))}")
    right.markdown(f"**Started at**  \n{safe_text(detail.get('started_at'))}")
    right.markdown(f"**Finished at**  \n{safe_text(detail.get('finished_at'))}")
    right.markdown(f"**Error summary**  \n{safe_text(detail.get('error_summary'))}")

    st.markdown("**Stored fundamentals**")
    fundamentals = [
        ("Current price", "current_price"),
        ("P/L", "p_l"),
        ("ROE", "roe"),
        ("Net Debt / EBITDA", "net_debt_ebitda"),
        ("Net Margin", "net_margin"),
        ("Dividend Yield", "dividend_yield"),
    ]
    columns = st.columns(3)
    for index, (label, field) in enumerate(fundamentals):
        with columns[index % 3]:
            st.metric(label, format_metric_value(field, detail.get(field)))

    with st.expander("News stored for this run"):
        news_items = detail.get("news_items") or []
        if not news_items:
            st.caption("No news items were stored.")
        for item in news_items:
            st.markdown(f"**{safe_text(item.get('title'))}**")
            st.caption(
                " | ".join(
                    part
                    for part in [
                        safe_text(item.get("source")),
                        compact_date(item.get("published_at")),
                    ]
                    if part != "Unavailable"
                )
            )
            if item.get("url"):
                st.markdown(f"[Open article]({item.get('url')})")
            st.divider()

    with st.expander("LLM report stored for this run"):
        llm_report = detail.get("llm_report") or {}
        if not llm_report:
            st.caption("No LLM output was stored.")
        else:
            st.markdown(f"**Status**  \n{safe_text(llm_report.get('status'))}")
            st.markdown(f"**Business summary**  \n{safe_text(llm_report.get('business_summary'))}")
            st.markdown(
                f"**Fundamentals interpretation**  \n{safe_text(llm_report.get('fundamentals_interpretation'))}"
            )
            st.markdown(f"**News overall**  \n{safe_text(llm_report.get('news_overall'))}")
            questions = llm_report.get("analyst_questions") or []
            if questions:
                st.markdown("**Analyst questions**")
                for question in questions:
                    st.markdown(f"- {safe_text(question)}")
            if llm_report.get("error_message"):
                st.caption(safe_text(llm_report.get("error_message")))

    with st.expander("Handled errors"):
        errors = detail.get("errors") or []
        if not errors:
            st.caption("No handled errors were stored for this run.")
        for error in errors:
            st.markdown(
                f"**{safe_text(error.get('stage'))} | {safe_text(error.get('error_type'))}**  \n"
                f"{safe_text(error.get('message'))}"
            )
            if error.get("details"):
                st.caption(safe_text(error.get("details")))

    with st.expander("Stored payload metadata"):
        metadata = {
            "profile_sources": detail.get("profile_sources"),
            "metric_sources": detail.get("metric_sources"),
            "metric_warnings": detail.get("metric_warnings"),
        }
        st.code(to_pretty_json(metadata), language="json")


def main() -> None:
    repository = BriefingRepository()
    st.title("Pipeline History")
    st.write("Review previous runs stored in SQLite and compare historical market snapshots.")

    tracked_tickers = repository.list_tracked_tickers()
    ticker_options = ["All tickers"] + tracked_tickers
    selected_ticker = st.selectbox("Filter by ticker", ticker_options, index=0)
    ticker_filter = None if selected_ticker == "All tickers" else selected_ticker

    recent_runs = repository.list_recent_runs(limit=50, ticker=ticker_filter)
    runs_table = build_runs_table(recent_runs)
    st.subheader("Recent runs")
    if runs_table.empty:
        st.info("No pipeline runs have been stored yet.")
        return
    st.dataframe(runs_table, use_container_width=True, hide_index=True)

    history_ticker = ticker_filter or runs_table.iloc[0]["Ticker"]
    render_metric_history(repository.get_ticker_metric_history(str(history_ticker), limit=30))

    run_ids = [int(row["id"]) for row in recent_runs]
    selected_run_id = st.selectbox("Inspect a run", run_ids, index=0)
    render_run_detail(repository.get_run_detail(int(selected_run_id)))


if __name__ == "__main__":
    main()

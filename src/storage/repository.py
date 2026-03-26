from __future__ import annotations

import json
from typing import Any

from src.storage.database import SQLiteDatabase


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _from_json(value: str | None) -> Any:
    if not value:
        return None
    return json.loads(value)


class BriefingRepository:
    def __init__(self, database: SQLiteDatabase | None = None) -> None:
        self.database = database or SQLiteDatabase()

    def start_run(self, requested_ticker: str, normalized_ticker: str, trigger: str, started_at: str) -> int:
        with self.database.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO pipeline_runs (
                    requested_ticker,
                    normalized_ticker,
                    trigger,
                    status,
                    llm_status,
                    started_at,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requested_ticker,
                    normalized_ticker,
                    trigger,
                    "running",
                    "not_requested",
                    started_at,
                    started_at,
                ),
            )
            return int(cursor.lastrowid)

    def upsert_company(
        self,
        ticker: str,
        cnpj: str | None,
        code_cvm: int | None,
        is_active: bool,
        observed_at: str,
    ) -> int:
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO companies (ticker, cnpj, code_cvm, is_active, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    cnpj = COALESCE(excluded.cnpj, companies.cnpj),
                    code_cvm = COALESCE(excluded.code_cvm, companies.code_cvm),
                    is_active = excluded.is_active,
                    last_seen_at = excluded.last_seen_at
                """,
                (ticker, cnpj, code_cvm, int(is_active), observed_at, observed_at),
            )
            row = connection.execute("SELECT id FROM companies WHERE ticker = ?", (ticker,)).fetchone()
            return int(row["id"])

    def save_company_profile_snapshot(
        self,
        run_id: int,
        company_id: int | None,
        profile: dict[str, Any],
        captured_at: str,
    ) -> None:
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO company_profile_snapshots (
                    run_id,
                    company_id,
                    company_name,
                    sector,
                    segment,
                    business_description,
                    sources_json,
                    captured_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    company_id,
                    profile.get("company_name"),
                    profile.get("sector"),
                    profile.get("segment"),
                    profile.get("business_description"),
                    _to_json(profile.get("_sources", [])),
                    captured_at,
                ),
            )

    def save_market_snapshot(
        self,
        run_id: int,
        market_data: dict[str, Any],
        price_history: list[dict[str, Any]],
        captured_at: str,
    ) -> None:
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO market_snapshots (
                    run_id,
                    current_price,
                    p_l,
                    roe,
                    net_debt_ebitda,
                    net_margin,
                    dividend_yield,
                    net_debt,
                    ebitda,
                    metric_sources_json,
                    metric_warnings_json,
                    price_history_json,
                    captured_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    market_data.get("current_price"),
                    market_data.get("p_l"),
                    market_data.get("roe"),
                    market_data.get("net_debt_ebitda"),
                    market_data.get("net_margin"),
                    market_data.get("dividend_yield"),
                    market_data.get("net_debt"),
                    market_data.get("ebitda"),
                    _to_json(market_data.get("metric_sources", {})),
                    _to_json(market_data.get("metric_warnings", [])),
                    _to_json(price_history),
                    captured_at,
                ),
            )

    def save_news_snapshot(self, run_id: int, news_items: list[dict[str, Any]]) -> None:
        with self.database.connection() as connection:
            connection.execute("DELETE FROM news_snapshots WHERE run_id = ?", (run_id,))
            for position, item in enumerate(news_items, start=1):
                connection.execute(
                    """
                    INSERT INTO news_snapshots (run_id, position, title, source, published_at, url)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        position,
                        item.get("title"),
                        item.get("source"),
                        item.get("date"),
                        item.get("url"),
                    ),
                )

    def save_llm_report(
        self,
        run_id: int,
        status: str,
        report_payload: dict[str, Any] | None,
        raw_response: str | None,
        error_message: str | None,
        created_at: str,
    ) -> None:
        business_summary = None
        fundamentals_interpretation = None
        news_overall = None
        news_items = None
        analyst_questions = None
        if report_payload:
            business_summary = report_payload.get("business_summary")
            fundamentals_interpretation = report_payload.get("fundamentals_interpretation")
            news_analysis = report_payload.get("news_analysis") or {}
            news_overall = news_analysis.get("overall")
            news_items = news_analysis.get("items")
            analyst_questions = report_payload.get("analyst_questions")

        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO llm_reports (
                    run_id,
                    status,
                    business_summary,
                    fundamentals_interpretation,
                    news_overall,
                    news_items_json,
                    analyst_questions_json,
                    raw_response,
                    error_message,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    status,
                    business_summary,
                    fundamentals_interpretation,
                    news_overall,
                    _to_json(news_items),
                    _to_json(analyst_questions),
                    raw_response,
                    error_message,
                    created_at,
                ),
            )

    def save_error_event(
        self,
        run_id: int,
        stage: str,
        error_type: str,
        message: str,
        details: str | None,
        created_at: str,
    ) -> None:
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO pipeline_errors (run_id, stage, error_type, message, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, stage, error_type, message, details, created_at),
            )

    def finalize_run(
        self,
        run_id: int,
        company_id: int | None,
        status: str,
        llm_status: str,
        error_summary: str | None,
        raw_payload: dict[str, Any],
        finished_at: str,
    ) -> None:
        with self.database.connection() as connection:
            connection.execute(
                """
                UPDATE pipeline_runs
                SET company_id = ?,
                    status = ?,
                    llm_status = ?,
                    error_summary = ?,
                    raw_payload_json = ?,
                    finished_at = ?
                WHERE id = ?
                """,
                (
                    company_id,
                    status,
                    llm_status,
                    error_summary,
                    _to_json(raw_payload),
                    finished_at,
                    run_id,
                ),
            )

    def list_recent_runs(self, limit: int = 50, ticker: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT
                pipeline_runs.id,
                pipeline_runs.normalized_ticker,
                pipeline_runs.status,
                pipeline_runs.llm_status,
                pipeline_runs.started_at,
                pipeline_runs.finished_at,
                pipeline_runs.error_summary,
                companies.cnpj,
                companies.code_cvm,
                company_profile_snapshots.company_name,
                market_snapshots.current_price,
                market_snapshots.p_l,
                market_snapshots.roe,
                market_snapshots.net_margin
            FROM pipeline_runs
            LEFT JOIN companies ON companies.id = pipeline_runs.company_id
            LEFT JOIN company_profile_snapshots ON company_profile_snapshots.run_id = pipeline_runs.id
            LEFT JOIN market_snapshots ON market_snapshots.run_id = pipeline_runs.id
        """
        params: list[Any] = []
        if ticker:
            query += " WHERE pipeline_runs.normalized_ticker = ?"
            params.append(ticker)
        query += " ORDER BY pipeline_runs.started_at DESC LIMIT ?"
        params.append(limit)

        with self.database.connection() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def list_tracked_tickers(self) -> list[str]:
        with self.database.connection() as connection:
            rows = connection.execute("SELECT ticker FROM companies ORDER BY ticker ASC").fetchall()
        return [str(row["ticker"]) for row in rows]

    def get_ticker_metric_history(self, ticker: str, limit: int = 30) -> list[dict[str, Any]]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    pipeline_runs.id AS run_id,
                    pipeline_runs.started_at,
                    market_snapshots.current_price,
                    market_snapshots.p_l,
                    market_snapshots.roe,
                    market_snapshots.net_margin,
                    market_snapshots.net_debt_ebitda,
                    market_snapshots.dividend_yield
                FROM pipeline_runs
                JOIN market_snapshots ON market_snapshots.run_id = pipeline_runs.id
                WHERE pipeline_runs.normalized_ticker = ?
                ORDER BY pipeline_runs.started_at DESC
                LIMIT ?
                """,
                (ticker, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def get_run_detail(self, run_id: int) -> dict[str, Any] | None:
        with self.database.connection() as connection:
            run_row = connection.execute(
                """
                SELECT
                    pipeline_runs.*,
                    companies.cnpj,
                    companies.code_cvm,
                    company_profile_snapshots.company_name,
                    company_profile_snapshots.sector,
                    company_profile_snapshots.segment,
                    company_profile_snapshots.business_description,
                    company_profile_snapshots.sources_json,
                    market_snapshots.current_price,
                    market_snapshots.p_l,
                    market_snapshots.roe,
                    market_snapshots.net_debt_ebitda,
                    market_snapshots.net_margin,
                    market_snapshots.dividend_yield,
                    market_snapshots.net_debt,
                    market_snapshots.ebitda,
                    market_snapshots.metric_sources_json,
                    market_snapshots.metric_warnings_json,
                    market_snapshots.price_history_json
                FROM pipeline_runs
                LEFT JOIN companies ON companies.id = pipeline_runs.company_id
                LEFT JOIN company_profile_snapshots ON company_profile_snapshots.run_id = pipeline_runs.id
                LEFT JOIN market_snapshots ON market_snapshots.run_id = pipeline_runs.id
                WHERE pipeline_runs.id = ?
                """,
                (run_id,),
            ).fetchone()
            if run_row is None:
                return None

            news_rows = connection.execute(
                """
                SELECT title, source, published_at, url, position
                FROM news_snapshots
                WHERE run_id = ?
                ORDER BY position ASC
                """,
                (run_id,),
            ).fetchall()
            llm_row = connection.execute("SELECT * FROM llm_reports WHERE run_id = ?", (run_id,)).fetchone()
            error_rows = connection.execute(
                """
                SELECT stage, error_type, message, details, created_at
                FROM pipeline_errors
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()

        detail = dict(run_row)
        detail["profile_sources"] = _from_json(detail.pop("sources_json", None)) or []
        detail["metric_sources"] = _from_json(detail.pop("metric_sources_json", None)) or {}
        detail["metric_warnings"] = _from_json(detail.pop("metric_warnings_json", None)) or []
        detail["price_history"] = _from_json(detail.pop("price_history_json", None)) or []
        detail["raw_payload"] = _from_json(detail.get("raw_payload_json")) or {}
        detail["news_items"] = [dict(row) for row in news_rows]
        detail["errors"] = [dict(row) for row in error_rows]
        if llm_row is None:
            detail["llm_report"] = None
        else:
            llm_payload = dict(llm_row)
            detail["llm_report"] = {
                "status": llm_payload["status"],
                "business_summary": llm_payload["business_summary"],
                "fundamentals_interpretation": llm_payload["fundamentals_interpretation"],
                "news_overall": llm_payload["news_overall"],
                "news_items": _from_json(llm_payload["news_items_json"]) or [],
                "analyst_questions": _from_json(llm_payload["analyst_questions_json"]) or [],
                "raw_response": llm_payload["raw_response"],
                "error_message": llm_payload["error_message"],
                "created_at": llm_payload["created_at"],
            }
        return detail

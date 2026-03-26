from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from src.config import get_settings


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL UNIQUE,
        cnpj TEXT,
        code_cvm INTEGER,
        is_active INTEGER NOT NULL DEFAULT 1,
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        requested_ticker TEXT NOT NULL,
        normalized_ticker TEXT NOT NULL,
        trigger TEXT NOT NULL,
        status TEXT NOT NULL,
        llm_status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        error_summary TEXT,
        raw_payload_json TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(company_id) REFERENCES companies(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS company_profile_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL UNIQUE,
        company_id INTEGER,
        company_name TEXT,
        sector TEXT,
        segment TEXT,
        business_description TEXT,
        sources_json TEXT,
        captured_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES pipeline_runs(id),
        FOREIGN KEY(company_id) REFERENCES companies(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL UNIQUE,
        current_price REAL,
        p_l REAL,
        roe REAL,
        net_debt_ebitda REAL,
        net_margin REAL,
        dividend_yield REAL,
        net_debt REAL,
        ebitda REAL,
        metric_sources_json TEXT,
        metric_warnings_json TEXT,
        price_history_json TEXT,
        captured_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES pipeline_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS news_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        position INTEGER NOT NULL,
        title TEXT,
        source TEXT,
        published_at TEXT,
        url TEXT,
        FOREIGN KEY(run_id) REFERENCES pipeline_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS llm_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL UNIQUE,
        status TEXT NOT NULL,
        business_summary TEXT,
        fundamentals_interpretation TEXT,
        news_overall TEXT,
        news_items_json TEXT,
        analyst_questions_json TEXT,
        raw_response TEXT,
        error_message TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES pipeline_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        stage TEXT NOT NULL,
        error_type TEXT NOT NULL,
        message TEXT NOT NULL,
        details TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES pipeline_runs(id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_runs_ticker_started_at ON pipeline_runs(normalized_ticker, started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_errors_run_id ON pipeline_errors(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_news_run_id ON news_snapshots(run_id)",
)


class SQLiteDatabase:
    def __init__(self, database_path: str | None = None) -> None:
        settings = get_settings()
        self.database_path = Path(database_path or settings.database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connection() as connection:
            for statement in SCHEMA_STATEMENTS:
                connection.execute(statement)

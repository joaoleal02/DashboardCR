from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    openai_api_key: str | None
    openai_model: str
    openai_base_url: str | None
    database_path: str
    log_level: str
    log_file_path: str
    request_timeout: int = 25
    google_news_rss_url: str = "https://news.google.com/rss/search"
    b3_listed_companies_url: str = (
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/{payload}"
    )
    statusinvest_price_url: str = "https://statusinvest.com.br/acao/tickerprice"
    statusinvest_price_range_url: str = "https://statusinvest.com.br/acao/tickerpricerange"
    statusinvest_provents_url: str = "https://statusinvest.com.br/acao/companytickerprovents"
    cvm_fca_general_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/fca_cia_aberta_{year}.zip"
    cvm_fre_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_{year}.zip"
    cvm_dfp_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{year}.zip"
    cvm_itr_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{year}.zip"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    database_path = os.getenv("DATABASE_PATH", str(Path("data") / "briefings.db"))
    log_file_path = os.getenv("LOG_FILE_PATH", str(Path("logs") / "pipeline.log"))
    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4-mini"),
        openai_base_url=os.getenv("OPENAI_BASE_URL"),
        database_path=database_path,
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        log_file_path=log_file_path,
    )

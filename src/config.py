from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    openai_api_key: str | None
    openai_model: str
    openai_base_url: str | None
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
    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL"),
        openai_base_url=os.getenv("OPENAI_BASE_URL"),
    )

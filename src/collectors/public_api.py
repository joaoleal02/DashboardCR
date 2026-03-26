from __future__ import annotations

import base64
from datetime import date, datetime, timedelta
import io
import json
from functools import lru_cache
import re
from typing import Any
import unicodedata
import zipfile

import pandas as pd
import requests

from src.config import get_settings


SCALE_MULTIPLIERS = {
    "UNIDADE": 1,
    "UNIDADES": 1,
    "MIL": 1_000,
    "MILHAR": 1_000,
    "MILHARES": 1_000,
    "MILHAO": 1_000_000,
    "MILHOES": 1_000_000,
    "BILHAO": 1_000_000_000,
    "BILHOES": 1_000_000_000,
}


class PublicDataAPI:
    """Thin API client around the public B3, CVM, and Status Invest endpoints used by the app."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def get_company_profile(self, ticker: str) -> dict[str, Any]:
        b3_company = self._fetch_b3_company(ticker)
        code_cvm = self._safe_int(b3_company.get("codeCVM"))
        cnpj = self._digits_only(b3_company.get("cnpj"))
        cvm_general = self._fetch_latest_cvm_general(code_cvm=code_cvm, cnpj=cnpj)

        return {
            "company_name": b3_company.get("companyName") or cvm_general.get("Nome_Empresarial"),
            "sector": cvm_general.get("Setor_Atividade"),
            "segment": b3_company.get("segment") or cvm_general.get("Setor_Atividade"),
            "business_description": cvm_general.get("Descricao_Atividade"),
            "_sources": ["b3-listed-companies-api", "cvm-open-data"],
            "_code_cvm": code_cvm,
            "_cnpj": cnpj,
        }

    def get_market_data(self, ticker: str) -> dict[str, Any]:
        b3_company = self._fetch_b3_company(ticker)
        code_cvm = self._safe_int(b3_company.get("codeCVM"))
        cnpj = self._digits_only(b3_company.get("cnpj"))
        cvm_general = self._fetch_latest_cvm_general(code_cvm=code_cvm, cnpj=cnpj)

        price_history = self._fetch_statusinvest_price_history(ticker)
        current_price = self._fetch_current_price(ticker, price_history)
        annual_fundamentals = self._build_annual_fundamentals(
            code_cvm=code_cvm,
            cnpj=cnpj,
            sector=cvm_general.get("Setor_Atividade"),
            current_price=current_price,
            ticker=ticker,
        )

        return {
            "current_price": current_price,
            "p_l": annual_fundamentals.get("p_l"),
            "roe": annual_fundamentals.get("roe"),
            "net_debt_ebitda": annual_fundamentals.get("net_debt_ebitda"),
            "net_margin": annual_fundamentals.get("net_margin"),
            "dividend_yield": annual_fundamentals.get("dividend_yield"),
            "net_debt": annual_fundamentals.get("net_debt"),
            "ebitda": annual_fundamentals.get("ebitda"),
            "metric_sources": annual_fundamentals.get("metric_sources", {}),
            "metric_warnings": annual_fundamentals.get("metric_warnings", []),
            "price_history": price_history,
            "_profile_fallbacks": {
                "company_name": b3_company.get("companyName") or cvm_general.get("Nome_Empresarial"),
                "sector": cvm_general.get("Setor_Atividade"),
                "segment": b3_company.get("segment") or cvm_general.get("Setor_Atividade"),
            },
            "_sources": ["b3-listed-companies-api", "yfinance-fundamentals", "cvm-open-data", "statusinvest-api"],
        }

    def _build_annual_fundamentals(
        self,
        code_cvm: int | None,
        cnpj: str | None,
        sector: str | None,
        current_price: float | None,
        ticker: str,
    ) -> dict[str, Any]:
        yfinance_fundamentals = self._fetch_yfinance_fundamentals(ticker)
        dre = pd.DataFrame()
        bpa = pd.DataFrame()
        bpp = pd.DataFrame()
        dfc = pd.DataFrame()
        total_shares = None

        if code_cvm is not None:
            dre = self._load_best_statement_frame("DRE", code_cvm)
            bpa = self._load_best_statement_frame("BPA", code_cvm)
            bpp = self._load_best_statement_frame("BPP", code_cvm)
            dfc = self._load_best_cash_flow_frame(code_cvm)
            total_shares = self._fetch_total_shares(cnpj)

        revenue = self._extract_first_value(dre, code="3.01")
        ebit = self._extract_ebit(dre, sector)
        net_income = self._extract_net_income(dre)
        equity = self._extract_equity(bpp)
        cash = self._extract_cash(bpa)
        debt = self._extract_debt(bpp, sector)
        depreciation = self._extract_depreciation_amortization(dfc)
        cvm_ebitda = ebit + depreciation if ebit is not None and depreciation is not None else None
        cvm_net_debt = debt - cash if debt is not None and cash is not None else None
        is_bank_like = self._is_bank_like_sector(sector)
        cvm_p_l = self._calculate_p_l(current_price=current_price, net_income=net_income, total_shares=total_shares)
        cvm_roe = self._calculate_ratio(numerator=net_income, denominator=equity, as_percent=True)
        cvm_net_margin = self._calculate_ratio(numerator=net_income, denominator=revenue, as_percent=True)
        statusinvest_dividend_yield = self._fetch_dividend_yield(ticker, current_price)
        cvm_net_debt_ebitda = self._calculate_ratio(
            numerator=cvm_net_debt,
            denominator=cvm_ebitda,
            as_percent=False,
        )

        p_l, p_l_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("p_l"),
            primary_source="Yahoo Finance API",
            primary_detail="Direct trailing P/L returned by Yahoo Finance fundamentals.",
            primary_status="direct",
            fallback_value=cvm_p_l,
            fallback_source="Fallback derived from Status Invest + CVM",
            fallback_detail="Computed as current price divided by earnings per share, where EPS = CVM net income / CVM total shares.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for P/L. If unavailable, the app falls back to the existing Status Invest and CVM-based calculation.",
            missing_reason="Could not get P/L from Yahoo Finance and could not derive it from current price, net income, and total shares.",
        )
        roe, roe_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("roe"),
            primary_source="Yahoo Finance API",
            primary_detail="Direct return on equity returned by Yahoo Finance fundamentals.",
            primary_status="direct",
            fallback_value=cvm_roe,
            fallback_source="Fallback derived from CVM",
            fallback_detail="Computed as CVM net income divided by CVM equity.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for ROE. If unavailable, the app falls back to the existing CVM-based calculation.",
            missing_reason="Could not get ROE from Yahoo Finance and could not derive it because net income or equity was unavailable in the latest filings.",
        )
        net_margin, net_margin_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("net_margin"),
            primary_source="Yahoo Finance API",
            primary_detail="Direct net margin returned by Yahoo Finance fundamentals.",
            primary_status="direct",
            fallback_value=cvm_net_margin,
            fallback_source="Fallback derived from CVM",
            fallback_detail="Computed as CVM net income divided by CVM revenue.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for Net Margin. If unavailable, the app falls back to the existing CVM-based calculation.",
            missing_reason="Could not get Net Margin from Yahoo Finance and could not derive it because revenue or net income was unavailable in the latest filings.",
        )
        dividend_yield, dividend_yield_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("dividend_yield"),
            primary_source="Yahoo Finance API",
            primary_detail="Direct dividend yield returned by Yahoo Finance fundamentals.",
            primary_status="direct",
            fallback_value=statusinvest_dividend_yield,
            fallback_source="Fallback derived from Status Invest",
            fallback_detail="Computed as last-year provents per share divided by the current price.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for Dividend Yield. If unavailable, the app falls back to the existing Status Invest-based calculation.",
            missing_reason="Could not get Dividend Yield from Yahoo Finance and could not derive it because current price or the provents history was unavailable.",
        )
        net_debt, net_debt_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("net_debt"),
            primary_source=yfinance_fundamentals.get("net_debt_source_label") or "Derived from Yahoo Finance",
            primary_detail=yfinance_fundamentals.get("net_debt_detail")
            or "Computed from Yahoo Finance total debt minus total cash because net debt was not directly returned.",
            primary_status="direct"
            if yfinance_fundamentals.get("net_debt_source_label") == "Yahoo Finance API"
            else "derived",
            fallback_value=cvm_net_debt,
            fallback_source="Fallback derived from CVM",
            fallback_detail="Computed as CVM financial debt minus cash and financial applications.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for Net Debt. If unavailable, the app falls back to the existing CVM-based calculation.",
            missing_reason=(
                "Could not get Net Debt from Yahoo Finance and could not derive it because the public APIs did not provide comparable debt and cash inputs for this ticker."
                if not is_bank_like
                else "Could not get Net Debt from Yahoo Finance and could not derive a comparable value for this bank from the available public APIs."
            ),
        )
        ebitda, ebitda_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("ebitda"),
            primary_source="Yahoo Finance API",
            primary_detail="Direct EBITDA returned by Yahoo Finance fundamentals.",
            primary_status="direct",
            fallback_value=cvm_ebitda,
            fallback_source="Fallback derived from CVM",
            fallback_detail="Computed as EBIT plus depreciation/amortization from CVM statements.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for EBITDA. If unavailable, the app falls back to the existing CVM-based calculation.",
            missing_reason=(
                "Could not get EBITDA from Yahoo Finance and could not derive it because EBIT or depreciation/amortization was unavailable in the latest filings."
                if not is_bank_like
                else "Could not get EBITDA from Yahoo Finance and could not derive a comparable value for this bank from the available public APIs."
            ),
        )
        net_debt_ebitda, net_debt_ebitda_source = self._select_metric_source(
            primary_value=yfinance_fundamentals.get("net_debt_ebitda"),
            primary_source="Derived from Yahoo Finance",
            primary_detail="Computed as Yahoo Finance net debt divided by Yahoo Finance EBITDA.",
            primary_status="derived",
            fallback_value=cvm_net_debt_ebitda,
            fallback_source="Fallback derived from CVM",
            fallback_detail="Computed as derived Net Debt divided by derived EBITDA using CVM statements.",
            fallback_status="derived",
            missing_source="Yahoo Finance API + fallback",
            missing_detail="Yahoo Finance is queried first for the inputs needed to derive Net Debt / EBITDA. If unavailable, the app falls back to the existing CVM-based calculation.",
            missing_reason=(
                "Could not get or derive Net Debt / EBITDA from Yahoo Finance and the CVM fallback also did not have the required inputs."
                if not is_bank_like
                else "Could not get or derive a comparable Net Debt / EBITDA value for this bank from Yahoo Finance or the available public APIs."
            ),
        )

        metric_sources = {
            "current_price": self._make_metric_source(
                value=current_price,
                source="Status Invest API",
                detail="Latest available quote from the Status Invest price endpoint.",
                missing_reason="Could not read the latest quote from the available public APIs.",
                status="direct",
            ),
            "p_l": p_l_source,
            "roe": roe_source,
            "net_debt": net_debt_source,
            "ebitda": ebitda_source,
            "net_debt_ebitda": net_debt_ebitda_source,
            "net_margin": net_margin_source,
            "dividend_yield": dividend_yield_source,
        }

        return {
            "p_l": p_l,
            "roe": roe,
            "net_margin": net_margin,
            "dividend_yield": dividend_yield,
            "net_debt": net_debt,
            "ebitda": ebitda,
            "net_debt_ebitda": net_debt_ebitda,
            "metric_sources": metric_sources,
            "metric_warnings": self._build_metric_warnings(metric_sources),
        }

    def _fetch_yfinance_fundamentals(self, ticker: str) -> dict[str, Any]:
        try:
            import yfinance as yf
        except Exception:
            return {}

        try:
            info = yf.Ticker(f"{ticker.upper()}.SA").get_info()
        except Exception:
            return {}

        if not isinstance(info, dict):
            return {}

        ebitda = self._safe_float(info.get("ebitda"))
        direct_net_debt = self._safe_float(info.get("netDebt"))
        total_debt = self._safe_float(info.get("totalDebt"))
        total_cash = self._safe_float(info.get("totalCash"))

        net_debt = direct_net_debt
        net_debt_source_label = "Yahoo Finance API" if direct_net_debt is not None else None
        net_debt_detail = "Direct net debt returned by Yahoo Finance fundamentals." if direct_net_debt is not None else None
        if net_debt is None and total_debt is not None and total_cash is not None:
            net_debt = total_debt - total_cash
            net_debt_source_label = "Derived from Yahoo Finance"
            net_debt_detail = "Computed as Yahoo Finance total debt minus Yahoo Finance total cash."

        net_debt_ebitda = None
        if net_debt is not None and ebitda not in (None, 0):
            net_debt_ebitda = float(net_debt) / float(ebitda)

        return {
            "p_l": self._safe_float(info.get("trailingPE")),
            "roe": self._decimal_ratio_to_percent(info.get("returnOnEquity")),
            "net_margin": self._decimal_ratio_to_percent(info.get("profitMargins")),
            "dividend_yield": self._decimal_ratio_to_percent(info.get("dividendYield")),
            "ebitda": ebitda,
            "net_debt": net_debt,
            "net_debt_ebitda": net_debt_ebitda,
            "net_debt_source_label": net_debt_source_label,
            "net_debt_detail": net_debt_detail,
        }

    def _fetch_b3_company(self, ticker: str) -> dict[str, Any]:
        payload = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 20,
            "company": ticker,
        }
        encoded_payload = base64.b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("utf-8")
        url = self.settings.b3_listed_companies_url.format(payload=encoded_payload)
        data = self._request_json(url)
        results = data.get("results") or []
        return results[0] if results else {}

    def _fetch_latest_cvm_general(self, code_cvm: int | None, cnpj: str | None) -> dict[str, Any]:
        for year in self._candidate_years():
            frame = self._load_cvm_fca_general(year)
            if frame.empty:
                continue

            subset = frame
            if code_cvm is not None:
                subset = subset[subset["Codigo_CVM"] == code_cvm]
            if subset.empty and cnpj:
                subset = frame[frame["CNPJ_DIGITS"] == cnpj]
            if subset.empty:
                continue

            ordered = subset.sort_values(["Data_Referencia", "Versao"], ascending=[True, True])
            return ordered.iloc[-1].to_dict()
        return {}

    def _fetch_total_shares(self, cnpj: str | None) -> float | None:
        if not cnpj:
            return None

        for year in self._candidate_years():
            frame = self._load_cvm_capital_social(year)
            if frame.empty:
                continue

            subset = frame[frame["CNPJ_DIGITS"] == cnpj]
            if subset.empty:
                continue

            preferred = subset[
                subset["Tipo_Capital_NORM"].isin({"CAPITAL INTEGRALIZADO", "CAPITAL EMITIDO", "CAPITAL SUBSCRITO"})
            ]
            if preferred.empty:
                preferred = subset

            ordered = preferred.sort_values(["Data_Referencia", "Versao"], ascending=[True, True])
            last_row = ordered.iloc[-1]
            try:
                return float(last_row["Quantidade_Total_Acoes"])
            except (TypeError, ValueError):
                return None
        return None

    def _fetch_current_price(self, ticker: str, price_history: list[dict[str, Any]]) -> float | None:
        quote_payload = self._request_json(
            self.settings.statusinvest_price_url,
            params={"ticker": ticker},
        )
        if isinstance(quote_payload, list) and quote_payload:
            prices = quote_payload[0].get("prices") or []
            if prices:
                last_price = prices[-1].get("price")
                try:
                    return float(last_price)
                except (TypeError, ValueError):
                    pass

        if price_history:
            last_close = price_history[-1].get("close")
            try:
                return float(last_close)
            except (TypeError, ValueError):
                return None
        return None

    def _fetch_statusinvest_price_history(self, ticker: str) -> list[dict[str, Any]]:
        end_date = date.today()
        start_date = end_date - timedelta(days=370)
        payload = self._request_json(
            self.settings.statusinvest_price_range_url,
            params={
                "ticker": ticker,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        )
        if not isinstance(payload, dict) or not payload.get("success"):
            return []

        series = payload.get("data") or []
        if not series:
            return []

        items: list[dict[str, Any]] = []
        seen_dates: set[str] = set()
        for point in series[0].get("prices") or []:
            parsed_date = self._parse_statusinvest_datetime(point.get("date"))
            raw_price = point.get("price")
            if parsed_date is None or raw_price is None:
                continue
            iso_date = parsed_date.date().isoformat()
            if iso_date in seen_dates:
                continue
            seen_dates.add(iso_date)
            items.append({"date": iso_date, "close": float(raw_price)})
        return items

    def _fetch_dividend_yield(self, ticker: str, current_price: float | None) -> float | None:
        if current_price in (None, 0):
            return None

        payload = self._request_json(
            self.settings.statusinvest_provents_url,
            params={"ticker": ticker, "chartProventsType": 2},
        )
        dividend_per_share = self._parse_decimal_string(payload.get("earningsLastYear"))
        if dividend_per_share is None:
            return None
        return (dividend_per_share / float(current_price)) * 100

    def _load_latest_statement_frame(self, document_type: str, statement: str, code_cvm: int) -> pd.DataFrame:
        for year in self._candidate_years():
            for scope in ("con", "ind"):
                frame = self._load_statement_csv(document_type, statement, scope, year)
                if frame.empty:
                    continue
                prepared = self._prepare_statement_frame(frame, code_cvm=code_cvm)
                if not prepared.empty:
                    return prepared
        return pd.DataFrame()

    def _load_best_statement_frame(self, statement: str, code_cvm: int) -> pd.DataFrame:
        for document_type in ("DFP", "ITR"):
            frame = self._load_latest_statement_frame(document_type, statement, code_cvm)
            if not frame.empty:
                return frame
        return pd.DataFrame()

    def _load_best_cash_flow_frame(self, code_cvm: int) -> pd.DataFrame:
        for document_type in ("DFP", "ITR"):
            frame = self._load_latest_cash_flow_frame(document_type, code_cvm)
            if not frame.empty:
                return frame
        return pd.DataFrame()

    def _load_latest_cash_flow_frame(self, document_type: str, code_cvm: int) -> pd.DataFrame:
        for statement in ("DFC_MI", "DFC_MD"):
            frame = self._load_latest_statement_frame(document_type, statement, code_cvm)
            if not frame.empty:
                return frame
        return pd.DataFrame()

    def _prepare_statement_frame(self, frame: pd.DataFrame, code_cvm: int) -> pd.DataFrame:
        subset = frame[frame["CD_CVM"] == code_cvm].copy()
        if subset.empty:
            return subset

        subset["DT_REFER"] = pd.to_datetime(subset["DT_REFER"], errors="coerce")
        subset["ORDEM_EXERC_NORM"] = subset["ORDEM_EXERC"].map(self._normalize_text)
        subset["CD_CONTA_STR"] = subset["CD_CONTA"].astype(str)
        subset["DS_CONTA_NORM"] = subset["DS_CONTA"].map(self._normalize_text)
        subset["VALUE"] = subset.apply(self._scale_statement_value, axis=1)

        latest_date = subset["DT_REFER"].max()
        subset = subset[(subset["DT_REFER"] == latest_date) & (subset["ORDEM_EXERC_NORM"] == "ULTIMO")]
        if subset.empty:
            return subset

        if "DT_INI_EXERC" in subset.columns and "DT_FIM_EXERC" in subset.columns:
            subset["DT_INI_EXERC"] = pd.to_datetime(subset["DT_INI_EXERC"], errors="coerce")
            subset["DT_FIM_EXERC"] = pd.to_datetime(subset["DT_FIM_EXERC"], errors="coerce")
            subset["PERIOD_DAYS"] = (
                subset["DT_FIM_EXERC"] - subset["DT_INI_EXERC"]
            ).dt.days.fillna(-1)
            subset = subset.sort_values(["CD_CONTA_STR", "PERIOD_DAYS"], ascending=[True, False]).drop_duplicates(
                subset=["CD_CONTA_STR"], keep="first"
            )
        else:
            subset = subset.sort_values(["CD_CONTA_STR"]).drop_duplicates(subset=["CD_CONTA_STR"], keep="first")
        return subset

    def _extract_first_value(self, frame: pd.DataFrame, code: str) -> float | None:
        if frame.empty:
            return None
        matched = frame[frame["CD_CONTA_STR"] == code]
        if matched.empty:
            return None
        return self._safe_float(matched.iloc[0]["VALUE"])

    def _extract_ebit(self, frame: pd.DataFrame, sector: str | None) -> float | None:
        if frame.empty or self._is_bank_like_sector(sector):
            return None
        candidates = frame[
            frame["DS_CONTA_NORM"].str.contains("RESULTADO ANTES DO RESULTADO FINANCEIRO E DOS TRIBUTOS", regex=False)
        ]
        if not candidates.empty:
            return self._safe_float(candidates.iloc[0]["VALUE"])
        return self._extract_first_value(frame, code="3.05")

    def _extract_net_income(self, frame: pd.DataFrame) -> float | None:
        if frame.empty:
            return None
        priority_patterns = (
            "LUCRO/PREJUIZO CONSOLIDADO DO PERIODO",
            "LUCRO/PREJUIZO DO PERIODO",
            "RESULTADO LIQUIDO DAS OPERACOES CONTINUADAS",
        )
        for pattern in priority_patterns:
            candidates = frame[frame["DS_CONTA_NORM"].str.contains(pattern, regex=False)]
            if not candidates.empty:
                return self._safe_float(candidates.iloc[0]["VALUE"])
        for code in ("3.11", "3.09", "3.07"):
            value = self._extract_first_value(frame, code=code)
            if value is not None:
                return value
        return None

    def _extract_cash(self, frame: pd.DataFrame) -> float | None:
        if frame.empty:
            return None
        total = 0.0
        found = False
        for code in ("1.01.01", "1.01.02", "1.02.01.03"):
            value = self._extract_first_value(frame, code=code)
            if value is None:
                continue
            total += value
            found = True
        return total if found else None

    def _extract_equity(self, frame: pd.DataFrame) -> float | None:
        if frame.empty:
            return None
        candidates = frame[frame["DS_CONTA_NORM"].str.contains("PATRIMONIO LIQUIDO", regex=False)].copy()
        if candidates.empty:
            return self._extract_first_value(frame, code="2.03")
        candidates["DEPTH"] = candidates["CD_CONTA_STR"].str.count(r"\.")
        candidates = candidates.sort_values(["DEPTH", "CD_CONTA_STR"], ascending=[True, True])
        return self._safe_float(candidates.iloc[0]["VALUE"])

    def _extract_debt(self, frame: pd.DataFrame, sector: str | None) -> float | None:
        if frame.empty or self._is_bank_like_sector(sector):
            return None

        current_long_term = frame[
            frame["CD_CONTA_STR"].str.fullmatch(r"2\.0[12]\.\d{2}")
            & frame["DS_CONTA_NORM"].str.contains(
                "EMPRESTIMOS|FINANCIAMENTOS|DEBENTURES|ARRENDAMENTO|PASSIVOS DE ARRENDAMENTO",
                regex=True,
            )
        ]
        if current_long_term.empty:
            return None
        return float(current_long_term["VALUE"].sum())

    def _extract_depreciation_amortization(self, frame: pd.DataFrame) -> float | None:
        if frame.empty:
            return None
        candidates = frame[
            frame["CD_CONTA_STR"].str.startswith("6.01.01")
            & frame["DS_CONTA_NORM"].str.contains("DEPRECI|AMORTIZ|EXAUST", regex=True)
            & ~frame["DS_CONTA_NORM"].str.contains(
                "EMPRESTIMOS|FINANCIAMENTOS|DEBENTURES|JUROS|DIVIDENDOS|CAPTACAO",
                regex=True,
            )
        ]
        if candidates.empty:
            return None
        return float(candidates["VALUE"].clip(lower=0).sum())

    def _calculate_p_l(
        self,
        current_price: float | None,
        net_income: float | None,
        total_shares: float | None,
    ) -> float | None:
        if current_price in (None, 0) or net_income in (None, 0) or total_shares in (None, 0):
            return None
        earnings_per_share = float(net_income) / float(total_shares)
        if earnings_per_share == 0:
            return None
        return float(current_price) / earnings_per_share

    def _calculate_ratio(
        self,
        numerator: float | None,
        denominator: float | None,
        as_percent: bool,
    ) -> float | None:
        if numerator is None or denominator in (None, 0):
            return None
        ratio = float(numerator) / float(denominator)
        return ratio * 100 if as_percent else ratio

    def _decimal_ratio_to_percent(self, value: Any) -> float | None:
        parsed = self._safe_float(value)
        if parsed is None:
            return None
        return parsed * 100

    def _select_metric_source(
        self,
        primary_value: float | None,
        primary_source: str,
        primary_detail: str,
        primary_status: str,
        fallback_value: float | None,
        fallback_source: str,
        fallback_detail: str,
        fallback_status: str,
        missing_source: str,
        missing_detail: str,
        missing_reason: str,
    ) -> tuple[float | None, dict[str, Any]]:
        if primary_value is not None:
            return primary_value, self._make_metric_source(
                value=primary_value,
                source=primary_source,
                detail=primary_detail,
                missing_reason=missing_reason,
                status=primary_status,
            )
        if fallback_value is not None:
            return fallback_value, self._make_metric_source(
                value=fallback_value,
                source=fallback_source,
                detail=fallback_detail,
                missing_reason=missing_reason,
                status=fallback_status,
            )
        return None, self._make_metric_source(
            value=None,
            source=missing_source,
            detail=missing_detail,
            missing_reason=missing_reason,
            status="missing",
        )

    def _make_metric_source(
        self,
        value: float | None,
        source: str,
        detail: str,
        missing_reason: str,
        status: str | None = None,
    ) -> dict[str, Any]:
        resolved_status = status or ("missing" if value is None else ("derived" if "derive" in source.lower() else "direct"))
        return {
            "status": resolved_status,
            "source": source,
            "detail": detail,
            "missing_reason": missing_reason if value is None else None,
        }

    def _build_metric_warnings(self, metric_sources: dict[str, dict[str, Any]]) -> list[str]:
        labels = {
            "current_price": "Current price",
            "p_l": "P/L",
            "roe": "ROE",
            "net_debt_ebitda": "Net Debt / EBITDA",
            "net_margin": "Net Margin",
            "dividend_yield": "Dividend Yield",
        }
        warnings: list[str] = []
        for field, label in labels.items():
            metadata = metric_sources.get(field) or {}
            reason = metadata.get("missing_reason")
            if reason:
                warnings.append(f"{label}: {reason}")
        return warnings

    def _scale_statement_value(self, row: pd.Series) -> float | None:
        value = self._safe_float(row.get("VL_CONTA"))
        if value is None:
            return None
        scale = self._normalize_text(row.get("ESCALA_MOEDA"))
        multiplier = SCALE_MULTIPLIERS.get(scale, 1)
        return value * multiplier

    def _load_cvm_fca_general(self, year: int) -> pd.DataFrame:
        url = self.settings.cvm_fca_general_url.format(year=year)
        frame = self._load_csv_from_zip(
            url=url,
            member_name=f"fca_cia_aberta_geral_{year}.csv",
            usecols=(
                "CNPJ_Companhia",
                "Data_Referencia",
                "Versao",
                "Codigo_CVM",
                "Nome_Empresarial",
                "Setor_Atividade",
                "Descricao_Atividade",
            ),
        )
        if frame.empty:
            return frame
        frame["CNPJ_DIGITS"] = frame["CNPJ_Companhia"].map(self._digits_only)
        return frame

    def _load_cvm_capital_social(self, year: int) -> pd.DataFrame:
        url = self.settings.cvm_fre_url.format(year=year)
        frame = self._load_csv_from_zip(
            url=url,
            member_name=f"fre_cia_aberta_capital_social_{year}.csv",
            usecols=(
                "CNPJ_Companhia",
                "Data_Referencia",
                "Versao",
                "Tipo_Capital",
                "Quantidade_Total_Acoes",
            ),
        )
        if frame.empty:
            return frame
        frame["CNPJ_DIGITS"] = frame["CNPJ_Companhia"].map(self._digits_only)
        frame["Tipo_Capital_NORM"] = frame["Tipo_Capital"].map(self._normalize_text)
        return frame

    def _load_statement_csv(self, document_type: str, statement: str, scope: str, year: int) -> pd.DataFrame:
        base_url = self.settings.cvm_dfp_url if document_type == "DFP" else self.settings.cvm_itr_url
        prefix = document_type.lower()
        member_name = f"{prefix}_cia_aberta_{statement}_{scope}_{year}.csv"
        usecols: tuple[str, ...] = (
            "CD_CVM",
            "DT_REFER",
            "ORDEM_EXERC",
            "CD_CONTA",
            "DS_CONTA",
            "VL_CONTA",
            "ESCALA_MOEDA",
        )
        if statement in {"DRE", "DFC_MI", "DFC_MD"}:
            usecols = usecols + ("DT_INI_EXERC", "DT_FIM_EXERC")
        return self._load_csv_from_zip(url=base_url.format(year=year), member_name=member_name, usecols=usecols)

    @lru_cache(maxsize=64)
    def _load_csv_from_zip(self, url: str, member_name: str, usecols: tuple[str, ...]) -> pd.DataFrame:
        try:
            response = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=self.settings.request_timeout * 3,
            )
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                with archive.open(member_name) as member:
                    return pd.read_csv(
                        member,
                        sep=";",
                        encoding="latin1",
                        usecols=list(usecols),
                        low_memory=False,
                    )
        except Exception:
            return pd.DataFrame()

    def _request_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        params_json = json.dumps(params or {}, sort_keys=True)
        return self._cached_request_json(url, params_json)

    @lru_cache(maxsize=128)
    def _cached_request_json(self, url: str, params_json: str) -> Any:
        params = json.loads(params_json)
        response = requests.get(
            url,
            params=params,
            headers={"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"},
            timeout=self.settings.request_timeout,
        )
        response.raise_for_status()
        return response.json()

    def _candidate_years(self) -> list[int]:
        current_year = date.today().year
        return list(range(current_year, current_year - 6, -1))

    def _parse_statusinvest_datetime(self, value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        for fmt in ("%d/%m/%y %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _parse_decimal_string(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        text = text.replace(".", "").replace(",", ".")
        text = re.sub(r"[^0-9.\-]", "", text)
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _normalize_text(self, value: Any) -> str:
        normalized = unicodedata.normalize("NFKD", str(value or ""))
        normalized = normalized.encode("ascii", "ignore").decode("ascii")
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized.upper()

    def _digits_only(self, value: Any) -> str | None:
        digits = re.sub(r"\D", "", str(value or ""))
        if not digits:
            return None
        return digits.zfill(14) if len(digits) <= 14 else digits

    def _is_bank_like_sector(self, sector: str | None) -> bool:
        normalized = self._normalize_text(sector)
        return "BANCO" in normalized or "BANCOS" in normalized

    def _safe_int(self, value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _safe_float(self, value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


@lru_cache(maxsize=1)
def get_public_data_api() -> PublicDataAPI:
    return PublicDataAPI()

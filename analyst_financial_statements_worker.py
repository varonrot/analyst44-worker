# analyst_financial_statements_worker.py
# Adds: D11 (last earnings date, earnings time, close before earnings)

import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime, timedelta

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# ---------------- FMP helpers ---------------- #

def fmp_get(path: str, params: dict | None = None):
    if params is None:
        params = {}
    params["apikey"] = FMP_API_KEY
    url = f"https://financialmodelingprep.com/api/v3{path}"
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def get_income_statements(symbol: str, limit: int = 2):
    return fmp_get(f"/income-statement/{symbol}",
                   {"limit": limit, "period": "quarter"})


def get_balance_sheets(symbol: str, limit: int = 8):
    return fmp_get(f"/balance-sheet-statement/{symbol}",
                   {"limit": limit, "period": "quarter"})


def get_cash_flows(symbol: str, limit: int = 8):
    return fmp_get(f"/cash-flow-statement/{symbol}",
                   {"limit": limit, "period": "quarter"})


def get_ratios(symbol: str, limit: int = 8):
    return fmp_get(f"/ratios/{symbol}",
                   {"limit": limit, "period": "quarter"})


def get_financial_growth(symbol: str, limit: int = 2):
    return fmp_get(f"/financial-growth/{symbol}",
                   {"limit": limit, "period": "quarter"})


def get_key_metrics(symbol: str, limit: int = 8):
    return fmp_get(f"/key-metrics/{symbol}",
                   {"limit": limit, "period": "quarter"})


def get_rating(symbol: str):
    try:
        data = fmp_get(f"/rating/{symbol}")
        return data[0] if data else None
    except:
        return None


def get_analyst_estimates(symbol: str):
    try:
        data = fmp_get(f"/analyst-estimates/{symbol}")
        return data[0] if data else None
    except:
        return None


# ---------------- D11 helpers ---------------- #

def get_last_earnings(symbol: str):
    url = f"https://financialmodelingprep.com/api/v3/historical/earning_calendar/{symbol}?limit=10&apikey={FMP_API_KEY}"
    r = requests.get(url).json()

    if not r:
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    past = [row for row in r if row.get("date") and row["date"] < today]

    if not past:
        return None

    return sorted(past, key=lambda x: x["date"], reverse=True)[0]


def get_close_before_earnings(symbol: str, earnings_date: str):
    try:
        ed = datetime.strptime(earnings_date, "%Y-%m-%d")
    except:
        return None

    prev_str = (ed - timedelta(days=1)).strftime("%Y-%m-%d")

    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={prev_str}&to={prev_str}&apikey={FMP_API_KEY}"
    r = requests.get(url).json()

    try:
        return r["historical"][0]["close"]
    except:
        return None


def get_d11(symbol: str):
    last = get_last_earnings(symbol)
    if not last:
        return None, None, None

    earnings_date = last.get("date")
    earnings_time = last.get("time")  # BMO / AMC
    close_before = get_close_before_earnings(symbol, earnings_date)

    return earnings_date, earnings_time, close_before


# ---------------- Core logic ---------------- #

def build_record_from_row(
    symbol: str,
    income_row: dict,
    balance_row: dict | None,
    cash_row: dict | None,
    ratios_row: dict | None,
    growth_row: dict | None,
    metrics_row: dict | None,
    rating_row: dict | None,
    estimates_row: dict | None,
    d11_data: tuple
) -> dict:

    if balance_row is None: balance_row = {}
    if cash_row is None: cash_row = {}
    if ratios_row is None: ratios_row = {}
    if growth_row is None: growth_row = {}
    if metrics_row is None: metrics_row = {}
    if rating_row is None: rating_row = {}
    if estimates_row is None: estimates_row = {}

    last_earnings_date, last_earnings_time, close_before = d11_data

    return {
        "symbol": symbol,
        "report_date": income_row.get("date"),

        # D1–D10 (קיים)
        "fiscal_year": income_row.get("calendarYear"),
        "fiscal_period": income_row.get("period"),
        "currency": income_row.get("reportedCurrency"),
        "revenue": income_row.get("revenue"),
        "gross_profit": income_row.get("grossProfit"),
        "operating_income": income_row.get("operatingIncome"),
        "net_income": income_row.get("netIncome"),
        "eps_basic": income_row.get("eps"),
        "ebitda": income_row.get("ebitda"),
        "operating_expenses": income_row.get("operatingExpenses"),

        "total_assets": balance_row.get("totalAssets"),
        "total_liabilities": balance_row.get("totalLiabilities"),
        "total_equity": balance_row.get("totalStockholdersEquity"),
        "cash_and_equivalents": balance_row.get("cashAndCashEquivalents"),
        "total_current_assets": balance_row.get("totalCurrentAssets"),
        "total_current_liabilities": balance_row.get("totalCurrentLiabilities"),
        "short_term_debt": balance_row.get("shortTermDebt"),
        "long_term_debt": balance_row.get("longTermDebt"),

        "operating_cash_flow": cash_row.get("netCashProvidedByOperatingActivities"),
        "investing_cash_flow": cash_row.get("netCashUsedForInvestingActivites"),
        "financing_cash_flow": cash_row.get("netCashUsedProvidedByFinancingActivities"),
        "free_cash_flow": cash_row.get("freeCashFlow"),
        "capital_expenditure": cash_row.get("capitalExpenditure"),
        "dividends_paid": cash_row.get("dividendsPaid"),
        "share_buybacks": cash_row.get("commonStockRepurchased")
            or cash_row.get("repurchaseOfCapitalStock"),
        "debt_issued": cash_row.get("debtIssued"),
        "debt_repaid": cash_row.get("debtRepayment"),
        "stock_based_compensation": cash_row.get("stockBasedCompensation"),

        "gross_margin": ratios_row.get("grossProfitMargin"),
        "operating_margin": ratios_row.get("operatingProfitMargin"),
        "net_margin": ratios_row.get("netProfitMargin"),
        "return_on_equity": ratios_row.get("returnOnEquity"),
        "return_on_assets": ratios_row.get("returnOnAssets"),
        "current_ratio": ratios_row.get("currentRatio"),
        "debt_to_equity": ratios_row.get("debtEquityRatio"),
        "interest_coverage": ratios_row.get("interestCoverage"),
        "payout_ratio": ratios_row.get("payoutRatio"),
        "free_cash_flow_margin": ratios_row.get("freeCashFlowMargin"),

        "revenue_growth": growth_row.get("revenueGrowth"),
        "gross_profit_growth": growth_row.get("grossProfitGrowth"),
        "operating_income_growth": growth_row.get("operatingIncomeGrowth"),
        "net_income_growth": growth_row.get("netIncomeGrowth"),
        "eps_growth": growth_row.get("epsGrowth"),
        "operating_cash_flow_growth": growth_row.get("operatingCashFlowGrowth"),
        "free_cash_flow_growth": growth_row.get("freeCashFlowGrowth"),

        "market_cap": metrics_row.get("marketCap"),
        "enterprise_value": metrics_row.get("enterpriseValue"),
        "pe_ratio": metrics_row.get("peRatio"),
        "forward_pe_ratio": metrics_row.get("peRatioForward"),
        "ps_ratio": metrics_row.get("psRatio") or metrics_row.get("priceToSalesRatio"),
        "pb_ratio": metrics_row.get("pbRatio"),
        "price_to_free_cash_flow": metrics_row.get("pfcfRatio"),
        "ev_to_ebitda": metrics_row.get("evToEbitda"),
        "ev_to_sales": metrics_row.get("evToSales"),
        "dividend_yield": metrics_row.get("dividendYield"),

        "roe": ratios_row.get("returnOnEquity"),
        "roa": ratios_row.get("returnOnAssets"),
        "inventory_turnover": ratios_row.get("inventoryTurnover"),
        "asset_turnover": ratios_row.get("assetTurnover"),
        "return_on_tangible_assets": ratios_row.get("returnOnTangibleAssets"),

        "rating": rating_row.get("rating"),
        "rating_score": rating_row.get("ratingScore"),
        "rating_recommendation": rating_row.get("ratingRecommendation"),
        "rating_dcf_score": rating_row.get("ratingDetailsDCFScore"),
        "rating_roe_score": rating_row.get("ratingDetailsROEScore"),
        "rating_roa_score": rating_row.get("ratingDetailsROAScore"),
        "rating_de_score": rating_row.get("ratingDetailsDEScore"),
        "rating_pe_score": rating_row.get("ratingDetailsPEScore"),
        "rating_pb_score": rating_row.get("ratingDetailsPBScore"),

        "estimate_date": estimates_row.get("date"),
        "estimate_period": estimates_row.get("period"),
        "estimated_eps_avg": estimates_row.get("estimatedEpsAvg"),
        "estimated_eps_low": estimates_row.get("estimatedEpsLow"),
        "estimated_eps_high": estimates_row.get("estimatedEpsHigh"),
        "number_analysts_estimated": estimates_row.get("numberAnalystEstimated"),

        # ---------------- D11 ---------------- #
        "last_earnings_date": last_earnings_date,
        "last_earnings_time": last_earnings_time,
        "close_before_earnings": close_before,
    }


def process_symbol(symbol: str):
    print(f"Processing {symbol} ...")

    # Base data
    try:
        income_list = get_income_statements(symbol, limit=2)
    except Exception as e:
        print(f"  Error fetching income statements for {symbol}: {e}")
        return

    if not income_list:
        print(f"  No income data for {symbol}")
        return

    income_rows = sorted(
        income_list,
        key=lambda x: x.get("date", ""),
        reverse=True
    )[:2]

    try: balance_list = get_balance_sheets(symbol, limit=8)
    except: balance_list = []
    balance_by_date = {r.get("date"): r for r in balance_list or []}

    try: cash_list = get_cash_flows(symbol, limit=8)
    except: cash_list = []
    cash_by_date = {r.get("date"): r for r in cash_list or []}

    try: ratios_list = get_ratios(symbol, limit=8)
    except: ratios_list = []
    ratios_by_date = {r.get("date"): r for r in ratios_list or []}

    try: growth_list = get_financial_growth(symbol, limit=2)
    except: growth_list = []
    growth_by_date = {r.get("date"): r for r in growth_list or []}

    try: metrics_list = get_key_metrics(symbol, limit=8)
    except: metrics_list = []
    metrics_by_date = {r.get("date"): r for r in metrics_list or []}

    rating_row = get_rating(symbol)
    estimates_row = get_analyst_estimates(symbol)

    # D11
    d11_data = get_d11(symbol)

    # Merge to records
    for income_row in income_rows:
        report_date = income_row.get("date")
        if not report_date:
            continue

        rec = build_record_from_row(
            symbol,
            income_row,
            balance_by_date.get(report_date),
            cash_by_date.get(report_date),
            ratios_by_date.get(report_date),
            growth_by_date.get(report_date),
            metrics_by_date.get(report_date),
            rating_row,
            estimates_row,
            d11_data,
        )

        try:
            supabase.table("analyst_input_financial_statements") \
                .upsert(rec, on_conflict="symbol,report_date") \
                .execute()
            print(f"  Upserted {symbol} {report_date}")
        except Exception as e:
            print(f"  Error upserting {symbol} {report_date}: {e}")


def run_worker():
    print("Starting analyst_financial_statements_worker...")

    try:
        res = supabase.table("earnings_calendar_us").select("symbol").execute()
        symbols = [row["symbol"] for row in res.data]
    except Exception as e:
        print(f"Error loading symbols: {e}")
        return

    for symbol in symbols:
        try:
            process_symbol(symbol)
        except Exception as e:
            print(f"Unexpected error processing {symbol}: {e}")

    print("Done.")


if __name__ == "__main__":
    run_worker()

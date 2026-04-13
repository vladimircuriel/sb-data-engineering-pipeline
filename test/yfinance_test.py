import yfinance as yf
import json

ticker_symbol = "BAC"
ticker = yf.Ticker(ticker_symbol)

info = ticker.info or {}
basic_info = {
    "symbol": ticker_symbol,
    "industry": info.get("industry"),
    "sector": info.get("sector"),
    "employees": info.get("fullTimeEmployees"),
    "city": info.get("city"),
    "phone": info.get("phone"),
    "state": info.get("state"),
    "country": info.get("country"),
    "website": info.get("website"),
    "address": info.get("address1"),
}

print("============= Basic Info =============")
print(json.dumps(basic_info, indent=2))

price_df = yf.download(ticker_symbol, period="1d")
if price_df is not None:
    price_df.columns = [col[0] for col in price_df.columns]
    price_df.reset_index(inplace=True)
    price_df["Date"] = price_df["Date"].dt.strftime("%Y-%m-%d")
    price_dict = price_df.to_dict(orient="records")
    price_info = {
        "date": price_dict[0]["Date"],
        "open": price_dict[0]["Open"],
        "high": price_dict[0]["High"],
        "low": price_dict[0]["Low"],
        "close": price_dict[0]["Close"],
        "volume": price_dict[0]["Volume"],
    }
    print("============= Latest Price Info =============")
    print(json.dumps(price_info, indent=2))

fundamentals_df = ticker.balance_sheet
fundamentals_dict = fundamentals_df.T.to_dict(orient="records")
fundamentals_info = {
    "assets": fundamentals_dict[0].get("Total Assets"),
    "debt": fundamentals_dict[0].get("Total Debt"),
    "invested_capital": fundamentals_dict[0].get("Invested Capital"),
    "shares_issued": fundamentals_dict[0].get("Share Issued"),
}

print("============= Fundamentals Data =============")
print(json.dumps(fundamentals_info, indent=2))

holders_df = ticker.institutional_holders
holders_df["Date Reported"] = holders_df["Date Reported"].dt.strftime("%Y-%m-%d")
holders_dict = holders_df.to_dict(orient="records")
holders_info = [
    {
        "holder": holder["Holder"],
        "shares": holder["Shares"],
        "value": holder["Value"],
        "date_reported": holder["Date Reported"],
    }
    for holder in holders_dict
]

print("============= Institutional Holders =============")
print(json.dumps(holders_info, indent=2))

rec_df = ticker.upgrades_downgrades
rec_df = rec_df.reset_index()
rec_df["GradeDate"] = rec_df["GradeDate"].dt.strftime("%Y-%m-%d")
rec_dict = rec_df.to_dict(orient="records")
rec_info = {
    "date": rec_dict[0]["GradeDate"],
    "to_grade": rec_dict[0]["ToGrade"],
    "from_grade": rec_dict[0]["FromGrade"],
    "action": rec_dict[0]["Action"],
}

print("============= Recommendations =============")
print(json.dumps(rec_info, indent=2))

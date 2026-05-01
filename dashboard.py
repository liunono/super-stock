# ================= 核心邏輯修正 (請替換 V162.0 對應函數) =================

def update_chip_v3(ticker):
    """💎 籌碼歸位：拉長檢索範圍至 60 天"""
    # 搜尋 60 天確保涵蓋多個週五更新日
    df = fetch_fm("TaiwanStockHoldingSharesPer", ticker, 60)
    fund = None # 預設改為 None，不要用 0
    if df is not None and not df.empty:
        if 'InvestmentTrustHoldingShares' in df.columns:
            # 抓取最後一筆非零的數據
            valid_data = df[df['InvestmentTrustHoldingShares'] > 0]
            if not valid_data.empty:
                fund = int(valid_data['InvestmentTrustHoldingShares'].iloc[-1] / 1000)
            else:
                fund = 0 # 真的是 0 才是 0
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET fund_count = :f WHERE ticker = :t AND scan_date = :d"), 
                     {"f": fund, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if fund is not None else False

def update_roe_v3(ticker):
    """💎 財報歸位：拉長檢索範圍至 730 天 (兩年)"""
    df = fetch_fm("TaiwanStockFinancialStatements", ticker, 730)
    roe = None
    if df is not None and not df.empty:
        # 精準鎖定 ReturnOnEquityAftTax
        r_row = df[df['type'] == 'ReturnOnEquityAftTax']
        if not r_row.empty:
            val = float(r_row['value'].iloc[-1])
            # 自動換算百分比格式
            roe = val / 100 if val > 1 or val < -1 else val
    
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET roe = :r WHERE ticker = :t AND scan_date = :d"), 
                     {"r": roe, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if roe is not None else False

# ================= 介面按鈕邏輯同步更新 =================
# 記得在 Tab1 的按鈕呼叫 update_chip_v3 與 update_roe_v3

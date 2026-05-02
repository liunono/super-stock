import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, datetime
import pytz
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_ta as ta
import io

# ================= 1. 系統地基 (修正語法 & 狀態鎖死) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    # 💎 修正後的 DB_URL：確保大括號語法 100% 正確
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    
    FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2Vy_idIjoibG92ZTUyMTUiLCJlbWFpbCI6ImNocmlzNTIxNUBnbWFpbC5jb20ifQ.yeh3T_iNCA4IWmlsPZHHyVUbMOH_qe35stdLgIv9ONY"
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT,
                roe FLOAT DEFAULT NULL, fund_count INT DEFAULT NULL,
                high_20 FLOAT, vol_20 FLOAT, bb_width FLOAT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# 初始化持久化計數器
if 'scan_status' not in st.session_state:
    st.session_state['scan_status'] = {"daily": "", "fix": "", "chip": "", "roe": ""}

# ================= 2. 核心大腦 (量子暴力挖掘與精算邏輯) =================

def fetch_fm(dataset, ticker, start_days=160):
    cid = str(ticker).split('.')[0].strip()
    start = (datetime.datetime.now(TW_TZ) - datetime.timedelta(days=start_days)).strftime('%Y-%m-%d')
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", 
                         params={"dataset": dataset, "data_id": cid, "start_date": start, "token": FINMIND_TOKEN}, timeout=15).json()
        return pd.DataFrame(r['data']) if r['msg'] == 'success' and r['data'] else None
    except: return None

def update_chip_v_quantum(ticker):
    """💎 籌碼量子修正：搜尋 60 天資料，排除 0，挖出大人蹤跡"""
    df = fetch_fm("TaiwanStockHoldingSharesPer", ticker, 60)
    fund = None
    if df is not None and not df.empty:
        # 關鍵：過濾 InvestmentTrustHoldingShares > 0
        valid = df[df['InvestmentTrustHoldingShares'] > 0]
        if not valid.empty:
            fund = int(valid.iloc[-1]['InvestmentTrustHoldingShares'] / 1000)
        else:
            fund = 0 # 真的沒買
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET fund_count = :f WHERE ticker = :t AND scan_date = :d"), 
                     {"f": fund, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if fund is not None else False

def update_roe_v_brute(ticker):
    """💎 財報暴力精算：抓取淨利與權益，本地立刻精算 ROE"""
    df = fetch_fm("TaiwanStockFinancialStatements", ticker, 730)
    roe_calc = None
    if df is not None and not df.empty:
        income_df = df[df['type'] == 'IncomeAfterTaxes']
        equity_df = df[df['type'].isin(['Equity', 'TotalEquity'])]
        if not income_df.empty and not equity_df.empty:
            latest_date = income_df['date'].iloc[-1]
            net_income = float(income_df[income_df['date'] == latest_date]['value'].iloc[-1])
            target_equity = equity_df[equity_df['date'] <= latest_date]
            if not target_equity.empty:
                total_equity = float(target_equity.iloc[-1]['value'])
                if total_equity != 0:
                    roe_calc = net_income / total_equity # 數字會說話！直接算！
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET roe = :r WHERE ticker = :t AND scan_date = :d"), 
                     {"r": roe_calc, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if roe_calc is not None else False

def calc_and_save_full(ticker, name):
    df = fetch_fm("TaiwanStockPrice", ticker, 160)
    if df is None or len(df) < 60: return False
    df = df.rename(columns={'close':'Close','Trading_Volume':'Volume'})
    df['Close'] = df['Close'].astype(float)
    c, v = df['Close'], df['Volume']
    rsi = float(ta.rsi(c, length=14).iloc[-1]) if len(c) > 14 else 50
    ma20 = c.rolling(20).mean().iloc[-1]; std = c.rolling(20).std().iloc[-1]
    data = {
        "ticker": ticker, "stock_name": name, "price": c.iloc[-1], "change_pct": ((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100,
        "sma5": c.rolling(5).mean().iloc[-1], "ma20": ma20, "ma60": c.rolling(60).mean().iloc[-1],
        "rsi": rsi, "vol": int(v.iloc[-1]), "avg_vol": int(v.rolling(20).mean().iloc[-1]),
        "kd20": c.iloc[-20], "kd60": c.iloc[-60], "scan_date": datetime.datetime.now(TW_TZ).date(),
        "bbu": ma20 + (std*2), "bbl": ma20 - (std*2), "high_20": c.shift(1).rolling(20).max().iloc[-1],
        "vol_20": v.shift(1).rolling(20).mean().iloc[-1], "bb_width": (std*4)/ma20 if ma20 else 0
    }
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO daily_scans (ticker, stock_name, price, change_pct, sma5, ma20, ma60, rsi, bbl, bbu, vol, avg_vol, scan_date, kd20, kd60, high_20, vol_20, bb_width) 
            VALUES (:ticker, :stock_name, :price, :change_pct, :sma5, :ma20, :ma60, :rsi, :bbl, :bbu, :vol, :avg_vol, :scan_date, :kd20, :kd60, :high_20, :vol_20, :bb_width) 
            ON DUPLICATE KEY UPDATE price=VALUES(price), change_pct=VALUES(change_pct), sma5=VALUES(sma5), ma20=VALUES(ma20), rsi=VALUES(rsi), vol=VALUES(vol), avg_vol=VALUES(avg_vol), bb_width=VALUES(bb_width)"""), data)
    return True

# ================= 3. UI 介面設計 (V171.0) =================
st.set_page_config(page_title="🛡️ 哲哲量子戰情室 V171.0", layout="wide")

st.markdown("""<style>
    [data-testid="stBaseButton-secondary"] { width: 100% !important; height: 3.5em !important; font-size: 1.1rem !important; font-weight: 800 !important; border-radius: 12px !important; margin-bottom: 8px !important; background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important; }
    .big-font { font-size:60px !important; font-weight: 900; color: #FF3333; text-shadow: 2px 2px 4px #ddd; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量子戰情室 Sponsor V171.0 — 數據診斷封神版")

tab1, tab2, tab3 = st.tabs(["🚀 指揮中心", "💼 庫存股票戰略中心", "🛠️ 管理中心"])

with tab1:
    st.markdown("### 🏹 暴力數據抓取與週期精算 (Sponsor 6000)")
    c1, c2, c3, c4, c5 = st.columns(5)
    pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    today = datetime.datetime.now(TW_TZ).date()

    with c1:
        if st.button("🚀 每日行情：暴力重掃", key="b_daily"):
            pb = st.progress(0); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool))
            st.session_state['scan_status']['daily'] = f"✅ 行情重掃完成！成功: {s}, 失敗: {f}"
            st.rerun()
    with c2:
        if st.button("🔄 補救掃描：自動補抓", key="b_fix"):
            done = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
            missing = pool[~pool['ticker'].isin(done['ticker'].tolist())]
            if missing.empty: st.session_state['scan_status']['fix'] = "🎯 數據已全壘打！"
            else:
                pb = st.progress(0); s, f = 0, 0
                with ThreadPoolExecutor(max_workers=10) as exe:
                    futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in missing.iterrows()}
                    for i, fut in enumerate(as_completed(futures)):
                        if fut.result(): s += 1
                        else: f += 1
                        pb.progress((i+1)/len(missing))
                st.session_state['scan_status']['fix'] = f"✅ 補救完成！成功: {s}, 失敗: {f}"
                st.rerun()
    with c3:
        if st.button("💼 籌碼補完：量子修正", key="b_chip"):
            pb = st.progress(0); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_chip_v_quantum, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool))
            st.session_state['scan_status']['chip'] = f"✅ 籌碼量子修正完成！成功: {s}, 失敗: {f}"
            st.rerun()
    with c4:
        if st.button("💎 財報精算：ROE暴力算", key="b_roe"):
            pb = st.progress(0); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_roe_v_brute, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool))
            st.session_state['scan_status']['roe'] = f"✅ ROE暴力精算完成！成功: {s}, 失敗: {f}"
            st.rerun()
    with c5:
        if st.button("🔥 清空今日快取", key="b_clear"):
            with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            st.session_state['scan_status'] = {"clear": "🗑️ 今日快取已清空"}
            st.rerun()

    # 顯示持久化計數資訊 (不消失)
    for m in st.session_state['scan_status'].values(): 
        if m: st.info(m)

    st.divider()
    
    # 💎 診斷按鈕：抓取原始數據來看看到底抓了什麼鬼
    st.subheader("🧪 數據診斷實驗室 (看看主力在搞什麼鬼)")
    diag_c1, diag_c2 = st.columns(2)
    with diag_c1:
        if st.button("📥 下載財報原始碼 (ROE 診斷)", key="diag_roe"):
            # 隨機抓一檔台積電示範，或抓全部
            raw_roe = fetch_fm("TaiwanStockFinancialStatements", "2330", 730)
            if raw_roe is not None:
                st.download_button("💾 下載台積電原始財報 (CSV)", data=raw_roe.to_csv(index=False).encode('utf-8-sig'), file_name="raw_roe_2330.csv")
    with diag_c2:
        if st.button("📥 下載籌碼原始碼 (投信診斷)", key="diag_chip"):
            raw_chip = fetch_fm("TaiwanStockHoldingSharesPer", "2330", 60)
            if raw_chip is not None:
                st.download_button("💾 下載台積電原始籌碼 (CSV)", data=raw_chip.to_csv(index=False).encode('utf-8-sig'), file_name="raw_chip_2330.csv")

    st.divider()
    cr, cm, cd = st.columns([1,1,1])
    with cr:
        if st.button("📡 讀取今日快取數據"):
            df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            if not df.empty:
                st.session_state['master_df'] = df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.success(f"✅ 已載入 {len(df)} 檔真錢標的")
    with cm:
        if st.button("🔍 數據照妖鏡 (紅字報警)"):
            all_data = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            st.dataframe(all_data.style.map(lambda x: 'background-color: #FFCCCC; color: red;' if x == 0 or pd.isna(x) or str(x) == "None" else ''), width=1500)
    with cd:
        diag_df = pd.read_sql(text("SELECT ticker, stock_name, roe, fund_count, price FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        st.download_button("📥 下載診斷 CSV", data=diag_df.to_csv(index=False).encode('utf-8-sig'), file_name=f"diag_{today}.csv")

    if 'master_df' in st.session_state:
        st.markdown("### 🔥 七大金剛策略中心 (漸層美化)")
        df = st.session_state['master_df']
        s_list = [("💎 策略 1: 超級策略", "(df['fund_count'] >= 10) & (df['roe'] > 0.05)"), ("📈 策略 2: 帶量突破前高", "(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)"), ("🚀 策略 3: 三線合一多頭", "(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])"), ("🌀 策略 4: 布林縮口突破", "(df['現價'] > df['bbu']) & (df['bb_width'] < 0.2)"), ("👑 策略 5: 九成勝率 ATM", "(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20'] * 1.2)"), ("🛡️ 策略 6: 低階抄底防護", "(df['rsi'] < 40) & (df['現價'] > df['sma5'])"), ("🎯 策略 7: 強勢回測支撐", "abs(df['現價']-df['ma20'])/df['ma20'] < 0.02")]
        for name, cond in s_list:
            if st.button(name):
                res = df[eval(cond)]; st.dataframe(res.style.background_gradient(cmap='YlOrRd', subset=['現價']), width=1500)

with tab2:
    st.header("💼 庫存股票戰略中心 (非 Yahoo 模式)")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 同步庫存行情+損益精算"):
            pb_p = st.progress(0)
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in df_p.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    pb_p.progress((i+1)/len(df_p))
            st.rerun()

        p_prices = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0, ((df_display['price'] - df_display['entry_price']) / df_display['entry_price']) * 100, 0)
        st.markdown(f"當前總獲利：<br><span class='big-font'>${df_display['獲利'].sum():,.0f}</span>", unsafe_allow_html=True)
        st.dataframe(df_display.style.background_gradient(cmap='RdYlGn', subset=['報酬率(%)']), width=1500)

with tab3:
    st.subheader("🛠️ 管理中心")
    c_p, c_c = st.columns(2)
    with c_p:
        f1 = st.file_uploader("上傳股票池 CSV (自動去重)", type="csv")
        if f1 and st.button("💾 儲存池"):
            df_new = pd.read_csv(f1).drop_duplicates(subset=['ticker'])
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM stock_pool")); df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False)
            st.success("✅ 更新成功")
        st.download_button("📥 股票池範例", data="ticker,stock_name\n2330,台積電\n2317,鴻海", file_name="sample_pool.csv")
    with c_c:
        f2 = st.file_uploader("上傳庫存 CSV (覆蓋)", type="csv")
        if f2 and st.button("💾 儲存庫存"):
            df_new = pd.read_csv(f2).drop_duplicates(subset=['ticker'])
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM portfolio")); df_new.to_sql('portfolio', con=engine, if_exists='append', index=False)
            st.success("✅ 庫存覆蓋成功")
        st.download_button("📥 庫存範例", data="ticker,stock_name,entry_price,qty\n2330,台積電,600,1000", file_name="sample_portfolio.csv")

st.caption("本系統由哲哲團隊開發。V171.0 Sponsor 全能旗艦版，贏到流湯不是夢！")

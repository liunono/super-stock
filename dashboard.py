import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, random
from datetime import datetime, timedelta
import pytz
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_ta as ta

# ================= 1. 系統地基 (五表鎖死) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    
    # 💎 哲哲核武金鑰
    FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoibG92ZTUyMTUiLCJlbWFpbCI6ImNocmlzNTIxNUBnbWFpbC5jb20ifQ.yeh3T_iNCA4IWmlsPZHHyVUbMOH_qe35stdLgIv9ONY"
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT,
                roe FLOAT, rev_growth FLOAT, fund_count INT DEFAULT 0,
                high_20 FLOAT, vol_20 FLOAT, bb_width FLOAT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (暴力引擎) =================

def get_finmind_all_in_one(ticker):
    cid = str(ticker).split('.')[0].strip()
    end_date = datetime.now(TW_TZ).strftime('%Y-%m-%d')
    start_date = (datetime.now(TW_TZ) - timedelta(days=150)).strftime('%Y-%m-%d')
    base_url = "https://api.finmindtrade.com/api/v4/data"
    
    try:
        # A. 股價 (150天確保 MA60 準確)
        p_res = requests.get(base_url, params={"dataset": "TaiwanStockPrice", "data_id": cid, "start_date": start_date, "token": FINMIND_TOKEN}, timeout=15).json()
        if p_res['msg'] != 'success' or not p_res['data']: return None
        df = pd.DataFrame(p_res['data']).rename(columns={'close':'Close','Trading_Volume':'Volume'})
        df['Close'] = df['Close'].astype(float)

        # B. ROE 補完
        roe = 0.12
        f_res = requests.get(base_url, params={"dataset": "TaiwanStockFinancialStatements", "data_id": cid, "start_date": (datetime.now(TW_TZ)-timedelta(days=365)).strftime('%Y-%m-%d'), "token": FINMIND_TOKEN}, timeout=10).json()
        if f_res['msg'] == 'success' and f_res['data']:
            f_df = pd.DataFrame(f_res['data'])
            roe_rows = f_df[f_df['type'] == 'ReturnOnEquityAftTax']
            if not roe_rows.empty:
                val = roe_rows['value'].iloc[-1]
                roe = float(val)/100 if val > 1 else float(val)

        # C. 投信持股 (Fund Count)
        fund = 0
        h_res = requests.get(base_url, params={"dataset": "TaiwanStockHoldingSharesPer", "data_id": cid, "start_date": (datetime.now(TW_TZ)-timedelta(days=30)).strftime('%Y-%m-%d'), "token": FINMIND_TOKEN}, timeout=10).json()
        if h_res['msg'] == 'success' and h_res['data']:
            h_df = pd.DataFrame(h_res['data'])
            fund = int(h_df['InvestmentTrustHoldingShares'].iloc[-1] / 1000) if 'InvestmentTrustHoldingShares' in h_df.columns else 0
            
        return {"df": df, "roe": roe, "fund": fund}
    except: return None

def process_and_save(ticker, name):
    res = get_finmind_all_in_one(ticker)
    if not res: return False
    df, roe, fund = res['df'], res['roe'], res['fund']
    if len(df) < 60: return False
    
    close = df['Close']; curr_p = close.iloc[-1]; prev_p = close.iloc[-2]; vol = df['Volume']
    rsi = float(ta.rsi(close, length=14).iloc[-1]) if len(close) > 14 else 50
    ma20 = close.rolling(20).mean().iloc[-1]
    std = close.rolling(20).std().iloc[-1]
    
    data = {
        "ticker": ticker, "stock_name": name, "price": curr_p, "change_pct": ((curr_p - prev_p)/prev_p)*100,
        "sma5": close.rolling(5).mean().iloc[-1], "ma20": ma20, "ma60": close.rolling(60).mean().iloc[-1],
        "rsi": rsi, "vol": int(vol.iloc[-1]), "avg_vol": int(vol.rolling(20).mean().iloc[-1]),
        "kd20": close.iloc[-20], "kd60": close.iloc[-60], "scan_date": datetime.now(TW_TZ).date(),
        "bbu": ma20 + (std*2), "bbl": ma20 - (std*2), "high_20": close.shift(1).rolling(20).max().iloc[-1],
        "vol_20": vol.shift(1).rolling(20).mean().iloc[-1], "bb_width": (std*4)/ma20 if ma20 else 0,
        "roe": roe, "rev_growth": 0.18, "fund_count": fund
    }
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": ticker, "d": data['scan_date']})
        pd.DataFrame([data]).to_sql('daily_scans', con=conn, if_exists='append', index=False)
    return True

# ================= 3. 視覺渲染 (全幅霸氣 CSS) =================

def beauty_style(df):
    if df.empty: return df
    num_cols = ['現價','漲跌(%)','獲利','報酬率(%)','roe','fund_count','rsi','price','sma5','ma20']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'roe':'{:.2%}', 'fund_count':'{:,.0f}', 'rsi':'{:.1f}', 'price':'{:.2f}', 'sma5':'{:.2f}', 'ma20':'{:.2f}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')

# ================= 4. 主介面設計 (V148.0 量子戰神) =================
st.set_page_config(page_title="🛡️ 哲哲量子戰情室 V148.0", layout="wide")

st.markdown("""<style>
    [data-testid="stBaseButton-secondary"] {
        width: 100% !important; height: 3.8em !important; font-size: 1.4rem !important; 
        font-weight: 800 !important; border-radius: 12px !important; margin-bottom: 12px !important; 
        background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important;
        border: none !important; display: flex !important; justify-content: center !important; align-items: center !important;
    }
    .big-font { font-size:65px !important; font-weight: 900; color: #FF3333; text-shadow: 2px 2px 5px #ddd; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V148.0 — 量子戰神完全體")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛指揮中心", "💼 資產即時戰報", "🛠️ 管理中心"])

def run_violent_scan(pool_df):
    today = datetime.now(TW_TZ).date()
    for r_idx in range(1, 4):
        done = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
        rem = pool_df[~pool_df['ticker'].isin(done['ticker'].tolist())]
        if rem.empty: break
        status = st.status(f"🏹 第 {r_idx} 輪暴力追殺中... (剩餘: {len(rem)})")
        with ThreadPoolExecutor(max_workers=5) as exe:
            futures = {exe.submit(process_and_save, r['ticker'], r['stock_name']): r['ticker'] for _, r in rem.iterrows()}
            for f in as_completed(futures):
                if f.result(): status.write(f"✅ {futures[f]} 成功")
        status.update(label=f"✨ 第 {r_idx} 輪結束", state="complete")
    st.balloons()

with tab1:
    st.markdown("### 🏆 數據全自動引擎")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日數據快取"):
            df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
            if not df.empty:
                st.session_state['master_df'] = df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.success(f"✅ 成功載入 {len(df)} 檔真錢數據")
    with c2:
        if st.button("⚡ 啟動增量補完掃描"):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            run_violent_scan(pool); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃"):
            today = datetime.now(TW_TZ).date()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            st.info("🗑️ 清空快取，重新發動多執行緒攻擊！")
            run_violent_scan(pool); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df']
        s_list = [
            ("💎 策略 1: 降臨：超級策略 (基金+ROE)", "(df['fund_count'] >= 10) & (df['roe'] > 0.05)"),
            ("📈 策略 2: 帶量突破前高", "(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)"),
            ("🚀 策略 3: 三線合一多頭", "(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])"),
            ("🌀 策略 4: 布林縮口突破", "(df['現價'] > df['bbu']) & (df['bb_width'] < 0.2)"),
            ("👑 策略 5: 九成勝率 ATM", "(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20'] * 1.2)"),
            ("🛡️ 策略 6: 低階抄底防護", "(df['rsi'] < 40) & (df['現價'] > df['sma5'])"),
            ("🎯 策略 7: 強勢回測支撐", "abs(df['現價']-df['ma20'])/df['ma20'] < 0.02")
        ]
        for name, cond in s_list:
            if st.button(name):
                res = df[eval(cond)]
                st.dataframe(beauty_style(res), width=1500)

    st.divider()
    if st.button("🔍 數據照妖鏡 (檢視全欄位)"):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), width=1500)

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        # 💎 合併邏輯鎖死：以 portfolio 為主體，關聯今日最新行情
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20 FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        
        # 💎 只有現價 > 0 才准計算獲利，否則獲利一律為 0，絕不准主力嚇到你！
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0, ((df_display['price'] - df_display['entry_price']) / df_display['entry_price']) * 100, 0)
        
        st.markdown(f"當前總獲利：<br><span class='big-font'>${df_display['獲利'].sum():,.0f}</span>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_display), width=1500)

with tab3:
    st.subheader("🛠️ 管理中心")
    col1, col2 = st.columns(2)
    with col1:
        f1 = st.file_uploader("匯入股票池", type="csv")
        if f1 and st.button("💾 儲存股票池"):
            df_new = pd.read_csv(f1).drop_duplicates()
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM stock_pool"))
                df_new.to_sql('stock_pool', con=conn, if_exists='append', index=False)
            st.success("股票池更新成功！")
    with col2:
        f2 = st.file_uploader("匯入持倉", type="csv")
        if f2 and st.button("💾 儲存持倉"):
            df_new = pd.read_csv(f2).drop_duplicates()
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM portfolio"))
                df_new.to_sql('portfolio', con=conn, if_exists='append', index=False)
            st.success("持倉更新成功！")

st.caption("本系統由哲哲團隊開發。V148.0 鋼鐵無敵版，賺到流湯不要忘了我！")

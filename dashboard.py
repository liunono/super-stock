import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, random
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# 🚀 嘗試導入 TLS 隱身引擎
try:
    from curl_cffi.requests import Session as CFSession
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ================= 1. 系統地基 (加入診斷表格) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 核心掃描表 (21 指標)
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
        # B. 診斷日誌表 (新增：記錄失敗原因)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS scan_errors (
                ticker VARCHAR(20), scan_date DATE, error_msg TEXT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (量子抓取與錯誤診斷) =================

def get_hidden_session():
    if HAS_CURL_CFFI:
        return CFSession(impersonate="chrome")
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'})
    return session

def process_diagnose_stock(ticker, name, df):
    """💎 診斷式處理：只要沒通過驗證就回報原因"""
    try:
        if df.empty: return None, "空數據包 (Empty DF)"
        if len(df) < 20: return None, f"天數不足 ({len(df)})"
        
        c = df['Close'].replace(0, np.nan).ffill()
        if c.empty or np.isnan(c.iloc[-1]): return None, "股價為 NaN"
        
        curr_p = float(c.iloc[-1])
        if curr_p <= 0: return None, "價格為 0"

        # 技術指標計算
        sma5 = ta.sma(c, 5).iloc[-1]
        ma20 = ta.sma(c, 20).iloc[-1]
        ma60 = ta.sma(c, 60).iloc[-1]
        rsi = ta.rsi(c, 14).iloc[-1]
        bb = ta.bbands(c, 20, 2)
        
        # 財報 (選配)
        roe, rev = 0.0, 0.0
        try:
            s = yf.Ticker(ticker)
            roe = float(s.fast_info.get('returnOnEquity', 0) or 0)
            rev = float(s.info.get('revenueGrowth', 0) or 0)
        except: pass

        return {
            "ticker": ticker, "stock_name": name, "price": curr_p,
            "change_pct": float(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100),
            "sma5": float(sma5 if not np.isnan(sma5) else curr_p),
            "ma20": float(ma20 if not np.isnan(ma20) else curr_p),
            "ma60": float(ma60 if not np.isnan(ma60) else curr_p),
            "rsi": float(rsi if not np.isnan(rsi) else 50),
            "vol": int(df['Volume'].iloc[-1]), "avg_vol": int(ta.sma(df['Volume'], 20).iloc[-1] if len(df)>=20 else 0),
            "kd20": float(c.iloc[-20] if len(c)>=20 else curr_p),
            "kd60": float(c.iloc[-60] if len(c)>=60 else curr_p),
            "scan_date": datetime.now(TW_TZ).date(),
            "bbu": float(bb.iloc[-1, 2] if bb is not None else 0),
            "bbl": float(bb.iloc[-1, 0] if bb is not None else 0),
            "high_20": float(c.shift(1).rolling(20).max().iloc[-1] if len(c)>=21 else curr_p),
            "vol_20": float(df['Volume'].shift(1).rolling(20).mean().iloc[-1] if len(df)>=21 else 0),
            "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20 if (bb is not None and ma20!=0) else 0),
            "roe": roe, "rev_growth": rev, "fund_count": 0
        }, "成功"
    except Exception as e:
        return None, str(e)

def quantum_diagnose_loop(pool_df, mode="incremental"):
    """🚀 診斷式掃描迴圈：揭開失敗原因"""
    today = datetime.now(TW_TZ).date()
    if mode == "reset":
        with engine.begin() as conn: 
            conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            conn.execute(text("DELETE FROM scan_errors WHERE scan_date = :t"), {"t": today})

    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today})
    remaining = pool_df[~pool_df['ticker'].isin(done_df['ticker'].tolist())].copy()
    
    if remaining.empty:
        st.balloons(); st.success("🏆 數據已全壘打！"); return

    total = len(pool_df)
    p_bar = st.progress(len(done_df)/total)
    p_text = st.empty()
    log_box = st.status(f"⚡ 量子診斷掃描中 (剩餘 {len(remaining)} 檔)...", expanded=True)

    batch_size = 25
    tickers = remaining['ticker'].tolist()
    names = dict(zip(remaining['ticker'], remaining['stock_name']))

    for i in range(0, len(tickers), batch_size):
        curr_batch = tickers[i : i + batch_size]
        try:
            data_all = yf.download(curr_batch, period="7mo", interval="1d", group_by='ticker', threads=True, progress=False)
            
            for t in curr_batch:
                df_single = data_all[t] if len(curr_batch) > 1 else data_all
                res, msg = process_diagnose_stock(t, names[t], df_single)
                
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    conn.execute(text("DELETE FROM scan_errors WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    if res:
                        pd.DataFrame([res]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                        log_box.write(f"✅ 入庫：{res['stock_name']}")
                    else:
                        # 紀錄失敗原因
                        pd.DataFrame([{"ticker": t, "scan_date": today, "error_msg": msg}]).to_sql('scan_errors', con=engine, if_exists='append', index=False)
                        log_box.write(f"❌ {t} 失敗：{msg}")
            
            current_done = pd.read_sql(text("SELECT count(*) FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today}).iloc[0,0]
            p_bar.progress(min(current_done/total, 1.0))
            p_text.markdown(f"**🚀 實際成功：`{current_done}` / `{total}`**")
            time.sleep(random.uniform(4, 6))
            
        except Exception as e:
            log_box.write(f"⚠️ 批量包崩潰：{e}"); time.sleep(8)
    log_box.update(label="✨ 診斷結束。", state="complete")

# ================= 3. 視覺美學渲染 =================

def beauty_style(df):
    if df.empty: return df
    num_cols = ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if 'ROE' in df.columns: styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.08, high=0.2)
        return styled
    except: return df

# ================= 4. 主介面設計 (V122.0 完全體) =================
st.set_page_config(page_title="哲哲量化診斷戰情室 V122.0", layout="wide")
st.markdown("""<style>
    div.stButton > button { height: 4em; font-size: 1.3rem !important; font-weight: bold !important; border-radius: 12px; width: 100% !important; margin-bottom: 12px; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V122.0 — 抓漏診斷完全體")

tab1, tab2, tab3 = st.tabs(["🚀 指揮官指揮中心", "💼 持倉監控戰報", "🛠️ 後台與診斷"])

with tab1:
    st.markdown("### 🏆 每日行情智慧突圍 (298 筆突破版)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日行情數據", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.5"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！共 {len(db_df)} 筆有價標的。")
    with c2:
        if st.button("⚡ 啟動量子渦輪掃描", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: quantum_diagnose_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: quantum_diagnose_loop(pool, mode="reset"); st.rerun()

    st.divider()
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            p_info = pd.read_sql("SELECT ticker, sector, fund_count FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            mask = (df['fund_count'] >= 100) & (df['ROE'] > 0.1)
            st.dataframe(beauty_style(df[mask]), use_container_width=True)
        else: st.error("⚠️ 請先讀取數據！")

    if st.button("🔍 揭開底牌：數據照妖鏡 (檢視所有原始數據)", use_container_width=True):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (量子突圍)", use_container_width=True):
            quantum_diagnose_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price FROM daily_scans WHERE scan_date = :t AND price > 0.5"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = np.where(df_p['price'] > 0.1, ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100, 0)
        st.markdown(f"當前總獲利：<p style='font-size:40px; color:#FF3333; font-weight:bold;'>${df_p[df_p['price']>0]['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_p[df_p['price']>0]), use_container_width=True)

with tab3:
    st.subheader("🛠️ 後台管理與「失敗診斷」中心")
    # 💎 顯示診斷日誌
    st.markdown("#### 🚫 失敗標的診斷區 (截圖給我看這區！)")
    err_df = pd.read_sql(text("SELECT * FROM scan_errors WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
    if not err_df.empty:
        st.dataframe(err_df, use_container_width=True)
    else: st.success("✅ 目前今日無失敗紀錄！")
    
    st.divider()
    f1 = st.file_uploader("上傳股票池 CSV", type="csv")
    if f1 and st.button("💾 鋼鐵匯入"):
        df_new = pd.read_csv(f1, encoding='utf-8-sig')
        with engine.begin() as conn:
            for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
        df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")

st.caption("本系統由哲哲團隊開發。V122.0 抓漏診斷完全體，賺到流湯不要忘了我！")

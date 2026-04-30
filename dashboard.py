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

# ================= 1. 系統地基 (鋼鐵都更，主鍵鎖死) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
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
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (數據清洗與批量引擎) =================

def process_single_stock_data(ticker, name, hist_df):
    """💎 哲哲數據清洗：排除 NaN 與 0"""
    try:
        if hist_df.empty or len(hist_df) < 20: return None
        
        # 排除 0 元與 NaN
        c = hist_df['Close'].replace(0, np.nan).ffill()
        v = hist_df['Volume'].replace(0, np.nan).ffill()
        
        if c.empty or np.isnan(c.iloc[-1]): return None
        
        curr_p = float(c.iloc[-1])
        # 技術指標
        sma5 = ta.sma(c, 5).iloc[-1]
        ma20 = ta.sma(c, 20).iloc[-1]
        ma60 = ta.sma(c, 60).iloc[-1]
        rsi = ta.rsi(c, 14).iloc[-1]
        bb = ta.bbands(c, 20, 2)
        
        # 財報 (獨立處理)
        roe, rev = 0.0, 0.0
        try:
            # 隨機延遲抓取 info，避免連發
            s = yf.Ticker(ticker)
            info = s.fast_info # 改用 fast_info 提速
            roe = float(s.info.get('returnOnEquity', 0) or 0)
            rev = float(s.info.get('revenueGrowth', 0) or 0)
        except: pass

        return {
            "ticker": ticker, "stock_name": name, "price": curr_p,
            "change_pct": float(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100) if len(c)>1 else 0,
            "sma5": float(sma5 if not np.isnan(sma5) else curr_p),
            "ma20": float(ma20 if not np.isnan(ma20) else curr_p),
            "ma60": float(ma60 if not np.isnan(ma60) else curr_p),
            "rsi": float(rsi if not np.isnan(rsi) else 50),
            "vol": int(v.iloc[-1] if not np.isnan(v.iloc[-1]) else 0),
            "avg_vol": int(ta.sma(v, 20).iloc[-1] if len(v)>=20 else 0),
            "kd20": float(c.iloc[-20] if len(c)>=20 else curr_p),
            "kd60": float(c.iloc[-60] if len(c)>=60 else curr_p),
            "scan_date": datetime.now(TW_TZ).date(),
            "bbu": float(bb.iloc[-1, 2] if bb is not None else 0),
            "bbl": float(bb.iloc[-1, 0] if bb is not None else 0),
            "high_20": float(c.shift(1).rolling(20).max().iloc[-1] if len(c)>=21 else curr_p),
            "vol_20": float(v.shift(1).rolling(20).mean().iloc[-1] if len(v)>=21 else 0),
            "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20 if (bb is not None and ma20!=0) else 0),
            "roe": roe, "rev_growth": rev, "fund_count": 0
        }
    except: return None

def quantum_batch_loop(pool_df, mode="incremental"):
    """🚀 量子批量引擎：絕滅 0 元與無限重刷"""
    today = datetime.now(TW_TZ).date()
    if mode == "reset":
        with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
    
    # 找出真正「有錢進帳」的標的
    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0.5"), con=engine, params={"t": today})
    done_list = done_df['ticker'].tolist()
    remaining = pool_df[~pool_df['ticker'].isin(done_list)].copy()
    
    if remaining.empty:
        st.balloons(); st.success("🏆 數據已全數歸位！"); return

    total = len(pool_df)
    p_bar = st.progress(len(done_list)/total)
    p_text = st.empty()
    log_box = st.status(f"⚡ 量子突圍中 (剩餘 {len(remaining)} 檔)...", expanded=True)

    batch_size = 30
    tickers = remaining['ticker'].tolist()
    names = dict(zip(remaining['ticker'], remaining['stock_name']))

    for i in range(0, len(tickers), batch_size):
        curr_batch = tickers[i : i + batch_size]
        try:
            # 🚀 批量下載 (這是關鍵)
            data_all = yf.download(curr_batch, period="7mo", interval="1d", group_by='ticker', threads=True, progress=False)
            
            for t in curr_batch:
                df_single = data_all[t] if len(curr_batch) > 1 else data_all
                res = process_single_stock_data(t, names[t], df_single)
                
                if res:
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    pd.DataFrame([res]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    log_box.write(f"✅ 真錢入庫：{res['stock_name']} (${res['price']})")
                else:
                    # 三振出局標記
                    pd.DataFrame([{"ticker": t, "stock_name": "無效標的", "scan_date": today, "price": 0.01}]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    log_box.write(f"⚠️ {t} 數據無效，標記棄子")
            
            # 更新 UI
            current_done = pd.read_sql(text("SELECT count(*) FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today}).iloc[0,0]
            p_bar.progress(min(current_done/total, 1.0))
            p_text.markdown(f"**🚀 實際成功進度：`{current_done}` / `{total}` ({current_done/total:.1%})**")
            time.sleep(random.uniform(3, 5))
            
        except Exception as e:
            log_box.write(f"❌ 批量包錯誤：{e}"); time.sleep(10)

    log_box.update(label="✨ 量子任務結束。", state="complete")

# ================= 3. 視覺渲染 (漸層美學) =================

def beauty_style(df):
    if df.empty: return df
    num_cols = ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if 'ROE' in df.columns: styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.08, high=0.25)
        if '營收成長' in df.columns: styled = styled.background_gradient(subset=['營收成長'], cmap='OrRd', low=0.05, high=0.6)
        if '報酬率(%)' in df.columns: styled = styled.background_gradient(subset=['報酬率(%)'], cmap='RdYlGn_r', low=-10, high=10)
        def color_t(v):
            if isinstance(v, (int, float)):
                if v > 0.1: return 'color: #FF3333; font-weight: bold'
                if v < -0.1: return 'color: #00AA00; font-weight: bold'
            return ''
        return styled.map(color_t, subset=[c for c in ['漲跌(%)', '報酬率(%)', '獲利'] if c in df.columns])
    except: return df

# ================= 4. 主介面設計 (V121.0 指揮官大按鈕) =================
st.set_page_config(page_title="哲哲量化封神戰情室 V121.0", layout="wide")
st.markdown("""<style>
    div.stButton > button { height: 4.2em; font-size: 1.4rem !important; font-weight: bold !important; border-radius: 15px; width: 100% !important; margin-bottom: 12px; transition: 0.3s; }
    div.stButton > button:hover { background-color: #FFF5F5; border-color: #FF3333; color: #FF3333; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V121.0 — 量子批量封神完全體")
now_tw = datetime.now(TW_TZ)
if now_tw.hour == 13 and now_tw.minute >= 31:
    st.warning("🔔 **收盤大紅燈！現在 1:31 PM，建議執行『暴力重掃』鎖定最終籌碼！**")

tab1, tab2, tab3 = st.tabs(["🚀 指揮官指揮中心", "💼 資產即時監控", "🛠️ 後台管理都更"])

with tab1:
    st.markdown("### 🏆 每日行情量子全掃描 (絕滅 0 元數據)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日數據快取", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.1"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 成功載入 {len(db_df)} 筆有價標的！")
    with c2:
        if st.button("⚡ 啟動量子渦輪突圍", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: quantum_batch_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃 (清空重來)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: quantum_batch_loop(pool, mode="reset"); st.rerun()

    st.divider()
    # 策略大按鈕 (一鍵全幅表格)
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            p_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            st.dataframe(beauty_style(df[mask].sort_values(by='營收成長', ascending=False)), use_container_width=True)
        else: st.error("⚠️ 請先讀取行情數據！")

    if st.button("📈 帶量突破前高 / 三線合一", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("👑 九成勝率 ATM / 抄底防護", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價']>df['kd20']) & (df['RSI'] < 40)]
            st.dataframe(beauty_style(res), use_container_width=True)

    st.divider()
    if st.button("🔍 揭開底牌：數據照妖鏡 (檢視今日所有數據進度)", use_container_width=True):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (量子批量)", use_container_width=True):
            quantum_batch_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = np.where(df_p['price'] > 0.1, ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100, 0)
        valid_p = df_p[df_p['price'] > 0.1].copy()
        st.markdown(f"當前總獲利：<p style='font-size:45px; color:#FF3333; font-weight:bold;'>${valid_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(valid_p), use_container_width=True)

with tab3:
    st.subheader("🛠️ 數據管理中心")
    f1 = st.file_uploader("上傳股票池 CSV", type="csv")
    if f1 and st.button("💾 鋼鐵匯入"):
        df_new = pd.read_csv(f1, encoding='utf-8-sig')
        with engine.begin() as conn:
            for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
        df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")

st.caption("本系統由哲哲團隊開發。V121.0 量子批量全壘打版，賺到流湯不要忘了我！")

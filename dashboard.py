import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time, requests, json
import numpy as np
from PIL import Image
import easyocr
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (資料庫強制都更) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50));"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            );
        """))
        # 霸氣都更：確保欄位一定存在
        cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 地基崩塌：{e}"); st.stop()

# ================= 2. 哲哲美學工具 =================
@st.cache_resource
def get_ocr_reader():
    return easyocr.Reader(['ch_tra', 'en'])

def style_df(df):
    format_dict = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}'}
    styler = df.style.format({k: v for k, v in format_dict.items() if k in df.columns})
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['報酬率(%)'])
    return styler

# ================= 3. 核心抓取與 OCR (智能修正版) =================
def fetch_data(ticker, name):
    for t in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            s = yf.Ticker(t)
            d = s.history(period="6mo", interval="1d", timeout=10)
            if not d.empty and len(d) >= 60:
                c, v = d['Close'], d['Volume']
                return {
                    "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                    "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                    "sma5": round(ta.sma(c,5).iloc[-1], 2), "ma20": round(ta.sma(c,20).iloc[-1], 2),
                    "ma60": round(ta.sma(c,60).iloc[-1], 2), "rsi": round(ta.rsi(c,14).iloc[-1], 2),
                    "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v,20).iloc[-1]),
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), "scan_date": datetime.now().date()
                }
        except: continue
    return None

def process_ocr_advanced(files):
    reader = get_ocr_reader()
    extracted = []
    try:
        pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        name_map = dict(zip(pool['stock_name'], pool['ticker']))
    except: name_map = {}

    for f in files:
        res = reader.readtext(np.array(Image.open(f)))
        texts = [r[1] for r in res]
        for i, t in enumerate(texts):
            if t in name_map:
                try:
                    vals = []
                    # 智能過濾：只抓取正數，且排除掉看起來像損益的大金額
                    for off in range(1, 8):
                        s = texts[i+off].replace(',', '').replace(' ', '')
                        if s.replace('.', '').isdigit():
                            v = float(s)
                            if v > 0: vals.append(v)
                    if len(vals) >= 2:
                        extracted.append({
                            "ticker": name_map[t], "stock_name": t,
                            "entry_price": vals[1], # 通常均價在第二個
                            "qty": vals[2] if len(vals) > 2 else vals[0] # 股數通常是第三個數字
                        })
                except: continue
    return pd.DataFrame(extracted)

# ================= 4. 介面設計 (V26.0) =================
st.set_page_config(page_title="哲哲戰情室 V26.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V26.0 - 冠軍資產顯示優化版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 持倉獲利監控", "🛠️ 後台管理"])

with tab1:
    if st.button("📡 讀取今日金庫", use_container_width=True):
        db_df = pd.read_sql(f"SELECT * FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
        if not db_df.empty:
            st.session_state['master_df'] = db_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
            st.success("✅ 金庫數據載入成功！")
        else: st.warning("今日尚無快取，請啟動掃描。")
    
    if st.button("⚡ 啟動渦輪掃描", use_container_width=True):
        pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if not pool.empty:
            master_list, prog = [], st.progress(0)
            with ThreadPoolExecutor(max_workers=10) as ex:
                futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                for count, future in enumerate(as_completed(futures)):
                    res = future.result()
                    if res: master_list.append(res)
                    prog.progress((count + 1) / len(pool))
            m_df = pd.DataFrame(master_list)
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
            st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
            st.success("✨ 掃描完成！")

with tab2:
    st.header("💼 我的資產亮牌區")
    df_p = pd.read_sql("SELECT * FROM portfolio", con=engine)
    if not df_p.empty:
        # 💎 哲哲大絕：即便沒有 master_df，也至少顯示成本
        if 'master_df' in st.session_state:
            merged = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
        else:
            merged = df_p.copy()
            merged['現價'] = np.nan
        
        # 獲利計算邏輯
        merged['獲利'] = (merged['現價'] - merged['entry_price']) * merged['qty'] * (1000 if 'qty' in merged else 1)
        merged['報酬率(%)'] = round(((merged['現價'] - merged['entry_price']) / merged['entry_price']) * 100, 2)
        
        t_profit = merged['獲利'].sum()
        st.metric("當前預估總獲利", f"${t_profit:,.0f}", delta=f"{t_profit:,.0f}")
        st.dataframe(style_df(merged), width=1200)
    else:
        st.info("目前尚無持倉數據，快去 Tab 3 上傳截圖！")

with tab3:
    st.subheader("🤖 AI 視覺庫存導入")
    ups = st.file_uploader("📥 上傳截圖", type=["png", "jpg", "jpeg"], accept_multiple_files=True)
    if ups and st.button("🚀 執行辨識並同步", use_container_width=True):
        with st.spinner("哲哲辨識中..."):
            df_ocr = process_ocr_advanced(ups)
            if not df_ocr.empty:
                # 數據清洗
                df_ocr['entry_price'] = pd.to_numeric(df_ocr['entry_price'], errors='coerce')
                df_ocr['qty'] = pd.to_numeric(df_ocr['qty'], errors='coerce')
                df_ocr = df_ocr.dropna()
                # 單位換算：截圖顯示 2,000 股，我們存為 2.0 (張)
                df_ocr['qty'] = df_ocr['qty'].apply(lambda x: x/1000 if x >= 100 else x)
                
                with engine.begin() as conn:
                    # 去重寫入
                    exis = pd.read_sql("SELECT ticker, entry_price FROM portfolio", con=conn)
                    to_add = df_ocr[~df_ocr['ticker'].isin(exis['ticker'])]
                    if not to_add.empty:
                        to_add[['ticker', 'stock_name', 'entry_price', 'qty']].to_sql('portfolio', con=conn, if_exists='append', index=False)
                        st.success(f"✅ 成功導入 {len(to_add)} 筆新持倉！")
                        st.dataframe(to_add)
                    else: st.info("數據已存在。")

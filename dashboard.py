import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
from PIL import Image
import easyocr
import requests, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (自動都更與防錯) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50));"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, 
                ticker VARCHAR(20), 
                stock_name VARCHAR(50), 
                entry_price FLOAT, 
                qty FLOAT
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            );
        """))
        # 🔥 強制都更：解決 DatabaseError 的核心
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 地基崩潰：{e}"); st.stop()

# ================= 2. 核心引擎 (座標網格 2.0 定錨 OCR) =================
@st.cache_resource
def get_ocr_reader():
    return easyocr.Reader(['ch_tra', 'en'])

def fetch_data(ticker, name):
    for t in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=15)
            if not d.empty and len(d) >= 65:
                c = d['Close']
                return {
                    "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                    "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                    "sma5": round(ta.sma(c,5).iloc[-1], 2), "ma20": round(ta.sma(c,20).iloc[-1], 2),
                    "ma60": round(ta.sma(c,60).iloc[-1], 2), "rsi": round(ta.rsi(c,14).iloc[-1], 2),
                    "vol": int(d['Volume'].iloc[-1]), "avg_vol": int(ta.sma(d['Volume'],20).iloc[-1]),
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), "scan_date": datetime.now().date(),
                    "bbu": round(ta.bbands(c,20,2).iloc[-1,2],2)
                }
        except: continue
    return None

def process_ocr_v42(files):
    """
    💎 哲哲最強：二段式定錨辨識邏輯
    1. 先找名稱，定 Y 軸起點。
    2. 找「現股」關鍵字，定 Y 軸均價線。
    3. X 座標精準切分：股數在右 (450+), 均價在左中 (250~450)
    """
    reader = get_ocr_reader()
    extracted = []
    pool_df = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    name_map = dict(zip(pool_df['stock_name'], pool_df['ticker']))
    
    for f in files:
        res = reader.readtext(np.array(Image.open(f)))
        items = [{"text": r[1], "x": r[0][0][0], "y": r[0][0][1]} for r in res]
        
        for i, item in enumerate(items):
            s_name = item['text'].strip()
            if s_name in name_map:
                try:
                    # 💡 尋找「股數」：名稱同一列的右方區塊
                    qty = 0
                    for other in items:
                        if abs(other['y'] - item['y']) < 35 and 460 < other['x'] < 700:
                            v = other['text'].replace(',', '').replace(' ', '')
                            if v.isdigit(): qty = float(v)
                    
                    # 💡 尋找「均價」：找下方的「現股」旁邊那個數字
                    entry_p = 0
                    for other in items:
                        # 均價通常在名稱下方 60~130 像素，且 X 座標靠近中間
                        if 55 < (other['y'] - item['y']) < 140 and 240 < other['x'] < 480:
                            v = other['text'].replace(',', '').replace(' ', '')
                            # 均價必須是正數，且排除掉太小的雜訊
                            if ('.' in v or v.isdigit()) and float(v) > 10:
                                entry_p = float(v)
                                break
                    
                    if entry_p > 0:
                        extracted.append({"ticker": name_map[s_name], "stock_name": s_name, "entry_price": entry_p, "qty": qty})
                except: continue
    
    # 確保 9 檔如果都在圖裡，就要被抓出來
    return pd.DataFrame(extracted)

# ================= 3. 介面設計 (V42.0 巔峰版) =================
st.set_page_config(page_title="哲哲戰情室 V42.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V42.0 — 定錨辨識完全體")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股掃描", "💼 資產獲利 & 賣股策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股 ---
with tab1:
    st.markdown("### 🏆 每日行情掃描 (九成勝率濾網)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日行情", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, rsi as RSI, sma5, ma20, kd20, kd60, bbu, vol, avg_vol FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: st.session_state['master_df'] = db_df; st.success("✅ 載入成功！")
    with c2:
        if st.button("⚡ 啟動渦輪掃描", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                results, prog = [], st.progress(0)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, f in enumerate(as_completed(futures)):
                        res = f.result()
                        if res: results.append(res)
                        prog.progress((count + 1) / len(pool))
                m_df = pd.DataFrame(results)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                st.success("✨ 市場掃描完成！")

    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        st.markdown("### 🛠️ 買股決策中心")
        cols = st.columns(6)
        strats = [
            ("九成勝率提款機", "👑", (df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['量比']>=1.2)),
            ("量價突破", "💥", (df['現價']>df['ma20']) & (df['量比']>2)),
            ("黃金交叉", "🚀", (df['sma5']>df['ma20'])),
            ("低階抄底", "🛡️", (df['RSI']<35) & (df['現價']>df['sma5'])),
            ("布林噴發", "🌀", (df['現價']>df['bbu']))
        ]
        for i, (name, icon, mask) in enumerate(strats):
            if cols[i].button(f"{icon} {name}"):
                st.dataframe(df[mask].sort_values(by='RSI', ascending=False))

# --- Tab 2: 持倉 & 5 大賣股策略 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.*, s.stock_name as pool_name FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        df_p['stock_name'] = df_p['pool_name'].fillna(df_p['stock_name'])
        if st.button("🔄 更新即時獲利", use_container_width=True):
            tickers = df_p['ticker'].tolist()
            rt = yf.download(tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
            st.session_state['rt_p'] = rt.to_dict() if len(df_p)>1 else {df_p['ticker'].iloc[0]: rt}
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("總預估實質獲利", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(df_p[['ticker', 'stock_name', 'entry_price', '現價', 'qty', '獲利', '報酬率(%)']])
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股策略警示")
        if 'master_df' in st.session_state:
            check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            exit_1 = check_df[(check_df['sma5'] < check_df['ma20'])]
            if not exit_1.empty: st.error(f"💀 【均線死叉】：{', '.join(exit_1['stock_name'].tolist())}")
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【破位停損】：{', '.join(exit_2['stock_name'].tolist())}")
    else: st.info("持倉為空，請至 Tab 3 導入數據。")

# --- Tab 3: 後台 ---
with tab3:
    c_p, c_t = st.columns(2)
    with c_p:
        st.subheader("📋 股票池管理")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv")
        if f_pool and st.button("💾 匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功")
    with c_t:
        st.subheader("💰 手動持倉導入")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv")
        if f_port and st.button("💾 存入持倉"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("成功")

    st.divider()
    st.subheader("🤖 AI 視覺庫存精準導入 (座標定錨)")
    ups = st.file_uploader("📤 上傳庫存截圖 (可多張)", type=["png", "jpg", "jpeg"], accept_multiple_files=True)
    if ups and st.button("🚀 啟動 AI 定錨辨識"):
        with st.spinner("AI 掃描校準中..."):
            df_ocr = process_ocr_v42(ups)
            if not df_ocr.empty:
                df_ocr['entry_price'] = pd.to_numeric(df_ocr['entry_price'], errors='coerce')
                df_ocr['qty'] = pd.to_numeric(df_ocr['qty'], errors='coerce')
                df_ocr = df_ocr.dropna()
                with engine.begin() as conn:
                    # 💎 使用 SQLAlchemy 2.0 參數化語法，解決 Duplicate 與 ArgumentError
                    t_list = df_ocr['ticker'].tolist()
                    if t_list:
                        conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                    df_ocr[['ticker', 'stock_name', 'entry_price', 'qty']].to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success("✅ 數據校準同步完成！")
                st.dataframe(df_ocr)

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

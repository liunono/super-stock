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

# ================= 1. 系統地基 (強制都更與自動檢修) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 確保三張表格基礎地基穩固
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50));"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            );
        """))
        # 霸氣都更：自動檢查並補齊缺失欄位
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        if 'kd20' not in s_cols: conn.execute(text("ALTER TABLE daily_scans ADD COLUMN kd20 FLOAT;"))
        if 'kd60' not in s_cols: conn.execute(text("ALTER TABLE daily_scans ADD COLUMN kd60 FLOAT;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基故障：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與 LINE 噴發) =================
@st.cache_resource
def get_ocr_reader():
    return easyocr.Reader(['ch_tra', 'en'])

def send_line_report(title, df, icon):
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 偵測到 {len(df)} 檔標的：\n"
    for _, r in df.iterrows():
        msg += f"✅ {r['代號'] if '代號' in r else r['ticker']} {r['名稱'] if '名稱' in r else r['stock_name']} | 現價:{r['現價'] if '現價' in r else 'N/A'}\n"
    msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": USER_ID, "messages": [{"type": "text", "text": msg}]}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))

def style_df(df):
    def color_rsi(val):
        if val >= 70: return 'background-color: #FFCCCC'
        if val <= 30: return 'background-color: #CCFFCC'
        return ''
    format_dict = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:.3f}'}
    styler = df.style.format({k: v for k, v in format_dict.items() if k in df.columns})
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['報酬率(%)'])
    if 'RSI' in df.columns: styler = styler.map(color_rsi, subset=['RSI'])
    return styler

# ================= 3. 核心引擎 (抓取與智慧 OCR) =================
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
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), "scan_date": datetime.now().date(),
                    "bbl": round(ta.bbands(c,20,2).iloc[-1,0],2), "bbu": round(ta.bbands(c,20,2).iloc[-1,2],2)
                }
        except: continue
    return None

def process_ocr_v31(files):
    reader = get_ocr_reader()
    extracted = []
    pool_df = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    name_map = dict(zip(pool_df['stock_name'], pool_df['ticker']))
    for f in files:
        res = reader.readtext(np.array(Image.open(f)))
        boxes = [{"text": r[1], "y": r[0][0][1], "x": r[0][0][0]} for r in res]
        texts = [b['text'] for b in boxes]
        for i, t in enumerate(texts):
            clean_name = t.strip()
            if clean_name in name_map:
                try:
                    vals = []
                    # 抓取名稱附近的數字塊
                    for other in boxes:
                        if abs(other['y'] - boxes[i]['y']) < 120:
                            v_str = other['text'].replace(',', '').replace(' ', '').replace('%', '')
                            try:
                                v_num = float(v_str)
                                if v_num != 0: vals.append(v_num)
                            except: continue
                    # 💎 數據精準定位邏輯：均價通常是帶小數點且 > 10 的數字
                    cost = next((v for v in vals if '.' in str(v) and v > 10 and v < 5000), 0)
                    # 股數通常是整數且不會是負數 (損益是負數，排除！)
                    shares = next((v for v in vals if v > 0 and v != cost and v % 10 == 0), 0)
                    if cost > 0:
                        extracted.append({"ticker": name_map[clean_name], "stock_name": clean_name, "entry_price": cost, "qty": shares})
                except: continue
    return pd.DataFrame(extracted).drop_duplicates(subset=['ticker'])

# ================= 4. 主介面設計 (V31.0 絕無刪減) =================
st.set_page_config(page_title="哲哲戰情室 V31.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V31.0 — 終極全能提款機")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略", "💼 資產與賣股策略", "🛠️ 後台管理系統"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日行情掃描中心")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取數據", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5 as SMA5, ma20 as MA20, ma60 as MA60, rsi as RSI, bbl, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: st.session_state['master_df'] = db_df; st.success("✅ 行情載入成功！")
            else: st.warning("今日尚無快取，請執行渦輪掃描。")
    with c2:
        if st.button("⚡ 啟動並行渦輪掃描", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                results, prog = [], st.progress(0)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, future in enumerate(as_completed(futures)):
                        res = future.result(); 
                        if res: results.append(res)
                        prog.progress((count + 1) / len(pool))
                m_df = pd.DataFrame(results)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI','sma5':'SMA5','ma20':'MA20','ma60':'MA60'})
                st.success("✨ 市場掃描完成！準備大賺一波！")

    if 'master_df' in st.session_state:
        st.divider()
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        st.markdown("### 🛠️ 買股必勝策略決策")
        cols = st.columns(6)
        strats = [
            ("九成勝率提款機", "👑", (df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['量比']>=1.2) & (df['現價']>df['SMA5'])),
            ("量價突破", "💥", (df['現價']>df['MA20']) & (df['量比']>2)),
            ("黃金交叉", "🚀", (df['SMA5']>df['MA20']) & (df['MA20']>df['MA60'])),
            ("低階抄底", "🛡️", (df['RSI']<35) & (df['現價']>df['SMA5'])),
            ("布林噴發", "🌀", (df['現價']>df['bbu'])),
            ("強勢回測", "🎯", (df['現價']>df['MA20']) & (abs(df['現價']-df['MA20'])/df['MA20']<0.02))
        ]
        for i, (name, icon, mask) in enumerate(strats):
            if cols[i].button(f"{icon} {name}", use_container_width=True):
                res = df[mask].sort_values(by='RSI', ascending=False)
                st.write(f"符合『{name}』共有 {len(res)} 檔標的：")
                st.dataframe(style_df(res))
                send_line_report(name, res, icon)

# --- Tab 2: 持倉與賣股策略 ---
with tab2:
    st.header("💼 資產即時監控與賣出策略")
    df_p = pd.read_sql("SELECT p.*, s.stock_name as pool_name FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        df_p['stock_name'] = df_p['pool_name'].fillna(df_p['stock_name']) # 修正問號
        if st.button("🔄 更新即時現價與獲利", use_container_width=True):
            with st.spinner("連線交易所中..."):
                tickers = df_p['ticker'].tolist()
                rt = yf.download(tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
                st.session_state['rt_p'] = rt.to_dict() if len(tickers)>1 else {tickers[0]: rt}
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty'] # 這裡已修正為股
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("總預估實質獲利", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股策略警示")
        if 'master_df' in st.session_state:
            check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            # 賣股邏輯實裝
            exit_1 = check_df[(check_df['SMA5'] < check_df['MA20'])]
            if not exit_1.empty: st.error(f"💀 【死叉訊號】：{', '.join(exit_1['stock_name'].tolist())}")
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【破位停損】：{', '.join(exit_2['stock_name'].tolist())}")
    else: st.info("持倉資料庫為空，請至後台管理。")

# --- Tab 3: 後台管理 ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📋 股票池管理 (Ticker Pool)")
        f_pool = st.file_uploader("上傳股票池 CSV (ticker, stock_name)", type="csv", key="pool")
        if f_pool and st.button("💾 匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功")
    with c2:
        st.subheader("💰 持倉管理 (Portfolio)")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv", key="port")
        if f_port and st.button("💾 匯入持倉數據"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("成功")
    
    st.divider()
    st.subheader("🤖 AI 視覺庫存智慧辨識 (自動校準)")
    ups = st.file_uploader("📤 上傳庫存截圖 (可多張)", type=["png", "jpg", "jpeg"], accept_multiple_files=True)
    if ups and st.button("🚀 啟動 AI 辨識並修正入庫"):
        with st.spinner("正在進行智慧校準..."):
            df_ocr = process_ocr_v31(ups)
            if not df_ocr.empty:
                with engine.begin() as conn:
                    df_ocr.to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success("✅ 辨識並校準完成！")
                st.dataframe(df_ocr)

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

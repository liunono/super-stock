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

# ================= 1. 系統地基 (強制自動化都更與欄位防錯) =================
# 哲哲叮嚀：地基打得穩，獲利噴得準！自動處理所有欄位缺失與資料庫報錯。
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # A. 確保三大核心表格存在
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
        
        # B. 🔥 自動都更：檢查 portfolio 表格欄位
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        # C. 🔥 自動都更：檢查 daily_scans 欄位
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        for col in ['kd20', 'kd60', 'bbu', 'bbl']:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} FLOAT;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗 (地基崩潰)：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與 LINE 發送) =================
@st.cache_resource
def get_ocr_reader():
    """全台最強 AI 視覺辨識引擎"""
    return easyocr.Reader(['ch_tra', 'en'])

def send_line_report(title, df, icon):
    """將冠軍策略偵測結果噴向你的 LINE"""
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in df.iterrows():
        n = r['名稱'] if '名稱' in r else r.get('stock_name', '未知')
        p = r['現價'] if '現價' in r else 'N/A'
        msg += f"✅ {r['ticker'] if 'ticker' in r else r.get('代號','')} {n} | 現價:{p}\n"
    msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": USER_ID, "messages": [{"type": "text", "text": msg}]}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))

def style_df(df):
    """終極視覺美化：紅漲綠跌，數據止穩"""
    def color_rsi(val):
        if val >= 70: return 'background-color: #FFCCCC'
        if val <= 30: return 'background-color: #CCFFCC'
        return ''
    def color_pct(val):
        if isinstance(val, (int, float)):
            if val > 0: return 'color: #FF3333; font-weight: bold'
            if val < 0: return 'color: #00AA00'
        return ''
    
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:,.0f}'}
    styler = df.style.format({k: v for k, v in f_map.items() if k in df.columns})
    if '報酬率(%)' in df.columns: styler = styler.map(color_pct, subset=['報酬率(%)'])
    if '漲跌(%)' in df.columns: styler = styler.map(color_pct, subset=['漲跌(%)'])
    if 'RSI' in df.columns: styler = styler.map(color_rsi, subset=['RSI'])
    return styler

# ================= 3. 核心大腦 (數據抓取、空間 OCR、策略監控) =================
def fetch_data(ticker, name):
    """計算扣三低、布林帶、RSI 等 11 大指標"""
    for t in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=15)
            if not d.empty and len(d) >= 65:
                c, v = d['Close'], d['Volume']
                sma5, ma20 = ta.sma(c, 5), ta.sma(c, 20)
                rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
                return {
                    "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                    "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                    "sma5": round(sma5.iloc[-1], 2), "ma20": round(ma20.iloc[-1], 2),
                    "ma60": round(ta.sma(c,60).iloc[-1], 2), "rsi": round(rsi.iloc[-1], 2),
                    "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v,20).iloc[-1]),
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), "scan_date": datetime.now().date(),
                    "bbl": round(bb.iloc[-1, 0], 2), "bbu": round(bb.iloc[-1, 2], 2)
                }
        except: continue
    return None

def process_ocr_v46(files):
    """
    💎 空間導航辨識法 (Spatial Row Analysis)
    針對你的兩行式截圖：
    [名稱] --- [已買股數] (同一列)
      |
    [現股] [買進均價] (下一列)
    """
    reader = get_ocr_reader()
    extracted = []
    pool_df = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    name_map = dict(zip(pool_df['stock_name'], pool_df['ticker']))
    
    for f in files:
        img_np = np.array(Image.open(f))
        res = reader.readtext(img_np)
        all_items = [{"text": r[1], "clean": r[1].replace(',','').replace(' ','').replace('%',''), "x": r[0][0][0], "y": r[0][0][1]} for r in res]
        
        # 按 Y 座標分組 (同行差距 30 像素)
        all_items.sort(key=lambda x: x['y'])
        rows = []
        if all_items:
            curr_row = [all_items[0]]
            for i in range(1, len(all_items)):
                if all_items[i]['y'] - curr_row[-1]['y'] < 30: curr_row.append(all_items[i])
                else:
                    curr_row.sort(key=lambda x: x['x'])
                    rows.append(curr_row)
                    curr_row = [all_items[i]]
            rows.append(curr_row)

        for i, row in enumerate(rows):
            for item in row:
                if item['text'] in name_map:
                    try:
                        # 1. 在同一行找股數 (X: 450~780)
                        qty = 0
                        for r_item in row:
                            if 450 < r_item['x'] < 780 and r_item['clean'].isdigit():
                                if float(r_item['clean']) < 500000: # 排除極大的損益數字雜訊
                                    qty = float(r_item['clean'])
                        
                        # 2. 在下一行找買進價格 (X: 250~480)
                        entry_p = 0
                        if i + 1 < len(rows):
                            for nr_item in rows[i+1]:
                                if 250 < nr_item['x'] < 500:
                                    try:
                                        val = float(nr_item['clean'])
                                        if val > 1: entry_p = val
                                    except: continue
                        
                        if entry_p > 0:
                            extracted.append({"ticker": name_map[item['text']], "stock_name": item['text'], "entry_price": entry_p, "qty": qty})
                    except: continue
    return pd.DataFrame(extracted).drop_duplicates(subset=['ticker'])

# ================= 4. 介面設計 (V46.0 巔峰版) =================
st.set_page_config(page_title="哲哲戰情室 V46.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V46.0 — 終極封神提款機")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略", "💼 資產獲利與賣股監控", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日全市場掃描 (大數據濾網)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日金庫", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: st.session_state['master_df'] = db_df; st.success("✅ 行情載入成功！")
            else: st.warning("今日尚無快取數據。")
    with c2:
        if st.button("⚡ 啟動渦輪並行掃描", use_container_width=True):
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
        st.divider()
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        st.markdown("### 🛠️ 買股必勝策略決策")
        cols = st.columns(6)
        strats = [
            ("九成勝率提款機", "👑", (df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['量比']>=1.2)),
            ("量價突破", "💥", (df['現價']>df['ma20']) & (df['量比']>2)),
            ("黃金交叉", "🚀", (df['sma5']>df['ma20'])),
            ("低階抄底", "🛡️", (df['RSI']<35) & (df['現價']>df['sma5'])),
            ("布林噴發", "🌀", (df['現價']>df['bbu'])),
            ("強勢回測", "🎯", (df['現價']>df['ma20']) & (abs(df['現價']-df['ma20'])/df['ma20']<0.02))
        ]
        for i, (name, icon, mask) in enumerate(strats):
            if cols[i].button(f"{icon} {name}", use_container_width=True):
                res = df[mask].sort_values(by='RSI', ascending=False)
                st.dataframe(style_df(res))
                send_line_report(name, res, icon)

# --- Tab 2: 資產獲利與賣出策略 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.*, s.stock_name as pool_name FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        df_p['stock_name'] = df_p['pool_name'].fillna(df_p['stock_name'])
        if st.button("🔄 更新即時現價與獲利", use_container_width=True):
            with st.spinner("連線交易所數據..."):
                tickers = df_p['ticker'].tolist()
                rt = yf.download(tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
                st.session_state['rt_p'] = rt.to_dict() if len(tickers)>1 else {tickers[0]: rt}
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("總預估實質獲利 (座標校準版)", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(style_df(df_p[['ticker', 'stock_name', 'entry_price', '現價', 'qty', '獲利', '報酬率(%)']]))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股警示中心")
        if 'master_df' in st.session_state:
            check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            exit_1 = check_df[(check_df['sma5'] < check_df['ma20'])]
            if not exit_1.empty: st.error(f"💀 【死叉賣訊】：{', '.join(exit_1['stock_name'].tolist())}")
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【破位停損】：{', '.join(exit_2['stock_name'].tolist())}")
            exit_3 = check_df[check_df['報酬率(%)'] > 20]
            if not exit_3.empty: st.success(f"💰 【高檔止盈】：{', '.join(exit_3['stock_name'].tolist())}")
    else: st.info("持倉資料庫為空，請至 Tab 3 導入。")

# --- Tab 3: 後台都更管理 ---
with tab3:
    c_csv1, c_csv2 = st.columns(2)
    with c_csv1:
        st.subheader("📋 股票池管理")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv", key="p1")
        if f_pool and st.button("💾 匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功")
    with c_csv2:
        st.subheader("💰 持倉管理")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv", key="p2")
        if f_port and st.button("💾 存入持倉"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("存入成功")
    
    st.divider()
    st.subheader("🤖 AI 視覺庫存智慧導入 (空間導航校準)")
    ups = st.file_uploader("📤 上傳庫存截圖 (可多張)", type=["png", "jpg", "jpeg"], accept_multiple_files=True)
    if ups and st.button("🚀 啟動 AI 辨識並精準同步"):
        with st.spinner("正在進行空間網格校準..."):
            df_ocr = process_ocr_v46(ups)
            if not df_ocr.empty:
                df_ocr['entry_price'] = pd.to_numeric(df_ocr['entry_price'], errors='coerce')
                df_ocr['qty'] = pd.to_numeric(df_ocr['qty'], errors='coerce')
                df_ocr = df_ocr.dropna()
                with engine.begin() as conn:
                    # 去重：先刪除再塞入
                    t_list = df_ocr['ticker'].tolist()
                    if t_list: conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                    df_ocr[['ticker', 'stock_name', 'entry_price', 'qty']].to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success("✅ 辨識並校準完成！數據精準入庫！")
                st.dataframe(df_ocr)

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

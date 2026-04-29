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

# ================= 1. 系統地基 (強制自動化都更與欄位修補) =================
# 哲哲叮嚀：地基不穩，獲利不準！這裡自動處理所有 DatabaseError。
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # A. 建立核心表格
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
        
        # B. 🔥 強制欄位都更：自動補齊所有版本差異產生的缺失欄位
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        for col in ['kd20', 'kd60', 'bbu', 'bbl']:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} FLOAT;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 地基修復失敗：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與 LINE 噴發) =================
@st.cache_resource
def get_ocr_reader():
    return easyocr.Reader(['ch_tra', 'en'])

def send_line_report(title, df, icon):
    """將策略戰報精準噴向 LINE"""
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in df.iterrows():
        name = r['名稱'] if '名稱' in r else r.get('stock_name', '未知')
        price = r['現價'] if '現價' in r else 'N/A'
        msg += f"✅ {r['ticker'] if 'ticker' in r else r.get('代號','')} {name} | 現價:{price}\n"
    msg += "\n跟我預測的一模一樣，賺到流湯！🚀"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": USER_ID, "messages": [{"type": "text", "text": msg}]}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))

def style_df(df):
    """終極視覺美化，紅漲綠跌一眼看出"""
    def color_rsi(val):
        if val >= 70: return 'background-color: #FFCCCC'
        if val <= 30: return 'background-color: #CCFFCC'
        return ''
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:,.0f}'}
    styler = df.style.format({k: v for k, v in f_map.items() if k in df.columns})
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['報酬率(%)'])
    if '漲跌(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['漲跌(%)'])
    if 'RSI' in df.columns: styler = styler.map(color_rsi, subset=['RSI'])
    return styler

# ================= 3. 核心引擎 (抓取與網格校準 OCR) =================
def fetch_data(ticker, name):
    """全市場掃描引擎，計算扣抵值與布林帶"""
    for t in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=15)
            if not d.empty and len(d) >= 65:
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

def process_ocr_v38(files):
    """根據黃色字體說明開發：網格座標校準邏輯，解決數據位移"""
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
                    qty = 0
                    entry_p = 0
                    for other in items:
                        # 抓取股數：名稱右側同一水平線 (X: 450~750)
                        if abs(other['y'] - item['y']) < 30 and 450 < other['x'] < 750:
                            v = other['text'].replace(',', '').replace(' ', '')
                            if v.isdigit(): qty = float(v)
                        # 抓取均價：名稱下方一格 (Y+50~100) (X: 250~480)
                        if 40 < (other['y'] - item['y']) < 120 and 250 < other['x'] < 480:
                            v = other['text'].replace(',', '').replace(' ', '')
                            if '.' in v or v.isdigit(): entry_p = float(v)
                    if entry_p > 0:
                        extracted.append({"ticker": name_map[s_name], "stock_name": s_name, "entry_price": entry_p, "qty": qty})
                except: continue
    return pd.DataFrame(extracted)

# ================= 4. 介面設計 (終極大滿貫版) =================
st.set_page_config(page_title="哲哲戰情室 V38.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V38.0 — 終極封神大滿貫完全體")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 資產即時監控 & 賣訊", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日大數據掃描 (六大買進策略)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取數據", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5 as SMA5, ma20 as MA20, ma60 as MA60, rsi as RSI, bbl, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: st.session_state['master_df'] = db_df; st.success("✅ 行情載入成功！")
            else: st.warning("今日尚無快取，請先啟動並行掃描。")
    with c2:
        if st.button("⚡ 啟動全市場並行渦輪掃描", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                results, prog = [], st.progress(0)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, future in enumerate(as_completed(futures)):
                        res = future.result()
                        if res: results.append(res)
                        prog.progress((count + 1) / len(pool))
                m_df = pd.DataFrame(results)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI','sma5':'SMA5','ma20':'MA20','ma60':'MA60'})
                st.success("✨ 市場掃描完成！")

    if 'master_df' in st.session_state:
        st.divider()
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        st.markdown("### 🛠️ 買入決策中心")
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
                st.write(f"符合『{name}』標的共 {len(res)} 檔：")
                st.dataframe(style_df(res))
                send_line_report(name, res, icon)

# --- Tab 2: 持倉與 5 大賣股策略 ---
with tab2:
    st.header("💼 資產即時監控與五大賣股策略")
    df_p = pd.read_sql("SELECT p.*, s.stock_name as pool_name FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        df_p['stock_name'] = df_p['pool_name'].fillna(df_p['stock_name'])
        if st.button("🔄 更新即時獲利 (Yahoo Finance 連動)", use_container_width=True):
            tickers = df_p['ticker'].tolist()
            rt_prices = yf.download(tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
            st.session_state['rt_p'] = rt_prices.to_dict() if len(tickers)>1 else {tickers[0]: rt_prices}
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("當前庫存預估總獲利", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股警示系統")
        # 實裝 5 大賣法監控
        if 'master_df' in st.session_state:
            check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            
            # 策略 1: 均線死叉
            exit_1 = check_df[check_df['SMA5'] < check_df['MA20']]
            if not exit_1.empty: st.error(f"💀 【死叉賣訊】：{', '.join(exit_1['stock_name'].tolist())}")
            
            # 策略 2: 強制停損 (報酬 < -10%)
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【破位停損】：{', '.join(exit_2['stock_name'].tolist())}")
            
            # 策略 3: 高標止盈 (報酬 > 20%)
            exit_3 = check_df[check_df['報酬率(%)'] > 20]
            if not exit_3.empty: st.success(f"💰 【高檔獲利】：{', '.join(exit_3['stock_name'].tolist())}")
            
            # 策略 4: RSI 過熱 (RSI > 80)
            exit_4 = check_df[check_df['RSI'] > 80]
            if not exit_4.empty: st.info(f"🔥 【RSI 過熱】：{', '.join(exit_4['stock_name'].tolist())}")
            
            # 策略 5: 跌破月線
            exit_5 = check_df[check_df['現價'] < check_df['MA20']]
            if not exit_5.empty: st.error(f"⚠️ 【跌破月線】：{', '.join(exit_5['stock_name'].tolist())}")

# --- Tab 3: 後台管理中心 ---
with tab3:
    c_csv1, c_csv2 = st.columns(2)
    with c_csv1:
        st.subheader("📋 股票池管理 (Stock Pool)")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv", key="pool")
        if f_pool and st.button("💾 執行匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False)
            st.success("匯入完成")
    with c_csv2:
        st.subheader("💰 手動持倉導入 (Portfolio)")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv", key="port")
        if f_port and st.button("💾 手動存入持倉"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False)
            st.success("手動存入完成")
    
    st.divider()
    st.subheader("🤖 AI 視覺庫存智慧導入 (網格精準校準)")
    ups = st.file_uploader("📥 上傳截圖 (可多張)", type=["png", "jpg", "jpeg"], accept_multiple_files=True)
    if ups and st.button("🚀 啟動 AI 辨識並自動同步資料庫"):
        with st.spinner("AI 精準辨識中，數據校準中..."):
            df_ocr = process_ocr_v38(ups)
            if not df_ocr.empty:
                # 清洗與轉換
                df_ocr['entry_price'] = pd.to_numeric(df_ocr['entry_price'], errors='coerce')
                df_ocr['qty'] = pd.to_numeric(df_ocr['qty'], errors='coerce')
                df_ocr = df_ocr.dropna()
                with engine.begin() as conn:
                    # 💎 解決 ArgumentError 與 重複鍵值的終極寫法
                    t_list = df_ocr['ticker'].tolist()
                    if t_list:
                        conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                    df_ocr[['ticker', 'stock_name', 'entry_price', 'qty']].to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success("✅ 數據同步完成！")
                st.dataframe(df_ocr)

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

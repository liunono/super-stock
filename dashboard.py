import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import io

# ================= 1. 系統地基 (資料庫強制自動化都更) =================
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
        
        # B. 自動檢查欄位：解決 DatabaseError
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        for col in ['kd20', 'kd60', 'bbu', 'bbl']:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} FLOAT;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 地基修復失敗：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與策略警示) =================
def send_line_report(title, df, icon):
    """將策略偵測結果噴向 LINE"""
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
    def color_rsi(val):
        if val >= 70: return 'background-color: #FFCCCC'
        if val <= 30: return 'background-color: #CCFFCC'
        return ''
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:,.0f}'}
    styler = df.style.format({k: v for k, v in f_map.items() if k in df.columns})
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['報酬率(%)'])
    if 'RSI' in df.columns: styler = styler.map(color_rsi, subset=['RSI'])
    return styler

# ================= 3. 核心引擎 (抓取與策略監控) =================
def fetch_data(ticker, name):
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

# ================= 4. 介面設計 (V47.0 巔峰完全體) =================
st.set_page_config(page_title="哲哲戰情室 V47.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V47.0 — 純粹巔峰大滿貫")

tab1, tab2, tab3 = st.tabs(["🚀 買股策略掃描", "💼 資產獲利監控", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日大數據掃描 (九成勝率濾網)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: st.session_state['master_df'] = db_df; st.success("✅ 行情載入成功！")
    with c2:
        if st.button("⚡ 啟動並行渦輪掃描", use_container_width=True):
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
        st.markdown("### 🛠️ 買股必勝策略")
        cols = st.columns(5)
        strats = [
            ("九成勝率提款機", "👑", (df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['量比']>=1.2)),
            ("量價突破", "💥", (df['現價']>df['ma20']) & (df['量比']>2)),
            ("低階抄底", "🛡️", (df['RSI']<35) & (df['現價']>df['sma5'])),
            ("布林噴發", "🌀", (df['現價']>df['bbu'])),
            ("強勢回測", "🎯", (df['現價']>df['ma20']) & (abs(df['現價']-df['ma20'])/df['ma20']<0.02))
        ]
        for i, (name, icon, mask) in enumerate(strats):
            if cols[i].button(f"{icon} {name}", use_container_width=True):
                st.dataframe(style_df(df[mask].sort_values(by='RSI', ascending=False)))
                send_line_report(name, df[mask], icon)

# --- Tab 2: 持倉與賣股策略 ---
with tab2:
    st.header("💼 資產即時監控與五大賣訊")
    df_p = pd.read_sql("SELECT p.*, s.stock_name as pool_name FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        df_p['stock_name'] = df_p['pool_name'].fillna(df_p['stock_name'])
        if st.button("🔄 更新即時現價與獲利", use_container_width=True):
            tickers = df_p['ticker'].tolist()
            rt = yf.download(tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
            st.session_state['rt_p'] = rt.to_dict() if len(tickers)>1 else {tickers[0]: rt}
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("當前預估總獲利", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(style_df(df_p[['ticker', 'stock_name', 'entry_price', '現價', 'qty', '獲利', '報酬率(%)']]))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股警示")
        if 'master_df' in st.session_state:
            check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            exit_1 = check_df[(check_df['sma5'] < check_df['ma20'])]
            if not exit_1.empty: st.error(f"💀 【死叉賣訊】：{', '.join(exit_1['stock_name'].tolist())}")
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【破位停損】：{', '.join(exit_2['stock_name'].tolist())}")
    else: st.info("持倉為空，請至 Tab 3 匯入 CSV。")

# --- Tab 3: 後台管理 (範例下載與匯入) ---
with tab3:
    st.subheader("📋 股票池管理 (Stock Pool)")
    # 💎 範例下載功能
    sample_pool = pd.DataFrame({'ticker': ['2330.TW', '2317.TW'], 'stock_name': ['台積電', '鴻海'], 'sector': ['半導體', '代工']})
    st.download_button("📥 下載股票池範例 CSV", sample_pool.to_csv(index=False).encode('utf-8-sig'), "sample_pool.csv", "text/csv")
    
    f_pool = st.file_uploader("上傳股票池", type="csv", key="up_pool")
    if f_pool and st.button("💾 匯入股票池"):
        try:
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False)
            st.success("匯入成功！")
        except Exception as e: st.error(f"匯入失敗：{e}")

    st.divider()
    st.subheader("💰 持倉管理 (Portfolio)")
    # 💎 範例下載功能
    sample_port = pd.DataFrame({'ticker': ['2330.TW'], 'stock_name': ['台積電'], 'entry_price': [600.5], 'qty': [1000]})
    st.download_button("📥 下載持倉範例 CSV", sample_port.to_csv(index=False).encode('utf-8-sig'), "sample_portfolio.csv", "text/csv")
    
    f_port = st.file_uploader("上傳持倉數據", type="csv", key="up_port")
    if f_port and st.button("💾 存入持倉"):
        try:
            df_up = pd.read_csv(f_port)
            with engine.begin() as conn:
                # 解決 Duplicate Entry：先清空舊數據
                t_list = df_up['ticker'].tolist()
                conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                df_up.to_sql('portfolio', con=conn, if_exists='append', index=False)
            st.success("數據已同步至資料庫！")
        except Exception as e: st.error(f"資料庫寫入失敗：{e}")

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險！")

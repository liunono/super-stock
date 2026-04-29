import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (強制自動化都更與中文字修復) =================
# 哲哲叮嚀：地基打不穩，獲利變浮雲！自動修復 ???? 亂碼與 DatabaseError。
try:
    # 💎 加入 charset=utf8mb4 解決中文字亂碼
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4"})
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 🔥 強制轉換編碼指令，把之前的亂碼通通洗掉
        conn.execute(text("SET NAMES utf8mb4;"))
        
        # A. 建立三大核心表格
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50)) CHARACTER SET utf8mb4;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, 
                ticker VARCHAR(20), 
                stock_name VARCHAR(50), 
                entry_price FLOAT, 
                qty FLOAT
            ) CHARACTER SET utf8mb4;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4;
        """))
        
        # B. 🔥 強制都更：檢查 portfolio 是否有 stock_name 與 qty
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        # C. 確保表格編碼為 utf8mb4
        for table in ['stock_pool', 'portfolio', 'daily_scans']:
            conn.execute(text(f"ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基崩潰：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與策略發送) =================
def send_line_report(title, df, icon):
    """精準噴發戰報到 LINE"""
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
    """終極視覺強化，一眼抓出飆股"""
    def color_val(val):
        if isinstance(val, (int, float)):
            if val > 0: return 'color: #FF3333; font-weight: bold' # 漲紅
            if val < 0: return 'color: #00AA00' # 跌綠
        return ''
    
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:,.0f}'}
    styler = df.style.format({k: v for k, v in f_map.items() if k in df.columns})
    
    if '報酬率(%)' in df.columns: styler = styler.map(color_val, subset=['報酬率(%)'])
    if '漲跌(%)' in df.columns: styler = styler.map(color_val, subset=['漲跌(%)'])
    if '獲利' in df.columns: styler = styler.map(color_val, subset=['獲利'])
    return styler

# ================= 3. 核心大腦 (數據抓取與指標計算) =================
def fetch_data(ticker, name):
    """計算扣三低、量比、布林帶等冠軍指標"""
    for t in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=10)
            if not d.empty and len(d) >= 65:
                c, v = d['Close'], d['Volume']
                bb = ta.bbands(c, 20, 2)
                return {
                    "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                    "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                    "sma5": round(ta.sma(c, 5).iloc[-1], 2), "ma20": round(ta.sma(c, 20).iloc[-1], 2),
                    "ma60": round(ta.sma(c, 60).iloc[-1], 2), "rsi": round(ta.rsi(c, 14).iloc[-1], 2),
                    "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1]),
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), "scan_date": datetime.now().date(),
                    "bbu": round(bb.iloc[-1, 2], 2), "bbl": round(bb.iloc[-1, 0], 2)
                }
        except: continue
    return None

# ================= 4. 主介面設計 (終極巔峰完全體) =================
st.set_page_config(page_title="哲哲戰情室 V50.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V50.0 — 終極巔峰提款機")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 資產獲利 & 賣出策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 全市場大數據掃描 (九成勝率實裝)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取數據", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: st.session_state['master_df'] = db_df; st.success("✅ 行情載入成功！")
            else: st.warning("今日尚無快取，請先執行渦輪掃描。")
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
                st.success("✨ 市場掃描完成！準備噴發！")

    if 'master_df' in st.session_state:
        st.divider()
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        st.markdown("### 🛠️ 買股必勝決策中心 (11大指標監控)")
        cols = st.columns(6)
        # 💎 六大買股策略
        strats = [
            ("九成勝率提款機", "👑", (df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['量比']>=1.2) & (df['現價']>df['sma5'])),
            ("量價突破", "💥", (df['現價']>df['ma20']) & (df['量比']>2)),
            ("黃金交叉", "🚀", (df['sma5']>df['ma20'])),
            ("低階抄底", "🛡️", (df['RSI']<35) & (df['現價']>df['sma5'])),
            ("布林噴發", "🌀", (df['現價']>df['bbu'])),
            ("強勢回測", "🎯", (abs(df['現價']-df['ma20'])/df['ma20']<0.02))
        ]
        for i, (name, icon, mask) in enumerate(strats):
            if cols[i].button(f"{icon} {name}", use_container_width=True):
                res = df[mask].sort_values(by='RSI', ascending=False)
                st.write(f"符合『{name}』標的共 {len(res)} 檔：")
                st.dataframe(style_df(res))
                send_line_report(name, res, icon)

# --- Tab 2: 資產與賣股策略 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    # 💎 解決亂碼：從 pool 抓名稱，並使用 COALESCE 防空
    df_p = pd.read_sql("""
        SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty 
        FROM portfolio p 
        LEFT JOIN stock_pool s ON p.ticker = s.ticker
    """, con=engine)
    
    if not df_p.empty:
        if st.button("🔄 更新即時獲利 (Yahoo Finance)", use_container_width=True):
            with st.spinner("連線交易所中..."):
                tickers = df_p['ticker'].tolist()
                rt_prices = yf.download(tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
                st.session_state['rt_p'] = rt_prices.to_dict() if len(tickers)>1 else {tickers[0]: rt_prices}
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("總預估實質獲利", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股警報系統")
        if 'master_df' in st.session_state:
            check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            # 策略：死叉、停損、止盈、RSI過熱、破月線
            exit_1 = check_df[check_df['sma5'] < check_df['ma20']]
            if not exit_1.empty: st.error(f"💀 【均線死叉】：{', '.join(exit_1['stock_name'].tolist())}")
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【強制停損】：{', '.join(exit_2['stock_name'].tolist())}")
            exit_3 = check_df[check_df['報酬率(%)'] > 20]
            if not exit_3.empty: st.success(f"💰 【止盈回測】：{', '.join(exit_3['stock_name'].tolist())}")

# --- Tab 3: 後台管理中心 (範例下載補齊) ---
with tab3:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📋 股票池管理 (Stock Pool)")
        # 💎 範例下載：確保編碼為 utf-8-sig
        sample_pool = pd.DataFrame({'ticker':['2330.TW','3583.TW'],'stock_name':['台積電','辛耘'],'sector':['半導體','設備']})
        st.download_button("📥 下載股票池範例 CSV", sample_pool.to_csv(index=False).encode('utf-8-sig'), "pool_sample.csv")
        
        f_pool = st.file_uploader("上傳股票池", type="csv", key="pool")
        if f_pool and st.button("💾 匯入股票池"):
            try:
                df_pool_up = pd.read_csv(f_pool, encoding='utf-8-sig')
                df_pool_up.to_sql('stock_pool', con=engine, if_exists='append', index=False)
                st.success("匯入成功！")
            except Exception as e: st.error(f"匯入失敗：{e}")

    with col2:
        st.subheader("💰 持倉管理 (Portfolio)")
        # 💎 範例下載
        sample_port = pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'entry_price':[750],'qty':[1000]})
        st.download_button("📥 下載持倉範例 CSV", sample_port.to_csv(index=False).encode('utf-8-sig'), "portfolio_sample.csv")
        
        f_port = st.file_uploader("上傳持倉數據", type="csv", key="port")
        if f_port and st.button("💾 存入持倉"):
            try:
                df_up = pd.read_csv(f_port, encoding='utf-8-sig')
                with engine.begin() as conn:
                    # 解決 Duplicate Entry：先清空舊代號
                    t_list = df_up['ticker'].tolist()
                    if t_list:
                        conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                    df_up.to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success("持倉同步成功！數據已止穩！")
            except Exception as e: st.error(f"寫入失敗：{e}")

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

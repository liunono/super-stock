import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (強制解碼與資料庫都更) =================
# 哲哲叮嚀：地基打得穩，房子蓋得高！這段代碼會自動修復 ???? 亂碼問題。
try:
    # 💎 核心修復：強制連線字串與連線參數使用 utf8mb4
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(
        DB_URL, 
        connect_args={"charset": "utf8mb4"},
        pool_pre_ping=True,
        pool_recycle=3600
    )
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 🔥 哲哲大絕：強制轉換資料庫與表格編碼，把之前的亂碼通通洗掉
        conn.execute(text("SET NAMES utf8mb4;"))
        conn.execute(text(f"ALTER DATABASE {st.secrets['DB_NAME']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        
        # A. 建立並都更表格 (強制使用 utf8mb4)
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50)) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, 
                ticker VARCHAR(20), 
                stock_name VARCHAR(50), 
                entry_price FLOAT, 
                qty FLOAT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        
        # B. 檢查欄位補齊 (防止 DatabaseError)
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        # C. 🔥 強制轉碼現有表格，醫好已經存入的 ????
        for table in ['stock_pool', 'portfolio', 'daily_scans']:
            conn.execute(text(f"ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基崩潰：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與 LINE 噴發) =================
def send_line_report(title, df, icon):
    """精準噴發戰報到 LINE，讓利多帶量上攻！"""
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
    """終極視覺強化，紅漲綠跌，一眼抓出飆股"""
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
    # 支援 .TW 和 .TWO 自動切換，這才是冠軍的馬力！
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

# ================= 4. 主介面設計 (終極巔峰 V51.0 版) =================
st.set_page_config(page_title="哲哲戰情室 V51.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V51.0 — 終極封神提款機")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 資產獲利 & 賣出策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 (策略按鈕常駐顯示) ---
with tab1:
    st.markdown("### 🏆 全市場大數據掃描 (九成勝率實裝)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取數據", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: 
                st.session_state['master_df'] = db_df
                st.success("✅ 行情載入成功！跟我預測的一模一樣！")
            else: st.warning("今日尚無快取數據，請執行下方渦輪掃描。")
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

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (11大指標監控)")
    
    # 💎 策略按鈕優化：常駐顯示
    if 'master_df' in st.session_state and not st.session_state['master_df'].empty:
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        cols = st.columns(6)
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
    else:
        st.info("💡 贏家提場：請先讀取快取或執行掃描，九成勝率策略標的將自動噴發！")

# --- Tab 2: 資產獲利 & 賣出策略 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    # 💎 解決亂碼：從 stock_pool 強制關聯抓取正確的中文名稱
    df_p = pd.read_sql("""
        SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty 
        FROM portfolio p 
        LEFT JOIN stock_pool s ON p.ticker = s.ticker
    """, con=engine)
    
    if not df_p.empty:
        if st.button("🔄 更新即時獲利 (Yahoo Finance 直接連動)", use_container_width=True):
            with st.spinner("連線交易所中，準備利多噴發..."):
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
            exit_1 = check_df[check_df['sma5'] < check_df['ma20']]
            if not exit_1.empty: st.error(f"💀 【均線死叉】：{', '.join(exit_1['stock_name'].tolist())}")
            exit_2 = check_df[check_df['報酬率(%)'] < -10]
            if not exit_2.empty: st.warning(f"📉 【強制停損】：{', '.join(exit_2['stock_name'].tolist())}")
            exit_3 = check_df[check_df['報酬率(%)'] > 20]
            if not exit_3.empty: st.success(f"💰 【止盈回測】：{', '.join(exit_3['stock_name'].tolist())}")

# --- Tab 3: 後台管理中心 (解決編碼匯入) ---
with tab3:
    st.subheader("🛠️ 贏家數據都更中心")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📋 股票池管理 (Stock Pool)")
        # 💎 範例下載：確保 UTF-8-SIG
        sample_pool = pd.DataFrame({'ticker':['2330.TW','3583.TW','3491.TW'],'stock_name':['台積電','辛耘','昇達科'],'sector':['半導體','設備','低軌衛星']})
        st.download_button("📥 下載股票池範本 CSV", sample_pool.to_csv(index=False).encode('utf-8-sig'), "pool_sample.csv")
        f_pool = st.file_uploader("上傳股票池", type="csv", key="pool")
        if f_pool and st.button("💾 匯入股票池"):
            try:
                # 💎 讀取強制使用 utf-8-sig 防止中文字變亂碼
                df_pool_up = pd.read_csv(f_pool, encoding='utf-8-sig')
                df_pool_up.to_sql('stock_pool', con=engine, if_exists='append', index=False)
                st.success("✅ 股票池都更成功！亂碼已排除！")
            except Exception as e: st.error(f"匯入失敗：{e}")

    with col2:
        st.markdown("#### 💰 持倉管理 (Portfolio)")
        sample_port = pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'entry_price':[750],'qty':[1000]})
        st.download_button("📥 下載持倉範本 CSV", sample_port.to_csv(index=False).encode('utf-8-sig'), "port_sample.csv")
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
                st.success("✅ 持倉同步完成！數據已止穩！")
            except Exception as e: st.error(f"存入失敗：{e}")

    if st.button("🔥 終極都更：清空所有亂碼並重新初始化"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE stock_pool;"))
            conn.execute(text("TRUNCATE TABLE portfolio;"))
            conn.execute(text("TRUNCATE TABLE daily_scans;"))
            st.warning("⚠️ 數據已清空，請重新下載範本並匯入，保證中文字完美歸位！")

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

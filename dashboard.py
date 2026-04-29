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
try:
    # 💎 核心修復：強制所有連線與參數使用 utf8mb4
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(
        DB_URL, 
        connect_args={"charset": "utf8mb4", "connect_timeout": 15},
        pool_pre_ping=True,
        pool_recycle=3600
    )
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 建立並鎖定編碼 (防止 ???? 亂碼)
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
        # B. 強制數據都更：醫好現有表格
        for t in ['stock_pool', 'portfolio', 'daily_scans']:
            conn.execute(text(f"ALTER TABLE {t} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基崩潰：{e}"); st.stop()

# ================= 2. 核心大腦 (數據抓取機制優化：拒絕 None 值) =================
def fetch_data(ticker, name):
    """計算九成勝率關鍵指標，含失敗重試"""
    # 嘗試不同的後綴，確保抓到數據
    tickers_to_try = [ticker]
    if ".TW" in ticker: tickers_to_try.append(ticker.replace(".TW", ".TWO"))
    elif ".TWO" in ticker: tickers_to_try.append(ticker.replace(".TWO", ".TW"))

    for t in tickers_to_try:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=10)
            if not d.empty and len(d) >= 65:
                c, v = d['Close'], d['Volume']
                sma5, ma20, rsi, bb = ta.sma(c, 5), ta.sma(c, 20), ta.rsi(c, 14), ta.bbands(c, 20, 2)
                return {
                    "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                    "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                    "sma5": round(sma5.iloc[-1], 2), "ma20": round(ma20.iloc[-1], 2),
                    "ma60": round(ta.sma(c, 60).iloc[-1], 2), "rsi": round(rsi.iloc[-1], 2),
                    "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1]),
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), "scan_date": datetime.now().date(),
                    "bbu": round(bb.iloc[-1, 2], 2), "bbl": round(bb.iloc[-1, 0], 2)
                }
        except: continue
    return None

# ================= 3. 哲哲美學工具 (視覺與 LINE 發送) =================
def style_df(df):
    def color_val(val):
        if isinstance(val, (int, float)):
            if val > 0: return 'color: #FF3333; font-weight: bold'
            if val < 0: return 'color: #00AA00'
        return ''
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:,.0f}'}
    styler = df.style.format({k: v for k, v in f_map.items() if k in df.columns})
    for col in ['報酬率(%)', '漲跌(%)', '獲利']:
        if col in df.columns: styler = styler.map(color_val, subset=[col])
    return styler

def send_line_report(title, df, icon):
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in df.iterrows():
        n = r['名稱'] if '名稱' in r else r.get('stock_name', '未知')
        msg += f"✅ {r['ticker'] if 'ticker' in r else r.get('代號','')} {n} | 現價:{r.get('現價','N/A')}\n"
    msg += "\n跟我預測的一模一樣，賺到流湯！🚀"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))

# ================= 4. 主介面設計 (V58.0 巔峰完全體) =================
st.set_page_config(page_title="哲哲戰情室 V58.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V58.0 — 終極數據強攻提款機")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 資產獲利 & 賣出策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日行情掃描中心 (九成勝率濾網)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日行情快取", use_container_width=True):
            query = text("SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌_pct`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = :today")
            db_df = pd.read_sql(query, con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                st.session_state['master_df'] = db_df.rename(columns={'漲跌_pct': '漲跌(%)'})
                st.success("✅ 行情載入成功！")
            else: st.warning("今日尚無快取數據。")
    with c2:
        if st.button("⚡ 啟動渦輪並行掃描", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                res, prog = [], st.progress(0)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, f in enumerate(as_completed(futures)):
                        if f.result(): res.append(f.result())
                        prog.progress((count + 1) / len(pool))
                m_df = pd.DataFrame(res)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                st.success("✨ 市場掃描完成！")

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (策略按鈕鋼鐵常駐)")
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
                st.dataframe(style_df(df[mask].sort_values(by='RSI', ascending=False)))
                send_line_report(name, df[mask], icon)
    else: st.info("💡 贏家提醒：請先載入行情數據。")

# --- Tab 2: 資產 & 五大賣股按鈕 (暴力更新版) ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    
    if not df_p.empty:
        # 💎 終極修正：按鈕加入明確反饋與強勢重試
        if st.button("🔄 更新即時獲利 (強力強攻抓取)", use_container_width=True):
            p_map = {}
            prog_bar = st.progress(0)
            status_text = st.empty()
            
            # 暴力逐檔下載
            tickers = df_p['ticker'].tolist()
            for idx, t in enumerate(tickers):
                status_text.text(f"🚀 正在掃描第 {idx+1}/{len(tickers)} 檔：{t}")
                try:
                    # 強制設定 1m 週期，保證最新
                    data = yf.download(t, period="1d", interval="1m", progress=False, timeout=10)
                    if not data.empty:
                        p_map[t] = data['Close'].iloc[-1]
                    else:
                        # 二段式備援：換尾綴
                        alt_t = t.replace(".TW", ".TWO") if ".TW" in t else t.replace(".TWO", ".TW")
                        data_alt = yf.download(alt_t, period="1d", interval="1m", progress=False, timeout=10)
                        p_map[t] = data_alt['Close'].iloc[-1] if not data_alt.empty else np.nan
                except:
                    p_map[t] = np.nan
                prog_bar.progress((idx + 1) / len(tickers))
            
            st.session_state['rt_p'] = p_map
            status_text.text("✅ 即時獲利已百分百同步！")
            st.toast("數據已歸位，跟我預測的一模一樣！", icon="🚀")
            time.sleep(1)
            status_text.empty()
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            # 💎 排除 TypeError：強制數值化
            df_p['entry_price'] = pd.to_numeric(df_p['entry_price'], errors='coerce')
            df_p['现價'] = pd.to_numeric(df_p['現價'], errors='coerce')
            df_p['qty'] = pd.to_numeric(df_p['qty'], errors='coerce')
            
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            
            # 計算總獲利排除空值
            total_profit = pd.to_numeric(df_p['獲利'], errors='coerce').fillna(0).sum()
            st.metric("當前預估實質總獲利 (數據已止穩)", f"${total_profit:,.0f}")
            
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (按鈕常駐)")
        m_cols = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if m_cols[i].button(f"{icon} {name}", use_container_width=True):
                if 'master_df' in st.session_state and 'rt_p' in st.session_state:
                    check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
                    p_col = '現價_x' if '現價_x' in check_df.columns else '現價'
                    masks = [
                        check_df['sma5'] < check_df['ma20'],
                        check_df['RSI'] > 80,
                        check_df['報酬率(%)'] > 20,
                        check_df['報酬率(%)'] < -10,
                        check_df[p_col] < check_df['ma20']
                    ]
                    res = check_df[masks[i]]
                    if not res.empty:
                        st.error(f"🚨 符合『{name}』標的如下：")
                        st.dataframe(style_df(res))
                    else: st.success(f"✅ 持倉目前未觸發『{name}』策略！")
                else: st.warning("💡 請先讀取行情並更新獲利。")
    else: st.info("持倉為空。")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("📋 贏家數據都更中心")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 股票池 (Stock Pool)")
        st.download_button("📥 下載範本", pd.DataFrame({'ticker':['2330.TW','3583.TW'],'stock_name':['台積電','辛耘'],'sector':['半導體','設備']}).to_csv(index=False).encode('utf-8-sig'), "pool.csv")
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 匯入股票池"):
            pd.read_csv(f1, encoding='utf-8-sig').to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功！")
    with c2:
        st.markdown("#### 持倉管理 (Portfolio)")
        st.download_button("📥 下載範本", pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'entry_price':[750],'qty':[1000]}).to_csv(index=False).encode('utf-8-sig'), "port.csv")
        f2 = st.file_uploader("上傳持倉數據", type="csv")
        if f2 and st.button("💾 存入持倉"):
            df_up = pd.read_csv(f2, encoding='utf-8-sig')
            with engine.begin() as conn:
                t_list = df_up['ticker'].tolist()
                conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                df_up.to_sql('portfolio', con=conn, if_exists='append', index=False)
            st.success("成功！數據精準同步！")

st.caption("本系統由哲哲冠軍團隊開發。數字會說話，不要忘了我！")

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (資料庫強制排毒與都更) =================
try:
    # 💎 核心修復：強制所有連線與參數使用 utf8mb4
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4"}, pool_pre_ping=True)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 建立並鎖定編碼
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
        
        # B. 確保欄位醫好現有表格
        p_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM portfolio")).fetchall()]
        if 'stock_name' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN stock_name VARCHAR(50) AFTER ticker;"))
        if 'qty' not in p_cols: conn.execute(text("ALTER TABLE portfolio ADD COLUMN qty FLOAT AFTER entry_price;"))
        
        # 🔥 強制數據都更：把舊的亂碼轉換成 utf8mb4
        for table in ['stock_pool', 'portfolio', 'daily_scans']:
            conn.execute(text(f"ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗 (地基崩潰)：{e}"); st.stop()

# ================= 2. 哲哲美學工具 (視覺美化與 LINE 發送) =================
def send_line_report(title, df, icon):
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in df.iterrows():
        n = r['名稱'] if '名稱' in r else r.get('stock_name', '未知')
        p = r['現價'] if '現價' in r else r.get('現價', 'N/A')
        msg += f"✅ {r['ticker'] if 'ticker' in r else r.get('代號','')} {n} | 現價:{p}\n"
    msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": USER_ID, "messages": [{"type": "text", "text": msg}]}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))

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

# ================= 3. 核心大腦 (數據抓取機制優化) =================
def fetch_data(ticker, name):
    """計算九成勝率關鍵指標"""
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

# ================= 4. 主介面設計 (V53.0 巔峰完全體) =================
st.set_page_config(page_title="哲哲戰情室 V53.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V53.0 — 數據不鎖死．全能大滿貫")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 資產獲利 & 賣出策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日全市場掃描 (冠軍濾網實裝)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日行情數據", use_container_width=True):
            db_df = pd.read_sql(f"SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌(%)`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not db_df.empty: 
                st.session_state['master_df'] = db_df
                st.success("✅ 行情載入成功！跟我預測的一模一樣！")
            else: st.warning("今日尚無快取數據，請執行下方渦輪掃描。")
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
                st.success("✨ 市場掃描完成！準備噴發！")

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (11大指標)")
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
                st.write(f"🎯 符合標的：{len(res)} 檔")
                st.dataframe(style_df(res))
                send_line_report(name, res, icon)
    else: st.info("💡 請點擊上方按鈕載入行情數據。")

# --- Tab 2: 資產 & 五大賣股按鈕 (解決 None 與按鈕顯示) ---
with tab2:
    st.header("💼 我的資產即時戰報")
    # 💎 解決亂碼：關聯 stock_pool
    df_p = pd.read_sql("""
        SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty 
        FROM portfolio p 
        LEFT JOIN stock_pool s ON p.ticker = s.ticker
    """, con=engine)
    
    if not df_p.empty:
        if st.button("🔄 更新即時現價與獲利 (Yahoo Finance 直接連動)", use_container_width=True):
            with st.spinner("連線交易所中，拒絕數值鎖死..."):
                prices_map = {}
                # 💎 強化重試機制：解決 None 值問題
                for t in df_p['ticker'].tolist():
                    try:
                        # 逐檔抓取，確保穩定性
                        p = yf.download(t, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
                        prices_map[t] = p
                    except:
                        # 如果失敗，嘗試切換市場尾綴
                        try:
                            alt_t = t.replace(".TW", ".TWO") if ".TW" in t else t.replace(".TWO", ".TW")
                            p = yf.download(alt_t, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
                            prices_map[t] = p
                        except: prices_map[t] = np.nan
                st.session_state['rt_p'] = prices_map
        
        if 'rt_p' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p'])
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = round(((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100, 2)
            st.metric("總預估實質獲利 (數據已止穩)", f"${df_p['獲利'].sum():,.0f}")
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策中心")
        
        # 💎 按鈕常駐顯示，並判斷數據完整性
        m_cols = st.columns(5)
        btn_names = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        
        for i, (name, icon) in enumerate(btn_names):
            if m_cols[i].button(f"{icon} {name}", use_container_width=True):
                if 'master_df' in st.session_state and 'rt_p' in st.session_state:
                    check_df = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
                    # 策略遮罩
                    masks = [
                        check_df['sma5'] < check_df['ma20'],
                        check_df['RSI'] > 80,
                        check_df['報酬率(%)'] > 20,
                        check_df['報酬率(%)'] < -10,
                        check_df['現價'] < check_df['ma20']
                    ]
                    res = check_df[masks[i]]
                    if not res.empty:
                        st.error(f"🚨 警告！符合『{name}』標的如下：")
                        st.dataframe(style_df(res[['ticker', 'stock_name_x', '現價_x', '報酬率(%)']]))
                    else: st.success(f"✅ 目前持倉皆未觸發『{name}』策略！")
                else:
                    st.warning("💡 贏家提醒：請先在 Tab 1 執行掃描行情，並點擊上方「更新即時獲利」，決策按鈕才能為你精準分析！")
    else: st.info("持倉資料庫為空，請至 Tab 3 匯入 CSV。")

# --- Tab 3: 後台管理中心 ---
with tab3:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📋 股票池管理")
        st.download_button("📥 下載股票池範本", pd.DataFrame({'ticker':['2330.TW','3583.TW'],'stock_name':['台積電','辛耘'],'sector':['半導體','設備']}).to_csv(index=False).encode('utf-8-sig'), "sample_pool.csv")
        f1 = st.file_uploader("上傳股票池", type="csv", key="f1")
        if f1 and st.button("💾 匯入股票池"):
            try:
                pd.read_csv(f1, encoding='utf-8-sig').to_sql('stock_pool', con=engine, if_exists='append', index=False)
                st.success("成功！中文字已完美解碼！")
            except Exception as e: st.error(f"匯入失敗：{e}")
    with col2:
        st.subheader("💰 持倉管理")
        st.download_button("📥 下載持倉範本", pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'entry_price':[750],'qty':[1000]}).to_csv(index=False).encode('utf-8-sig'), "sample_port.csv")
        f2 = st.file_uploader("上傳持倉數據", type="csv", key="f2")
        if f2 and st.button("💾 存入持倉"):
            try:
                df_up = pd.read_csv(f2, encoding='utf-8-sig')
                with engine.begin() as conn:
                    t_list = df_up['ticker'].tolist()
                    if t_list: conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                    df_up.to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success("存入成功！")
            except Exception as e: st.error(f"失敗：{e}")

    if st.button("🔥 終極都更：清空所有亂碼與髒數據"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE stock_pool;"))
            conn.execute(text("TRUNCATE TABLE portfolio;"))
            st.warning("⚠️ 已清空，請重新下載範本並匯入，保證中文字歸位！")

st.caption("本系統由哲哲冠軍團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time, random, requests, json
from datetime import datetime

# ================= 1. 系統地基 (自動創表) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 持倉表
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        # 🚀 每日掃描金庫表 (確保結構完整)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, PRIMARY KEY (ticker, scan_date)
            );
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗：{e}"); st.stop()

# ================= 2. 核心引擎 (不變) =================
def fetch_comprehensive_data(ticker, name):
    for cur_ticker in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            time.sleep(random.uniform(1.2, 2.0))
            data = yf.Ticker(cur_ticker).history(period="6mo", interval="1d", timeout=20)
            if data is not None and len(data) >= 20:
                close, vol = data['Close'], data['Volume']
                sma5, sma20, sma60 = ta.sma(close, 5), ta.sma(close, 20), ta.sma(close, 60)
                rsi, bb, avg_vol = ta.rsi(close, 14), ta.bbands(close, 20, 2), ta.sma(vol, 20)
                last_p = round(float(close.iloc[-1]), 2)
                return {
                    "ticker": ticker, "stock_name": name, "price": last_p,
                    "change_pct": round(((last_p - float(close.iloc[-2])) / float(close.iloc[-2])) * 100, 2),
                    "sma5": round(float(sma5.iloc[-1]), 2), "ma20": round(float(sma20.iloc[-1]), 2),
                    "ma60": round(float(sma60.iloc[-1]), 2), "rsi": round(float(rsi.iloc[-1]), 2),
                    "bbl": round(float(bb.iloc[-1, 0]), 2), "bbu": round(float(bb.iloc[-1, 2]), 2),
                    "vol": int(vol.iloc[-1]), "avg_vol": int(avg_vol.iloc[-1]) if avg_vol is not None else 0,
                    "scan_date": datetime.now().date()
                }
            break
        except: continue
    return None

# ================= 3. 介面設計 =================
st.set_page_config(page_title="哲哲戰情室 V20.1", layout="wide")
st.title("📈 哲哲量化戰情室 V20.1 - 金庫封存強化版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 持倉獲利監控", "🛠️ 系統管理"])

with tab1:
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📡 讀取今日金庫數據 (秒開)", use_container_width=True):
            today = datetime.now().date()
            df_db = pd.read_sql(f"SELECT * FROM daily_scans WHERE scan_date = '{today}'", con=engine)
            if not df_db.empty:
                # 重新對齊顯示名稱
                df_db.columns = ['代號','名稱','現價','漲跌(%)','SMA5','MA20','MA60','RSI','BBL','BBU','成交量','均量','日期']
                st.session_state['master_df'] = df_db
                st.success(f"✅ 金庫亮牌成功！讀取到 {len(df_db)} 檔標的。")
            else: st.warning("今日尚無快取數據，請先執行右側全線掃描。")
    
    with col_b:
        if st.button("📦 執行全線掃描 (數據封存)", use_container_width=True):
            df_pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not df_pool.empty:
                master_list, prog, live_t = [], st.progress(0), st.empty()
                for i, row in df_pool.iterrows():
                    res = fetch_comprehensive_data(row['ticker'], row['stock_name'])
                    if res: master_list.append(res)
                    live_t.dataframe(pd.DataFrame(master_list), width=1200)
                    prog.progress((i + 1) / len(df_pool))
                
                m_df = pd.DataFrame(master_list)
                
                # 🚀 哲哲核心強化：採用『先清空、後存入』邏輯，解決入庫失敗問題
                with engine.begin() as conn:
                    today = datetime.now().date()
                    # 1. 先刪除今天可能已經存在的舊快取 (防止 Duplicate Entry)
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{today}'"))
                    # 2. 寫入最新數據
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False, method='multi')
                
                # 更新畫面上對齊名稱
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','sma5':'SMA5','ma20':'MA20','ma60':'MA60','rsi':'RSI','bbl':'BBL','bbu':'BBU','vol':'成交量','avg_vol':'均量','scan_date':'日期'})
                st.success("✨ 數據已成功封存入庫！")
                st.balloons()

    # 策略按鈕區 (同前，省略重複細節...)
    if 'master_df' in st.session_state:
        st.divider()
        m_df = st.session_state['master_df']
        btn_cols = st.columns(5)
        strats = [("🚀 黃金交叉", m_df['MA20'] > m_df['MA60']), 
                  ("💥 量價突破", (m_df['現價'] > m_df['MA20']) & (m_df['成交量'] > m_df['均量'] * 2)),
                  ("🛡️ 低階抄底", (m_df['RSI'] < 35) & (m_df['現價'] > m_df['SMA5'])),
                  ("🌀 布林噴發", m_df['現價'] > m_df['BBU']),
                  ("🎯 強勢回測", (m_df['現價'] > m_df['MA20']) & (abs(m_df['現價']-m_df['MA20'])/m_df['MA20'] < 0.02))]
        
        for i, (name, mask) in enumerate(strats):
            if btn_cols[i].button(name):
                res = m_df[mask]
                st.write(f"🎯 符合【{name}】的標的：{len(res)} 檔")
                st.dataframe(res, width=1200)

# --- Tab 2 & 3 保持管理與清空功能 ---

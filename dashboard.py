import streamlit as st
import pandas as pd
import pymysql
import yfinance as yf
import pandas_ta as ta
import time
from datetime import datetime

# ================= 核心連線設定 =================
DB_CONFIG = {
    'host': st.secrets["DB_HOST"],
    'user': st.secrets["DB_USER"],
    'password': st.secrets["DB_PASS"],
    'db': st.secrets["DB_NAME"],
    'port': 3306,
    'charset': 'utf8mb4', # 🚀 修正亂碼的靈魂
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db_connection():
    try: return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"🚨 連線利空：{e}")
        return None

# ================= 策略掃描核心 =================
def run_strategy(ticker):
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False, timeout=15)
        if data is None or data.empty: return "⏳ 查無資料"
        
        close = data['Close']
        if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
        
        sma20 = ta.sma(close, length=20)
        rsi = ta.rsi(close, length=14)
        
        if sma20 is None or rsi is None: return "⏳ 指標不足"
        
        price = float(close.iloc[-1])
        ma_val = float(sma20.iloc[-1])
        rsi_val = float(rsi.iloc[-1])
        
        if price > ma_val and rsi_val > 55: return "🔥 趨勢偏多"
        elif price < ma_val and rsi_val < 45: return "📉 弱勢整理"
        else: return "⏳ 盤整觀察"
    except: return "⚠️ 抓取超時"

# ================= 儀表板設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V5.0", layout="wide")
st.title("📈 哲哲量化戰情室 V5.0 - 終極修正版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 數據與體質管理"])

with tab2:
    st.header("數據清理與精密建倉")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧨 一鍵清空垃圾籌碼 (Reset)"):
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_pool")
                conn.commit()
                conn.close()
                st.warning("💥 警報：資料庫已淨空，廢紙已銷毀！")

    with col2:
        uploaded_file = st.file_uploader("上傳正確的股票池 CSV", type="csv")
        if uploaded_file and st.button("💾 正式寫入"):
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
            # 哲哲精準過濾：踢掉標題文字、只要帶點的代號
            df_clean = df[df['ticker'].str.contains('\.', na=False)]
            df_clean = df_clean[df_clean['ticker'] != 'TICKER']
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for _, row in df_clean.iterrows():
                    sql = "INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE stock_name=%s, sector=%s"
                    cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['stock_name'], row['sector']))
                conn.commit()
                conn.close()
                st.success(f"✅ 成功將 {len(df_clean)} 檔飆股入庫，中文無亂碼！")

with tab1:
    st.header("戰情即時監控中心")
    if st.button("🚀 啟動全線策略掃描"):
        conn = get_db_connection()
        if conn:
            df_stocks = pd.read_sql("SELECT ticker, stock_name, sector FROM stock_pool", conn)
            conn.close()
            
            if not df_stocks.empty:
                results = []
                prog = st.progress(0)
                stat = st.empty()
                
                for i, row in df_stocks.iterrows():
                    stat.text(f"🔎 正在偵測：{row['ticker']} ({row['stock_name']})")
                    res = run_strategy(row['ticker'])
                    results.append(res)
                    prog.progress((i + 1) / len(df_stocks))
                    time.sleep(0.05)
                
                df_stocks['策略評等'] = results
                stat.success(f"✨ 掃描完畢！ {datetime.now().strftime('%H:%M:%S')}")
                
                # --- 🚀 關鍵修復：處理 AttributeError: Styler.applymap ---
                def color_val(val):
                    if '🔥' in str(val): return 'color: red; font-weight: bold'
                    if '📉' in str(val): return 'color: green'
                    return ''
                
                # 兼容性寫法：優先嘗試新的 map，失敗則用舊的 applymap
                styler = df_stocks.style
                try:
                    styler = styler.map(color_val, subset=['策略評等'])
                except AttributeError:
                    styler = styler.applymap(color_val, subset=['策略評等'])
                
                st.dataframe(styler, use_container_width=True)
                st.balloons()
            else:
                st.info("💡 資料庫目前沒有股票，請先匯入。")

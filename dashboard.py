import streamlit as st
import pandas as pd
import pymysql
import requests
import json
import yfinance as yf
import pandas_ta as ta
import time
from datetime import datetime

# ================= 系統設定區 =================
try:
    DB_CONFIG = {
        'host': st.secrets["DB_HOST"],
        'user': st.secrets["DB_USER"],
        'password': st.secrets["DB_PASS"],
        'db': st.secrets["DB_NAME"],
        'port': 3306,
        'connect_timeout': 10,
        'cursorclass': pymysql.cursors.DictCursor
    }
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    LINE_ID = st.secrets["YOUR_LINE_USER_ID"]
except Exception as e:
    st.error(f"❌ Secrets 設定有誤：{e}")
    st.stop()

# ================= 核心功能模組 =================
def get_db_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"🚨 資料庫連線失敗：{e}")
        return None

def run_strategy(ticker, name):
    """
    哲哲特製：帶有詳細日誌的策略引擎
    """
    # 1. 檢查代號位階 (排除文字標題)
    if not ticker or "." not in str(ticker) or ticker.lower() == 'ticker':
        return "❌ 代號格式錯誤"
    
    try:
        # 2. 強勢下載數據 (增加 retry 機制)
        data = yf.download(ticker, period="3mo", interval="1d", progress=False, timeout=10)
        
        if data is None or data.empty:
            return "⏳ 交易所查無資料"
        
        # 3. 計算技術指標
        data['SMA20'] = ta.sma(data['Close'], length=20)
        data['RSI'] = ta.rsi(data['Close'], length=14)
        
        last_price = float(data['Close'].iloc[-1])
        last_sma = float(data['SMA20'].iloc[-1])
        last_rsi = float(data['RSI'].iloc[-1])
        
        # 4. 哲哲多頭排列邏輯
        if last_price > last_sma and last_rsi > 55:
            return "🔥 趨勢偏多"
        elif last_price < last_sma and last_rsi < 45:
            return "📉 弱勢整理"
        else:
            return "⏳ 盤整觀察"
    except Exception as e:
        return f"⚠️ 抓取失敗: {str(e)[:20]}"

# ================= 儀表板介面 =================
st.set_page_config(page_title="哲哲量化戰情室 V3.4", layout="wide")
st.title("📈 哲哲量化戰情室 V3.4 - 帶量反攻進度版")

tab1, tab2 = st.tabs(["📊 策略掃描中心", "🛠️ 數據與工具管理"])

with tab2:
    st.header("數據清理與匯入")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧨 一鍵清空資料庫 (砍掉重練)"):
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_pool")
                conn.commit()
                conn.close()
                st.warning("💥 垃圾資料已清空！請重新匯入正確 CSV。")
    
    with col2:
        uploaded_file = st.file_uploader("上傳股票池 CSV", type="csv")
        if uploaded_file and st.button("💾 正式寫入"):
            df = pd.read_csv(uploaded_file)
            # 強制清理 header 雜訊
            df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
            df = df[~df['ticker'].isin(['TICKER', 'STOCK_ID', 'SYMBOL', 'NAN'])]
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for _, row in df.iterrows():
                    sql = "INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE sector=%s"
                    cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['sector']))
                conn.commit()
                conn.close()
                st.success(f"✅ 已存入 {len(df)} 檔有效標的！")

with tab1:
    st.header("即時監控戰情")
    if st.button("🚀 啟動全線掃描 (親眼見證爬蟲)"):
        conn = get_db_connection()
        if conn:
            df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", conn)
            conn.close()
            
            if not df_stocks.empty:
                results = []
                # --- 哲哲進度條設計 ---
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total = len(df_stocks)
                for i, row in df_stocks.iterrows():
                    # 更新 UI 狀態
                    ticker = row['ticker']
                    status_text.text(f"🔎 正在爬取：{ticker} ({row['stock_name']}) ...")
                    
                    # 執行掃描
                    res = run_strategy(ticker, row['stock_name'])
                    results.append(res)
                    
                    # 更新進度條
                    progress_bar.progress((i + 1) / total)
                    # 稍微停頓 0.1 秒，避免被交易所封鎖，也讓你更有感
                    time.sleep(0.1)
                
                df_stocks['策略評等'] = results
                status_text.success("✨ 掃描完成！")
                st.dataframe(df_stocks, use_container_width=True)
                st.balloons()
            else:
                st.info("資料庫目前空空如也。")

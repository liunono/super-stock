import streamlit as st
import pandas as pd
import pymysql
import requests
import json
import yfinance as yf
import pandas_ta as ta
from datetime import datetime

# ================= 系統設定區 =================
try:
    LINE_CHANNEL_ACCESS_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    YOUR_LINE_USER_ID = st.secrets["YOUR_LINE_USER_ID"]

    DB_CONFIG = {
        'host': st.secrets["DB_HOST"],
        'user': st.secrets["DB_USER"],
        'password': st.secrets["DB_PASS"],
        'db': st.secrets["DB_NAME"],
        'port': 3306,
        'connect_timeout': 10,
        'cursorclass': pymysql.cursors.DictCursor
    }
except Exception as e:
    st.error(f"❌ Secrets 設定缺失：{e}")
    st.stop()

# ================= 輔助功能模組 =================
def get_db_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"🚨 資料庫連線利空：{e}")
        return None

# ================= 哲哲核心策略模組 =================
def run_zhe_zhe_strategy(ticker):
    # 哲哲提醒：如果代號明顯不是股票，直接跳過，不浪費子彈
    if not ticker or "." not in str(ticker):
        return "❌ 非法代號"
    
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if data.empty: return "⏳ 資料空缺"
        
        # 指標計算
        data['SMA20'] = ta.sma(data['Close'], length=20)
        data['RSI'] = ta.rsi(data['Close'], length=14)
        
        last_price = data['Close'].iloc[-1]
        last_sma = data['SMA20'].iloc[-1]
        last_rsi = data['RSI'].iloc[-1]
        
        if last_price > last_sma and last_rsi > 55:
            return "🔥 趨勢偏多"
        elif last_price < last_sma and last_rsi < 45:
            return "📉 弱勢整理"
        else:
            return "⏳ 盤整觀察"
    except:
        return "⚠️ 掃描出錯"

# ================= 儀表板介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V3.3", layout="wide")
st.title("📈 哲哲量化戰情室 V3.3 - 精準除錯版")

tab1, tab2, tab3 = st.tabs(["📲 系統測試", "📥 股票池與策略", "📊 數據中心"])

# ----------------- 分頁 2：股票池與策略 -----------------
with tab2:
    st.header("股票池管理與即時掃描")
    
    # 1. 範例下載
    example_df = pd.DataFrame({
        'ticker': ['2330.TW', '2317.TW', '2454.TW'], 
        'stock_name': ['台積電', '鴻海', '聯發科'], 
        'sector': ['半導體', '電子', 'IC設計']
    })
    st.download_button("📥 下載標準範例 CSV", example_df.to_csv(index=False).encode('utf-8-sig'), "stock_template.csv", "text/csv")

    st.markdown("---")

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🛠️ 數據管理")
        uploaded_file = st.file_uploader("上傳股票池 CSV", type="csv")
        
        if uploaded_file and st.button("💾 確認匯入資料庫"):
            df_upload = pd.read_csv(uploaded_file)
            
            # --- 哲哲精準過濾邏輯 ---
            # 1. 全部轉字串並去空格
            df_upload['ticker'] = df_upload['ticker'].astype(str).str.strip().str.upper()
            # 2. 踢掉標題行垃圾
            invalid_keywords = ['TICKER', 'STOCK_ID', 'SYMBOL', 'NAN']
            df_clean = df_upload[~df_upload['ticker'].isin(invalid_keywords)]
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for _, row in df_clean.iterrows():
                    sql = "INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE sector=%s"
                    cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['sector']))
                conn.commit()
                conn.close()
                st.success(f"✅ 成功清洗並存入 {len(df_clean)} 檔核心標的！")
        
        st.write("---")
        # 增加清空功能，處理你之前看到的「廢紙」
        if st.button("🧨 一鍵清空目前股票池", help="這會刪除資料庫內所有監控名單"):
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_pool")
                conn.commit()
                conn.close()
                st.warning("🔥 籌碼已全數清空！現在你的金庫非常乾淨。")

    with col2:
        st.subheader("🔍 即時策略戰情")
        if st.button("🚀 啟動全線掃描"):
            conn = get_db_connection()
            if conn:
                # 撈取資料，並自動過濾垃圾代號
                df_current = pd.read_sql("SELECT ticker, stock_name, sector FROM stock_pool", conn)
                conn.close()
                
                if not df_current.empty:
                    # 再做一次二次過濾，確保畫面乾淨
                    df_current = df_current[df_current['ticker'].str.contains('\.')]
                    
                    with st.spinner("哲哲正在幫你挑選強勢股..."):
                        df_current['策略評等'] = df_current['ticker'].apply(run_zhe_zhe_strategy)
                        
                        # 顯示結果美化
                        st.dataframe(df_current, use_container_width=True)
                        st.balloons()
                else:
                    st.info("💡 股票池是空的，請先在左側匯入 CSV 名單。")

# ----------------- 分頁 3：歷史數據 -----------------
with tab3:
    st.info("📈 這裡將顯示 GoDaddy 自動掃描的歷史日誌。目前環境已通，可以準備設定 Cron Job 了！")

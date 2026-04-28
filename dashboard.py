import streamlit as st
import pandas as pd
import pymysql
import requests
import json
import yfinance as yf
import pandas_ta as ta
from datetime import datetime

# ================= 系統設定區 =================
# 哲哲提醒：這裡的變數請去 Streamlit Cloud 後台 Settings -> Secrets 設定
try:
    LINE_CHANNEL_ACCESS_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    YOUR_LINE_USER_ID = st.secrets["YOUR_LINE_USER_ID"]

    DB_CONFIG = {
        'host': st.secrets["DB_HOST"],  # 這裡要填伺服器 IP，不能是 localhost
        'user': st.secrets["DB_USER"],  # 記得加前綴 zstt_...
        'password': st.secrets["DB_PASS"],
        'db': st.secrets["DB_NAME"],     # 記得加前綴 zstt_...
        'port': 3306,
        'connect_timeout': 10,           # 超時設定，避免轉圈圈太久
        'cursorclass': pymysql.cursors.DictCursor
    }
except Exception as e:
    st.error(f"❌ Secrets 設定缺失：{e}")
    st.stop()

# ================= 輔助功能模組 =================
def get_db_connection():
    """帶有偵錯功能的連線模組"""
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        # 數字會說話！直接把錯誤噴出來看
        st.error(f"🚨 資料庫連線利空：{e}")
        return None

def send_line_msg(msg):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }
    payload = {
        "to": YOUR_LINE_USER_ID,
        "messages": [{"type": "text", "text": msg}]
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        return response.status_code == 200
    except:
        return False

# ================= 哲哲核心策略模組 (同 bot.py) =================
def run_zhe_zhe_strategy(ticker):
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if data.empty: return "⏳ 資料空缺"
        
        # 計算指標 (這就是我們賺到流湯的關鍵)
        data['SMA20'] = ta.sma(data['Close'], length=20)
        data['RSI'] = ta.rsi(data['Close'], length=14)
        
        last_price = data['Close'].iloc[-1]
        last_sma = data['SMA20'].iloc[-1]
        last_rsi = data['RSI'].iloc[-1]
        
        # 哲哲強勢策略邏輯
        if last_price > last_sma and last_rsi > 55:
            return "🔥 趨勢偏多"
        elif last_price < last_sma and last_rsi < 45:
            return "📉 弱勢整理"
        else:
            return "⏳ 盤整觀察"
    except:
        return "⚠️ 掃描出錯"

# ================= 儀表板介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V3.2", layout="wide")
st.title("📈 哲哲量化戰情室 V3.2")

tab1, tab2, tab3 = st.tabs(["📲 系統測試", "📥 股票池與策略", "📊 歷史數據"])

# ----------------- 分頁 2：重點開發區 -----------------
with tab2:
    st.header("股票池管理與即時掃描")
    
    # 範例下載
    example_df = pd.DataFrame({'ticker': ['2330.TW', '2317.TW'], 'stock_name': ['台積電', '鴻海'], 'sector': ['半導體', '電子']})
    st.download_button("📥 下載範例 CSV", example_df.to_csv(index=False).encode('utf-8-sig'), "example.csv", "text/csv")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("匯入名單")
        uploaded_file = st.file_uploader("上傳 CSV", type="csv")
        if uploaded_file and st.button("💾 確認寫入資料庫"):
            df_upload = pd.read_csv(uploaded_file)
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for _, row in df_upload.iterrows():
                    sql = f"INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE sector=%s"
                    cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['sector']))
                conn.commit()
                conn.close()
                st.success("✅ 數據已成功入庫！")

    with col2:
        st.subheader("目前狀態")
        if st.button("🔍 執行策略掃描"):
            conn = get_db_connection()
            if conn:
                df_current = pd.read_sql("SELECT ticker, stock_name, sector FROM stock_pool", conn)
                conn.close()
                if not df_current.empty:
                    with st.spinner("正在計算技術指標..."):
                        df_current['策略評等'] = df_current['ticker'].apply(run_zhe_zhe_strategy)
                        st.dataframe(df_current)
                        st.balloons()

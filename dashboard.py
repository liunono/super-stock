import streamlit as st
import pandas as pd
import pymysql
import requests
import json
import yfinance as yf
import pandas_ta as ta  # 確保你的環境已安裝此套件
from datetime import datetime

# ================= 系統設定區 =================
LINE_CHANNEL_ACCESS_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
YOUR_LINE_USER_ID = st.secrets["YOUR_LINE_USER_ID"]

DB_CONFIG = {
    'host': st.secrets["DB_HOST"],
    'user': st.secrets["DB_USER"],
    'password': st.secrets["DB_PASS"],
    'db': st.secrets["DB_NAME"],
    'port': 3306,
    'cursorclass': pymysql.cursors.DictCursor
}

# ================= 輔助功能模組 =================
def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def send_line_msg(msg):
    """發送 LINE 訊息"""
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
    """
    這就是我們的『獲利方程式』！
    結合 SMA、RSI 與 MACD 的多頭排列判斷。
    """
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if data.empty: return "資料空缺"
        
        # 計算指標
        data['SMA20'] = ta.sma(data['Close'], length=20)
        data['RSI'] = ta.rsi(data['Close'], length=14)
        macd = ta.macd(data['Close'])
        data['MACD_Line'] = macd['MACD_12_26_9']
        data['MACD_Sig'] = macd['MACDs_12_26_9']
        
        last_price = data['Close'].iloc[-1]
        last_sma = data['SMA20'].iloc[-1]
        last_rsi = data['RSI'].iloc[-1]
        
        # 哲哲策略邏輯：股價在月線之上 + RSI 強勢
        if last_price > last_sma and last_rsi > 55:
            return "🔥 趨勢偏多"
        elif last_price < last_sma and last_rsi < 45:
            return "📉 弱勢整理"
        else:
            return "⏳ 盤整觀察"
    except:
        return "掃描出錯"

# ================= 儀表板介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V3.1", layout="wide")
st.title("📈 哲哲量化戰情室 V3.1 - 指揮官模式")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["📲 系統與 LINE 測試", "📥 股票池與策略掃描", "📊 數據中心"])

# ----------------- 分頁 1：LINE 測試 -----------------
with tab1:
    st.header("系統連線與推播測試")
    test_message = st.text_input("自訂測試訊息內容：", "趨勢突破！2330 台積電 站上月線，準備發車！")
    if st.button("🚀 發送測試推播"):
        with st.spinner("正在呼叫 LINE API..."):
            success = send_line_msg(f"🚨 【哲哲戰情室測試】 🚨\n{test_message}")
            if success:
                st.success("✅ 發送成功！請檢查手機 LINE 通知！")
                st.balloons()
            else:
                st.error("❌ 發送失敗！請檢查金鑰設定。")

# ----------------- 分頁 2：股票池與策略 -----------------
with tab2:
    st.header("股票池管理與即時策略掃描")
    
    # --- CSV 下載範例功能 ---
    st.subheader("1. 下載範例 CSV 格式")
    example_df = pd.DataFrame({
        'ticker': ['2330.TW', '2317.TW', '2454.TW'],
        'stock_name': ['台積電', '鴻海', '聯發科'],
        'sector': ['半導體', '電子代工', 'IC設計']
    })
    csv = example_df.to_csv(index=False).encode('utf-8-sig') # 加上 sig 避免 Excel 亂碼
    st.download_button(
        label="📥 點我下載範例股票池 CSV",
        data=csv,
        file_name='stock_pool_example.csv',
        mime='text/csv',
    )
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("2. 匯入新名單")
        uploaded_file = st.file_uploader("上傳 CSV 檔案", type="csv")
        if uploaded_file:
            df_upload = pd.read_csv(uploaded_file)
            st.write("預覽上傳內容：")
            st.dataframe(df_upload)
            
            if st.button("💾 確認寫入資料庫"):
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    # 哲哲強勢寫入法：先清空或使用 INSERT INTO ... ON DUPLICATE KEY UPDATE
                    for index, row in df_upload.iterrows():
                        sql = "INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE stock_name=%s, sector=%s"
                        cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['stock_name'], row['sector']))
                    conn.commit()
                    conn.close()
                    st.success(f"✅ 成功將 {len(df_upload)} 檔股票存入共用資料庫！")
                except Exception as e:
                    st.error(f"❌ 寫入失敗：{e}")

    with col2:
        st.subheader("3. 目前監控名單與策略狀態")
        if st.button("🔍 啟動即時策略掃描"):
            try:
                conn = get_db_connection()
                df_current = pd.read_sql("SELECT ticker, stock_name, sector FROM stock_pool", conn)
                conn.close()
                
                if not df_current.empty:
                    with st.spinner("哲哲正在幫你掃描盤勢..."):
                        # 執行跟 bot.py 一樣的策略判斷
                        df_current['策略評等'] = df_current['ticker'].apply(run_zhe_zhe_strategy)
                        
                        # 顯示結果美化
                        def color_strategy(val):
                            color = 'red' if '🔥' in val else ('green' if '📉' in val else 'black')
                            return f'color: {color}'
                        
                        st.dataframe(df_current.style.applymap(color_strategy, subset=['策略評等']))
                        st.info(f"掃描時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    st.warning("股票池目前空空如也，請先匯入資料。")
            except Exception as e:
                st.error("⚠️ 資料庫讀取失敗。")

# ----------------- 分頁 3：數據中心 -----------------
with tab3:
    st.header("📊 量化數據歷史中心")
    st.write("這是在 GoDaddy 後台全自動掃描後的歷史戰果。")
    st.info("系統累積數據中... 只要 Cron Job 開始跑，這裡就會噴出漂亮的勝率曲線圖！")

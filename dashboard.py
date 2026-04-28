import streamlit as st
import pandas as pd
import pymysql
import yfinance as yf
import pandas_ta as ta
import time

# ================= 系統設定區 =================
DB_CONFIG = {
    'host': st.secrets["DB_HOST"],
    'user': st.secrets["DB_USER"],
    'password': st.secrets["DB_PASS"],
    'db': st.secrets["DB_NAME"],
    'port': 3306,
    'connect_timeout': 10,
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"🚨 資料庫連線利空：{e}")
        return None

# ================= 儀表板介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V3.5", layout="wide")
st.title("📈 哲哲量化戰情室 V3.5 - 籌碼大清洗版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 數據管理與工具"])

# ----------------- 分頁 2：數據管理 (請先做這裡！) -----------------
with tab2:
    st.header("數據清理與匯入校準")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("第一步：利空出盡 (清空垃圾)")
        if st.button("🧨 一鍵清空目前資料庫", help="這會刪除資料庫內所有股票，讓你重新開始"):
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_pool") # 強勢清場
                conn.commit()
                conn.close()
                st.warning("💥 警報：資料庫已完全清空！廢紙已全部銷毀。")

    with col2:
        st.subheader("第二步：精準建倉 (重新匯入)")
        # 範例下載
        example_df = pd.DataFrame({'ticker': ['2330.TW', '2317.TW'], 'stock_name': ['台積電', '鴻海'], 'sector': ['半導體', '電子']})
        st.download_button("📥 下載標準範例 CSV", example_df.to_csv(index=False).encode('utf-8-sig'), "template.csv", "text/csv")
        
        uploaded_file = st.file_uploader("上傳正確的股票池 CSV", type="csv")
        if uploaded_file and st.button("💾 確認匯入資料庫"):
            df_upload = pd.read_csv(uploaded_file)
            
            # --- 哲哲強效過濾邏輯 ---
            # 1. 強制轉大寫並去空格
            df_upload['ticker'] = df_upload['ticker'].astype(str).str.strip().str.upper()
            # 2. 踢掉標題列 (只要代號叫 ticker 的通通不要)
            df_clean = df_upload[df_upload['ticker'] != 'TICKER']
            # 3. 只要含有 "." 的代號 (確保是 2330.TW 格式)
            df_clean = df_clean[df_clean['ticker'].str.contains('\.', na=False)]
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for _, row in df_clean.iterrows():
                    sql = "INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE sector=%s"
                    cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['sector']))
                conn.commit()
                conn.close()
                st.success(f"✅ 成功清洗並存入 {len(df_clean)} 檔真正的飆股！")

# ----------------- 分頁 1：核心掃描 -----------------
with tab1:
    st.header("戰情即時掃描")
    if st.button("🚀 啟動全線掃描"):
        conn = get_db_connection()
        if conn:
            df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", conn)
            conn.close()
            
            if not df_stocks.empty:
                results = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total = len(df_stocks)
                for i, row in df_stocks.iterrows():
                    ticker = row['ticker']
                    status_text.text(f"🔎 正在爬取：{ticker}...")
                    
                    try:
                        # 真正的 yfinance 爬蟲在此！
                        data = yf.download(ticker, period="3mo", progress=False, timeout=10)
                        if data is None or data.empty:
                            res = "⏳ 查無資料"
                        else:
                            # 哲哲策略：股價 > 月線 且 RSI > 50
                            sma20 = ta.sma(data['Close'], length=20).iloc[-1]
                            rsi = ta.rsi(data['Close'], length=14).iloc[-1]
                            price = data['Close'].iloc[-1]
                            
                            if price > sma20 and rsi > 50:
                                res = "🔥 趨勢偏多"
                            else:
                                res = "⏳ 盤整觀察"
                    except:
                        res = "⚠️ 抓取超時"
                    
                    results.append(res)
                    progress_bar.progress((i + 1) / total)
                    time.sleep(0.1) # 保護 IP 避免被封鎖
                
                df_stocks['策略評等'] = results
                status_text.success("✨ 掃描完成！")
                st.dataframe(df_stocks, use_container_width=True)
                st.balloons()
            else:
                st.info("💡 資料庫目前空空如也，請先去管理頁面匯入。")

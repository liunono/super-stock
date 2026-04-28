import streamlit as st
import pandas as pd
import pymysql
import yfinance as yf
import pandas_ta as ta
import time
from datetime import datetime

# ================= 系統設定區 =================
# 哲哲提醒：這裡的變數請去 Streamlit Cloud 後台 Secrets 設定
try:
    DB_CONFIG = {
        'host': st.secrets["DB_HOST"],       # 外部連線請填 GoDaddy 實體 IP
        'user': st.secrets["DB_USER"],       # 記得加前綴，例如 zstt_admin
        'password': st.secrets["DB_PASS"],
        'db': st.secrets["DB_NAME"],         # 記得加前綴
        'port': 3306,
        'charset': 'utf8mb4',                # 🚀 關鍵！修正中文亂碼問題
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

def run_zhe_zhe_strategy(ticker):
    """
    哲哲核心策略：SMA20 + RSI
    """
    try:
        # yfinance 抓取最近三個月數據
        data = yf.download(ticker, period="3mo", interval="1d", progress=False, timeout=10)
        
        if data is None or data.empty:
            return "⏳ 交易所無回應"
        
        # 計算指標
        # 處理新版 yfinance 可能出現的 Multi-Index 問題
        close_prices = data['Close']
        if isinstance(close_prices, pd.DataFrame):
            close_prices = close_prices.iloc[:, 0]
            
        sma20 = ta.sma(close_prices, length=20)
        rsi = ta.rsi(close_prices, length=14)
        
        if sma20 is None or rsi is None: return "⏳ 指標不足"
        
        last_price = float(close_prices.iloc[-1])
        last_sma = float(sma20.iloc[-1])
        last_rsi = float(rsi.iloc[-1])
        
        # 哲哲強勢多頭邏輯
        if last_price > last_sma and last_rsi > 55:
            return "🔥 趨勢偏多"
        elif last_price < last_sma and last_rsi < 45:
            return "📉 弱勢整理"
        else:
            return "⏳ 盤整觀察"
    except Exception as e:
        return f"⚠️ 抓取失敗"

# ================= 儀表板介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V3.9", layout="wide")
st.title("📈 哲哲量化戰情室 V3.9 - 黃金終極版")
st.markdown("---")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 數據管理中心"])

# ----------------- 分頁 2：數據管理 -----------------
with tab2:
    st.header("數據清理與精密匯入")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("第一步：利空出盡")
        if st.button("🧨 一鍵清空目前亂碼垃圾", help="清空資料庫，準備重新認列獲利"):
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_pool")
                conn.commit()
                conn.close()
                st.warning("💥 警報：金庫已掃地出門！所有舊數據已銷毀。")

    with col2:
        st.subheader("第二步：精準建倉")
        # 提供正確編碼的範例
        example_df = pd.DataFrame({
            'ticker': ['2330.TW', '2317.TW', '2454.TW'], 
            'stock_name': ['台積電', '鴻海', '聯發科'], 
            'sector': ['半導體', '電子代工', 'IC設計']
        })
        st.download_button("📥 下載 UTF-8 範例 CSV", example_df.to_csv(index=False).encode('utf-8-sig'), "template.csv", "text/csv")
        
        uploaded_file = st.file_uploader("上傳正確的股票池 CSV", type="csv")
        if uploaded_file and st.button("💾 確認寫入資料庫"):
            # 🚀 關鍵：讀取時強制 utf-8-sig，防止中文變亂碼
            df_upload = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            
            # --- 哲哲強效過濾邏輯 ---
            df_upload['ticker'] = df_upload['ticker'].astype(str).str.strip().str.upper()
            
            # 1. 踢掉標題文字
            df_clean = df_upload[df_upload['ticker'] != 'TICKER']
            # 2. 強制檢查代號格式：必須有點(.)且長度>2 (過濾掉垃圾文字)
            df_clean = df_clean[df_clean['ticker'].str.contains('\.', na=False)]
            
            if not df_clean.empty:
                conn = get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    for _, row in df_clean.iterrows():
                        sql = "INSERT INTO stock_pool (ticker, stock_name, sector) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE stock_name=%s, sector=%s"
                        cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], row['stock_name'], row['sector']))
                    conn.commit()
                    conn.close()
                    st.success(f"✅ 成功清洗並存入 {len(df_clean)} 檔真正的飆股！中文字已完美顯示！")
            else:
                st.error("❌ CSV 檔案格式不對，找不到帶有點(.)的股票代號（例如 2330.TW）")

# ----------------- 分頁 1：核心掃描 -----------------
with tab1:
    st.header("戰情即時掃描")
    if st.button("🚀 啟動全線掃描"):
        conn = get_db_connection()
        if conn:
            # 讀取現有股票
            df_stocks = pd.read_sql("SELECT ticker, stock_name, sector FROM stock_pool", conn)
            conn.close()
            
            if not df_stocks.empty:
                # 再次雙重過濾，確保不掃描垃圾文字
                df_stocks = df_stocks[df_stocks['ticker'].str.contains('\.', na=False)]
                
                results = []
                # 進度條組件
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total = len(df_stocks)
                for i, row in df_stocks.iterrows():
                    ticker = row['ticker']
                    status_text.text(f"🔎 正在爬取：{ticker} ({row['stock_name']})...")
                    
                    # 執行掃描邏輯
                    res = run_zhe_zhe_strategy(ticker)
                    results.append(res)
                    
                    # 更新進度
                    progress_bar.progress((i + 1) / total)
                    time.sleep(0.1) # 保護 IP 避開交易所封鎖
                
                df_stocks['策略評等'] = results
                status_text.success(f"✨ 掃描完成！時間：{datetime.now().strftime('%H:%M:%S')}")
                
                # 美化顯示
                def color_rule(val):
                    color = 'red' if '🔥' in str(val) else ('green' if '📉' in str(val) else 'black')
                    return f'color: {color}'
                
                st.dataframe(df_stocks.style.applymap(color_rule, subset=['策略評等']), use_container_width=True)
                st.balloons()
            else:
                st.info("💡 目前金庫是空的。請先去『數據管理』頁面點清空後，再上傳 CSV。")

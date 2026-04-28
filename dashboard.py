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
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db_connection():
    try: return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"🚨 資料庫連線失敗：{e}")
        return None

# ================= 策略引擎 (強化抗封鎖版) =================
def run_strategy(ticker):
    try:
        # 加上 timeout 與 proxy 邏輯 (雖然 Streamlit 無法自訂 proxy，但我們增加延遲)
        data = yf.download(ticker, period="3mo", interval="1d", progress=False, timeout=20)
        
        if data is None or data.empty:
            return "⏳ 查無資料"
        
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
    except Exception as e:
        if "Too Many Requests" in str(e) or "Rate limited" in str(e):
            return "🚫 頻率受限 (休息中)"
        return "⚠️ 抓取失敗"

# ================= 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V7.0", layout="wide")
st.title("📈 哲哲量化戰情室 V7.0 - 抗封鎖終極修正版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 數據與體質管理"])

with tab2:
    st.header("數據清理與精密建倉")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧨 一鍵清空所有籌碼 (Reset)"):
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute("DELETE FROM stock_pool;")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
                conn.commit()
                conn.close()
                st.warning("💥 警報：資料庫已淨空！")

    with col2:
        uploaded_file = st.file_uploader("上傳正確的股票池 CSV", type="csv")
        if uploaded_file and st.button("💾 正式寫入"):
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
            
            # 🚀 修正 SyntaxWarning: 使用 r'\.'
            df_clean = df[df['ticker'].str.contains(r'\.', na=False)]
            df_clean = df_clean[df_clean['ticker'] != 'TICKER']
            
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                for _, row in df_clean.iterrows():
                    sql = """INSERT INTO stock_pool (ticker, stock_name, sector) 
                             VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE 
                             stock_name=%s, sector=%s"""
                    cursor.execute(sql, (row['ticker'], row['stock_name'], row['sector'], 
                                         row['stock_name'], row['sector']))
                conn.commit()
                conn.close()
                st.success(f"✅ 成功將 {len(df_clean)} 檔飆股入庫！")

with tab1:
    st.header("戰情即時掃描")
    st.warning("💡 哲哲提示：由於 Yahoo Finance 目前對 Cloud IP 限制嚴格，建議分批掃描或間隔 1 分鐘後再執行。")
    
    if st.button("🚀 啟動全線策略掃描"):
        conn = get_db_connection()
        if conn:
            # 修正 UserWarning，雖然 pymysql 直接用 pd.read_sql 會報警告，但仍可執行
            df_stocks = pd.read_sql("SELECT ticker, stock_name, sector FROM stock_pool", conn)
            conn.close()
            
            if not df_stocks.empty:
                # 🚀 修正 SyntaxWarning
                df_stocks = df_stocks[df_stocks['ticker'].str.contains(r'\.', na=False)]
                
                results = []
                prog = st.progress(0)
                stat = st.empty()
                
                total_stocks = len(df_stocks)
                for i, row in df_stocks.iterrows():
                    stat.text(f"🔎 正在偵測 ({i+1}/{total_stocks})：{row['ticker']} ({row['stock_name']})")
                    res = run_strategy(row['ticker'])
                    results.append(res)
                    prog.progress((i + 1) / total_stocks)
                    
                    # 🚀 抗封鎖戰術：增加延遲，讓 Yahoo 覺得我們是人類
                    if i % 10 == 0 and i > 0:
                        time.sleep(2) # 每 10 支多休息 2 秒
                    else:
                        time.sleep(0.5) # 每支基本休息 0.5 秒
                    
                    # 如果遇到封鎖就提早結束，避免被永封
                    if res == "🚫 頻率受限 (休息中)":
                        st.error("🚨 偵測到 Yahoo 頻率封鎖，暫停掃描，請 5 分鐘後再試。")
                        break
                
                # 補足長度 (如果中途跳出)
                if len(results) < len(df_stocks):
                    results += ["未完成"] * (len(df_stocks) - len(results))
                
                df_stocks['策略評等'] = results
                stat.success(f"✨ 掃描完畢！更新時間：{datetime.now().strftime('%H:%M:%S')}")
                
                # --- 🚀 核心修復：徹底改用 map 以符合 Pandas 3.12 規範 ---
                def color_val(val):
                    if '🔥' in str(val): return 'color: red; font-weight: bold'
                    if '📉' in str(val): return 'color: green'
                    return ''
                
                # 直接使用 map 並搭配 width='stretch' 修正警告
                try:
                    styled_df = df_stocks.style.map(color_val, subset=['策略評等'])
                    st.dataframe(styled_df, width='stretch')
                except Exception as e:
                    st.dataframe(df_stocks, width='stretch') # 萬一染色失敗，至少顯示原始表
                
                st.balloons()
            else:
                st.info("💡 資料庫目前是空的，請先匯入名單。")

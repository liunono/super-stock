import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import yfinance as yf
import pandas_ta as ta
import time
import random
import requests
import json
from datetime import datetime

# ================= 系統設定區 =================
try:
    # 建立資料庫引擎
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    
    # LINE 設定 (請確保 Secrets 裡有這兩個 Key)
    LINE_CHANNEL_ACCESS_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    YOUR_LINE_USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    # 瀏覽器偽裝 Session
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    })
except Exception as e:
    st.error(f"❌ 啟動失敗，請檢查 Secrets 設定：{e}")
    st.stop()

# ================= 輔助功能模組 =================
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

def run_strategy(ticker):
    """哲哲核心策略引擎"""
    tickers_to_try = [ticker]
    if ".TWO" in ticker: tickers_to_try.append(ticker.replace(".TWO", ".TW"))
    elif ".TW" in ticker: tickers_to_try.append(ticker.replace(".TW", ".TWO"))

    for current_ticker in tickers_to_try:
        try:
            time.sleep(random.uniform(1.2, 2.5)) # 隨機延遲避開封鎖
            data = yf.download(current_ticker, period="3mo", interval="1d", progress=False, timeout=20, session=session)
            
            if data is not None and not data.empty:
                close = data['Close']
                if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
                
                sma20 = ta.sma(close, length=20)
                rsi = ta.rsi(close, length=14)
                
                if sma20 is not None and not sma20.dropna().empty:
                    price = float(close.iloc[-1])
                    ma_val = float(sma20.iloc[-1])
                    rsi_val = float(rsi.iloc[-1])
                    
                    if price > ma_val and rsi_val > 55: return "🔥 趨勢偏多"
                    elif price < ma_val and rsi_val < 45: return "📉 弱勢整理"
                    else: return "⏳ 盤整觀察"
            break # 抓到資料就跳出嘗試迴圈
        except:
            continue
    return "❌ 查無資料"

# ================= 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V10.0", layout="wide")
st.title("📈 哲哲量化戰情室 V10.0 - 終極自動推播版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 數據與工具管理"])

with tab1:
    if st.button("🚀 啟動全線掃描並發送 LINE 推播"):
        df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        
        if not df_stocks.empty:
            results = []
            prog = st.progress(0)
            stat = st.empty()
            
            total = len(df_stocks)
            for i, row in df_stocks.iterrows():
                stat.text(f"🔎 正在偵測 ({i+1}/{total})：{row['ticker']} {row['stock_name']}...")
                res = run_strategy(row['ticker'])
                results.append(res)
                prog.progress((i + 1) / total)
            
            df_stocks['策略評等'] = results
            stat.success(f"✨ 掃描完成！時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # --- 🚀 哲哲推播邏輯：只抓出趨勢偏多的飆股 ---
            bullish_stocks = df_stocks[df_stocks['策略評等'] == "🔥 趨勢偏多"]
            
            # 建立推播訊息
            msg = f"🚀【哲哲量化戰情室】掃描報告 🚀\n"
            msg += f"📅 時間：{datetime.now().strftime('%m/%d %H:%M')}\n"
            msg += f"📊 掃描標的總數：{len(df_stocks)} 檔\n"
            msg += "----------------------------\n"
            
            if not bullish_stocks.empty:
                msg += f"🔥 偵測到 {len(bullish_stocks)} 檔強勢標的：\n"
                for _, row in bullish_stocks.iterrows():
                    msg += f"✅ {row['ticker']} {row['stock_name']}\n"
                msg += "\n📈 趨勢偏多，準備發車！"
            else:
                msg += "⏳ 目前盤勢保守，尚無強勢突破標的。"
            
            msg += "\n----------------------------\n不要說我沒提醒你，賺到流湯！"
            
            # 發送 LINE
            with st.spinner("正在將精選名單發送到你的 LINE..."):
                if send_line_msg(msg):
                    st.success("📲 強勢標的名單已成功推播至您的 LINE！")
                else:
                    st.error("❌ LINE 推播失敗，請檢查金鑰設定。")
            
            # 顯示表格
            def color_val(val):
                if '🔥' in str(val): return 'color: red; font-weight: bold'
                if '📉' in str(val): return 'color: green'
                return ''
                
            st.dataframe(df_stocks.style.map(color_val, subset=['策略評等']), width='stretch')
            st.balloons()
        else:
            st.info("💡 目前資料庫是空的。")

with tab2:
    st.header("系統連線測試")
    if st.button("🔔 發送一次純文字測試通知"):
        if send_line_msg("📣 哲哲測試：系統連線成功！"):
            st.success("測試訊息已發送！")
        else:
            st.error("發送失敗。")

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
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    
    LINE_CHANNEL_ACCESS_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    YOUR_LINE_USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    })
except Exception as e:
    st.error(f"❌ 啟動失敗：{e}")
    st.stop()

# ================= 功能模組 =================
def send_line_msg(msg):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    payload = {"to": YOUR_LINE_USER_ID, "messages": [{"type": "text", "text": msg}]}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload))
        return res.status_code == 200
    except:
        return False

def run_strategy(ticker, mode="strict"):
    """
    mode='strict': 確定要飛 (RSI > 55)
    mode='potential': 準備起飛 (RSI > 50)
    """
    tickers_to_try = [ticker]
    if ".TWO" in ticker: tickers_to_try.append(ticker.replace(".TWO", ".TW"))
    elif ".TW" in ticker: tickers_to_try.append(ticker.replace(".TW", ".TWO"))

    for current_ticker in tickers_to_try:
        try:
            time.sleep(random.uniform(1.2, 2.5))
            data = yf.download(current_ticker, period="3mo", interval="1d", progress=False, timeout=20, session=session)
            
            if data is not None and not data.empty:
                close = data['Close']
                if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
                
                sma20 = ta.sma(close, length=20)
                rsi = ta.rsi(close, length=14)
                
                if sma20 is not None and not sma20.dropna().empty:
                    p = float(close.iloc[-1])
                    ma = float(sma20.iloc[-1])
                    r = float(rsi.iloc[-1])
                    
                    # 邏輯區分
                    if p > ma:
                        if mode == "strict" and r > 55:
                            return "🔥 確定要飛"
                        elif mode == "potential" and r > 50:
                            return "✨ 準備起飛"
                        else:
                            return "⏳ 盤整觀察"
                    elif p < ma and r < 45:
                        return "📉 弱勢整理"
                    else:
                        return "⏳ 盤整觀察"
            break
        except: continue
    return "❌ 查無資料"

# ================= 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V11.0", layout="wide")
st.title("📈 哲哲量化戰情室 V11.0 - 雙引擎噴發版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 系統測試"])

with tab1:
    col_a, col_b = st.columns(2)
    
    # 定義通用的掃描邏輯
    def perform_scan(mode_name, mode_key, icon):
        df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if not df_stocks.empty:
            results = []
            prog = st.progress(0)
            stat = st.empty()
            total = len(df_stocks)
            for i, row in df_stocks.iterrows():
                stat.text(f"🔎 {mode_name}偵測中 ({i+1}/{total})：{row['ticker']}...")
                res = run_strategy(row['ticker'], mode=mode_key)
                results.append(res)
                prog.progress((i + 1) / total)
            
            df_stocks['策略評等'] = results
            stat.success(f"✨ {mode_name}掃描完成！")
            
            # 推播邏輯
            hit_stocks = df_stocks[df_stocks['策略評等'] == f"{icon} {mode_name}"]
            msg = f"{icon}【哲哲戰情室 - {mode_name}戰報】\n"
            msg += f"📅 時間：{datetime.now().strftime('%m/%d %H:%M')}\n"
            if not hit_stocks.empty:
                msg += f"🎯 偵測到 {len(hit_stocks)} 檔標的：\n"
                for _, row in hit_stocks.iterrows():
                    msg += f"✅ {row['ticker']} {row['stock_name']}\n"
                msg += "\n跟我預測的一模一樣，準備賺到流湯！"
            else:
                msg += "⏳ 暫無符合標的，耐心是獲利的關鍵！"
            
            send_line_msg(msg)
            st.success(f"📲 {mode_name}清單已推播！")
            
            def color_val(val):
                if '🔥' in str(val): return 'color: red; font-weight: bold'
                if '✨' in str(val): return 'color: orange; font-weight: bold'
                if '📉' in str(val): return 'color: green'
                return ''
            st.dataframe(df_stocks.style.map(color_val, subset=['策略評等']), width='stretch')
            st.balloons()
        else:
            st.info("資料庫是空的。")

    with col_a:
        st.subheader("🔥 主升段模式")
        st.write("標準：股價 > 月線 & RSI > 55")
        if st.button("🚀 啟動：確定要飛 (噴發版)"):
            perform_scan("確定要飛", "strict", "🔥")

    with col_b:
        st.subheader("✨ 潛力股模式")
        st.write("標準：股價 > 月線 & RSI > 50")
        if st.button("🚀 啟動：準備起飛 (轉強版)"):
            perform_scan("準備起飛", "potential", "✨")

with tab2:
    if st.button("🔔 LINE 連線測試"):
        if send_line_msg("📣 哲哲連線測試成功！賺到流湯！"):
            st.info("測試訊息已發送")

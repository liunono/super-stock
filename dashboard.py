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

def run_strategy_with_data(ticker, mode="strict"):
    """
    回傳：(評等, 現價, MA20, RSI)
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
                    p = round(float(close.iloc[-1]), 2)
                    ma = round(float(sma20.iloc[-1]), 2)
                    r = round(float(rsi.iloc[-1]), 2)
                    
                    # 判定邏輯
                    res = "⏳ 盤整觀察"
                    if p > ma:
                        if mode == "strict" and r > 55: res = "🔥 確定要飛"
                        elif mode == "potential" and r > 50: res = "✨ 準備起飛"
                    elif p < ma and r < 45:
                        res = "📉 弱勢整理"
                    
                    return res, p, ma, r
            break
        except: continue
    return "❌ 查無資料", 0, 0, 0

# ================= 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V12.0", layout="wide")
st.title("📈 哲哲量化戰情室 V12.0 - 數據透明版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 系統工具"])

with tab1:
    # 🚀 哲哲新增：數據透明勾選框
    show_details = st.checkbox("🔍 顯示即時數據計算資訊 (現價/月線/RSI)", value=True)
    
    col_a, col_b = st.columns(2)
    
    def perform_scan(mode_name, mode_key, icon):
        df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if not df_stocks.empty:
            results, prices, ma20s, rsis = [], [], [], []
            prog = st.progress(0)
            stat = st.empty()
            
            total = len(df_stocks)
            for i, row in df_stocks.iterrows():
                stat.text(f"🔎 {mode_name}偵測中 ({i+1}/{total})：{row['ticker']}...")
                res, p, ma, r = run_strategy_with_data(row['ticker'], mode=mode_key)
                results.append(res)
                prices.append(p)
                ma20s.append(ma)
                rsis.append(r)
                prog.progress((i + 1) / total)
            
            df_stocks['策略評等'] = results
            if show_details:
                df_stocks['現價'] = prices
                df_stocks['月線(MA20)'] = ma20s
                df_stocks['RSI(14)'] = rsis
            
            stat.success(f"✨ {mode_name}掃描完成！")
            
            # LINE 推播
            hit_stocks = df_stocks[df_stocks['策略評等'].str.contains(mode_name)]
            msg = f"{icon}【哲哲戰情室 - {mode_name}戰報】\n📅 {datetime.now().strftime('%m/%d %H:%M')}\n"
            if not hit_stocks.empty:
                msg += f"🎯 偵測到 {len(hit_stocks)} 檔標的：\n"
                for _, row in hit_stocks.iterrows():
                    detail = f"(RSI:{row['RSI(14)']})" if show_details else ""
                    msg += f"✅ {row['ticker']} {row['stock_name']} {detail}\n"
                msg += "\n跟我預測的一模一樣，準備賺到流湯！"
            else:
                msg += "⏳ 暫無符合標的，耐心是獲利的關鍵！"
            
            send_line_msg(msg)
            
            def color_val(val):
                if '🔥' in str(val): return 'background-color: #FFCCCC; color: red; font-weight: bold'
                if '✨' in str(val): return 'background-color: #FFF3CD; color: orange; font-weight: bold'
                return ''
            
            st.dataframe(df_stocks.style.applymap(color_val, subset=['策略評等']), width='stretch')
            st.balloons()
        else:
            st.info("資料庫目前是空的。")

    with col_a:
        if st.button("🔥 啟動：確定要飛 (RSI > 55)"):
            perform_scan("確定要飛", "strict", "🔥")

    with col_b:
        if st.button("✨ 啟動：準備起飛 (RSI > 50)"):
            perform_scan("準備起飛", "potential", "✨")

with tab2:
    st.write("系統連線正常。")

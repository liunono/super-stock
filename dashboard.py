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

# ================= 1. 系統設定區 =================
try:
    # 建立資料庫引擎
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    
    # LINE 設定
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    # 瀏覽器偽裝 Session
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    })
except Exception as e:
    st.error(f"❌ 系統啟動失敗，請檢查 Secrets：{e}")
    st.stop()

# ================= 2. 樣式工具模組 (放在頂層避免對齊錯誤) =================
def style_rows(row):
    """處理表格整行背景色"""
    if '🔥' in str(row['評等']): 
        return ['background-color: #FFCCCC'] * len(row)
    if '✨' in str(row['評等']): 
        return ['background-color: #FFF3CD'] * len(row)
    return [''] * len(row)

def color_text(val):
    """處理特定文字加粗"""
    if '🔥' in str(val) or '✨' in str(val): 
        return 'color: red; font-weight: bold'
    return ''

# ================= 3. 策略引擎 =================
def fetch_full_data(ticker, mode="strict"):
    """回傳：(評等, 現價, 漲跌幅, 月線, RSI, 成交量)"""
    tickers_to_try = [ticker]
    if ".TWO" in ticker: tickers_to_try.append(ticker.replace(".TWO", ".TW"))
    elif ".TW" in ticker: tickers_to_try.append(ticker.replace(".TW", ".TWO"))

    for current_ticker in tickers_to_try:
        try:
            time.sleep(random.uniform(1.2, 2.2))
            data = yf.download(current_ticker, period="3mo", interval="1d", progress=False, timeout=20, session=session)
            
            if data is not None and len(data) >= 2:
                close_series = data['Close']
                if isinstance(close_series, pd.DataFrame): close_series = close_series.iloc[:, 0]
                
                sma20 = ta.sma(close_series, length=20)
                rsi = ta.rsi(close_series, length=14)
                
                last_close = float(close_series.iloc[-1])
                prev_close = float(close_series.iloc[-2])
                change_pct = round(((last_close - prev_close) / prev_close) * 100, 2)
                
                vol_series = data['Volume']
                if isinstance(vol_series, pd.DataFrame): vol_series = vol_series.iloc[:, 0]
                last_vol = int(vol_series.iloc[-1])
                
                ma20_val = round(float(sma20.iloc[-1]), 2) if sma20 is not None else 0
                rsi_val = round(float(rsi.iloc[-1]), 2) if rsi is not None else 0
                
                res = "⏳ 盤整觀察"
                if last_close > ma20_val:
                    if mode == "strict" and rsi_val > 55: res = "🔥 確定要飛"
                    elif mode == "potential" and rsi_val > 50: res = "✨ 準備起飛"
                elif last_close < ma20_val and rsi_val < 45:
                    res = "📉 弱勢整理"
                
                return res, round(last_close, 2), change_pct, ma20_val, rsi_val, last_vol
            break
        except: continue
    return "❌ 查無資料", 0, 0, 0, 0, 0

# ================= 4. 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V14.0", layout="wide")
st.title("📈 哲哲量化戰情室 V14.0 - 終極全線噴發版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描", "🛠️ 系統工具"])

with tab1:
    st.info("💡 哲哲提示：數據全亮牌！現價、MA20、RSI 數值一覽無遺！")
    col_a, col_b = st.columns(2)
    
    def perform_full_scan(mode_name, mode_key, icon):
        df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if not df_stocks.empty:
            results, prices, changes, ma20s, rsis, volumes = [], [], [], [], [], []
            prog = st.progress(0)
            stat = st.empty()
            total = len(df_stocks)
            
            for i, row in df_stocks.iterrows():
                stat.text(f"🔎 {mode_name}偵測中 ({i+1}/{total})：{row['ticker']} {row['stock_name']}...")
                res, p, c, ma, r, v = fetch_full_data(row['ticker'], mode=mode_key)
                results.append(res)
                prices.append(p)
                changes.append(c)
                ma20s.append(ma)
                rsis.append(r)
                volumes.append(v)
                prog.progress((i + 1) / total)
            
            df_stocks['評等'] = results
            df_stocks['現價'] = prices
            df_stocks['漲跌幅(%)'] = changes
            df_stocks['月線(MA20)'] = ma20s
            df_stocks['RSI(14)'] = rsis
            df_stocks['今日成交量'] = volumes
            
            stat.success(f"✨ {mode_name}掃描完成！")
            
            # --- LINE 推播 ---
            hit_stocks = df_stocks[df_stocks['評等'].str.contains(mode_name)]
            msg = f"{icon}【哲哲戰情室 - {mode_name}戰報】\n📅 {datetime.now().strftime('%m/%d %H:%M')}\n"
            if not hit_stocks.empty:
                msg += f"🎯 偵測到 {len(hit_stocks)} 檔強勢標的：\n"
                for _, row in hit_stocks.iterrows():
                    msg += f"✅ {row['ticker']} {row['stock_name']} (RSI:{row['RSI(14)']})\n"
                msg += "\n跟我預測的一模一樣，準備賺到流湯！"
            else:
                msg += "⏳ 暫無符合標的，耐心是獲利的關鍵！"
            
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
            payload = {"to": USER_ID, "messages": [{"type": "text", "text": msg}]}
            requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))
            
            # --- 樣式渲染 ---
            styler = df_stocks.style.apply(style_rows, axis=1)
            if hasattr(styler, 'map'):
                styler = styler.map(color_text, subset=['評等'])
            else:
                styler = styler.applymap(color_text, subset=['評等'])
            
            st.dataframe(styler, width='stretch')
            st.balloons()
        else:
            st.info("資料庫是空的。")

    with col_a:
        if st.button("🔥 啟動：確定要飛 (RSI > 55)"):
            perform_full_scan("確定要飛", "strict", "🔥")

    with col_b:
        if st.button("✨ 啟動：準備起飛 (RSI > 50)"):
            perform_full_scan("準備起飛", "potential", "✨")

with tab2:
    if st.button("🔔 測試 LINE 通訊"):
        st.write("連線測試中...")

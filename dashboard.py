import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time
import random
import requests
import json
from datetime import datetime

# ================= 1. 系統地基 (Secrets 連線) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    })
except Exception as e:
    st.error(f"❌ 系統啟動失敗，請檢查 Secrets：{e}")
    st.stop()

# ================= 2. 樣式模組 =================
def style_final_df(df):
    """掃描完成後的最終染色處理"""
    def row_color(row):
        if '🔥' in str(row['評等']): return ['background-color: #FFCCCC'] * len(row)
        if '✨' in str(row['評等']): return ['background-color: #FFF3CD'] * len(row)
        return [''] * len(row)
    
    styler = df.style.apply(row_color, axis=1)
    # 文字加粗修正
    def bold_text(val):
        if '🔥' in str(val) or '✨' in str(val): return 'color: red; font-weight: bold'
        return ''
    
    if hasattr(styler, 'map'): styler = styler.map(bold_text, subset=['評等'])
    else: styler = styler.applymap(bold_text, subset=['評等'])
    return styler

# ================= 3. 策略引擎 (含自動校準機制) =================
def fetch_row_data(ticker, name, mode="strict"):
    # 自動嘗試上市(.TW)或上櫃(.TWO)
    tickers_to_try = [ticker]
    if ".TWO" in ticker: tickers_to_try.append(ticker.replace(".TWO", ".TW"))
    elif ".TW" in ticker: tickers_to_try.append(ticker.replace(".TW", ".TWO"))

    for current_ticker in tickers_to_try:
        try:
            time.sleep(random.uniform(1.0, 2.0)) # 抗封鎖
            data = yf.download(current_ticker, period="3mo", interval="1d", progress=False, timeout=15, session=session)
            if data is not None and len(data) >= 2:
                # 數據對齊
                close = data['Close'].iloc[:, 0] if isinstance(data['Close'], pd.DataFrame) else data['Close']
                sma20 = ta.sma(close, length=20)
                rsi = ta.rsi(close, length=14)
                
                last_p = round(float(close.iloc[-1]), 2)
                prev_p = float(close.iloc[-2])
                change = round(((last_p - prev_p) / prev_p) * 100, 2)
                vol = int(data['Volume'].iloc[-1])
                ma_val = round(float(sma20.iloc[-1]), 2) if sma20 is not None else 0
                r_val = round(float(rsi.iloc[-1]), 2) if rsi is not None else 0
                
                # 判定邏輯
                res = "⏳ 盤整觀察"
                if last_p > ma_val:
                    if mode == "strict" and r_val > 55: res = "🔥 確定要飛"
                    elif mode == "potential" and r_val > 50: res = "✨ 準備起飛"
                elif last_p < ma_val and r_val < 45: res = "📉 弱勢整理"
                
                return {"代號": ticker, "名稱": name, "評等": res, "現價": last_p, "漲跌(%)": change, "月線": ma_val, "RSI": r_val, "成交量": vol}
            break
        except: continue
    return {"代號": ticker, "名稱": name, "評等": "❌ 查無", "現價": 0, "漲跌(%)": 0, "月線": 0, "RSI": 0, "成交量": 0}

# ================= 4. 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V15.1", layout="wide")
st.title("📈 哲哲量化戰情室 V15.1 - 全能冠軍無懈可擊版")

tab1, tab2 = st.tabs(["🚀 核心策略掃描中心", "🛠️ 數據管理中心"])

# --- Tab 1: 策略掃描 ---
with tab1:
    st.info("💡 哲哲提示：本系統具備『流體動態亮牌』技術，數據將逐筆即時呈現！")
    c1, c2 = st.columns(2)
    
    def run_live_scan(m_name, m_key, icon):
        df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if df_stocks.empty:
            st.warning("資料庫是空的，請先去數據管理頁面補貨。")
            return

        prog = st.progress(0)
        stat = st.empty()
        live_table = st.empty() # 這裡就是你要的『即時運算顯示窗』
        all_results = []

        total = len(df_stocks)
        for i, row in df_stocks.iterrows():
            stat.text(f"🔎 {m_name}偵測中 ({i+1}/{total})：{row['ticker']} {row['stock_name']}...")
            
            # 即時抓取與計算
            result = fetch_row_data(row['ticker'], row['stock_name'], mode=m_key)
            all_results.append(result)
            
            # --- 🚀 關鍵：每抓一支，表格就跳出一行 ---
            live_table.dataframe(pd.DataFrame(all_results), width=1200)
            prog.progress((i + 1) / total)

        # 掃描完成後的最終美化
        stat.success(f"✨ {m_name}全線掃描完成！")
        final_df = pd.DataFrame(all_results)
        live_table.dataframe(style_final_df(final_df), width=1200)
        st.balloons()
        
        # LINE 彙整通知
        hits = final_df[final_df['評等'].str.contains(m_name)]
        msg = f"{icon}【哲哲戰報-{m_name}】\n📅 {datetime.now().strftime('%m/%d %H:%M')}\n"
        if not hits.empty:
            msg += f"🎯 偵測到 {len(hits)} 檔強勢標的：\n"
            for _, r in hits.iterrows():
                msg += f"✅ {r['代號']} {r['名稱']} (RSI:{r['RSI']})\n"
            msg += "\n跟我預測的一模一樣，準備賺到流湯！"
        else:
            msg += "⏳ 盤勢整理中，暫無符合標的。"
        
        requests.post("https://api.line.me/v2/bot/message/push", 
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"},
                      data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))

    with c1:
        if st.button("🔥 確定要飛 (RSI > 55)"): run_live_scan("確定要飛", "strict", "🔥")
    with c2:
        if st.button("✨ 準備起飛 (RSI > 50)"): run_live_scan("準備起飛", "potential", "✨")

# --- Tab 2: 數據管理 ---
with tab2:
    st.header("🛠️ 數據管理補給站")
    col_x, col_y = st.columns(2)
    
    with col_x:
        st.subheader("1. 利空出盡 (清場)")
        if st.button("🧨 一鍵清空所有股票", help="徹底清空資料庫"):
            with engine.connect() as conn:
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
                conn.execute(text("DELETE FROM stock_pool;"))
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                conn.commit()
            st.warning("💥 金庫已清空，準備重新布局！")

    with col_y:
        st.subheader("2. 財富補貨 (上傳)")
        up_file = st.file_uploader("上傳股票名單 CSV (ticker, stock_name, sector)", type="csv")
        if up_file and st.button("💾 正式匯入資料庫"):
            df_up = pd.read_csv(up_file, encoding='utf-8-sig')
            df_up['ticker'] = df_up['ticker'].astype(str).str.strip().str.upper()
            # 只要代號有點(.)的標的
            df_up = df_up[df_up['ticker'].str.contains(r'\.', na=False)]
            df_up.to_sql('stock_pool', con=engine, if_exists='append', index=False)
            st.success(f"✅ 成功匯入 {len(df_up)} 檔飆股！")

    st.markdown("---")
    if st.button("🔔 測試 LINE 通訊"):
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
        payload = {"to": USER_ID, "messages": [{"type": "text", "text": "📣 哲哲戰情室：連線測試成功！賺到流湯！"}]}
        requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))
        st.info("測試訊號已發出，請檢查手機！")

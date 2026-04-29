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
except Exception as e:
    st.error(f"❌ 系統啟動失敗：{e}")
    st.stop()

# ================= 2. 核心數據引擎 (一次抓取所有維度) =================
def fetch_master_data(ticker, name):
    tickers_to_try = [ticker]
    if ".TWO" in ticker: tickers_to_try.append(ticker.replace(".TWO", ".TW"))
    elif ".TW" in ticker: tickers_to_try.append(ticker.replace(".TW", ".TWO"))

    for current_ticker in tickers_to_try:
        try:
            time.sleep(random.uniform(1.2, 2.2))
            stock = yf.Ticker(current_ticker)
            data = stock.history(period="6mo", interval="1d", timeout=20)
            
            if data is not None and len(data) >= 20:
                close = data['Close']
                vol = data['Volume']
                
                # 計算全套指標 (為後續所有策略鋪路)
                sma5 = ta.sma(close, length=5)
                sma20 = ta.sma(close, length=20)
                sma60 = ta.sma(close, length=60)
                rsi = ta.rsi(close, length=14)
                bb = ta.bbands(close, length=20, std=2)
                avg_vol = ta.sma(vol, length=20)
                
                last_p = round(float(close.iloc[-1]), 2)
                prev_p = float(close.iloc[-2])
                
                return {
                    "代號": ticker, "名稱": name, "現價": last_p,
                    "漲跌(%)": round(((last_p - prev_p) / prev_p) * 100, 2),
                    "SMA5": round(float(sma5.iloc[-1]), 2) if sma5 is not None else 0,
                    "MA20": round(float(sma20.iloc[-1]), 2) if sma20 is not None else 0,
                    "MA60": round(float(sma60.iloc[-1]), 2) if sma60 is not None else 0,
                    "RSI": round(float(rsi.iloc[-1]), 2) if rsi is not None else 0,
                    "BBL": round(float(bb.iloc[-1, 0]), 2) if bb is not None else 0,
                    "BBU": round(float(bb.iloc[-1, 2]), 2) if bb is not None else 0,
                    "成交量": int(vol.iloc[-1]),
                    "均量": int(avg_vol.iloc[-1]) if avg_vol is not None else 0
                }
            break
        except: continue
    return None

# ================= 3. 策略判定邏輯 (按鈕呼叫用) =================
def apply_buy_strategy(df, strat_name):
    if strat_name == "🚀 黃金交叉":
        return df[df['MA20'] > df['MA60']]
    elif strat_name == "💥 量價突破":
        return df[(df['現價'] > df['MA20']) & (df['成交量'] > df['均量'] * 1.5)]
    elif strat_name == "🛡️ 低階抄底":
        return df[(df['RSI'] < 35) & (df['現價'] > df['SMA5'])]
    elif strat_name == "🌀 布林噴發":
        return df[df['現價'] > df['BBU']]
    elif strat_name == "🎯 強勢回測":
        return df[(df['現價'] > df['MA20']) & (abs(df['現價']-df['MA20'])/df['MA20'] < 0.02)]
    return df

def get_sell_advice(r):
    advices = []
    if r['RSI'] > 80: advices.append("🛑 RSI過熱")
    if r['現價'] < r['MA20']: advices.append("💀 跌破月線")
    if r['現價'] >= r['BBU']: advices.append("🔔 觸碰上軌")
    if r['現價'] < r['SMA5']: advices.append("📉 跌破五日線")
    if r['漲跌(%)'] < -3: advices.append("⚠️ 急殺轉弱")
    return ", ".join(advices) if advices else "💎 續抱"

# ================= 4. 介面設計 =================
st.set_page_config(page_title="哲哲量化戰情室 V18.5", layout="wide")
st.title("📈 哲哲量化戰情室 V18.5 - 整合管理版")

tab1, tab2, tab3 = st.tabs(["🚀 全能策略掃描", "💼 我的持倉獲利", "🛠️ 數據與股票池管理"])

# --- Tab 1: 掃描與策略切換 ---
with tab1:
    st.subheader("第一步：獲取即時大數據")
    if st.button("📦 開始全線掃描 (一次獲取所有指標)"):
        df_pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if not df_pool.empty:
            master_list = []
            prog = st.progress(0)
            live_t = st.empty()
            for i, row in df_pool.iterrows():
                res = fetch_master_data(row['ticker'], row['stock_name'])
                if res: master_list.append(res)
                live_t.dataframe(pd.DataFrame(master_list), width=1200)
                prog.progress((i + 1) / len(df_pool))
            st.session_state['master_df'] = pd.DataFrame(master_list)
            st.success("✨ 大數據獲取完畢！請由下方切換必勝策略：")
            st.balloons()

    if 'master_df' in st.session_state:
        st.divider()
        st.subheader("第二步：切換量化策略按鈕")
        strats = ["🚀 黃金交叉", "💥 量價突破", "🛡️ 低階抄底", "🌀 布林噴發", "🎯 強勢回測"]
        cols = st.columns(len(strats))
        for i, s in enumerate(strats):
            if cols[i].button(s):
                filtered = apply_buy_strategy(st.session_state['master_df'], s)
                st.write(f"🎯 符合【{s}】的標的：{len(filtered)} 檔")
                st.dataframe(filtered.style.background_gradient(subset=['RSI'], cmap='RdYlGn_r'), width=1200)

# --- Tab 2: 持倉與獲利 ---
with tab2:
    st.header("💼 私人資產監控")
    if 'master_df' in st.session_state:
        df_p = pd.read_sql("SELECT * FROM portfolio", con=engine)
        if not df_p.empty:
            m_df = st.session_state['master_df'][['代號', '現價', 'SMA5', 'MA20', 'BBU', 'RSI', '漲跌(%)']]
            merged = pd.merge(df_p, m_df, left_on='ticker', right_on='代號', how='left')
            
            # 計算獲利 (台灣股以張計，一單位=1000股)
            merged['獲利'] = (merged['現價'] - merged['entry_price']) * merged['qty'] * 1000
            merged['%'] = round(((merged['現價'] - merged['entry_price']) / merged['entry_price']) * 100, 2)
            merged['賣出攻略建議'] = merged.apply(get_sell_advice, axis=1)
            
            # 亮眼顯示
            st.metric("總預估獲利", f"${merged['獲利'].sum():,.0f}")
            st.dataframe(merged[['ticker', 'stock_name', 'entry_price', '現價', 'qty', '獲利', '%', '賣出攻略建議']].style.map(
                lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', 
                subset=['獲利', '%']
            ), width=1200)
        else:
            st.info("目前無持倉數據。請先至管理頁面匯入。")
    else:
        st.warning("請先在『全能策略掃描』完成數據獲取。")

# --- Tab 3: 管理頁 (股票池 + 持倉匯入) ---
with tab3:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📋 股票池管理 (掃描用)")
        up_pool = st.file_uploader("上傳股票池 CSV (ticker, stock_name, sector)", type="csv", key="pool")
        if up_pool and st.button("💾 匯入股票池"):
            df = pd.read_csv(up_pool, encoding='utf-8-sig')
            df.to_sql('stock_pool', con=engine, if_exists='append', index=False)
            st.success("股票池補貨成功！")
        if st.button("🧨 清空股票池"):
            with engine.connect() as conn:
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0; DELETE FROM stock_pool; SET FOREIGN_KEY_CHECKS = 1;"))
                conn.commit()
            st.warning("股票池已清空")

    with col2:
        st.subheader("💰 持倉管理 (獲利計算用)")
        up_port = st.file_uploader("上傳持倉 CSV (ticker, stock_name, entry_price, qty)", type="csv", key="port")
        if up_port and st.button("💾 匯入持倉"):
            df = pd.read_csv(up_port, encoding='utf-8-sig')
            df.to_sql('portfolio', con=engine, if_exists='append', index=False)
            st.success("持倉匯入成功！")
        if st.button("🧨 清空持倉"):
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM portfolio;"))
                conn.commit()
            st.warning("持倉數據已清空")

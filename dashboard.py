import streamlit as st
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import time, random, requests, json
from sqlalchemy import create_engine, text
from datetime import datetime

# ================= 1. 系統設定 =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
except Exception as e:
    st.error(f"❌ 初始化失敗：{e}")
    st.stop()

# ================= 2. 數據獲取引擎 (擴充數據指標) =================
def fetch_master_data(ticker):
    try:
        time.sleep(random.uniform(1.1, 2.0))
        stock = yf.Ticker(ticker)
        data = stock.history(period="6mo", interval="1d", timeout=20)
        
        if data is not None and len(data) >= 60:
            close = data['Close']
            vol = data['Volume']
            
            # --- 哲哲指標大補帖 ---
            sma5 = ta.sma(close, length=5)
            sma20 = ta.sma(close, length=20)
            sma60 = ta.sma(close, length=60)
            rsi = ta.rsi(close, length=14)
            bb = ta.bbands(close, length=20, std=2)
            avg_vol = ta.sma(vol, length=20) # 20日均量
            
            last_p = round(float(close.iloc[-1]), 2)
            prev_p = float(close.iloc[-2])
            
            return {
                "Ticker": ticker, "Price": last_p, 
                "Change": round(((last_p - prev_p) / prev_p) * 100, 2),
                "SMA5": round(float(sma5.iloc[-1]), 2),
                "MA20": round(float(sma20.iloc[-1]), 2),
                "MA60": round(float(sma60.iloc[-1]), 2),
                "RSI": round(float(rsi.iloc[-1]), 2),
                "BBL": round(float(bb.iloc[-1, 0]), 2),
                "BBU": round(float(bb.iloc[-1, 2]), 2),
                "Vol": int(vol.iloc[-1]),
                "AvgVol": int(avg_vol.iloc[-1]) if avg_vol is not None else 0
            }
    except: pass
    return None

# ================= 3. 五大必勝買入策略 (新註冊) =================
def strat_buy_gold_cross(r):
    # MA20 穿過 MA60 多頭排列
    return "🚀 黃金交叉" if r['Price'] > r['MA20'] > r['MA60'] else "⏳"

def strat_buy_vol_breakout(r):
    # 股價突破月線且爆量 (大於均量2倍)
    return "💥 量價突破" if r['Price'] > r['MA20'] and r['Vol'] > (r['AvgVol'] * 2) else "⏳"

def strat_buy_rsi_oversold(r):
    # RSI 低檔背離反彈
    return "🛡️ 超跌反彈" if r['RSI'] < 35 and r['Price'] > r['SMA5'] else "⏳"

def strat_buy_bb_squeeze(r):
    # 布林縮窄後向上突破
    return "🌀 布林噴發" if r['Price'] > r['BBU'] and r['Price'] > r['MA20'] else "⏳"

def strat_buy_backtest(r):
    # 強勢股回測月線不破
    return "🎯 強勢回測" if abs(r['Price'] - r['MA20'])/r['MA20'] < 0.02 and r['Price'] > r['MA20'] else "⏳"

# ================= 4. 五大專業賣出攻略 (新註冊) =================
def strat_sell_rsi_hot(r): return "🛑 RSI過熱" if r['RSI'] > 80 else "✅ 持有"
def strat_sell_dead_cross(r): return "💀 死亡交叉" if r['Price'] < r['MA20'] else "✅ 持有"
def strat_sell_bb_touch(r): return "🔔 觸碰上軌" if r['Price'] >= r['BBU'] else "✅ 持有"
def strat_sell_trailing(r): return "📉 破五日線" if r['Price'] < r['SMA5'] else "✅ 持有"
def strat_sell_weak(r): return "⚠️ 轉弱訊號" if r['Change'] < -3 and r['RSI'] < 50 else "✅ 持有"

BUY_STRATEGIES = {
    "🚀 黃金交叉版": strat_buy_gold_cross,
    "💥 量價突破版": strat_buy_vol_breakout,
    "🛡️ 低階抄底版": strat_buy_rsi_oversold,
    "🌀 布林擠壓版": strat_buy_bb_squeeze,
    "🎯 強勢回測版": strat_buy_backtest
}

SELL_STRATEGIES = {
    "🛑 RSI過熱賣出": strat_sell_rsi_hot,
    "💀 跌破月線賣出": strat_sell_dead_cross,
    "🔔 觸碰上軌賣出": strat_sell_bb_touch,
    "📉 破五日線快跑": strat_sell_trailing,
    "⚠️ 趨勢轉弱賣出": strat_sell_weak
}

# ================= 5. UI 邏輯 =================
st.set_page_config(page_title="哲哲戰情室 V18.0", layout="wide")
st.title("📈 哲哲量化戰情室 V18.0 - 買賣全能版")

tab1, tab2, tab3 = st.tabs(["🚀 買入策略掃描", "💼 我的持倉管理", "🛠️ 系統補給站"])

# --- Tab 1: 買入掃描 ---
with tab1:
    if st.button("📦 第一步：全線獲取數據"):
        df_stocks = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        if not df_stocks.empty:
            master_list = []
            prog = st.progress(0)
            for i, row in df_stocks.iterrows():
                data = fetch_master_data(row['ticker'])
                if data:
                    data['Name'] = row['stock_name']
                    master_list.append(data)
                prog.progress((i + 1) / len(df_stocks))
            st.session_state['master_df'] = pd.DataFrame(master_list)
            st.success("數據更新完成！")

    if 'master_df' in st.session_state:
        st.divider()
        cols = st.columns(len(BUY_STRATEGIES))
        for i, (name, func) in enumerate(BUY_STRATEGIES.items()):
            if cols[i].button(name):
                df = st.session_state['master_df'].copy()
                df['評等'] = df.apply(func, axis=1)
                hits = df[df['評等'] != "⏳"]
                st.write(f"🎯 符合 {name}: {len(hits)} 檔")
                st.dataframe(df[df['評等'] != "⏳"], width=1200)

# --- Tab 2: 持倉管理 (新功能！) ---
with tab2:
    st.header("💼 我的現有部位管理")
    
    # 匯入界面
    with st.expander("➕ 新增/匯入持倉"):
        up_portfolio = st.file_uploader("上傳持倉 CSV (ticker, stock_name, entry_price, qty)", type="csv")
        if up_portfolio and st.button("💾 確認匯入"):
            df_p = pd.read_csv(up_portfolio)
            df_p.to_sql('portfolio', con=engine, if_exists='append', index=False)
            st.success("持倉匯入成功！")

    # 獲利計算與賣出策略
    st.subheader("📊 部位即時監控與賣出建議")
    if 'master_df' in st.session_state:
        df_port = pd.read_sql("SELECT * FROM portfolio", con=engine)
        if not df_port.empty:
            # 合併最新價格
            m_df = st.session_state['master_df'][['Ticker', 'Price', 'SMA5', 'MA20', 'BBU', 'RSI', 'Change']]
            merged = pd.merge(df_port, m_df, left_on='ticker', right_on='Ticker', how='left')
            
            # 計算獲利
            merged['獲利($)'] = (merged['Price'] - merged['entry_price']) * merged['qty'] * 1000 # 假設單位是張
            merged['報酬率(%)'] = round(((merged['Price'] - merged['entry_price']) / merged['entry_price']) * 100, 2)
            
            # 應用賣出策略 (選一個你最想參考的，這裡示範全部運算後列出建議)
            def get_sell_advice(r):
                advices = []
                for s_name, s_func in SELL_STRATEGIES.items():
                    if s_func(r) != "✅ 持有": advices.append(s_name)
                return ", ".join(advices) if advices else "💎 續抱"

            merged['賣出建議'] = merged.apply(get_sell_advice, axis=1)
            
            # 樣式處理
            st.dataframe(merged[['ticker', 'stock_name', 'entry_price', 'Price', '報酬率(%)', '獲利($)', '賣出建議']].style.map(
                lambda x: 'color: red' if x > 0 else 'color: green', subset=['報酬率(%)']
            ))
            
            total_profit = merged['獲利($)'].sum()
            st.metric("總預估獲利", f"${total_profit:,.0f}", f"{total_profit/10000:.2f} 萬")
        else:
            st.info("目前尚無持倉數據。")
    else:
        st.warning("請先到『買入策略掃描』點擊獲取數據，才能計算持倉獲利！")

# --- Tab 3: 管理 ---
with tab3:
    if st.button("🧨 清空持倉"):
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM portfolio;"))
            conn.commit()
        st.success("持倉已歸零。")

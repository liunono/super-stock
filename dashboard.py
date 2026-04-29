import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time, random, requests, json
from datetime import datetime

# ================= 1. 系統地基 (自動檢查並補齊 SQL 欄位) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 股票池表
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50));"))
        # 持倉表
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        # 🚀 每日掃描金庫表 (新增所有策略指標欄位)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, PRIMARY KEY (ticker, scan_date)
            );
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗：{e}"); st.stop()

# ================= 2. 樣式美化模組 (回歸！) =================
def style_output(df):
    def row_style(row):
        if '評等' in row.index:
            if '🔥' in str(row['評等']): return ['background-color: #FFCCCC'] * len(row)
            if '✨' in str(row['評等']): return ['background-color: #FFF3CD'] * len(row)
        return [''] * len(row)
    
    styler = df.style.apply(row_style, axis=1)
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if x > 0 else 'color: green', subset=['報酬率(%)'])
    return styler

# ================= 3. 核心引擎 (一次抓取，全能計算) =================
def fetch_comprehensive_data(ticker, name):
    for cur_ticker in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            time.sleep(random.uniform(1.2, 2.0))
            data = yf.Ticker(cur_ticker).history(period="6mo", interval="1d", timeout=20)
            if data is not None and len(data) >= 20:
                close, vol = data['Close'], data['Volume']
                sma5, sma20, sma60 = ta.sma(close, 5), ta.sma(close, 20), ta.sma(close, 60)
                rsi, bb, avg_vol = ta.rsi(close, 14), ta.bbands(close, 20, 2), ta.sma(vol, 20)
                last_p = round(float(close.iloc[-1]), 2)
                return {
                    "ticker": ticker, "stock_name": name, "price": last_p,
                    "change_pct": round(((last_p - float(close.iloc[-2])) / float(close.iloc[-2])) * 100, 2),
                    "sma5": round(float(sma5.iloc[-1]), 2), "ma20": round(float(sma20.iloc[-1]), 2),
                    "ma60": round(float(sma60.iloc[-1]), 2), "rsi": round(float(rsi.iloc[-1]), 2),
                    "bbl": round(float(bb.iloc[-1, 0]), 2), "bbu": round(float(bb.iloc[-1, 2]), 2),
                    "vol": int(vol.iloc[-1]), "avg_vol": int(avg_vol.iloc[-1]) if avg_vol is not None else 0,
                    "scan_date": datetime.now().date()
                }
            break
        except: continue
    return None

# ================= 4. 介面設計 =================
st.set_page_config(page_title="哲哲戰情室 V20.0", layout="wide")
st.title("📈 哲哲量化戰情室 V20.0 - 終極大數據金庫版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 持倉獲利監控", "🛠️ 系統後台管理"])

with tab1:
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📡 讀取金庫數據 (今日秒開)", use_container_width=True):
            today = datetime.now().date()
            df_db = pd.read_sql(f"SELECT * FROM daily_scans WHERE scan_date = '{today}'", con=engine)
            if not df_db.empty:
                st.session_state['master_df'] = df_db
                st.success(f"✅ 已從 SQL 金庫提取 {len(df_db)} 檔數據！")
            else: st.warning("今日尚無快取，請先執行全線掃描。")
    
    with col_b:
        if st.button("📦 執行全線掃描 (數據入庫)", use_container_width=True):
            df_pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not df_pool.empty:
                master_list, prog, live_t = [], st.progress(0), st.empty()
                for i, row in df_pool.iterrows():
                    res = fetch_comprehensive_data(row['ticker'], row['stock_name'])
                    if res: master_list.append(res)
                    live_t.dataframe(pd.DataFrame(master_list), width=1200)
                    prog.progress((i + 1) / len(df_pool))
                
                m_df = pd.DataFrame(master_list)
                st.session_state['master_df'] = m_df
                # 🚀 寫入 SQL 金庫 (REPLACE 模式)
                m_df.to_sql('daily_scans', con=engine, if_exists='append', index=False, method='multi')
                st.success("✨ 數據已全數入庫！")
                st.balloons()

    if 'master_df' in st.session_state:
        st.divider()
        st.subheader("🛠️ 執行量化策略切換")
        btn_cols = st.columns(5)
        m_df = st.session_state['master_df']
        
        # 5 大買入策略
        if btn_cols[0].button("🚀 黃金交叉"):
            f = m_df[m_df['ma20'] > m_df['ma60']]; st.write(f"🎯 符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[1].button("💥 量價突破"):
            f = m_df[(m_df['price'] > m_df['ma20']) & (m_df['vol'] > m_df['avg_vol'] * 2)]; st.write(f"🎯 符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[2].button("🛡️ 低階抄底"):
            f = m_df[(m_df['rsi'] < 35) & (m_df['price'] > m_df['sma5'])]; st.write(f"🎯 符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[3].button("🌀 布林噴發"):
            f = m_df[m_df['price'] > m_df['bbu']]; st.write(f"🎯 符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[4].button("🎯 強勢回測"):
            f = m_df[(m_df['price'] > m_df['ma20']) & (abs(m_df['price']-m_df['ma20'])/m_df['ma20'] < 0.02)]; st.write(f"🎯 符合: {len(f)} 檔"); st.dataframe(style_output(f))

with tab2:
    st.header("💼 持倉部位與賣出建議")
    if 'master_df' in st.session_state:
        df_p = pd.read_sql("SELECT * FROM portfolio", con=engine)
        if not df_p.empty:
            m_df = st.session_state['master_df']
            merged = pd.merge(df_p, m_df, on='ticker', how='left')
            merged['獲利'] = (merged['price'] - merged['entry_price']) * merged['qty'] * 1000
            merged['報酬率(%)'] = round(((merged['price'] - merged['entry_price']) / merged['entry_price']) * 100, 2)
            
            # 5 大賣出攻略邏輯
            def get_sell_advice(r):
                adv = []
                if r['rsi'] > 80: adv.append("🛑 RSI過熱")
                if r['price'] < r['ma20']: adv.append("💀 跌破月線")
                if r['price'] >= r['bbu']: adv.append("🔔 觸碰上軌")
                if r['price'] < r['sma5']: adv.append("📉 跌破五日線")
                if r['change_pct'] < -3: adv.append("⚠️ 趨勢轉弱")
                return ", ".join(adv) if adv else "💎 續抱"
            
            merged['賣出建議'] = merged.apply(get_sell_advice, axis=1)
            st.metric("當前預估總獲利", f"${merged['獲利'].sum():,.0f}")
            st.dataframe(style_output(merged[['ticker','stock_name_x','entry_price','price','qty','獲利','報酬率(%)','賣出建議']]))
    else: st.warning("請先完成掃描或讀取金庫。")

with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📋 股票池 (掃描目標)")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv", key="up_pool")
        if f_pool and st.button("💾 匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("完成！")
        if st.button("🧨 清空股票池"):
            with engine.connect() as conn: conn.execute(text("DELETE FROM stock_pool;")); conn.commit(); st.warning("已清空")
    with c2:
        st.subheader("💰 持倉部位 (計算獲利)")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv", key="up_port")
        if f_port and st.button("💾 匯入持倉"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("完成！")
        if st.button("🧨 清空持倉"):
            with engine.connect() as conn: conn.execute(text("DELETE FROM portfolio;")); conn.commit(); st.warning("已清空")

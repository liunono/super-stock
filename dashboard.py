import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time, random, requests, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (自動創表與 Secrets) =================
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
        # 每日掃描快取金庫 (含所有技術指標)
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

# ================= 2. 樣式美化模組 (紅綠燈渲染) =================
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

# ================= 3. 核心引擎 (並行抓取，解開速限) =================
def fetch_comprehensive_data(ticker, name):
    for cur_ticker in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            # 🚀 渦輪版：移除長延遲，僅留極小緩衝
            stock = yf.Ticker(cur_ticker)
            data = stock.history(period="6mo", interval="1d", timeout=15)
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
st.set_page_config(page_title="哲哲戰情室 V21.1", layout="wide")
st.title("📈 哲哲量化戰情室 V21.1 - 渦輪坦克完全體")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 持倉獲利監控", "🛠️ 系統後臺管理"])

# --- Tab 1: 掃描與 5 大買入策略 ---
with tab1:
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📡 讀取今日金庫數據 (秒開)", use_container_width=True):
            today = datetime.now().date()
            df_db = pd.read_sql(f"SELECT * FROM daily_scans WHERE scan_date = '{today}'", con=engine)
            if not df_db.empty:
                df_db.columns = ['代號','名稱','現價','漲跌(%)','SMA5','MA20','MA60','RSI','BBL','BBU','成交量','均量','日期']
                st.session_state['master_df'] = df_db
                st.success(f"✅ 金庫亮牌成功！讀取到 {len(df_db)} 檔。")
            else: st.warning("今日尚無快取數據，請先執行掃描。")
    
    with col_b:
        if st.button("⚡ 啟動渦輪掃描 (10核心並行)", use_container_width=True):
            df_pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not df_pool.empty:
                master_list, prog, live_t = [], st.progress(0), st.empty()
                total = len(df_pool)
                # 🚀 啟動並行運算
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {executor.submit(fetch_comprehensive_data, r['ticker'], r['stock_name']): i for i, r in df_pool.iterrows()}
                    for count, future in enumerate(as_completed(futures)):
                        res = future.result()
                        if res: 
                            master_list.append(res)
                            live_t.dataframe(pd.DataFrame(master_list), width=1200)
                        prog.progress((count + 1) / total)
                
                m_df = pd.DataFrame(master_list)
                # 寫入金庫 (先刪後補)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False, method='multi')
                
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','sma5':'SMA5','ma20':'MA20','ma60':'MA60','rsi':'RSI','bbl':'BBL','bbu':'BBU','vol':'成交量','avg_vol':'均量','scan_date':'日期'})
                st.success("✨ 數據已全數入庫！")
                st.balloons()

    if 'master_df' in st.session_state:
        st.divider()
        st.subheader("🛠️ 切換 5 大必勝買入策略")
        m_df = st.session_state['master_df']
        btn_cols = st.columns(5)
        
        if btn_cols[0].button("🚀 黃金交叉"):
            f = m_df[m_df['MA20'] > m_df['MA60']]; st.write(f"符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[1].button("💥 量價突破"):
            f = m_df[(m_df['現價'] > m_df['MA20']) & (m_df['成交量'] > m_df['均量'] * 2)]; st.write(f"符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[2].button("🛡️ 低階抄底"):
            f = m_df[(m_df['RSI'] < 35) & (m_df['現價'] > m_df['SMA5'])]; st.write(f"符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[3].button("🌀 布林噴發"):
            f = m_df[m_df['現價'] > m_df['BBU']]; st.write(f"符合: {len(f)} 檔"); st.dataframe(style_output(f))
        if btn_cols[4].button("🎯 強勢回測"):
            f = m_df[(m_df['現價'] > m_df['MA20']) & (abs(m_df['現價']-m_df['MA20'])/m_df['MA20'] < 0.02)]; st.write(f"符合: {len(f)} 檔"); st.dataframe(style_output(f))

# --- Tab 2: 持倉與 5 大賣出攻略 ---
with tab2:
    st.header("💼 私人持倉監控與賣出建議")
    if 'master_df' in st.session_state:
        df_p = pd.read_sql("SELECT * FROM portfolio", con=engine)
        if not df_p.empty:
            m_df = st.session_state['master_df']
            merged = pd.merge(df_p, m_df, left_on='ticker', right_on='代號', how='left')
            merged['獲利'] = (merged['現價'] - merged['entry_price']) * merged['qty'] * 1000
            merged['報酬率(%)'] = round(((merged['現價'] - merged['entry_price']) / merged['entry_price']) * 100, 2)
            
            def get_sell_advice(r):
                adv = []
                if r['RSI'] > 80: adv.append("🛑 RSI過熱")
                if r['現價'] < r['MA20']: adv.append("💀 跌破月線")
                if r['現價'] >= r['BBU']: adv.append("🔔 觸碰上軌")
                if r['現價'] < r['SMA5']: adv.append("📉 跌破五日線")
                if r['漲跌(%)'] < -3: adv.append("⚠️ 趨勢轉弱")
                return ", ".join(adv) if adv else "💎 續抱"
            
            merged['賣出建議'] = merged.apply(get_sell_advice, axis=1)
            st.metric("當前預估總獲利", f"${merged['獲利'].sum():,.0f}")
            st.dataframe(style_output(merged[['ticker','stock_name','entry_price','現價','qty','獲利','報酬率(%)','賣出建議']]), width=1200)
        else: st.info("目前無持倉。")
    else: st.warning("請先完成掃描或讀取金庫。")

# --- Tab 3: 管理 (股票池 + 持倉匯入) ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📋 股票池管理")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv", key="pool")
        if f_pool and st.button("💾 匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("完成！")
        if st.button("🧨 清空股票池"):
            with engine.connect() as conn: conn.execute(text("DELETE FROM stock_pool;")); conn.commit(); st.warning("已清空")
    with c2:
        st.subheader("💰 持倉部位管理")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv", key="port")
        if f_port and st.button("💾 匯入持倉"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("完成！")
        if st.button("🧨 清空持倉"):
            with engine.connect() as conn: conn.execute(text("DELETE FROM portfolio;")); conn.commit(); st.warning("已清空")

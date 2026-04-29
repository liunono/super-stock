import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time, random, requests, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (自動修復資料庫) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 確保所有表格結構完整
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50));"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, PRIMARY KEY (ticker, scan_date)
            );
        """))
        # 🔥 哲哲都更邏輯：自動補齊 kd20, kd60 欄位
        existing_cols = [row[0] for row in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        if 'kd20' not in existing_cols:
            conn.execute(text("ALTER TABLE daily_scans ADD COLUMN kd20 FLOAT;"))
        if 'kd60' not in existing_cols:
            conn.execute(text("ALTER TABLE daily_scans ADD COLUMN kd60 FLOAT;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗：{e}"); st.stop()

# ================= 2. 哲哲美學：LINE 發送與完整樣式 =================
def send_line_report(title, df, icon):
    """將策略結果精美地噴向 LINE"""
    if df.empty:
        msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n⏳ 目前暫無符合標的，耐心是獲利的關鍵！"
    else:
        msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 偵測到 {len(df)} 檔潛力股：\n"
        for _, r in df.iterrows():
            msg += f"✅ {r['代號']} {r['名稱']} | RSI:{r['RSI']} | 現價:{r['現價']}\n"
        msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": USER_ID, "messages": [{"type": "text", "text": msg}]}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(payload))

def style_df(df):
    """美化 DataFrame 顯示 - 完整顏色版"""
    def color_rsi(val):
        if val >= 70: color = '#FFCCCC' # 過熱紅
        elif val >= 55: color = '#FFE5E5' # 偏高粉
        elif val <= 30: color = '#CCFFCC' # 超跌綠
        else: color = 'transparent'
        return f'background-color: {color}'

    # 處理數字格式
    format_dict = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}'}
    # 僅針對存在的欄位進行格式化
    styler = df.style.format({k: v for k, v in format_dict.items() if k in df.columns})
    
    # 漲跌與報酬率顏色
    if '漲跌(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['漲跌(%)'])
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['報酬率(%)'])
    
    if 'RSI' in df.columns:
        styler = styler.map(color_rsi, subset=['RSI'])
    
    return styler

# ================= 3. 核心抓取引擎 (10核心並行) =================
def fetch_data(ticker, name):
    for cur_ticker in [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]:
        try:
            stock = yf.Ticker(cur_ticker)
            data = stock.history(period="6mo", interval="1d", timeout=15)
            if data is not None and len(data) >= 60:
                close, vol = data['Close'], data['Volume']
                sma5, sma20, sma60 = ta.sma(close, 5), ta.sma(close, 20), ta.sma(close, 60)
                rsi, bb, avg_vol = ta.rsi(close, 14), ta.bbands(close, 20, 2), ta.sma(vol, 20)
                return {
                    "ticker": ticker, "stock_name": name, "price": round(float(close.iloc[-1]), 2),
                    "change_pct": round(((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]) * 100, 2),
                    "sma5": round(float(sma5.iloc[-1]), 2), "ma20": round(float(sma20.iloc[-1]), 2),
                    "ma60": round(float(sma60.iloc[-1]), 2), "rsi": round(float(rsi.iloc[-1]), 2),
                    "bbl": round(float(bb.iloc[-1, 0]), 2), "bbu": round(float(bb.iloc[-1, 2]), 2),
                    "vol": int(vol.iloc[-1]), "avg_vol": int(avg_vol.iloc[-1]) if avg_vol is not None else 0,
                    "kd20": round(float(close.iloc[-20]), 2), "kd60": round(float(close.iloc[-60]), 2),
                    "scan_date": datetime.now().date()
                }
            break
        except: continue
    return None

# ================= 4. 介面設計 =================
st.set_page_config(page_title="哲哲戰情室 V22.2", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V22.2 - 大滿貫提款機版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 持倉獲利監控", "🛠️ 後台管理"])

with tab1:
    c_btn1, c_btn2 = st.columns(2)
    with c_btn1:
        if st.button("📡 讀取今日金庫 (秒開)", use_container_width=True):
            df_db = pd.read_sql(f"SELECT * FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not df_db.empty:
                rename_map = {'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','sma5':'SMA5','ma20':'MA20','ma60':'MA60','rsi':'RSI','bbl':'BBL','bbu':'BBU','vol':'成交量','avg_vol':'均量','kd20':'KD20','kd60':'KD60','scan_date':'日期'}
                st.session_state['master_df'] = df_db.rename(columns=rename_map)
                st.success("✅ 金庫數據載入成功！")
            else: st.warning("今日尚無快取。")
    
    with c_btn2:
        if st.button("⚡ 啟動並行掃描 (渦輪加速)", use_container_width=True):
            df_pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not df_pool.empty:
                master_list, prog, live_t = [], st.progress(0), st.empty()
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in df_pool.iterrows()}
                    for count, future in enumerate(as_completed(futures)):
                        res = future.result()
                        if res: master_list.append(res); live_t.dataframe(pd.DataFrame(master_list), width=1200)
                        prog.progress((count + 1) / len(df_pool))
                
                m_df = pd.DataFrame(master_list)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False, method='multi')
                st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','sma5':'SMA5','ma20':'MA20','ma60':'MA60','rsi':'RSI','bbl':'BBL','bbu':'BBU','vol':'成交量','avg_vol':'均量','kd20':'KD20','kd60':'KD60','scan_date':'日期'})
                st.success("✨ 掃描完成！")

    if 'master_df' in st.session_state:
        st.divider()
        m_df = st.session_state['master_df']
        st.markdown("### 🛠️ 策略決策中心 (包含江江老師扣三低)")
        btn_cols = st.columns(6)
        
        strats = [
            ("黃金交叉", "🚀", m_df['MA20'] > m_df['MA60']),
            ("量價突破", "💥", (m_df['現價'] > m_df['MA20']) & (m_df['成交量'] > m_df['均量'] * 2)),
            ("低階抄底", "🛡️", (m_df['RSI'] < 35) & (m_df['現價'] > m_df['SMA5'])),
            ("布林噴發", "🌀", m_df['現價'] > m_df['BBU']),
            ("強勢回測", "🎯", (m_df['現價'] > m_df['MA20']) & (abs(m_df['現價']-m_df['MA20'])/m_df['MA20'] < 0.02))
        ]
        if 'KD20' in m_df.columns:
            strats.append(("扣三低提款機", "💰", (m_df['現價'] > m_df['KD20']) & (m_df['現價'] > m_df['KD60']) & (m_df['MA20'] > m_df['MA60'])))

        for i, (name, icon, mask) in enumerate(strats):
            if btn_cols[i].button(f"{icon} {name}", use_container_width=True):
                res_df = m_df[mask].sort_values(by='RSI', ascending=False)
                m1, m2, m3 = st.columns(3)
                m1.metric("符合檔數", f"{len(res_df)} 檔")
                if not res_df.empty:
                    m2.metric("最強標的", res_df.iloc[0]['名稱'], f"RSI: {res_df.iloc[0]['RSI']}")
                    m3.metric("平均漲跌", f"{res_df['漲跌(%)'].mean():.2f}%")
                st.dataframe(style_df(res_df), width=1200)
                send_line_report(name, res_df, icon)
                st.toast(f"戰報已噴發！")

# --- Tab 2: 持倉與獲利 (完整回歸) ---
with tab2:
    st.header("💼 我的資產亮牌區")
    if 'master_df' in st.session_state:
        df_p = pd.read_sql("SELECT * FROM portfolio", con=engine)
        if not df_p.empty:
            merged = pd.merge(df_p, st.session_state['master_df'], left_on='ticker', right_on='代號', how='left')
            merged['獲利'] = (merged['現價'] - merged['entry_price']) * merged['qty'] * 1000
            merged['報酬率(%)'] = round(((merged['現價'] - merged['entry_price']) / merged['entry_price']) * 100, 2)
            t_profit = merged['獲利'].sum()
            st.metric("當前預估總獲利", f"${t_profit:,.0f}", f"{'🔥' if t_profit > 0 else '📉'}")
            st.dataframe(style_df(merged[['ticker','stock_name','entry_price','現價','qty','獲利','報酬率(%)']]), width=1200)
        else: st.info("目前尚無持倉數據。")
    else: st.warning("請先讀取今日數據。")

# --- Tab 3: 後台管理 (完整回歸) ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📋 股票池管理")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv")
        if f_pool and st.button("💾 匯入股票池"):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功匯入股票池")
    with c2:
        st.subheader("💰 持倉管理")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv")
        if f_port and st.button("💾 存入持倉"):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("成功存入持倉")

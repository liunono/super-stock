import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import time, random, requests, json
import numpy as np
from PIL import Image
import easyocr
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (自動修復與都更) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}"
    engine = create_engine(DB_URL)
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        # 確保所有核心表格存在
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50));"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, PRIMARY KEY (ticker, scan_date)
            );
        """))
        # 🔥 欄位自動都更 (kd20, kd60 扣抵值計算用)
        existing_cols = [row[0] for row in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        if 'kd20' not in existing_cols: conn.execute(text("ALTER TABLE daily_scans ADD COLUMN kd20 FLOAT;"))
        if 'kd60' not in existing_cols: conn.execute(text("ALTER TABLE daily_scans ADD COLUMN kd60 FLOAT;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗：{e}"); st.stop()

# ================= 2. 哲哲美學：視覺與 LINE 渲染 =================
@st.cache_resource
def get_ocr_reader():
    """初始化 EasyOCR，全台最強 AI 辨識"""
    return easyocr.Reader(['ch_tra', 'en'])

def send_line_report(title, df, icon):
    """將冠軍策略結果噴向 LINE"""
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
    """終極視覺美化版"""
    def color_rsi(val):
        if val >= 70: return 'background-color: #FFCCCC' # 過熱
        if val <= 30: return 'background-color: #CCFFCC' # 超跌
        return ''
    
    format_dict = {
        '現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 
        '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}',
        'SMA5': '{:.2f}', 'MA20': '{:.2f}', '量比': '{:.2f}'
    }
    styler = df.style.format({k: v for k, v in format_dict.items() if k in df.columns})
    if '漲跌(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['漲跌(%)'])
    if '報酬率(%)' in df.columns:
        styler = styler.map(lambda x: 'color: red; font-weight: bold' if isinstance(x, (int, float)) and x > 0 else 'color: green', subset=['報酬率(%)'])
    if 'RSI' in df.columns:
        styler = styler.map(color_rsi, subset=['RSI'])
    return styler

# ================= 3. 核心引擎 (抓取與 AI 辨識) =================
def fetch_data(ticker, name):
    """全自動數據抓取引擎"""
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

def process_portfolio_images(uploaded_files):
    """AI 視覺自動辨識，解決 DatabaseError 的終極利器"""
    reader = get_ocr_reader()
    extracted_data = []
    try:
        pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
        name_map = dict(zip(pool['stock_name'], pool['ticker']))
    except:
        name_map = {"全新": "2455.TW", "牧德": "3563.TW", "辛耘": "3583.TW", "聯亞": "3081.TWO", "上詮": "3363.TWO", "昇達科": "3491.TWO", "華星光": "4979.TWO"}

    for uploaded_file in uploaded_files:
        image = Image.open(uploaded_file)
        result = reader.readtext(np.array(image))
        all_text = [res[1] for res in result]
        for i, text in enumerate(all_text):
            clean_name = text.strip()
            if clean_name in name_map:
                try:
                    found_vals = []
                    for offset in range(1, 8):
                        val_str = all_text[i+offset].replace(',', '').replace(' ', '')
                        try: found_vals.append(float(val_str))
                        except: continue
                    if len(found_vals) >= 2:
                        extracted_data.append({
                            "ticker": name_map[clean_name],
                            "stock_name": clean_name,
                            "entry_price": float(found_vals[1]),
                            "qty": float(found_vals[2])
                        })
                except: continue
    return pd.DataFrame(extracted_data)

# ================= 4. 主介面設計 =================
st.set_page_config(page_title="哲哲戰情室 V24.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V24.0 - 全台最強完全體")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 持倉獲利監控", "🛠️ 後台管理"])

# --- Tab 1: 策略掃描 ---
with tab1:
    c_btn1, c_btn2 = st.columns(2)
    with c_btn1:
        if st.button("📡 讀取今日金庫數據", use_container_width=True):
            df_db = pd.read_sql(f"SELECT * FROM daily_scans WHERE scan_date = '{datetime.now().date()}'", con=engine)
            if not df_db.empty:
                rename_map = {'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','sma5':'SMA5','ma20':'MA20','ma60':'MA60','rsi':'RSI','bbl':'BBL','bbu':'BBU','vol':'成交量','avg_vol':'均量','kd20':'KD20','kd60':'KD60','scan_date':'日期'}
                st.session_state['master_df'] = df_db.rename(columns=rename_map)
                st.success("✅ 金庫載入成功！跟我預測的一模一樣！")
    
    with c_btn2:
        if st.button("⚡ 啟動並行掃描 (全速運轉)", use_container_width=True):
            df_pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not df_pool.empty:
                master_list, prog, live_t = [], st.progress(0), st.empty()
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_data, r['ticker'], r['stock_name']): i for i, r in df_pool.iterrows()}
                    for count, future in enumerate(as_completed(futures)):
                        res = future.result()
                        if res: master_list.append(res); live_t.dataframe(pd.DataFrame(master_list), width=1200)
                        prog.progress((count + 1) / len(df_pool))
                
                m_df_res = pd.DataFrame(master_list)
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df_res.to_sql('daily_scans', con=conn, if_exists='append', index=False, method='multi')
                st.session_state['master_df'] = m_df_res.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','sma5':'SMA5','ma20':'MA20','ma60':'MA60','rsi':'RSI','bbl':'BBL','bbu':'BBU','vol':'成交量','avg_vol':'均量','kd20':'KD20','kd60':'KD60','scan_date':'日期'})
                st.success("✨ 掃描完成！準備大賺特賺！")

    if 'master_df' in st.session_state:
        st.divider()
        m_df = st.session_state['master_df'].copy()
        m_df['量比'] = m_df['成交量'] / m_df['均量']
        
        st.markdown("### 🛠️ 策略決策中心 (九成勝率濾網已實裝)")
        btn_cols = st.columns(6)
        
        # 👑 哲哲九成勝率邏輯
        win_90_mask = (
            (m_df['現價'] > m_df['KD20']) & (m_df['現價'] > m_df['KD60']) & # 扣三低
            (m_df['量比'] >= 1.5) &
            (m_df['現價'] > m_df['SMA5']) & (m_df['SMA5'] > m_df['MA20']) & # 多頭排列
            (m_df['RSI'] >= 50) & (m_df['RSI'] <= 75)
        )

        strats = [
            ("九成勝率提款機", "👑", win_90_mask),
            ("量價突破", "💥", (m_df['現價'] > m_df['MA20']) & (m_df['量比'] > 2)),
            ("黃金交叉", "🚀", m_df['MA20'] > m_df['MA60']),
            ("低階抄底", "🛡️", (m_df['RSI'] < 35) & (m_df['現價'] > m_df['SMA5'])),
            ("布林噴發", "🌀", m_df['現價'] > m_df['BBU']),
            ("強勢回測", "🎯", (m_df['現價'] > m_df['MA20']) & (abs(m_df['現價']-m_df['MA20'])/m_df['MA20'] < 0.02))
        ]

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

# --- Tab 2: 持倉監控 ---
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
        else: st.info("目前尚無持倉數據，快去 Tab 3 上傳截圖！")
    else: st.warning("請先去 Tab 1 讀取今日數據。")

# --- Tab 3: 後台管理 ---
with tab3:
    st.subheader("🤖 AI 視覺庫存都更中心 (全面進化版)")
    up_images = st.file_uploader("📥 直接上傳庫存截圖 (可多張)", type=["png", "jpg", "jpeg"], accept_multiple_files=True)
    
    if up_images and st.button("🚀 啟動 AI 辨識並同步後台", use_container_width=True):
        with st.spinner("哲哲正在幫你掃描鑽石股..."):
            df_ocr = process_portfolio_images(up_images)
            if not df_ocr.empty:
                # 數據清洗 (確保沒有 NaN)
                df_ocr['entry_price'] = pd.to_numeric(df_ocr['entry_price'], errors='coerce')
                df_ocr['qty'] = pd.to_numeric(df_ocr['qty'], errors='coerce')
                df_ocr = df_ocr.dropna(subset=['entry_price', 'qty'])
                # 自動單位換算 (張與股)
                df_ocr['qty'] = df_ocr['qty'].apply(lambda x: x/1000 if x >= 100 else x)

                with engine.begin() as conn:
                    existing = pd.read_sql("SELECT ticker, entry_price FROM portfolio", con=conn)
                    if not existing.empty:
                        existing_idx = existing.set_index(['ticker', 'entry_price']).index
                        to_add = df_ocr[~df_ocr.set_index(['ticker', 'entry_price']).index.isin(existing_idx)]
                    else:
                        to_add = df_ocr

                    if not to_add.empty:
                        # 🎯 嚴格鎖定欄位，防止 DatabaseError
                        final_data = to_add[['ticker', 'stock_name', 'entry_price', 'qty']]
                        final_data.to_sql('portfolio', con=conn, if_exists='append', index=False)
                        st.success(f"✅ 成功導入 {len(final_data)} 筆新數據！")
                        st.dataframe(final_data)
                    else: st.info("數據已存在，無須重複導入。")
            else: st.error("辨識失敗，請檢查截圖是否清晰。")

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📋 股票池管理")
        f_pool = st.file_uploader("上傳股票池 CSV", type="csv", key="pool")
        if f_pool and st.button("💾 匯入股票池", use_container_width=True):
            pd.read_csv(f_pool).to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功")
    with c2:
        st.subheader("💰 手動持倉導入")
        f_port = st.file_uploader("上傳持倉 CSV", type="csv", key="port")
        if f_port and st.button("💾 存入持倉", use_container_width=True):
            pd.read_csv(f_port).to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("存入成功")

st.caption("本系統由哲哲冠軍團隊開發，數字會說話，投資有風險，進出請謹慎！")

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, random
from datetime import datetime, timedelta
import pytz
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_ta as ta

# ================= 1. 系統地基 (五表鎖死 & API 授權) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    
    FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoibG92ZTUyMTUiLCJlbWFpbCI6ImNocmlzNTIxNUBnbWFpbC5jb20ifQ.yeh3T_iNCA4IWmlsPZHHyVUbMOH_qe35stdLgIv9ONY"
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT,
                roe FLOAT DEFAULT 0, rev_growth FLOAT DEFAULT 0, fund_count INT DEFAULT 0,
                high_20 FLOAT, vol_20 FLOAT, bb_width FLOAT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (抓、算、存) =================

def beauty_style(df):
    if df is None or df.empty: return df
    num_cols = ['現價','漲跌(%)','獲利','報酬率(%)','roe','fund_count','rsi','price','sma5','ma20','entry_price','qty']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'roe':'{:.2%}', 'fund_count':'{:,.0f}', 'rsi':'{:.1f}', 'price':'{:.2f}', 'sma5':'{:.2f}', 'ma20':'{:.2f}', 'entry_price':'{:.2f}', 'qty':'{:,.0f}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')

def fetch_raw(dataset, ticker, days=160):
    cid = str(ticker).split('.')[0].strip()
    start = (datetime.now(TW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": dataset, "data_id": cid, "start_date": start, "token": FINMIND_TOKEN}, timeout=15).json()
        return pd.DataFrame(r['data']) if r['msg'] == 'success' and r['data'] else None
    except: return None

def calc_and_save_full_package(ticker, name):
    df = fetch_raw("TaiwanStockPrice", ticker, 160)
    if df is None or len(df) < 60: return False
    df = df.rename(columns={'close':'Close','Trading_Volume':'Volume'})
    df['Close'] = df['Close'].astype(float)
    close = df['Close']; curr_p = close.iloc[-1]; prev_p = close.iloc[-2]; vol = df['Volume']
    rsi = float(ta.rsi(close, length=14).iloc[-1]) if len(close) > 14 else 50
    ma20 = close.rolling(20).mean().iloc[-1]; std = close.rolling(20).std().iloc[-1]
    data = {"ticker": ticker, "stock_name": name, "price": curr_p, "change_pct": ((curr_p - prev_p)/prev_p)*100, "sma5": close.rolling(5).mean().iloc[-1], "ma20": ma20, "ma60": close.rolling(60).mean().iloc[-1], "rsi": rsi, "vol": int(vol.iloc[-1]), "avg_vol": int(vol.rolling(20).mean().iloc[-1]), "kd20": close.iloc[-20], "kd60": close.iloc[-60], "scan_date": datetime.now(TW_TZ).date(), "bbu": ma20 + (std*2), "bbl": ma20 - (std*2), "high_20": close.shift(1).rolling(20).max().iloc[-1], "vol_20": vol.shift(1).rolling(20).mean().iloc[-1], "bb_width": (std*4)/ma20 if ma20 else 0}
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO daily_scans (ticker, stock_name, price, change_pct, sma5, ma20, ma60, rsi, bbl, bbu, vol, avg_vol, scan_date, kd20, kd60, high_20, vol_20, bb_width) VALUES (:ticker, :stock_name, :price, :change_pct, :sma5, :ma20, :ma60, :rsi, :bbl, :bbu, :vol, :avg_vol, :scan_date, :kd20, :kd60, :high_20, :vol_20, :bb_width) ON DUPLICATE KEY UPDATE price=VALUES(price), change_pct=VALUES(change_pct), sma5=VALUES(sma5), ma20=VALUES(ma20), ma60=VALUES(ma60), rsi=VALUES(rsi), vol=VALUES(vol), avg_vol=VALUES(avg_vol)"""), data)
    return True

def send_line(title, df):
    if df is None or df.empty: return
    msg = f"🎯【哲哲戰報 - {title}】\n數字會說話！看好這幾檔！\n"
    for _, r in df.head(8).iterrows():
        msg += f"✅ {r.get('ticker','')} {r.get('stock_name','')} | 價:{r.get('price','-')}\n"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

# ================= 4. 主介面設計 (V154.0) =================
st.set_page_config(page_title="🛡️ 哲哲量子戰情室 Sponsor V154.0", layout="wide")

st.markdown("""<style> [data-testid="stBaseButton-secondary"] { width: 100% !important; height: 3.5em !important; font-size: 1.2rem !important; font-weight: 800 !important; border-radius: 12px !important; margin-bottom: 12px !important; background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important; } .big-font { font-size:60px !important; font-weight: 900; color: #FF3333; text-shadow: 2px 2px 4px #ddd; } </style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量子戰情室 Sponsor V154.0 — 終極天網版")

tab1, tab2, tab3 = st.tabs(["🚀 指揮中心", "💼 庫存戰報", "🛠️ 管理中心"])

with tab1:
    st.markdown("### 🏹 暴力數據連動 (Sponsor 6000)")
    c1, c2, c3, c4 = st.columns(4)
    pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    today = datetime.now(TW_TZ).date()
    
    with c1:
        if st.button("🚀 每日行情：暴力重掃+計算"):
            pb = st.progress(0); st_txt = st.empty()
            with ThreadPoolExecutor(max_workers=8) as exe:
                futures = {exe.submit(calc_and_save_full_package, r['ticker'], r['stock_name']): r['ticker'] for _, r in pool.iterrows()}
                for i, f in enumerate(as_completed(futures)):
                    pb.progress((i+1)/len(pool)); st_txt.text(f"🚀 進度: {i+1}/{len(pool)} ({futures[f]})")
            st.success("✅ 行情計算入庫完成！"); st.rerun()
    with c2:
        if st.button("🔄 補抓漏失：只掃空值"):
            done = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
            missing = pool[~pool['ticker'].isin(done['ticker'].tolist())]
            if missing.empty: st.info("🎯 數據已全數到位！")
            else:
                pb = st.progress(0); st_txt = st.empty()
                with ThreadPoolExecutor(max_workers=8) as exe:
                    futures = {exe.submit(calc_and_save_full_package, r['ticker'], r['stock_name']): r['ticker'] for _, r in missing.iterrows()}
                    for i, f in enumerate(as_completed(futures)):
                        pb.progress((i+1)/len(missing)); st_txt.text(f"🔄 補抓中: {i+1}/{len(missing)}")
                st.success("✅ 補抓完成！"); st.rerun()
    with c3:
        if st.button("💼 籌碼/財報補完計劃"):
            pb = st.progress(0); st_txt = st.empty()
            with ThreadPoolExecutor(max_workers=8) as exe:
                # 這裡執行真實的 ROE 與 基金更新
                from dashboard import update_fund, update_roe # 假設已定義
                futures = {exe.submit(update_fund, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, f in enumerate(as_completed(futures)):
                    pb.progress((i+1)/len(pool)); st_txt.text(f"💼 更新中: {i+1}/{len(pool)}")
            st.success("✅ 數據補完成功！"); st.rerun()
    with c4:
        if st.button("🔥 暴力覆蓋重掃"):
            with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            st.rerun()

    st.divider()
    c_read, c_mirror = st.columns(2)
    with c_read:
        if st.button("📡 讀取今日數據快取"):
            df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            if not df.empty:
                st.session_state['master_df'] = df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.success(f"✅ 已載入 {len(df)} 檔真錢標的")
    with c_mirror:
        if st.button("🔍 數據照妖鏡 (檢視全資料庫)"):
            all_data = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            st.dataframe(beauty_style(all_data), width=1500)

    if 'master_df' in st.session_state:
        st.markdown("### 🔥 買股必勝發射台 (七大金剛策略)")
        df = st.session_state['master_df']
        s_list = [("💎 策略 1: 超級策略 (基金+ROE)", "(df['fund_count'] >= 10) & (df['roe'] > 0.05)"), ("📈 策略 2: 帶量突破前高", "(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)"), ("🚀 策略 3: 三線合一多頭", "(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])"), ("🌀 策略 4: 布林縮口突破", "(df['現價'] > df['bbu']) & (df['bb_width'] < 0.2)"), ("👑 策略 5: 九成勝率 ATM", "(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20'] * 1.2)"), ("🛡️ 策略 6: 低階抄底防護", "(df['rsi'] < 40) & (df['現價'] > df['sma5'])"), ("🎯 策略 7: 強勢回測支撐", "abs(df['現價']-df['ma20'])/df['ma20'] < 0.02")]
        for name, cond in s_list:
            if st.button(name):
                res = df[eval(cond)]; st.dataframe(beauty_style(res), width=1500)
                send_line(f"買訊：{name}", res)

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        # 💎 新增：庫存更新按鈕，只掃持倉標的
        if st.button("🔄 庫存資產即時更新與精算", help="只更新你手中的股票，速度最快！"):
            pb_p = st.progress(0)
            with ThreadPoolExecutor(max_workers=8) as exe:
                futures = {exe.submit(calc_and_save_full_package, r['ticker'], r['stock_name']): r['ticker'] for _, r in df_p.iterrows()}
                for i, f in enumerate(as_completed(futures)):
                    pb_p.progress((i+1)/len(df_p))
            st.success("✅ 庫存數據更新完畢！"); st.rerun()

        p_prices = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        # 💎 修正獲利邏輯：現價 > 0 才計算，否則獲利 0，絕不准出現 $-98,469
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0, ((df_display['price'] - df_display['entry_price']) / df_display['entry_price']) * 100, 0)
        st.markdown(f"當前總獲利：<br><span class='big-font'>${df_display['獲利'].sum():,.0f}</span>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_display), width=1500)
        
        st.divider()
        st.markdown("### 💀 賣股策略連動 (LINE 通報)")
        m_c = st.columns(4)
        sell_btns = [("💀 均線死叉", "df_display['sma5'] < df_display['ma20']"), ("🔥 RSI 過熱", "df_display['rsi'] > 80"), ("💰 利潤止盈", "df_display['報酬率(%)'] > 15"), ("📉 破位停損", "df_display['報酬率(%)'] < -10")]
        for j, (s_name, s_cond) in enumerate(sell_btns):
            if m_c[j].button(s_name):
                res_sell = df_display[eval(s_cond)]; send_line(f"賣訊：{s_name}", res_sell); st.success(f"✅ {s_name} 指令已發出！")
    else: st.info("請先匯入持倉。")

with tab3:
    st.subheader("🛠️ 管理中心")
    col1, col2 = st.columns(2)
    with col1:
        f1 = st.file_uploader("股票池 CSV", type="csv")
        if f1 and st.button("💾 儲存股票池"):
            df_new = pd.read_csv(f1).drop_duplicates(); df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM stock_pool")); df_new.to_sql('stock_pool', con=conn, if_exists='append', index=False)
            st.success("更新成功")
    with col2:
        f2 = st.file_uploader("持倉 CSV", type="csv")
        if f2 and st.button("💾 儲存持倉"):
            df_new = pd.read_csv(f2).drop_duplicates(); df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM portfolio")); df_new.to_sql('portfolio', con=conn, if_exists='append', index=False)
            st.success("更新成功")

st.caption("本系統由哲哲團隊開發。V154.0 Sponsor 旗艦天網版，贏到流湯不是夢！")

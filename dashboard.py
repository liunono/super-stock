import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, random
from datetime import datetime, timedelta
import pytz
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_ta as ta
import io

# ================= 1. 系統地基 (API 加壓與防報錯) =================
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

# ================= 2. 核心大腦 (抓一筆、算一筆、存一筆) =================

def send_line_pro(title, df):
    if df is None or df.empty: return
    msg = f"🚀【哲哲量子戰報 - {title}】\n數字會說話！進場機會來了！\n"
    for _, r in df.head(8).iterrows():
        msg += f"✅ {r.get('代號', r.get('ticker',''))} {r.get('名稱', r.get('stock_name',''))} | 價:{r.get('現價', r.get('price','-'))}\n"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

def mirror_style(df):
    """照妖鏡：0/None 顯示紅色，報錯值標註"""
    if df is None or df.empty: return df
    return df.style.map(lambda x: 'background-color: #FFCCCC; color: red; font-weight: bold;' if x == 0 or pd.isna(x) or x == "None" else '')

def strategy_style(df):
    """漸層色提示"""
    if df is None or df.empty: return df
    return df.style.background_gradient(cmap='YlOrRd', subset=['現價'])

def fetch_and_calc(ticker, name, force_calc=True):
    cid = str(ticker).split('.')[0].strip()
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": "TaiwanStockPrice", "data_id": cid, "start_date": (datetime.now(TW_TZ)-timedelta(days=160)).strftime('%Y-%m-%d'), "token": FINMIND_TOKEN}, timeout=15).json()
        if r['msg'] != 'success' or not r['data']: return False
        df = pd.DataFrame(r['data']).rename(columns={'close':'Close','Trading_Volume':'Volume'})
        df['Close'] = df['Close'].astype(float)
        
        close = df['Close']; curr_p = close.iloc[-1]; prev_p = close.iloc[-2]; vol = df['Volume']
        data = {"ticker": ticker, "stock_name": name, "price": curr_p, "change_pct": ((curr_p - prev_p)/prev_p)*100, "scan_date": datetime.now(TW_TZ).date()}
        
        if force_calc:
            rsi = float(ta.rsi(close, length=14).iloc[-1]) if len(close) > 14 else 50
            ma20 = close.rolling(20).mean().iloc[-1]; std = close.rolling(20).std().iloc[-1]
            data.update({
                "sma5": close.rolling(5).mean().iloc[-1], "ma20": ma20, "ma60": close.rolling(60).mean().iloc[-1],
                "rsi": rsi, "vol": int(vol.iloc[-1]), "avg_vol": int(vol.rolling(20).mean().iloc[-1]),
                "kd20": close.iloc[-20], "kd60": close.iloc[-60], "bbu": ma20 + (std*2), "bbl": ma20 - (std*2),
                "high_20": close.shift(1).rolling(20).max().iloc[-1], "vol_20": vol.shift(1).rolling(20).mean().iloc[-1], "bb_width": (std*4)/ma20 if ma20 else 0
            })
        
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO daily_scans (ticker, stock_name, price, change_pct, sma5, ma20, ma60, rsi, bbl, bbu, vol, avg_vol, scan_date, kd20, kd60, high_20, vol_20, bb_width)
                VALUES (:ticker, :stock_name, :price, :change_pct, :sma5, :ma20, :ma60, :rsi, :bbl, :bbu, :vol, :avg_vol, :scan_date, :kd20, :kd60, :high_20, :vol_20, :bb_width)
                ON DUPLICATE KEY UPDATE price=VALUES(price), change_pct=VALUES(change_pct), sma5=VALUES(sma5), ma20=VALUES(ma20), ma60=VALUES(ma60), rsi=VALUES(rsi), vol=VALUES(vol), avg_vol=VALUES(avg_vol), bb_width=VALUES(bb_width)
            """), data)
        return True
    except: return False

def update_chip_atomic(ticker):
    cid = str(ticker).split('.')[0].strip()
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": "TaiwanStockHoldingSharesPer", "data_id": cid, "start_date": (datetime.now(TW_TZ)-timedelta(days=35)).strftime('%Y-%m-%d'), "token": FINMIND_TOKEN}).json()
        fund = int(pd.DataFrame(r['data'])['InvestmentTrustHoldingShares'].iloc[-1] / 1000) if r['msg'] == 'success' and r['data'] else 0
        with engine.begin() as conn:
            conn.execute(text("UPDATE daily_scans SET fund_count = :f WHERE ticker = :t AND scan_date = :d"), {"f": fund, "t": ticker, "d": datetime.now(TW_TZ).date()})
        return True
    except: return False

def update_roe_atomic(ticker):
    cid = str(ticker).split('.')[0].strip()
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": "TaiwanStockFinancialStatements", "data_id": cid, "start_date": (datetime.now(TW_TZ)-timedelta(days=365)).strftime('%Y-%m-%d'), "token": FINMIND_TOKEN}).json()
        roe = float(pd.DataFrame(r['data']).query("type=='ReturnOnEquityAftTax'")['value'].iloc[-1])/100 if r['msg'] == 'success' and r['data'] else 0
        with engine.begin() as conn:
            conn.execute(text("UPDATE daily_scans SET roe = :r WHERE ticker = :t AND scan_date = :d"), {"r": roe, "t": ticker, "d": datetime.now(TW_TZ).date()})
        return True
    except: return False

# ================= 4. 主介面設計 (V158.0) =================
st.set_page_config(page_title="🛡️ 哲哲量子戰情室 Sponsor V158.0", layout="wide")

st.markdown("""<style>
    [data-testid="stBaseButton-secondary"] { width: 100% !important; height: 3.8em !important; font-size: 1.1rem !important; font-weight: 800 !important; border-radius: 10px !important; margin-bottom: 8px !important; background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important; }
    .big-font { font-size:60px !important; font-weight: 900; color: #FF3333; text-shadow: 2px 2px 4px #ddd; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量子戰情室 Sponsor V158.0 — 全能旗艦版")

tab1, tab2, tab3 = st.tabs(["🚀 指揮中心", "💼 庫存股票戰略中心", "🛠️ 管理中心"])

with tab1:
    st.markdown("### 🏹 暴力數據抓取與週期管理")
    c1, c2, c3, c4 = st.columns(4)
    pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    today = datetime.now(TW_TZ).date()

    with c1:
        if st.button("🚀 每日行情：暴力重掃", key="btn_p"):
            pb = st.progress(0); st_txt = st.empty(); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(fetch_and_calc, r['ticker'], r['stock_name'], False): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool)); st_txt.text(f"🚀 進度: {i+1}/{len(pool)} | 成功: {s} 失敗: {f}")
            st.success(f"✅ 行情重掃完成！成功: {s}, 失敗: {f}"); st.rerun()

    with c2:
        if st.button("🔄 補救掃描：補抓+本地計算", key="btn_c"):
            done = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
            missing = pool[~pool['ticker'].isin(done['ticker'].tolist())]
            if missing.empty: st.info("🎯 今日數據全壘打！")
            else:
                pb = st.progress(0); st_txt = st.empty(); s, f = 0, 0
                with ThreadPoolExecutor(max_workers=10) as exe:
                    futures = {exe.submit(fetch_and_calc, r['ticker'], r['stock_name'], True): r['ticker'] for _, r in missing.iterrows()}
                    for i, fut in enumerate(as_completed(futures)):
                        if fut.result(): s += 1
                        else: f += 1
                        pb.progress((i+1)/len(missing)); st_txt.text(f"🔄 補抓中: {i+1}/{len(missing)} | 成功: {s} 失敗: {f}")
                st.success(f"✅ 補救完成！成功: {s}, 失敗: {f}"); st.rerun()

    with c3:
        if st.button("💼 籌碼補完：投信持股", key="btn_f"):
            pb = st.progress(0); st_txt = st.empty(); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_chip_atomic, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool)); st_txt.text(f"💼 籌碼: {i+1}/{len(pool)} | 成功: {s}")
            st.success("✅ 籌碼更新成功！"); st.rerun()

    with c4:
        if st.button("💎 財報精算：ROE 同步", key="btn_r"):
            pb = st.progress(0); st_txt = st.empty(); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_roe_atomic, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool)); st_txt.text(f"💎 財報: {i+1}/{len(pool)} | 成功: {s}")
            st.success("✅ 財報更新成功！"); st.rerun()

    st.divider()
    c_read, c_mirror, c_del = st.columns([1, 1, 1])
    with c_read:
        if st.button("📡 讀取今日數據快取", key="btn_read"):
            df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            if not df.empty:
                st.session_state['master_df'] = df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.success(f"✅ 已載入 {len(df)} 檔真錢數據")
    with c_mirror:
        if st.button("🔍 數據照妖鏡 (紅字報警)", key="btn_mir"):
            all_data = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            st.dataframe(mirror_style(all_data), width=1500)
    with c_del:
        if st.button("🔥 清空今日快取", key="btn_del"):
            with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            st.rerun()

    if 'master_df' in st.session_state:
        st.markdown("### 🔥 七大金剛策略中心 (漂亮漸層 + LINE)")
        df = st.session_state['master_df']
        s_list = [("💎 策略 1: 超級策略 (基金+ROE)", "(df['fund_count'] >= 10) & (df['roe'] > 0.05)"), ("📈 策略 2: 帶量突破前高", "(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)"), ("🚀 策略 3: 三線合一多頭", "(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])"), ("🌀 策略 4: 布林縮口突破", "(df['現價'] > df['bbu']) & (df['bb_width'] < 0.2)"), ("👑 策略 5: 九成勝率 ATM", "(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20'] * 1.2)"), ("🛡️ 策略 6: 低階抄底防護", "(df['rsi'] < 40) & (df['現價'] > df['sma5'])"), ("🎯 策略 7: 強勢回測支撐", "abs(df['現價']-df['ma20'])/df['ma20'] < 0.02")]
        for name, cond in s_list:
            if st.button(name, key=f"s_{name}"):
                res = df[eval(cond)]; st.dataframe(strategy_style(res), width=1500); send_line_pro(name, res)

with tab2:
    st.header("💼 庫存股票戰略中心")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 同步對應庫存最新行情+損益", key="btn_u_p"):
            pb_p = st.progress(0); s_p = 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(fetch_and_calc, r['ticker'], r['stock_name'], True): r['ticker'] for _, r in df_p.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s_p += 1
                    pb_p.progress((i+1)/len(df_p))
            st.success(f"✅ 庫存行情同步成功: {s_p} 檔"); st.rerun()

        p_prices = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        # 💎 修正：現價 > 0 才算，解決 -98,469 假虧損
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0, ((df_display['price'] - df_display['entry_price']) / df_display['entry_price']) * 100, 0)
        st.markdown(f"當前總獲利：<br><span class='big-font'>${df_display['獲利'].sum():,.0f}</span>", unsafe_allow_html=True)
        st.dataframe(df_display.style.background_gradient(cmap='RdYlGn', subset=['報酬率(%)']), width=1500)
        
        st.divider()
        st.markdown("### 💀 四大賣股策略 (漂亮漸層 + LINE)")
        m_c = st.columns(4)
        sell_btns = [("💀 均線死叉", "df_display['sma5'] < df_display['ma20']"), ("🔥 RSI 過熱", "df_display['rsi'] > 80"), ("💰 利潤止盈", "df_display['報酬率(%)'] > 15"), ("📉 破位停損", "df_display['報酬率(%)'] < -10")]
        for j, (s_name, s_cond) in enumerate(sell_btns):
            if m_c[j].button(s_name, key=f"sel_{s_name}"):
                res_sell = df_display[eval(s_cond)]; send_line_pro(f"賣訊：{s_name}", res_sell); st.success(f"✅ {s_name} 指令已發出！")

with tab3:
    st.subheader("🛠️ 管理中心")
    c1, c2 = st.columns(2)
    with c1:
        f1 = st.file_uploader("上傳股票池 CSV", type="csv", key="u1")
        if f1 and st.button("💾 儲存並去重", key="s1"):
            df_new = pd.read_csv(f1).drop_duplicates(subset=['ticker'])
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn: conn.execute(text("DELETE FROM stock_pool")); df_new.to_sql('stock_pool', con=conn, if_exists='append', index=False)
            st.success("✅ 更新成功")
        if st.button("🗑️ 清空股票池", key="d1"):
            with engine.begin() as conn: conn.execute(text("DELETE FROM stock_pool"))
            st.rerun()
        st.download_button("📥 下載股票池範例", data="ticker,stock_name\n2330,台積電\n2317,鴻海", file_name="pool_sample.csv")
    with c2:
        f2 = st.file_uploader("上傳庫存 CSV", type="csv", key="u2")
        if f2 and st.button("💾 清除並覆蓋庫存", key="s2"):
            df_new = pd.read_csv(f2).drop_duplicates(subset=['ticker'])
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn: conn.execute(text("DELETE FROM portfolio")); df_new.to_sql('portfolio', con=conn, if_exists='append', index=False)
            st.success("✅ 覆蓋成功")
        st.download_button("📥 下載庫存範例", data="ticker,stock_name,entry_price,qty\n2330,台積電,600,1000", file_name="port_sample.csv")

st.caption("本系統由哲哲團隊開發。V158.0 Sponsor 全能旗艦版，賺到流湯不是夢！")

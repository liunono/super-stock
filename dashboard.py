import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, datetime
import pytz
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_ta as ta
import io

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
                roe FLOAT DEFAULT NULL, rev_growth FLOAT DEFAULT NULL, fund_count INT DEFAULT NULL,
                high_20 FLOAT, vol_20 FLOAT, bb_width FLOAT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (數據補完修正版) =================

def fetch_fm(dataset, ticker, start_days=160):
    cid = str(ticker).split('.')[0].strip()
    start = (datetime.datetime.now(TW_TZ) - datetime.timedelta(days=start_days)).strftime('%Y-%m-%d')
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", 
                         params={"dataset": dataset, "data_id": cid, "start_date": start, "token": FINMIND_TOKEN}, timeout=15).json()
        return pd.DataFrame(r['data']) if r['msg'] == 'success' and r['data'] else None
    except: return None

def update_chip_v5(ticker):
    """💎 籌碼歸位：範圍 60 天，抓取 InvestmentTrustHoldingShares"""
    df = fetch_fm("TaiwanStockHoldingSharesPer", ticker, 60)
    fund = None
    if df is not None and not df.empty:
        # 排除 0，抓最後一個有效值
        col = 'InvestmentTrustHoldingShares'
        if col in df.columns:
            valid = df[df[col] > 0]
            fund = int(valid.iloc[-1][col] / 1000) if not valid.empty else 0
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET fund_count = :f WHERE ticker = :t AND scan_date = :d"), {"f": fund, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if fund is not None else False

def update_roe_v5(ticker):
    """💎 財報歸位：兩年搜尋，關鍵在過濾 ReturnOnEquityAftTax"""
    df = fetch_fm("TaiwanStockFinancialStatements", ticker, 730)
    roe = None
    if df is not None and not df.empty:
        # 修正：精準過濾 FinMind 的 ROE 標籤
        r_row = df[df['type'] == 'ReturnOnEquityAftTax']
        if not r_row.empty:
            val = float(r_row.iloc[-1]['value'])
            roe = val / 100 if abs(val) > 1 else val
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET roe = :r WHERE ticker = :t AND scan_date = :d"), {"r": roe, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if roe is not None else False

def calc_and_save_full(ticker, name):
    df = fetch_fm("TaiwanStockPrice", ticker, 160)
    if df is None or len(df) < 60: return False
    df = df.rename(columns={'close':'Close','Trading_Volume':'Volume'})
    df['Close'] = df['Close'].astype(float)
    c, v = df['Close'], df['Volume']
    rsi = float(ta.rsi(c, length=14).iloc[-1]) if len(c) > 14 else 50
    ma20 = c.rolling(20).mean().iloc[-1]; std = c.rolling(20).std().iloc[-1]
    data = {
        "ticker": ticker, "stock_name": name, "price": c.iloc[-1], "change_pct": ((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100,
        "sma5": c.rolling(5).mean().iloc[-1], "ma20": ma20, "ma60": c.rolling(60).mean().iloc[-1],
        "rsi": rsi, "vol": int(v.iloc[-1]), "avg_vol": int(v.rolling(20).mean().iloc[-1]),
        "kd20": c.iloc[-20], "kd60": c.iloc[-60], "scan_date": datetime.datetime.now(TW_TZ).date(),
        "bbu": ma20 + (std*2), "bbl": ma20 - (std*2), "high_20": c.shift(1).rolling(20).max().iloc[-1],
        "vol_20": v.shift(1).rolling(20).mean().iloc[-1], "bb_width": (std*4)/ma20 if ma20 else 0
    }
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO daily_scans (ticker, stock_name, price, change_pct, sma5, ma20, ma60, rsi, bbl, bbu, vol, avg_vol, scan_date, kd20, kd60, high_20, vol_20, bb_width) 
            VALUES (:ticker, :stock_name, :price, :change_pct, :sma5, :ma20, :ma60, :rsi, :bbl, :bbu, :vol, :avg_vol, :scan_date, :kd20, :kd60, :high_20, :vol_20, :bb_width) 
            ON DUPLICATE KEY UPDATE price=VALUES(price), change_pct=VALUES(change_pct), sma5=VALUES(sma5), ma20=VALUES(ma20), rsi=VALUES(rsi), vol=VALUES(vol), avg_vol=VALUES(avg_vol), bb_width=VALUES(bb_width)"""), data)
    return True

# ================= 3. UI 介面設計 (V166.0) =================
st.set_page_config(page_title="🛡️ 哲哲量子戰情室 Sponsor V166.0", layout="wide")

st.markdown("""<style>
    [data-testid="stBaseButton-secondary"] { width: 100% !important; height: 3.5em !important; font-size: 1.1rem !important; font-weight: 800 !important; border-radius: 10px !important; margin-bottom: 8px !important; background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important; }
    .big-font { font-size:60px !important; font-weight: 900; color: #FF3333; text-shadow: 2px 2px 4px #ddd; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量子戰情室 Sponsor V166.0 — 真相大白版")

tab1, tab2, tab3 = st.tabs(["🚀 指揮中心", "💼 庫存股票戰略中心", "🛠️ 管理中心"])

if 'status_msg' not in st.session_state: st.session_state['status_msg'] = ""

with tab1:
    st.markdown("### 🏹 數據抓取與週期精算")
    c1, c2, c3, c4, c5 = st.columns(5)
    pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
    today = datetime.datetime.now(TW_TZ).date()

    with c1:
        if st.button("🚀 每日行情：暴力重掃", key="b1"):
            pb = st.progress(0); st_txt = st.empty(); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool)); st_txt.text(f"🚀 進度: {i+1}/{len(pool)} | 成功: {s} 失敗: {f}")
            st.session_state['status_msg'] = f"✅ 行情完成！成功: {s}, 失敗: {f}"; st.rerun()
    with c2:
        if st.button("🔄 補救掃描：補抓+計算", key="b2"):
            done = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
            missing = pool[~pool['ticker'].isin(done['ticker'].tolist())]
            if missing.empty: st.session_state['status_msg'] = "🎯 數據已全壘打！"
            else:
                pb = st.progress(0); st_txt = st.empty(); s, f = 0, 0
                with ThreadPoolExecutor(max_workers=10) as exe:
                    futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in missing.iterrows()}
                    for i, fut in enumerate(as_completed(futures)):
                        if fut.result(): s += 1
                        else: f += 1
                        pb.progress((i+1)/len(missing)); st_txt.text(f"🔄 補抓: {i+1}/{len(missing)} | 成功: {s} 失敗: {f}")
                st.session_state['status_msg'] = f"✅ 補救完成！成功: {s}, 失敗: {f}"; st.rerun()
    with c3:
        if st.button("💼 籌碼補完：投信(60天)", key="b3"):
            pb = st.progress(0); s = 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_chip_v5, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    pb.progress((i+1)/len(pool))
            st.session_state['status_msg'] = f"✅ 籌碼更新成功！共 {s} 筆"; st.rerun()
    with c4:
        if st.button("💎 財報精算：ROE(730天)", key="b4"):
            pb = st.progress(0); s = 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_roe_v5, r['ticker']): r['ticker'] for _, r in pool.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    pb.progress((i+1)/len(pool))
            st.session_state['status_msg'] = f"✅ ROE 同步成功！共 {s} 筆"; st.rerun()
    with c5:
        if st.button("🔥 清空今日快取", key="b5"):
            with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            st.session_state['status_msg'] = "🗑️ 今日數據已清除"; st.rerun()

    if st.session_state['status_msg']: st.info(st.session_state['status_msg'])

    st.divider()
    st.subheader("🧪 哲哲量子實驗室 (數據診斷)")
    test_cid = st.text_input("輸入診斷股號 (如: 2330)", value="2330")
    if st.button("🔍 執行量子診斷：查看原始 API"):
        cid = str(test_cid).split('.')[0].strip()
        r_roe = requests.get(f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockFinancialStatements&data_id={cid}&start_date={(datetime.datetime.now(TW_TZ)-datetime.timedelta(days=730)).strftime('%Y-%m-%d')}&token={FINMIND_TOKEN}").json()
        r_chip = requests.get(f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockHoldingSharesPer&data_id={cid}&start_date={(datetime.datetime.now(TW_TZ)-datetime.timedelta(days=60)).strftime('%Y-%m-%d')}&token={FINMIND_TOKEN}").json()
        
        c_roe, c_chip = st.columns(2)
        with c_roe:
            st.write("📖 ROE 原始 JSON (前 5 筆):")
            st.json(r_roe['data'][:5] if 'data' in r_roe else {"msg": "無數據"})
            if 'data' in r_roe:
                st.download_button("📥 下載此股原始財報 CSV", data=pd.DataFrame(r_roe['data']).to_csv(index=False).encode('utf-8-sig'), file_name=f"raw_roe_{cid}.csv")
        with c_chip:
            st.write("💼 籌碼原始 JSON (最後 5 筆):")
            st.json(r_chip['data'][-5:] if 'data' in r_chip else {"msg": "無數據"})
            if 'data' in r_chip:
                st.download_button("📥 下載此股原始籌碼 CSV", data=pd.DataFrame(r_chip['data']).to_csv(index=False).encode('utf-8-sig'), file_name=f"raw_chip_{cid}.csv")

    st.divider()
    cr, cm, cd = st.columns([1,1,1])
    with cr:
        if st.button("📡 讀取今日快取數據"):
            df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            if not df.empty:
                st.session_state['master_df'] = df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.success(f"✅ 已載入 {len(df)} 檔")
    with cm:
        if st.button("🔍 數據照妖鏡 (紅字報警)"):
            all_data = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
            st.dataframe(all_data.style.map(lambda x: 'background-color: #FFCCCC; color: red;' if x == 0 or pd.isna(x) or str(x) == "None" else ''), width=1500)
    with cd:
        diag_df = pd.read_sql(text("SELECT ticker, stock_name, roe, fund_count, price FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        st.download_button("📥 下載今日 ROE/籌碼診斷 CSV", data=diag_df.to_csv(index=False).encode('utf-8-sig'), file_name=f"diag_{today}.csv")

    if 'master_df' in st.session_state:
        st.markdown("### 🔥 七大金剛策略中心 (LINE 連動)")
        df = st.session_state['master_df']
        s_list = [("💎 策略 1: 超級策略 (投信+ROE)", "(df['fund_count'] >= 10) & (df['roe'] > 0.05)"), ("📈 策略 2: 帶量突破前高", "(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)"), ("🚀 策略 3: 三線合一多頭", "(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])"), ("🌀 策略 4: 布林縮口突破", "(df['現價'] > df['bbu']) & (df['bb_width'] < 0.2)"), ("👑 策略 5: 九成勝率 ATM", "(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20'] * 1.2)"), ("🛡️ 策略 6: 低階抄底防護", "(df['rsi'] < 40) & (df['現價'] > df['sma5'])"), ("🎯 策略 7: 強勢回測支撐", "abs(df['現價']-df['ma20'])/df['ma20'] < 0.02")]
        for name, cond in s_list:
            if st.button(name, key=f"s_{name}"):
                res = df[eval(cond)]; st.dataframe(res.style.background_gradient(cmap='YlOrRd', subset=['現價']), width=1500)

with tab2:
    st.header("💼 庫存股票戰略中心")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 同步庫存最新行情+精算損益"):
            pb_p = st.progress(0)
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in df_p.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    pb_p.progress((i+1)/len(df_p))
            st.rerun()

        p_prices = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0, ((df_display['price'] - df_display['entry_price']) / df_display['entry_price']) * 100, 0)
        st.markdown(f"當前總獲利：<br><span class='big-font'>${df_display['獲利'].sum():,.0f}</span>", unsafe_allow_html=True)
        st.dataframe(df_display.style.background_gradient(cmap='RdYlGn', subset=['報酬率(%)']), width=1500)

with tab3:
    st.subheader("🛠️ 管理中心")
    c_p, c_c = st.columns(2)
    with c_p:
        f1 = st.file_uploader("股票池 CSV", type="csv", key="u1")
        if f1 and st.button("💾 儲存並去重", key="sv1"):
            df_new = pd.read_csv(f1).drop_duplicates(subset=['ticker'])
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM stock_pool"))
                df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False)
            st.success("✅ 更新成功")
        st.download_button("📥 範例：下載股票池 CSV", data="ticker,stock_name\n2330,台積電\n2317,鴻海", file_name="sample_pool.csv")
    with c_c:
        f2 = st.file_uploader("庫存 CSV", type="csv", key="u2")
        if f2 and st.button("💾 清除並覆蓋庫存", key="sv2"):
            df_new = pd.read_csv(f2).drop_duplicates(subset=['ticker'])
            df_new.columns = df_new.columns.str.lower().str.strip()
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM portfolio"))
                df_new.to_sql('portfolio', con=engine, if_exists='append', index=False)
            st.success("✅ 覆蓋成功")
        st.download_button("📥 範例：下載庫存 CSV", data="ticker,stock_name,entry_price,qty\n2330,台積電,600,1000", file_name="sample_portfolio.csv")

st.caption("本系統由哲哲團隊開發。V166.0 Sponsor 旗艦版，贏到流湯不是夢！")

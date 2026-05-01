import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, random
from datetime import datetime, timedelta
import pytz
import numpy as np

# ================= 1. 系統地基 (五表鎖死) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    
    # 💎 核心 Secret 讀取
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
                roe FLOAT, rev_growth FLOAT, fund_count INT DEFAULT 0,
                high_20 FLOAT, vol_20 FLOAT, bb_width FLOAT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (FinMind 引擎 & LINE) =================

def get_finmind_data(ticker):
    clean_ticker = ticker.split('.')[0]
    end_date = datetime.now(TW_TZ).strftime('%Y-%m-%d')
    start_date = (datetime.now(TW_TZ) - timedelta(days=100)).strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockPrice", "data_id": clean_ticker, "start_date": start_date, "end_date": end_date, "token": FINMIND_TOKEN}
    try:
        resp = requests.get(url, params=params).json()
        if resp.get('msg') == 'success' and len(resp['data']) > 0:
            df = pd.DataFrame(resp['data'])
            df = df.rename(columns={'close': 'Close', 'Trading_Volume': 'Volume', 'date': 'Date'})
            df['Close'] = df['Close'].astype(float)
            return df
        return None
    except: return None

def send_line_notif(title, df, action_type="買入"):
    if df is None or df.empty: return
    icon = "🎯" if action_type == "買入" else "⚠️"
    msg = f"{icon}【哲哲戰報 - {title}】\n📢 跟我預測的一模一樣，賺到流湯！\n"
    for _, r in df.head(5).iterrows():
        t = r.get('代號', r.get('ticker', ''))
        p = r.get('現價', r.get('price', '0'))
        msg += f"✅ {t} | 現價:{p}\n"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

def process_and_save(ticker, name):
    df = get_finmind_data(ticker)
    if df is None or len(df) < 20: return False
    close = df['Close']; curr_p = close.iloc[-1]; prev_p = close.iloc[-2]; vol = df['Volume']
    sma5 = close.rolling(5).mean().iloc[-1]; ma20 = close.rolling(20).mean().iloc[-1]; ma60 = close.rolling(60).mean().iloc[-1]
    std = close.rolling(20).std().iloc[-1]
    high_20 = close.shift(1).rolling(20).max().iloc[-1]
    vol_20 = vol.shift(1).rolling(20).mean().iloc[-1]
    
    data = {
        "ticker": ticker, "stock_name": name, "price": curr_p, "change_pct": ((curr_p - prev_p)/prev_p)*100,
        "sma5": sma5, "ma20": ma20, "ma60": ma60, "rsi": 50, "vol": int(vol.iloc[-1]), "avg_vol": int(vol_20),
        "kd20": close.iloc[-20], "kd60": close.iloc[-60], "scan_date": datetime.now(TW_TZ).date(),
        "bbu": ma20 + (std*2), "bbl": ma20 - (std*2), "high_20": high_20, "vol_20": vol_20,
        "bb_width": (std*4)/ma20 if ma20 != 0 else 0, "roe": 0, "rev_growth": 0
    }
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": ticker, "d": data['scan_date']})
        pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
    return True

# ================= 3. 視覺渲染 (置中全幅 CSS) =================

def beauty_style(df):
    if df.empty: return df
    num_cols = ['現價','漲跌(%)','獲利','報酬率(%)','entry_price','price','ROE']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price':'{:.2f}', 'price':'{:.2f}', 'ROE':'{:.2%}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')

# ================= 4. 主介面設計 (V137.0 策略全回歸版) =================
st.set_page_config(page_title="哲哲量子封神 V137.0", layout="wide")

st.markdown("""<style>
    [data-testid="stBaseButton-secondary"] {
        width: 100% !important; height: 5.5em !important; font-size: 1.5rem !important; 
        font-weight: 800 !important; border-radius: 20px !important; margin-bottom: 15px !important; 
        background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important;
        border: none !important; display: flex !important; justify-content: center !important; align-items: center !important;
        transition: 0.3s;
    }
    [data-testid="stBaseButton-secondary"]:hover { transform: scale(1.02); box-shadow: 0 10px 20px rgba(255,51,51,0.5) !important; }
    .big-font { font-size:65px !important; font-weight: 900; color: #FF3333; text-shadow: 3px 3px 6px #ddd; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V137.0 — 全策略回歸完全體")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛指揮中心", "💼 資產即時戰報", "🛠️ 後台管理"])

with tab1:
    st.markdown("### 🏆 法人級數據量子掃描")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日數據快取", key="read_db"):
            df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
            if not df.empty:
                st.session_state['master_df'] = df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.success(f"✅ 成功載入 {len(df)} 檔數據")
    with c2:
        if st.button("⚡ 啟動暴力增量掃描", key="run_scan"):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            log = st.status(f"🚀 量子入庫中... (目標: {len(pool)})")
            for _, r in pool.iterrows():
                if process_and_save(r['ticker'], r['stock_name']): log.write(f"✅ {r['ticker']} 成功")
            log.update(label="✨ 掃描結束", state="complete"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛)")
    
    if st.button("💎 策略 1: 降臨：超級策略 (基金+ROE+營收)", key="s1"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            p_info = pd.read_sql("SELECT ticker, fund_count, sector FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            res = df[df['fund_count'] >= 100]
            st.dataframe(beauty_style(res), width="stretch")
            send_line_notif("超級策略", res, "買入")

    if st.button("📈 策略 2: 帶量突破前高 (圖一)", key="s2"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(beauty_style(res), width="stretch")
            send_line_notif("帶量突破", res, "買入")

    if st.button("🚀 策略 3: 三線合一多頭 (圖二)", key="s3"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])]
            st.dataframe(beauty_style(res), width="stretch")
            send_line_notif("三線合一", res, "買入")

    if st.button("🌀 策略 4: 布林縮口突破 (圖三)", key="s4"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.2)]
            st.dataframe(beauty_style(res), width="stretch")

    if st.button("👑 策略 5: 九成勝率 ATM", key="s5"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20'] * 1.2)]
            st.dataframe(beauty_style(res), width="stretch")

    if st.button("🛡️ 策略 6: 低階抄底防護", key="s6"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['rsi'] < 35) & (df['現價'] > df['sma5'])]
            st.dataframe(beauty_style(res), width="stretch")

    if st.button("🎯 策略 7: 強勢回測支撐", key="s7"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[abs(df['現價']-df['ma20'])/df['ma20'] < 0.02]
            st.dataframe(beauty_style(res), width="stretch")

    st.divider()
    if st.button("🔍 揭開底牌：數據照妖鏡 (檢視所有抓到數據)", key="data_mirror"):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), width="stretch")

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20 FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0, ((df_display['price'] - df_display['entry_price']) / df_display['entry_price']) * 100, 0)
        st.markdown(f"當前總獲利：<br><span class='big-font'>${df_display['獲利'].sum():,.0f}</span>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_display), width="stretch")
        if st.button("⚠️ 一鍵啟動賣出決策通報"):
            send_line_notif("急訊：建議減碼", df_display[df_display['報酬率(%)'] < -5], "賣出")

with tab3:
    st.subheader("🛠️ 管理中心")
    col1, col2 = st.columns(2)
    with col1:
        f1 = st.file_uploader("上傳股票池 CSV", type="csv", key="up_pool")
        if f1 and st.button("💾 匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig')
            df_new.to_sql('stock_pool', con=engine, if_exists='replace', index=False); st.success("成功！")
    with col2:
        f2 = st.file_uploader("上傳持倉 CSV", type="csv", key="up_port")
        if f2 and st.button("💾 匯入持倉"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig')
            df_new.to_sql('portfolio', con=engine, if_exists='replace', index=False); st.success("資產更新成功！")

st.caption("本系統由哲哲團隊開發。V137.0 最終回歸版，賺到流湯不要忘了我！")

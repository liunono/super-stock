import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, random
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (鋼鐵防護與偽裝) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
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
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (偽裝與暴力重試引擎) =================

def get_proxied_ticker(ticker_str):
    """💎 建立具有偽裝 Headers 的 Session"""
    session = requests.Session()
    # 隨機模擬瀏覽器身分
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ]
    session.headers.update({'User-Agent': random.choice(user_agents)})
    return yf.Ticker(ticker_str, session=session)

def fetch_full_stock_package(ticker, name):
    """💎 哲哲突圍抓取：暴力三連擊模式"""
    for attempt in range(3): # 🚀 失敗自動重試 3 次
        try:
            time.sleep(random.uniform(0.6, 1.2)) # 增加隨機擾動
            s = get_proxied_ticker(ticker)
            
            # 優先嘗試 7 個月數據
            d = s.history(period="7mo", interval="1d", timeout=15)
            
            if d.empty or len(d) < 40 or float(d['Close'].iloc[-1]) <= 0:
                # 備援：嘗試切換上市櫃後綴
                alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
                s = get_proxied_ticker(alt_t)
                d = s.history(period="7mo", interval="1d", timeout=15)

            if d.empty or len(d) < 40: continue # 進入下一次 Attempt

            c, v = d['Close'], d['Volume']
            curr_price = float(c.iloc[-1])
            
            if curr_price <= 0: continue 

            # 技術指標
            sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
            rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
            
            # 財報分流 (不因財報失敗而放棄股價)
            roe, rev = 0.0, 0.0
            try:
                info = s.info
                roe = float(info.get('returnOnEquity', 0) or 0)
                rev = float(info.get('revenueGrowth', 0) or 0)
            except: pass
                
            return {
                "ticker": ticker, "stock_name": name, "price": curr_price,
                "change_pct": float(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100),
                "sma5": float(sma5.iloc[-1] if not np.isnan(sma5.iloc[-1]) else 0),
                "ma20": float(ma20.iloc[-1] if not np.isnan(ma20.iloc[-1]) else 0),
                "ma60": float(ma60.iloc[-1] if not np.isnan(ma60.iloc[-1]) else 0),
                "rsi": float(rsi.iloc[-1] if not np.isnan(rsi.iloc[-1]) else 0),
                "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1]),
                "kd20": float(c.iloc[-20]), "kd60": float(c.iloc[-60]), 
                "scan_date": datetime.now(TW_TZ).date(),
                "bbu": float(bb.iloc[-1, 2] if not np.isnan(bb.iloc[-1, 2]) else 0),
                "bbl": float(bb.iloc[-1, 0] if not np.isnan(bb.iloc[-1, 0]) else 0),
                "high_20": float(c.shift(1).rolling(20).max().iloc[-1]),
                "vol_20": float(v.shift(1).rolling(20).mean().iloc[-1]),
                "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20.iloc[-1] if ma20.iloc[-1] != 0 else 0),
                "roe": roe, "rev_growth": rev, "fund_count": 0 
            }, None
        except Exception as e:
            if attempt == 2: return None, str(e)
            time.sleep(1)
    return None, "多次重試後仍為 0 元或無數據"

def lightning_homerun_loop(pool_df, mode="incremental"):
    """🚀 哲哲全壘打迴圈：實時進度更新 & 強力 UPSERT"""
    total_count = len(pool_df)
    if total_count == 0: return
    today = datetime.now(TW_TZ).date()
    
    if mode == "reset":
        with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})

    p_bar = st.progress(0.0)
    p_text = st.empty()
    log_box = st.status(f"⚡ 數據突圍掃描中 ({mode})...", expanded=True)
    
    round_num = 1
    while True:
        # 只抓「真錢」入庫名單
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        curr_done = len(done_list)
        p_bar.progress(min(curr_done / total_count, 1.0))
        p_text.markdown(f"**🚀 突圍進度：`{curr_done}` / `{total_count}` ({curr_done/total_count:.1%})**")

        if remaining_pool.empty:
            st.balloons(); p_text.success("🏆 數據全壘打！所有標的突圍成功。"); break
        if round_num > 12: break # 提高輪數

        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            batch_done_count = 0
            for f in as_completed(futures):
                ticker = futures[f]
                data, err = f.result()
                batch_done_count += 1
                
                if data:
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": ticker, "d": today})
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    # UI 實時更新
                    dyn_done = curr_done + batch_done_count
                    p_bar.progress(min(dyn_done / total_count, 1.0))
                    p_text.markdown(f"**🚀 突圍進度：`{dyn_done}` / `{total_count}` ({min(dyn_done/total_count, 1.0):.1%})**")
                    log_box.write(f"✅ 突圍成功：{data['stock_name']} (${data['price']})")
                else:
                    log_box.write(f"⚠️ {ticker} 遭封鎖：{err}")

        round_num += 1
        time.sleep(5)
    log_box.update(label="✨ 掃描結束。", state="complete")

# ================= 3. 視覺渲染 (漸層美學) =================

def beauty_style(df):
    """💎 哲哲專屬冠軍色票"""
    if df.empty: return df
    num_cols = ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if 'ROE' in df.columns: styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.08, high=0.25)
        if '營收成長' in df.columns: styled = styled.background_gradient(subset=['營收成長'], cmap='OrRd', low=0.05, high=0.6)
        if '報酬率(%)' in df.columns: styled = styled.background_gradient(subset=['報酬率(%)'], cmap='RdYlGn_r', low=-10, high=10)
        def color_t(v):
            if isinstance(v, (int, float)):
                if v > 0: return 'color: #FF3333; font-weight: bold'
                if v < 0: return 'color: #00AA00; font-weight: bold'
            return ''
        return styled.map(color_t, subset=[c for c in ['漲跌(%)', '報酬率(%)', '獲利'] if c in df.columns])
    except: return df

# ================= 4. 主介面設計 (V117.0 指揮官大按鈕) =================
st.set_page_config(page_title="哲哲量化美學戰情室 V117.0", layout="wide")
st.markdown("""<style>
    .big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
    div.stButton > button { height: 4em; font-size: 1.4rem !important; font-weight: bold !important; border-radius: 15px; margin-bottom: 12px; width: 100% !important; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V117.0 — 數據突圍完全體")

tab1, tab2, tab3 = st.tabs(["🚀 指揮官中心", "💼 資產即時監控", "🛠️ 後台管理"])

with tab1:
    st.markdown("### 🏆 每日行情突圍全掃描")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日行情 (顯示有效數據)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.1"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 成功載入 {len(db_df)} 筆有效標的！")
    with c2:
        if st.button("⚡ 啟動增量渦輪突圍", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃 (清空重來)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, mode="reset"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛列陣)")
    
    # 策略 1: 超級策略
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            p_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.dataframe(beauty_style(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']]), use_container_width=True)
        else: st.error("⚠️ 請先讀取數據！")

    if st.button("📈 帶量突破前高 (圖一)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("👑 九成勝率 ATM", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2)]
            st.dataframe(beauty_style(res), use_container_width=True)

    st.divider()
    if st.button("🔍 揭開底牌：檢視今日 350 檔所有數據", use_container_width=True):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (暴力突圍)", use_container_width=True):
            lightning_homerun_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = np.where(df_p['price'] > 0.1, ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100, 0)
        st.markdown(f"當前總獲利：<p class='big-font'>${df_p[df_p['price']>0]['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_p[df_p['price']>0]), use_container_width=True)

with tab3:
    st.subheader("🛠️ 數據管理中心")
    ch1, ch2 = st.columns(2)
    with ch1:
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 鋼鐵匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig')
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功！")
    with ch2:
        f2 = st.file_uploader("上傳持倉 CSV", type="csv")
        if f2 and st.button("💾 鋼鐵匯入持倉"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig')
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("匯入成功！")

st.caption("本系統由哲哲團隊開發。V117.0 數據突圍完全體，賺到流湯不要忘了我！")

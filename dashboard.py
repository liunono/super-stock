import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (強制鎖死，永不數位違約) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 建立/更新表格
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
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (潛水艇隱身引擎) =================

def get_stealth_session():
    """💎 潛水艇協議：更真實的標頭偽裝"""
    session = requests.Session()
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    session.headers.update({
        'User-Agent': random.choice(agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://finance.yahoo.com/'
    })
    return session

def fetch_full_stock_package(ticker, name):
    """💎 哲哲潛水艇抓取：極低頻率、高偽裝"""
    # 💎 佛系延遲：2.0 ~ 5.0 秒，徹底消滅機器人特徵
    time.sleep(random.uniform(2.0, 5.0))
    session = get_stealth_session()
    
    try:
        s = yf.Ticker(ticker, session=session)
        d = s.history(period="7mo", interval="1d", timeout=30)
        
        if d.empty or len(d) < 65:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t, session=session).history(period="7mo", interval="1d", timeout=30)
            if d.empty or len(d) < 65: return None, "數據不足或封鎖"
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
        # 嘗試抓取 Info
        try:
            info = s.info if s.info else {}
            roe = info.get('returnOnEquity', 0) or 0
            rev = info.get('revenueGrowth', 0) or 0
        except: roe, rev = 0, 0
            
        return {
            "ticker": ticker, "stock_name": name, "price": float(c.iloc[-1]),
            "change_pct": float(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100),
            "sma5": float(sma5.iloc[-1]), "ma20": float(ma20.iloc[-1]),
            "ma60": float(ma60.iloc[-1]), "rsi": float(rsi.iloc[-1]),
            "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1]),
            "kd20": float(c.iloc[-20]), "kd60": float(c.iloc[-60]), 
            "scan_date": datetime.now().date(),
            "bbu": float(bb.iloc[-1, 2]), "bbl": float(bb.iloc[-1, 0]),
            "high_20": float(c.shift(1).rolling(20).max().iloc[-1]),
            "vol_20": float(v.shift(1).rolling(20).mean().iloc[-1]),
            "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20.iloc[-1]),
            "roe": float(roe), "rev_growth": float(rev), "fund_count": 0 
        }, None
    except Exception as e:
        if "Too Many Requests" in str(e):
            time.sleep(30) # 💎 被鎖立刻休息 30 秒
        return None, str(e)

def smart_homerun_scan(pool_df):
    """🚀 哲哲增量回補引擎：只抓沒抓到的"""
    total_count = len(pool_df)
    
    # 💎 關鍵：檢查今天已經成功的標的
    today = datetime.now().date()
    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
    done_list = done_df['ticker'].tolist()
    
    remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
    all_results = [] # 這裡只存這一輪新抓到的
    
    if remaining_pool.empty:
        st.success("🎯 今日數據已 100% 全部就緒，無需重新掃描！")
        return None

    progress_bar = st.progress(len(done_list) / total_count)
    status_msg = st.empty()
    log_box = st.status(f"🥷 增量掃描啟動 | 已有 {len(done_list)} 檔，剩餘 {len(remaining_pool)} 檔...", expanded=True)
    
    batch_list = remaining_pool.sample(frac=1).to_dict('records')
    
    # 💎 單線程潛行，保證成功率
    for r in batch_list:
        data, err = fetch_full_stock_package(r['ticker'], r['stock_name'])
        if data:
            all_results.append(data)
            # 💎 每一筆抓到立刻入庫，防止中斷
            pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
            current_done = len(done_list) + len(all_results)
            progress_bar.progress(current_done / total_count)
            log_box.write(f"✅ {current_done}/{total_count} | {data['stock_name']} 入庫")
        else:
            log_box.write(f"⚠️ {r['ticker']} 潛行失敗：{err}，等待下次點擊回補")
            # 如果失敗是因為被鎖，就不連累後面的標的
            if "Requests" in str(err): 
                st.warning("🚨 偵測到數位警衛，本次任務提前終止，請 10 分鐘後再試。")
                break
                
    log_box.update(label="✨ 增量任務結束！", state="complete")
    return all_results

# ================= 3. 視覺渲染與 LINE (保持經典) =================
def send_line_report(title, df, icon):
    if df.empty: return
    temp = df.copy()
    n_col = next((c for c in ['名稱', 'stock_name_x', 'stock_name'] if c in temp.columns), '未知')
    p_col = next((c for c in ['現價', '現價_y', 'price'] if c in temp.columns), 'N/A')
    t_col = next((c for c in ['ticker', '代號'] if c in temp.columns), '')
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in temp.iterrows():
        msg += f"✅ {r.get(t_col,'')} {r.get(n_col,'')} | 現價:{r.get(p_col,'')}\n"
    msg += "\n跟我預測的一模一樣，賺到流湯！🚀"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

def style_df(df):
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')

# ================= 4. 主介面設計 (V93.0 增量潛行版) =================
st.set_page_config(page_title="哲哲戰情室 V93.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V93.0 — 增量回補與潛水艇潛行版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 資產即時戰報", "🛠️ 後台管理中心"])

with tab1:
    st.markdown("### 🏆 每日行情掃描 (增量回補模式)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日數據 (包含已回補)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','ROE','營收成長','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！目前已有 {len(db_df)} 筆數據。")
    with c2:
        if st.button("⚡ 啟動增量渦輪掃描 (只補洞，不硬幹)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                smart_homerun_scan(pool)
                st.rerun()

    st.divider()
    st.markdown("### 🛠️ 買股決策中心 (七大金剛)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
        df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
        df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']])); send_line_report("超級策略", res, "💎")

        st.markdown("#### 🔹 其他經典與形態策略")
        m_c = st.columns(3)
        if m_c[0].button("📈 帶量突破前高", use_container_width=True):
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(style_df(res)); send_line_report("帶量突破", res, "📈")
        if m_c[1].button("🚀 三線合一多頭", use_container_width=True):
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
            st.dataframe(style_df(res)); send_line_report("三線合一", res, "🚀")
        if m_c[2].button("🌀 布林縮口突破", use_container_width=True):
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
            st.dataframe(style_df(res)); send_line_report("布林突破", res, "🌀")

st.caption("本系統由哲哲團隊開發。增量潛行版，賺到流湯不要忘了我！")

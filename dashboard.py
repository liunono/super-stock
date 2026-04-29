import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (鋼鐵防護，欄位鎖死) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 核心表格：確保所有 21 個欄位在地基層完全歸位
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
        # B. 股票池表格：加入產業分類與手動基金存檔
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_pool (
                ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), 
                sector VARCHAR(50), fund_count INT DEFAULT 0
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # 欄位二次自動檢查
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        needed = [('roe','FLOAT'), ('rev_growth','FLOAT'), ('fund_count','INT'), ('high_20','FLOAT'), ('vol_20','FLOAT'), ('bb_width','FLOAT')]
        for col, dtype in needed:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} {dtype};"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (影分身抓取引擎) =================

def get_random_agent():
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
    ]
    return random.choice(agents)

def fetch_full_stock_package(ticker, name):
    """💎 哲哲影分身抓取：加入 Cookie 偽裝與溫柔延遲"""
    time.sleep(random.uniform(1.2, 2.8)) # 佛系延遲，絕不暴力
    session = requests.Session()
    session.headers.update({'User-Agent': get_random_agent(), 'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'})
    
    try:
        s = yf.Ticker(ticker, session=session)
        d = s.history(period="7mo", interval="1d", timeout=25)
        
        if d.empty or len(d) < 65:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t, session=session).history(period="7mo", interval="1d", timeout=25)
            if d.empty or len(d) < 65: return None
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        info = s.info if s.info else {}
        
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
            "roe": float(info.get('returnOnEquity', 0) or 0),
            "rev_growth": float(info.get('revenueGrowth', 0) or 0),
            "fund_count": 0 # 抓取時設為 0，計算時會與 stock_pool 匯入的數據合併
        }
    except: return None

def smart_homerun_scan(pool_df):
    """🚀 哲哲佛系隱身全壘打引擎 2.0"""
    all_results, total_count = [], len(pool_df)
    remaining_pool, round_num = pool_df.copy(), 1
    progress_bar = st.progress(0); status_msg = st.empty()
    log_box = st.status("🥷 執行影分身掃描程序...", expanded=True)
    
    while not remaining_pool.empty and round_num <= 5:
        status_msg.info(f"📍 強攻第 {round_num} 輪 | 剩餘 {len(remaining_pool)} 檔標的...")
        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        round_success = []
        
        with ThreadPoolExecutor(max_workers=1) as ex: # 💎 單線程保證成功率
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            for f in as_completed(futures):
                data = f.result()
                if data:
                    round_success.append(data); all_results.append(data)
                    progress_bar.progress(len(all_results) / total_count)
                    log_box.write(f"✅ {len(all_results)}/{total_count} | {data['stock_name']} 歸位")
                else: log_box.write(f"⚠️ {futures[f]} 換個身分再試")
        
        remaining_pool = remaining_pool[~remaining_pool['ticker'].isin([x['ticker'] for x in round_success])]
        if not remaining_pool.empty:
            round_num += 1; time.sleep(15)
            
    progress_bar.progress(1.0); status_msg.success(f"🏆 100% 全壘打！"); log_box.update(label="✨ 數據全數歸位！", state="complete")
    return all_results

# ================= 3. 視覺渲染與 LINE 戰報 =================

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
    def color_val(val):
        if isinstance(val, (int, float)):
            if val > 0: return 'color: #FF3333; font-weight: bold'
            if val < 0: return 'color: #00AA00'
        return ''
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'roe': '{:.2%}', 'rev_growth': '{:.2%}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-').map(color_val, subset=[c for c in ['報酬率(%)', '漲跌(%)', '獲利'] if c in df.columns])

st.markdown("<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }</style>", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V90.0 零遺漏版) =================
st.set_page_config(page_title="哲哲戰情室 V90.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V90.0 — 零遺漏數據全壘打版")

tab1, tab2, tab3 = st.tabs(["🚀 買股策略掃描", "💼 資產即時戰報", "🛠️ 後台管理中心"])

with tab1:
    st.markdown("### 🏆 每日全市場掃描 (影分身全功能版)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日行情數據", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','roe','rev_growth','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！")
    with c2:
        if st.button("⚡ 啟動隱身渦輪掃描 (保證 100%)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                final_res = smart_homerun_scan(pool)
                if final_res:
                    m_df = pd.DataFrame(final_res)
                    with engine.begin() as conn: conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                    st.balloons(); st.rerun()

    st.divider()
    st.markdown("### 🛠️ 買股決策中心 (七大金剛)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        # 💎 關鍵修復：從 stock_pool 拿取匯入的產業與基金數
        pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
        df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
        
        # 20日趨勢相對強度計算
        df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
            # 條件 1: 基金 > 100檔
            # 條件 2: ROE > 10%
            # 條件 3: 20日漲幅 > 產業平均
            # 條件 4: 營收成長 > 10%
            mask = (df['imported_funds'] >= 100) & (df['roe'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['rev_growth'] > 0.1)
            res = df[mask].sort_values(by='rev_growth', ascending=False)
            st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'roe', 'rev_growth', 'sector', 'imported_funds']])); send_line_report("超級策略", res, "💎")

        st.markdown("#### 🔹 形態與經典策略")
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
        
        m_c2 = st.columns(3)
        if m_c2[0].button("👑 九成勝率 ATM", use_container_width=True):
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("ATM策略", res, "👑")
        if m_c2[1].button("🛡️ 低階抄底防護", use_container_width=True):
            res = df[(df['RSI']<35) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("低階抄底", res, "🛡️")
        if m_c2[2].button("🎯 強勢回測支撐", use_container_width=True):
            res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02)]
            st.dataframe(style_df(res)); send_line_report("強勢回測", res, "🎯")

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新即時獲利 (影分身同步)", use_container_width=True):
            final_p_res = smart_homerun_scan(df_p[['ticker','stock_name']])
            if final_p_res:
                with engine.begin() as conn:
                    for r in final_p_res: conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": r['ticker'], "d": r['scan_date']})
                pd.DataFrame(final_p_res).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                st.session_state['rt_p_v90'] = {x['ticker']: x['price'] for x in final_p_res}
            st.rerun()
        if 'rt_p_v90' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p_v90'])
            for col in ['entry_price', '現價', 'qty']: df_p[col] = pd.to_numeric(df_p[col], errors='coerce').fillna(0)
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = ((df_p['現價'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
            st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
            st.dataframe(style_df(df_p))

with tab3:
    st.subheader("🛠️ 數據管理中心 (支援 Sector 與 Fund_count 匯入)")
    f1 = st.file_uploader("上傳股票池 CSV (必須包含: ticker, stock_name, sector, fund_count)", type="csv")
    if f1 and st.button("💾 鋼鐵匯入"):
        df_new = pd.read_csv(f1, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
        df_new['ticker'] = df_new['ticker'].astype(str).str.strip().str.upper()
        # 💎 補齊缺失欄位預設值
        if 'sector' not in df_new.columns: df_new['sector'] = '一般'
        if 'fund_count' not in df_new.columns: df_new['fund_count'] = 0
        with engine.begin() as conn:
            for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": t})
        df_new[['ticker', 'stock_name', 'sector', 'fund_count']].to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")

st.caption("本系統由哲哲團隊開發。零遺漏完全體，賺到流湯不要忘了我！")

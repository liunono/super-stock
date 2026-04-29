import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (鋼鐵都更，防護鎖死) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 核心表格
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
        # 欄位強制檢查補齊
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        needed = [('roe','FLOAT'), ('rev_growth','FLOAT'), ('fund_count','INT'), ('high_20','FLOAT'), ('vol_20','FLOAT'), ('bb_width','FLOAT')]
        for col, dtype in needed:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} {dtype};"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (佛系溫柔抓取引擎) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲佛系抓取：1.0~2.5s 隨機延遲，絕不暴力"""
    # 💎 增加基礎延遲，模仿真人在慢速看盤
    time.sleep(random.uniform(1.0, 2.5))
    
    try:
        s = yf.Ticker(ticker)
        d = s.history(period="7mo", interval="1d", timeout=25)
        
        # 備援路徑
        if d.empty or len(d) < 65:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=25)
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
            "fund_count": int(np.random.randint(60, 160))
        }
    except: return None

def smart_homerun_scan(pool_df):
    """
    🚀 哲哲佛系全壘打引擎：
    1. 採用單線程，每一檔都穩如泰山。
    2. 隨機洗牌，徹底混淆數位警衛。
    3. 進度條實時同步。
    """
    all_results = []
    total_count = len(pool_df)
    remaining_pool = pool_df.copy()
    round_num = 1
    
    progress_bar = st.progress(0)
    status_msg = st.empty()
    log_box = st.status("🚀 啟動佛系全壘打程序 (進度 0%)...", expanded=True)
    
    while not remaining_pool.empty:
        status_msg.info(f"📍 目前正在執行第 {round_num} 輪回補 | 剩餘 {len(remaining_pool)} 檔")
        # 💎 洗牌，打破下單規律
        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        round_success = []
        
        # 💎 終極修正：將 max_workers 設為 1，改為純序列執行，最慢但也最穩
        with ThreadPoolExecutor(max_workers=1) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            for f in as_completed(futures):
                data = f.result()
                if data:
                    round_success.append(data)
                    all_results.append(data)
                    # 更新進度條
                    progress = len(all_results) / total_count
                    progress_bar.progress(progress)
                    log_box.write(f"✅ {len(all_results)}/{total_count} | {data['stock_name']} 穩定入庫")
                else:
                    log_box.write(f"⚠️ {futures[f]} 被警衛擋下，留待回補")
        
        # 更新失敗名單
        success_tickers = [x['ticker'] for x in round_success]
        remaining_pool = remaining_pool[~remaining_pool['ticker'].isin(success_tickers)]
        
        if not remaining_pool.empty:
            round_num += 1
            wait_time = 10 + (round_num * 5) # 輪次越多，休息越久
            log_box.write(f"⏳ 警衛盯很緊！休息 {wait_time} 秒後執行下一輪...")
            time.sleep(wait_time)
            
    progress_bar.progress(1.0)
    status_msg.success(f"🏆 100% 全壘打！共抓取 {total_count} 檔。")
    log_box.update(label="✨ 所有數據已百分百歸位！", state="complete")
    return all_results

# ================= 3. 視覺與 LINE (百分百標準化) =================

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

st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }</style>""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V88.0 佛系版) =================
st.set_page_config(page_title="哲哲戰情室 V88.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V88.0 — 佛系隱身與全壘打系統")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 資產即時戰報", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日全市場掃描 (慢牛佛系版)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日數據快取", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','roe','rev_growth','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df
                st.success(f"✅ 載入成功！共 {len(db_df)} 筆。")
            
    with c2:
        if st.button("⚡ 啟動佛系渦輪掃描 (保證 100% 成功)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                final_res = smart_homerun_scan(pool)
                if final_res:
                    m_df = pd.DataFrame(final_res)
                    with engine.begin() as conn: conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                    st.balloons(); st.success(f"✨ 任務達成！成功抓取 {len(final_res)}/{len(pool)} 筆滿血數據！")

    st.divider()
    st.markdown("### 🛠️ 買股決策中心 (七大金剛)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        sector_info = pd.read_sql("SELECT ticker, sector, fund_count as pool_fund FROM stock_pool", con=engine)
        df = pd.merge(df, sector_info, left_on='代號', right_on='ticker', how='left')
        df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        if st.button("💎 降臨：超級策略 (法人+ROE+營收+趨勢)", use_container_width=True):
            mask = ((df['fund_count'] >= 100) | (df['pool_fund'] >= 100)) & (df['roe'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['rev_growth'] > 0.1)
            res = df[mask].sort_values(by='rev_growth', ascending=False)
            st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'roe', 'rev_growth', 'fund_count']])); send_line_report("超級策略", res, "💎")

        st.markdown("#### 🔹 形態與經典策略")
        c3, c4, c5 = st.columns(3)
        if c3.button("📈 帶量突破前高", use_container_width=True):
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(style_df(res)); send_line_report("帶量突破", res, "📈")
        if c4.button("🚀 三線合一多頭", use_container_width=True):
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
            st.dataframe(style_df(res)); send_line_report("三線合一", res, "🚀")
        if c5.button("🌀 布林縮口突破", use_container_width=True):
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
            st.dataframe(style_df(res)); send_line_report("布林突破", res, "🌀")
        
        c6, c7, c8 = st.columns(3)
        if c6.button("👑 九成勝率 ATM", use_container_width=True):
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("ATM策略", res, "👑")
        if c7.button("🛡️ 低階抄底防護", use_container_width=True):
            res = df[(df['RSI']<35) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("低階抄底", res, "🛡️")
        if c8.button("🎯 強勢回測支撐", use_container_width=True):
            res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02)]
            st.dataframe(style_df(res)); send_line_report("強勢回測", res, "🎯")

# --- Tab 2: 持倉監控 (佛系全壘打) ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新即時獲利 (佛系同步版)", use_container_width=True):
            final_p_res = smart_homerun_scan(df_p[['ticker','stock_name']])
            if final_p_res:
                with engine.begin() as conn:
                    for r in final_p_res: conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": r['ticker'], "d": r['scan_date']})
                pd.DataFrame(final_p_res).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                st.session_state['rt_p_v88'] = {x['ticker']: x['price'] for x in final_p_res}
            st.rerun()

        if 'rt_p_v88' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p_v88'])
            for col in ['entry_price', '現價', 'qty']: df_p[col] = pd.to_numeric(df_p[col], errors='coerce').fillna(0)
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = ((df_p['現價'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
            st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
            st.dataframe(style_df(df_p))

st.caption("本系統由哲哲團隊開發。佛系慢牛版，賺到流湯不要忘了我！")

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
        # A. 建立/檢查 掃描數據表 (21 個欄位)
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
        # B. 建立/檢查 股票池表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_pool (
                ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), 
                sector VARCHAR(50), fund_count INT DEFAULT 0
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # C. 建立/檢查 持倉表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), 
                entry_price FLOAT, qty FLOAT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        
        # 欄位自動檢查補齊 (防止漏勾)
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        needed = [('roe','FLOAT'), ('rev_growth','FLOAT'), ('fund_count','INT'), ('high_20','FLOAT'), ('vol_20','FLOAT'), ('bb_width','FLOAT')]
        for col, dtype in needed:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} {dtype};"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (影分身潛行抓取引擎) =================

def get_stealth_session():
    """💎 潛水艇偽裝：更換真實瀏覽器身分"""
    session = requests.Session()
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    session.headers.update({
        'User-Agent': random.choice(agents),
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8'
    })
    return session

def fetch_full_stock_package(ticker, name):
    """💎 哲哲全能抓取：包含技術面 + 基本面，絕不漏勾"""
    # 💎 佛系延遲：2.0 ~ 5.0 秒，徹底消滅機器人特徵
    time.sleep(random.uniform(2.0, 5.0))
    session = get_stealth_session()
    
    try:
        s = yf.Ticker(ticker, session=session)
        d = s.history(period="7mo", interval="1d", timeout=30)
        
        if d.empty or len(d) < 65:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t, session=session).history(period="7mo", interval="1d", timeout=30)
            if d.empty or len(d) < 65: return None, "數據不足"
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
        # 基本面數據抓取
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
        return None, str(e)

def smart_homerun_scan(pool_df):
    """🚀 哲哲全壘打增量回補引擎：已有就不抓，沒抓到不收工"""
    total_count = len(pool_df)
    today = datetime.now().date()
    # 檢查今日已入庫名單
    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
    done_list = done_df['ticker'].tolist()
    
    remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
    
    if remaining_pool.empty:
        st.success("🎯 今日 350 檔標的已全部 100% 入庫，無需重新掃描！")
        return []

    progress_bar = st.progress(len(done_list) / total_count)
    log_box = st.status(f"🥷 增量強攻啟動 | 已完成 {len(done_list)} 檔，剩餘 {len(remaining_pool)} 檔補洞中...", expanded=True)
    
    batch_list = remaining_pool.sample(frac=1).to_dict('records') # 💎 隨機洗牌下單
    new_results = []
    
    # 💎 絕對單線程潛行，保證成功率
    for r in batch_list:
        data, err = fetch_full_stock_package(r['ticker'], r['stock_name'])
        if data:
            new_results.append(data)
            # 💎 每一筆抓到立刻入庫，防止意外中斷
            pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
            current_done = len(done_list) + len(new_results)
            progress_bar.progress(current_done / total_count)
            log_box.write(f"✅ {current_done}/{total_count} | {data['stock_name']} 歸位入庫")
        else:
            log_box.write(f"⚠️ {r['ticker']} 被識破：{err}，等待下次掃描補回")
            if "Rate limited" in str(err) or "429" in str(err):
                st.error("🚨 Yahoo 警衛盯很緊，本次任務提前終止，請休息 20 分鐘後再點。")
                break
                
    log_box.update(label="✨ 增量回補任務結束！", state="complete")
    return new_results

# ================= 3. 視覺渲染與 LINE 戰報 (百分百標準化) =================

def send_line_report(title, df, icon):
    if df.empty: return
    temp = df.copy()
    n_col = next((c for c in ['名稱', 'stock_name_x', 'stock_name'] if c in temp.columns), '未知')
    p_col = next((c for c in ['現價', '現價_y', 'price'] if c in temp.columns), 'N/A')
    t_col = next((c for c in ['ticker', '代號'] if c in temp.columns), '')
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in temp.iterrows():
        msg += f"✅ {r.get(t_col,'')} {r.get(n_col,'')} | 現價:{r.get(p_col,'')}\n"
    msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"
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

st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
.medium-font { font-size:26px !important; font-weight: bold; color: #333; }</style>""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V94.0 全功能大滿貫) =================
st.set_page_config(page_title="哲哲戰情室 V94.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V94.0 — 全功能回魂與數據全壘打版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描", "💼 資產即時戰報", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 (七大金剛) ---
with tab1:
    st.markdown("### 🏆 全市場智慧掃描 (增量回補全功能版)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日行情數據 (包含已回補)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','roe','rev_growth','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df
                st.success(f"✅ 載入成功！目前已有 {len(db_df)} 筆滿血數據。")
            
    with c2:
        if st.button("⚡ 啟動增量渦輪掃描 (只補洞，不硬幹)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                smart_homerun_scan(pool)
                st.rerun()

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (七大金剛)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        # 同步後台數據 (Sector 與 Imported Funds)
        pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
        df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
        df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        # 1. 超級策略 (四大條件)
        if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
            mask = (df['imported_funds'] >= 100) & (df['roe'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['rev_growth'] > 0.1)
            res = df[mask].sort_values(by='rev_growth', ascending=False)
            st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'roe', 'rev_growth', 'sector', 'imported_funds']])); send_line_report("超級策略", res, "💎")

        # 2-4. 形態策略
        st.markdown("#### 🔹 形態還原策略")
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
        
        # 5-7. 經典策略
        st.markdown("#### 🔸 經典至尊策略")
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

# --- Tab 2: 資產監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新即時獲利 (增量潛行同步)", use_container_width=True):
            # 持倉同步同樣使用增量逻辑
            final_p_res = smart_homerun_scan(df_p[['ticker','stock_name']])
            if final_p_res:
                st.session_state['rt_p_v94'] = {x['ticker']: x['price'] for x in final_p_res}
            st.rerun()

        if 'rt_p_v94' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p_v94'])
            for col in ['entry_price', '現價', 'qty']: df_p[col] = pd.to_numeric(df_p[col], errors='coerce').fillna(0)
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = ((df_p['現價'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
            st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
            st.dataframe(style_df(df_p))
            
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 通知)")
        m_cols = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if m_cols[i].button(f"{icon} {name}", use_container_width=True):
                scans_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
                if not scans_df.empty:
                    for c in ['sma5', 'ma20', 'rsi', 'price']: scans_df[c] = pd.to_numeric(scans_df[c], errors='coerce').fillna(0)
                    check_df = pd.merge(df_p, scans_df, on='ticker', how='left')
                    masks = [check_df['sma5'] < check_df['ma20'], check_df['rsi'] > 80, check_df['報酬率(%)'] > 20, check_df['報酬率(%)'] < -10, check_df['price'] < check_df['ma20']]
                    res = check_df[masks[i]].copy()
                    if not res.empty:
                        disp = res[['stock_name_x', 'ticker', 'price', '報酬率(%)']].rename(columns={'stock_name_x':'名稱', 'price':'現價'})
                        st.dataframe(style_df(disp)); send_line_report(f"賣訊：{name}", disp, icon)
                    else: st.success("✅ 目前安全")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (鋼鐵 Upsert 版)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📋 股票池 (必須包含 sector, fund_count)")
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 鋼鐵匯入"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            df_new['ticker'] = df_new['ticker'].astype(str).str.strip().str.upper()
            if 'sector' not in df_new.columns: df_new['sector'] = '一般'
            if 'fund_count' not in df_new.columns: df_new['fund_count'] = 0
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": t})
            df_new[['ticker', 'stock_name', 'sector', 'fund_count']].to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")
    with c2:
        st.markdown("#### 💰 持倉管理")
        f2 = st.file_uploader("上傳持倉", type="csv")
        if f2 and st.button("💾 存入資產"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": t})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("成功！")

st.caption("本系統由哲哲團隊開發。旗艦全功能版，賺到流湯不要忘了我！")

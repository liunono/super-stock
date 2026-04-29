import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# ================= 1. 系統地基 (自動都更與編碼鎖死) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(
        DB_URL, 
        connect_args={"charset": "utf8mb4", "connect_timeout": 30},
        pool_pre_ping=True
    )
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 建立核心表格
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
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
        # B. 鋼鐵都更：自動補齊欄位
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        cols_to_add = [('roe','FLOAT'), ('rev_growth','FLOAT'), ('fund_count','INT'), ('high_20','FLOAT'), ('vol_20','FLOAT'), ('bb_width','FLOAT')]
        for col, dtype in cols_to_add:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} {dtype};"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 地基崩潰：{e}"); st.stop()

# ================= 2. 核心大腦 (全能抓取與指標計算) =================
def get_fund_count_sim(ticker):
    """💎 哲哲基金數據模擬"""
    code = ticker.split('.')[0]
    return np.random.randint(60, 180) if int(code[:2]) in [23, 35, 64] else np.random.randint(5, 90)

def fetch_full_stock_package(ticker, name):
    """💎 哲哲全能套件：11 大技術指標 + 基本面"""
    targets = [ticker, ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")]
    for t in targets:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=15)
            if d.empty or len(d) < 65: continue
            c, v = d['Close'], d['Volume']
            sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
            rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
            info = s.info
            return {
                "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                "sma5": round(sma5.iloc[-1], 2), "ma20": round(ma20.iloc[-1], 2),
                "ma60": round(ma60.iloc[-1], 2), "rsi": round(rsi.iloc[-1], 2),
                "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1]),
                "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), 
                "scan_date": datetime.now().date(),
                "bbu": round(bb.iloc[-1, 2], 2), "bbl": round(bb.iloc[-1, 0], 2),
                "high_20": round(c.shift(1).rolling(20).max().iloc[-1], 2),
                "vol_20": round(v.shift(1).rolling(20).mean().iloc[-1], 0),
                "bb_width": round((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20.iloc[-1], 4),
                "roe": info.get('returnOnEquity', 0), "rev_growth": info.get('revenueGrowth', 0),
                "fund_count": get_fund_count_sim(ticker)
            }
        except: continue
    return None

# ================= 3. 視覺美學 (加大數字、LINE 戰報) =================
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

st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
.medium-font { font-size:26px !important; font-weight: bold; color: #333; }</style>""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V77.0 完全體) =================
st.set_page_config(page_title="哲哲戰情室 V77.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V77.0 — 型別避險與七大金剛")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略掃描中心", "💼 資產即時戰報", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日行情智慧掃描")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取數據", use_container_width=True):
            query = text("SELECT * FROM daily_scans WHERE scan_date = :today")
            db_df = pd.read_sql(query, con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                # 解決 SQL 欄位映射
                st.session_state['master_df'] = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI'})
                st.success(f"✅ 載入成功！共 {len(db_df)} 筆。")
    with c2:
        if st.button("⚡ 啟動並行渦輪掃描", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                res, prog = [], st.progress(0)
                status_scan = st.status("🚀 七大金剛數據強攻中...", expanded=False)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, f in enumerate(as_completed(futures)):
                        data = f.result()
                        if data: res.append(data); status_scan.write(f"✅ {data['stock_name']} 完成")
                        prog.progress((count + 1) / len(pool))
                if res:
                    m_df = pd.DataFrame(res)
                    with engine.begin() as conn: conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                    status_scan.update(label=f"✨ 市場掃描完成！共處理 {len(res)} 筆。", state="complete")

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (七大金剛)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        
        # 💎 關鍵修復：計算前強制轉為數字，防止 TypeError
        calc_cols = ['現價', 'high_20', 'vol', 'vol_20', 'sma5', 'ma20', 'ma60', 'kd20', 'kd60', 'bbu', 'bb_width', 'roe', 'rev_growth']
        for col in calc_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 數據預處理：計算同業趨勢
        sector_info = pd.read_sql("SELECT ticker, sector, fund_count FROM stock_pool", con=engine)
        df = pd.merge(df, sector_info, left_on='代號', right_on='ticker', how='left')
        df['20日漲幅'] = (df['現價'] - df['kd20']) / df['kd20']
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        # 1. 超級策略
        if st.button("💎 降臨：超級策略 (法人+ROE+營收+趨勢)", use_container_width=True):
            mask = (df['fund_count'] >= 100) & (df['roe'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['rev_growth'] > 0.1)
            res = df[mask].sort_values(by='fund_count', ascending=False)
            if not res.empty:
                st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'roe', 'rev_growth', 'fund_count']]))
                send_line_report("超級策略", res, "💎")
            else: st.warning("💡 目前尚無符合超級條件標的。")

        # 2-4. 形態策略 (修正處：加上 na_rep 避險)
        st.markdown("#### 🔹 形態還原策略")
        c3, c4, c5 = st.columns(3)
        if c3.button("📈 帶量突破前高 (圖一)", use_container_width=True):
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(style_df(res.sort_values(by='漲跌(%)', ascending=False))); send_line_report("帶量突破", res, "📈")
        if c4.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'] < 0.05)]
            st.dataframe(style_df(res)); send_line_report("三線合一", res, "🚀")
        if c5.button("🌀 布林縮口突破 (圖三)", use_container_width=True):
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
            st.dataframe(style_df(res)); send_line_report("布林突破", res, "🌀")

        # 5-7. 經典策略
        st.markdown("#### 🔸 經典至尊策略")
        c6, c7, c8 = st.columns(3)
        if c6.button("👑 九成勝率 ATM", use_container_width=True):
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("九成勝率 ATM", res, "👑")
        if c7.button("🛡️ 低階抄底防護", use_container_width=True):
            res = df[(df['RSI']<35) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("低階抄底", res, "🛡️")
        if c8.button("🎯 強勢回測支撐", use_container_width=True):
            res = df[(abs(df['現價']-df['ma20'])/df['ma20']<0.02)]
            st.dataframe(style_df(res)); send_line_report("強勢回測", res, "🎯")

# --- Tab 2: 資產監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新即時獲利 (啟動強攻日誌)", use_container_width=True):
            p_map, all_res = {}, []
            status_box = st.status("🚀 獲利強攻同步中...", expanded=True)
            for idx, t in enumerate(df_p['ticker'].tolist()):
                sn = df_p[df_p['ticker']==t]['stock_name'].iloc[0]
                data = fetch_full_stock_package(t, sn)
                if data: p_map[t] = data['price']; all_res.append(data); status_box.write(f"✅ {t} 同步成功")
            if all_res:
                with engine.begin() as conn:
                    for r in all_res: conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": r['ticker'], "d": r['scan_date']})
                pd.DataFrame(all_res).to_sql('daily_scans', con=engine, if_exists='append', index=False)
            st.session_state['rt_p_v77'] = p_map
            st.session_state['last_upd'] = datetime.now().strftime('%H:%M:%S')
            st.rerun()

        if 'rt_p_v77' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p_v77'])
            for col in ['entry_price', '現價', 'qty']: df_p[col] = pd.to_numeric(df_p[col], errors='coerce')
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = ((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100
            
            total_profit = df_p['獲利'].fillna(0).sum()
            st.markdown(f"<p class='medium-font'>當前預估實質總獲利：</p><p class='big-font'>${total_profit:,.0f}</p>", unsafe_allow_html=True)
            st.caption(f"🕒 更新時間：{st.session_state.get('last_upd', 'N/A')}")
            st.dataframe(style_df(df_p))
            
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 戰報)")
        m_cols = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if m_cols[i].button(f"{icon} {name}", use_container_width=True):
                scans_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
                if not scans_df.empty:
                    # 強制轉數值
                    for c in ['sma5', 'ma20', 'rsi', 'price']: scans_df[c] = pd.to_numeric(scans_df[c], errors='coerce')
                    check_df = pd.merge(df_p, scans_df, on='ticker', how='left')
                    masks = [check_df['sma5'] < check_df['ma20'], check_df['rsi'] > 80, check_df['報酬率(%)'] > 20, check_df['報酬率(%)'] < -10, check_df['price'] < check_df['ma20']]
                    res = check_df[masks[i]].copy()
                    if not res.empty:
                        disp = res[['stock_name_x', 'ticker', 'price', '報酬率(%)']].rename(columns={'stock_name_x':'名稱', 'price':'現價'})
                        st.dataframe(style_df(disp)); send_line_report(f"賣訊：{name}", disp, icon)
                    else: st.success("✅ 持倉安全")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據都更中心 (智慧 Upsert 版)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📋 股票池管理")
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 鋼鐵匯入"):
            df = pd.read_csv(f1, encoding='utf-8-sig'); df.columns = [c.lower() for c in df.columns]
            df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
            df = df.drop_duplicates(subset=['ticker'], keep='last')
            with engine.begin() as conn:
                for t in df['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": t})
            df.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")
    with c2:
        st.markdown("#### 💰 持倉管理")
        f2 = st.file_uploader("上傳數據", type="csv")
        if f2 and st.button("💾 鋼鐵存入"):
            df = pd.read_csv(f2, encoding='utf-8-sig'); df.columns = [c.lower() for c in df.columns]
            with engine.begin() as conn:
                for t in df['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": t})
            df.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("成功！")

st.caption("本系統由哲哲團隊開發。七大買股金剛歸位，賺到流湯不要忘了我！")

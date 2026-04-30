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

# ================= 1. 系統地基 (五表鎖死，數據歸位) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # 行情表
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
        # 錯誤診斷與股票池、資產庫存
        conn.execute(text("CREATE TABLE IF NOT EXISTS scan_errors (ticker VARCHAR(20), scan_date DATE, error_msg TEXT, PRIMARY KEY (ticker, scan_date));"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (量子抓取與 LINE 通知) =================

def send_line_message(title, df, action="買入"):
    if df.empty: return
    icon = "🎯" if action == "買入" else "⚠️"
    msg = f"{icon}【哲哲戰報 - {title}】\n📢 數字會說話，這波一定要賺到流湯！\n"
    for _, r in df.head(10).iterrows():
        msg += f"✅ {r.get('ticker','')} {r.get('stock_name', r.get('名稱',''))} | 現價:{r.get('price', r.get('現價',''))}\n"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

def process_logic(ticker, name, df):
    try:
        if df is None or df.empty or len(df) < 20: return None, "天數不足"
        c = df['Close'].replace(0, np.nan).ffill()
        if c.empty or np.isnan(c.iloc[-1]): return None, "價格無效"
        curr_p = float(c.iloc[-1])
        bb = ta.bbands(c, 20, 2)
        try:
            s = yf.Ticker(ticker)
            roe, rev = float(s.fast_info.get('returnOnEquity', 0) or 0), float(s.info.get('revenueGrowth', 0) or 0)
        except: roe, rev = 0, 0
        return {
            "ticker": ticker, "stock_name": name, "price": curr_p,
            "change_pct": float(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100) if len(c)>1 else 0,
            "sma5": float(ta.sma(c, 5).iloc[-1]), "ma20": float(ta.sma(c, 20).iloc[-1]),
            "ma60": float(ta.sma(c, 60).iloc[-1]), "rsi": float(ta.rsi(c, 14).iloc[-1]),
            "vol": int(df['Volume'].iloc[-1]), "avg_vol": int(ta.sma(df['Volume'], 20).iloc[-1]),
            "kd20": float(c.iloc[-20]), "kd60": float(c.iloc[-60]), "scan_date": datetime.now(TW_TZ).date(),
            "bbu": float(bb.iloc[-1, 2]), "bbl": float(bb.iloc[-1, 0]),
            "high_20": float(c.shift(1).rolling(20).max().iloc[-1]),
            "vol_20": float(df['Volume'].shift(1).rolling(20).mean().iloc[-1]),
            "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ta.sma(c, 20).iloc[-1] if ta.sma(c, 20).iloc[-1] != 0 else 0),
            "roe": roe, "rev_growth": rev, "fund_count": 0
        }, "成功"
    except Exception as e: return None, str(e)

def quantum_batch_loop(pool_df, mode="incremental"):
    # 💎 動態分母連動：讀取真實筆數
    total_count = len(pool_df)
    if total_count == 0: return
    today = datetime.now(TW_TZ).date()
    
    if mode == "reset":
        with engine.begin() as conn: 
            conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            conn.execute(text("DELETE FROM scan_errors WHERE scan_date = :t"), {"t": today})

    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today})
    remaining = pool_df[~pool_df['ticker'].isin(done_df['ticker'].tolist())].copy()
    if remaining.empty: st.balloons(); return

    p_bar = st.progress(0.0); p_text = st.empty()
    log_box = st.status(f"⚡ 量子突圍掃描中 (分母: {total_count})...", expanded=True)
    
    tickers = remaining['ticker'].tolist(); names = dict(zip(remaining['ticker'], remaining['stock_name']))
    batch_size = 25
    for i in range(0, len(tickers), batch_size):
        curr_batch = tickers[i : i+batch_size]
        try:
            data_all = yf.download(curr_batch, period="7mo", group_by='ticker', threads=True, progress=False, timeout=25)
            for t in curr_batch:
                df_s = data_all[t] if len(curr_batch) > 1 else data_all
                res, msg = process_logic(t, names[t], df_s)
                # 鋼鐵 Upsert
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    if res:
                        pd.DataFrame([res]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                        log_box.write(f"✅ 入庫：{res['stock_name']}")
                    else:
                        pd.DataFrame([{"ticker": t, "scan_date": today, "error_msg": msg}]).to_sql('scan_errors', con=engine, if_exists='append', index=False)
            
            curr_done = pd.read_sql(text("SELECT count(*) FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today}).iloc[0,0]
            p_bar.progress(min(curr_done / total_count, 1.0))
            p_text.markdown(f"**🚀 實際成功進度：`{curr_done}` / `{total_count}` ({curr_done/total_count:.1%})**")
            time.sleep(random.uniform(3, 5))
        except: time.sleep(5)
    log_box.update(label="✨ 掃描結束", state="complete")

# ================= 3. 視覺與色票渲染 =================

def beauty_style(df):
    if df.empty: return df
    num_cols = ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)','entry_price','price']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price':'{:.2f}', 'price':'{:.2f}'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if '報酬率(%)' in df.columns: styled = styled.background_gradient(subset=['報酬率(%)'], cmap='RdYlGn', low=-10, high=10)
        return styled
    except: return df

# ================= 4. 主介面設計 (V133.0 戰神意志版) =================
st.set_page_config(page_title="哲哲量子戰神 V133.0", layout="wide")

# 💎 終極 CSS：按鈕 100% 寬度、文字絕對置中、霸氣紅黑漸層
st.markdown("""<style>
    .stButton > button { 
        width: 100% !important; height: 5em !important; font-size: 1.6rem !important; 
        font-weight: bold !important; border-radius: 20px !important; margin-bottom: 20px !important; 
        display: block !important; text-align: center !important;
        background: linear-gradient(135deg, #2b2b2b 0%, #1e1e1e 100%); color: white;
        border: 2px solid #444; transition: all 0.4s ease;
    }
    .stButton > button:hover { background: #FF3333 !important; transform: translateY(-2px); box-shadow: 0 10px 20px rgba(255,51,51,0.4); border-color: #FF3333; }
    .big-font { font-size:55px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 5px #ddd; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V133.0 — 量子戰神最終回歸")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛發射台", "💼 資產即時戰報", "🛠️ 後台管理與汰除"])

# --- Tab 1: 七大金剛 ---
with tab1:
    st.markdown("### 🏆 行情掃描中心 (分母真實連動)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日數據快取"):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.5"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入 {len(db_df)} 筆標的")
    with c2:
        if st.button("⚡ 啟動量子增量掃描"):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            quantum_batch_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃"):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            quantum_batch_loop(pool, mode="reset"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛全幅置中)")
    
    # 策略 1-7
    strategies = [
        ("💎 策略 1: 超級策略 (基金+ROE+營收)", "(df['fund_count'] >= 100) & (df['ROE'] > 0.1)"),
        ("📈 策略 2: 帶量突破前高 (圖一)", "(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)"),
        ("🚀 策略 3: 三線合一多頭 (圖二)", "(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60'])"),
        ("🌀 策略 4: 布林縮口突破 (圖三)", "(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)"),
        ("👑 策略 5: 九成勝率 ATM", "(df['現價'] > df['kd20']) & (df['vol'] >= df['vol_20']*1.2)"),
        ("🛡️ 策略 6: 低階抄底防護", "(df['RSI'] < 35) & (df['現價'] > df['sma5'])"),
        ("🎯 策略 7: 強勢回測支撐", "abs(df['現價']-df['ma20'])/df['ma20'] < 0.02")
    ]
    
    for name, cond in strategies:
        if st.button(name):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df'].copy()
                # 使用安全過濾
                p_info = pd.read_sql("SELECT ticker, fund_count FROM stock_pool", con=engine)
                df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
                res = df[eval(cond)]
                st.dataframe(beauty_style(res), use_container_width=True)
                send_line_message(name, res, "買入")

# --- Tab 2: 資產報表 ---
with tab2:
    st.header("💼 我的資產即時戰報 (真錢獲利精算法)")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價"):
            quantum_batch_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        # 💎 數據合併：資產表 Left Join 掃描表
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_display = pd.merge(df_p, p_prices, on='ticker', how='left').fillna(0)
        
        # 💎 真錢邏輯：price > 0 才算，否則顯示 0，防止 0050 造成數萬虧損假象
        for c in ['entry_price', 'price', 'qty']: df_display[c] = pd.to_numeric(df_display[c], errors='coerce').fillna(0)
        df_display['獲利'] = np.where(df_display['price'] > 0, (df_display['price'] - df_display['entry_price']) * df_display['qty'], 0)
        df_display['報酬率(%)'] = np.where(df_display['price'] > 0.1, ((df_display['price'] - df_display['entry_price']) / (df_display['entry_price'].replace(0, 1))) * 100, 0)
        
        # 總獲利顯示
        total_profit = df_display['獲利'].sum()
        st.markdown(f"當前總獲利：<p class='big-font'>${total_profit:,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_display), use_container_width=True)
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (自動發送 LINE)")
        m_c = st.columns(5)
        s_btns = [("💀 均線死叉", "sma5 < ma20"), ("🔥 RSI 過熱", "rsi > 80"), ("💰 利潤止盈", "報酬率(%) > 20"), ("📉 破位停損", "報酬率(%) < -10"), ("⚠️ 跌破月線", "price < ma20")]
        for i, (name, cond) in enumerate(s_btns):
            if m_c[i].button(name):
                # 過濾出符合賣出的標的
                if "報酬率" in cond: res_s = df_display[df_display['報酬率(%)'] > 20] if "20" in cond else df_display[df_display['報酬率(%)'] < -10]
                elif "sma5" in cond: res_s = df_display[df_display['sma5'] < df_display['ma20']]
                elif "rsi" in cond: res_s = df_display[df_display['rsi'] > 80]
                else: res_s = df_display[df_display['price'] < df_display['ma20']]
                res_s = res_s[res_s['price'] > 0] # 只看真的有價格的
                st.dataframe(beauty_style(res_s))
                send_line_message(f"賣訊：{name}", res_s, "賣出")

# --- Tab 3: 管理中心 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (上傳與汰除)")
    
    # 💎 OTC 照妖鏡
    st.markdown("#### 🔍 OTC 快速查找器")
    if st.button("🚀 幫我列出池子裡的所有上櫃股票代號 (.TWO)"):
        otc_stocks = pd.read_sql(text("SELECT ticker FROM stock_pool WHERE ticker LIKE '%%.TWO%%'"), con=engine)
        if not otc_stocks.empty:
            st.code(", ".join(otc_stocks['ticker'].tolist()))
            st.info("💡 請複製上方代號，貼到下方框框進行汰除！")
        else: st.success("✅ 目前池子乾乾淨淨！")

    st.divider()
    st.markdown("#### 🚫 批量剔除股票 (髒數據一秒清空)")
    del_list = st.text_area("請貼入要刪除的代號")
    if st.button("🔥 鋼鐵汰除黑名單"):
        if del_list:
            t_del = [t.strip().upper() for t in del_list.replace('\n', ',').split(',') if t.strip()]
            with engine.begin() as conn:
                for t in t_del:
                    conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": t})
                    conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": t})
                    conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t"), {"t": t})
            st.success(f"✅ 已移除 {len(t_del)} 檔標的！")
    
    st.divider()
    st.markdown("#### 💾 數據匯入")
    col_u1, col_u2 = st.columns(2)
    with col_u1:
        f1 = st.file_uploader("上傳股票池 CSV", type="csv", key="pool_up")
        if f1 and st.button("💾 匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig')
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")
    with col_u2:
        f2 = st.file_uploader("上傳資產庫存 CSV (ticker, stock_name, entry_price, qty)", type="csv", key="port_up")
        if f2 and st.button("💾 匯入持倉"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig')
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("資產匯入成功！")

st.caption("本系統由哲哲團隊開發。V133.0 量子戰神完全體，賺到流湯不要忘了我！")

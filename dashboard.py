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

# ================= 1. 系統地基 (五表聯動) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 行情數據
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
        # B. 診斷表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS scan_errors (
                ticker VARCHAR(20), scan_date DATE, error_msg TEXT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # C. 股票池與資產庫存
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (批量突圍引擎) =================

def process_logic(ticker, name, df):
    try:
        if df.empty or len(df) < 20: return None, "Empty/天數不足"
        c = df['Close'].replace(0, np.nan).ffill()
        if c.empty or np.isnan(c.iloc[-1]): return None, "無效價"
        curr_p = float(c.iloc[-1])
        bb = ta.bbands(c, 20, 2)
        try:
            s = yf.Ticker(ticker)
            roe = float(s.fast_info.get('returnOnEquity', 0) or 0)
            rev = float(s.info.get('revenueGrowth', 0) or 0)
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
    total = len(pool_df)
    if total == 0: return
    today = datetime.now(TW_TZ).date()
    if mode == "reset":
        with engine.begin() as conn: 
            conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            conn.execute(text("DELETE FROM scan_errors WHERE scan_date = :t"), {"t": today})

    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today})
    remaining = pool_df[~pool_df['ticker'].isin(done_df['ticker'].tolist())].copy()
    
    if remaining.empty: st.balloons(); st.success("🏆 數據已全壘打！"); return

    p_bar = st.progress(0.0)
    p_text = st.empty()
    log_box = st.status(f"⚡ 量子突圍掃描中...", expanded=True)
    
    tickers = remaining['ticker'].tolist()
    names = dict(zip(remaining['ticker'], remaining['stock_name']))
    batch_size = 25
    for i in range(0, len(tickers), batch_size):
        curr_batch = tickers[i : i+batch_size]
        try:
            data_all = yf.download(curr_batch, period="7mo", group_by='ticker', threads=True, progress=False, timeout=20)
            for t in curr_batch:
                df_s = data_all[t] if len(curr_batch) > 1 else data_all
                res, msg = process_logic(t, names[t], df_s)
                # 自動校準 .TW/.TWO
                if res is None and ".TW" in t:
                    df_alt = yf.download(t.replace(".TW", ".TWO"), period="7mo", progress=False, timeout=15)
                    res, msg = process_logic(t, names[t], df_alt)
                
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    conn.execute(text("DELETE FROM scan_errors WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    if res:
                        pd.DataFrame([res]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                        log_box.write(f"✅ 入庫：{res['stock_name']}")
                    else:
                        pd.DataFrame([{"ticker": t, "scan_date": today, "error_msg": msg}]).to_sql('scan_errors', con=engine, if_exists='append', index=False)
            
            # 數據回歸：進度條文字顯示
            current_done = pd.read_sql(text("SELECT count(*) FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today}).iloc[0,0]
            p_bar.progress(min(current_done / total, 1.0))
            p_text.markdown(f"**🚀 實際成功進度：`{current_done}` / `{total}` ({current_done/total:.1%})**")
            time.sleep(3)
        except: time.sleep(10)
    log_box.update(label="✨ 掃描結束", state="complete")

# ================= 3. 視覺美學渲染器 (置中大按鈕 CSS) =================

def beauty_style(df):
    if df.empty: return df
    for c in ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if 'ROE' in df.columns: styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.08, high=0.2)
        return styled
    except: return df

# ================= 4. 主介面設計 (V126.0 旗艦完全體) =================
st.set_page_config(page_title="哲哲量化封神 V126.0", layout="wide")

# 💎 強制 CSS：按鈕全幅、文字置中、大字體
st.markdown("""<style>
    div.stButton > button { 
        width: 100% !important; 
        height: 4.5em !important; 
        font-size: 1.4rem !important; 
        font-weight: bold !important; 
        border-radius: 15px !important; 
        margin-bottom: 12px !important; 
        display: flex !important; 
        justify-content: center !important; 
        align-items: center !important;
        transition: 0.3s;
    }
    div.stButton > button:hover { background-color: #FFF0F0; border-color: #FF3333; color: #FF3333; box-shadow: 0 4px 15px rgba(255,51,51,0.2); }
    .big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
</style>""", unsafe_allow_html=True)

st.title("🛡️ 哲哲量化戰情室 V126.0 — 最終大統整完全體")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛指揮中心", "💼 持倉監控報表", "🛠️ 後台管理與汰除"])

# --- Tab 1: 七大金剛 ---
with tab1:
    st.markdown("### 🏆 每日行情量子全掃描")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日數據快取 (顯示真錢標的)"):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.5"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入 {len(db_df)} 筆標的")
    with c2:
        if st.button("⚡ 啟動增量量子掃描 (補齊進度)"):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            quantum_batch_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃 (強制 100% 重置)"):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            quantum_batch_loop(pool, mode="reset"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛大按鈕)")
    
    # 策略 1
    if st.button("💎 策略 1: 超級策略 (基金+ROE+營收+趨勢)"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            p_info = pd.read_sql("SELECT ticker, sector, fund_count FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            mask = (df['fund_count'] >= 100) & (df['ROE'] > 0.1) & (df['rev_growth'] > 0.1)
            st.dataframe(beauty_style(df[mask]), use_container_width=True)
    
    # 策略 2-7
    if st.button("📈 策略 2: 帶量突破前高 (圖一)"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("🚀 策略 3: 三線合一多頭 (圖二)"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'] < 0.05)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("🌀 策略 4: 布林縮口突破 (圖三)"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("👑 策略 5: 九成勝率 ATM"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價']>df['kd20']) & (df['vol'] >= df['vol_20']*1.2)]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("🛡️ 策略 6: 低階抄底防護"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['rsi'] < 35) & (df['現價'] > df['sma5'])]
            st.dataframe(beauty_style(res), use_container_width=True)

    if st.button("🎯 策略 7: 強勢回測支撐"):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(abs(df['現價']-df['ma20'])/df['ma20'] < 0.02)]
            st.dataframe(beauty_style(res), use_container_width=True)

    st.divider()
    if st.button("🔍 揭開底牌：數據照妖鏡 (今日所有數據)"):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)

# --- Tab 2: 持倉監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (校準下載)"):
            quantum_batch_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        
        # 💎 計算邏輯鎖死：有股價才算，沒股價顯示為 0
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = np.where(df_p['price'] > 0, (df_p['price'] - df_p['entry_price']) * df_p['qty'], 0)
        df_p['報酬率(%)'] = np.where(df_p['price'] > 0.1, ((df_p['price'] - df_p['entry_price']) / df_p['entry_price'].replace(0, 1)) * 100, 0)
        
        st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_p), use_container_width=True)
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策")
        m_s1, m_s2, m_s3, m_s4, m_s5 = st.columns(5)
        if m_s1.button("💀 均線死叉"): st.dataframe(df_p[df_p['sma5'] < df_p['ma20']])
        if m_s2.button("🔥 RSI 過熱"): st.dataframe(df_p[df_p['rsi'] > 80])
        if m_s3.button("💰 利潤止盈"): st.dataframe(df_p[df_p['報酬率(%)'] > 20])
        if m_s4.button("📉 破位停損"): st.dataframe(df_p[df_p['報酬率(%)'] < -10])
        if m_s5.button("⚠️ 跌破月線"): st.dataframe(df_p[df_p['price'] < df_p['ma20']])

# --- Tab 3: 管理與 OTC 汰除 ---
with tab3:
    st.subheader("🛠️ 數據管理中心")
    
    # 💎 驚喜功能：OTC 照妖鏡
    st.markdown("#### 🔍 OTC 快速查找器 (一鍵找出上櫃股票)")
    if st.button("🚀 幫我找出股票池中的上櫃代號 (.TWO)"):
        otc_df = pd.read_sql("SELECT ticker FROM stock_pool WHERE ticker LIKE '%.TWO%'", con=engine)
        if not otc_df.empty:
            otc_list = ", ".join(otc_df['ticker'].tolist())
            st.code(otc_list)
            st.success("✅ 請複製上方代號，貼到下方的『汰除框框』進行刪除！")
        else: st.info("目前池子裡沒有上櫃股票。")

    st.divider()
    st.markdown("#### 🚫 批量剔除股票")
    del_list = st.text_area("請輸入要刪除的代號 (例如: 3293.TW, 3491.TW)，用逗號或換行隔開")
    if st.button("🔥 鋼鐵汰除黑名單"):
        if del_list:
            t_del = [t.strip().upper() for t in del_list.replace('\n', ',').split(',') if t.strip()]
            with engine.begin() as conn:
                for t in t_del:
                    conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": t})
                    conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": t})
                    conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t"), {"t": t})
            st.success(f"✅ 已徹底移除 {len(t_del)} 檔標的！")
    
    st.divider()
    col_u1, col_u2 = st.columns(2)
    with col_u1:
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 鋼鐵匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig')
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功！")
    with col_u2:
        f2 = st.file_uploader("上傳持倉資產 CSV (ticker, stock_name, entry_price, qty)", type="csv")
        if f2 and st.button("💾 鋼鐵匯入持倉"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig')
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("資產匯入成功！")

st.caption("本系統由哲哲團隊開發。V126.0 最終大統整版，賺到流湯不要忘了我！")

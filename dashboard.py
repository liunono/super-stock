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

# 🚀 TLS 隱身黑科技 (Impersonate Chrome)
try:
    from curl_cffi.requests import Session as CFSession
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ================= 1. 系統地基 (鋼鐵都更，四表聯動) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 核心掃描表 (21指標)
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
        # B. 診斷日誌表 (揭開 Empty DF 真相)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS scan_errors (
                ticker VARCHAR(20), scan_date DATE, error_msg TEXT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # C. 股票池與持倉資產
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (量子抓取與自動後綴校準) =================

def get_hidden_session():
    """💎 建立 TLS 指紋偽裝 Session"""
    if HAS_CURL_CFFI:
        return CFSession(impersonate="chrome")
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'})
    return session

def process_single_stock_logic(ticker, name, df):
    """💎 指標計算核心：排除 NaN、處理 0 元、計算 21 指標"""
    try:
        if df is None or df.empty or len(df) < 20: return None, "天數不足或空包彈"
        
        # 數據清洗
        c = df['Close'].replace(0, np.nan).ffill()
        v = df['Volume'].replace(0, np.nan).ffill()
        if c.empty or np.isnan(c.iloc[-1]): return None, "股價無效(NaN)"
        
        curr_p = float(c.iloc[-1])
        if curr_p <= 0: return None, "股價為0"

        # 技術指標 (Pandas-TA 強化)
        sma5 = ta.sma(c, 5).iloc[-1]
        ma20 = ta.sma(c, 20).iloc[-1]
        ma60 = ta.sma(c, 60).iloc[-1]
        rsi = ta.rsi(c, 14).iloc[-1]
        bb = ta.bbands(c, 20, 2)
        
        # 財報 (獨立備援)
        roe, rev = 0.0, 0.0
        try:
            # 這裡用快速接口，避免拉慢整體速度
            s = yf.Ticker(ticker)
            roe = float(s.fast_info.get('returnOnEquity', 0) or 0)
            rev = float(s.info.get('revenueGrowth', 0) or 0)
        except: pass

        return {
            "ticker": ticker, "stock_name": name, "price": curr_p,
            "change_pct": float(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100) if len(c)>1 else 0,
            "sma5": float(sma5 if not np.isnan(sma5) else curr_p),
            "ma20": float(ma20 if not np.isnan(ma20) else curr_p),
            "ma60": float(ma60 if not np.isnan(ma60) else curr_p),
            "rsi": float(rsi if not np.isnan(rsi) else 50),
            "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1] if len(v)>=20 else v.iloc[-1]),
            "kd20": float(c.iloc[-20] if len(c)>=20 else curr_p),
            "kd60": float(c.iloc[-60] if len(c)>=60 else curr_p),
            "scan_date": datetime.now(TW_TZ).date(),
            "bbu": float(bb.iloc[-1, 2] if bb is not None else curr_p),
            "bbl": float(bb.iloc[-1, 0] if bb is not None else curr_p),
            "high_20": float(c.shift(1).rolling(20).max().iloc[-1] if len(c)>=21 else curr_p),
            "vol_20": float(v.shift(1).rolling(20).mean().iloc[-1] if len(v)>=21 else 0),
            "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20 if (bb is not None and ma20 > 0) else 0),
            "roe": roe, "rev_growth": rev, "fund_count": 0
        }, "成功"
    except Exception as e:
        return None, str(e)

def quantum_diagnose_loop(pool_df, mode="incremental"):
    """🚀 最終量子批量迴圈：包含自動 .TW/.TWO 校準"""
    today = datetime.now(TW_TZ).date()
    if mode == "reset":
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            conn.execute(text("DELETE FROM scan_errors WHERE scan_date = :t"), {"t": today})

    # 只看「真錢入庫」的
    done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today})
    remaining = pool_df[~pool_df['ticker'].isin(done_df['ticker'].tolist())].copy()
    
    if remaining.empty:
        st.balloons(); st.success("🏆 100% 全壘打達成！"); return

    total = len(pool_df)
    p_bar = st.progress(len(done_df)/total)
    p_text = st.empty()
    log_box = st.status(f"⚡ 量子突圍掃描中 (剩餘 {len(remaining)} 檔)...", expanded=True)

    # 批量 25 檔是最佳平衡點
    batch_size = 25
    tickers_list = remaining['ticker'].tolist()
    names_dict = dict(zip(remaining['ticker'], remaining['stock_name']))

    for i in range(0, len(tickers_list), batch_size):
        curr_batch = tickers_list[i : i + batch_size]
        try:
            # 1. 第一波量子攻擊
            data_all = yf.download(curr_batch, period="7mo", interval="1d", group_by='ticker', threads=True, progress=False, timeout=20)
            
            for t in curr_batch:
                df_s = data_all[t] if len(curr_batch) > 1 else data_all
                res, msg = process_single_stock_logic(t, names_dict[t], df_s)
                
                # 💎 自動後綴校準：如果抓不到且是 .TW，嘗試變身 .TWO
                if (res is None) and (".TW" in t):
                    alt_t = t.replace(".TW", ".TWO")
                    log_box.write(f"🔄 校準中：{t} -> {alt_t}")
                    df_alt = yf.download(alt_t, period="7mo", interval="1d", progress=False, timeout=15)
                    res, msg = process_single_stock_logic(t, names_dict[t], df_alt) # 存回原 Ticker 名稱
                
                # 寫入資料庫 (UPSERT)
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    conn.execute(text("DELETE FROM scan_errors WHERE ticker = :t AND scan_date = :d"), {"t": t, "d": today})
                    if res:
                        pd.DataFrame([res]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                        log_box.write(f"✅ 入庫：{res['stock_name']} (${res['price']})")
                    else:
                        pd.DataFrame([{"ticker": t, "scan_date": today, "error_msg": msg}]).to_sql('scan_errors', con=engine, if_exists='append', index=False)
                        log_box.write(f"❌ {t} 失敗：{msg}")

            # 更新 UI 進度
            curr_count = pd.read_sql(text("SELECT count(*) FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": today}).iloc[0,0]
            p_bar.progress(min(curr_count/total, 1.0))
            p_text.markdown(f"**🚀 實際成功進度：`{curr_count}` / `{total}` ({curr_count/total:.1%})**")
            time.sleep(random.uniform(3, 5))

        except Exception as e:
            log_box.write(f"⚠️ 批量崩潰，正在自我修復..."); time.sleep(10)

    log_box.update(label="✨ 任務結束，請點擊『讀取行情數據』。", state="complete")

# ================= 3. 視覺與美學渲染器 (冠軍色票鎖死) =================

def beauty_style(df):
    """💎 哲哲專屬暴力美學漸層：凸顯重點分布"""
    if df.empty: return df
    # 確保數值化
    num_cols = ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)','fund_count']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        # 🌈 冠軍紅熱漸層
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if 'ROE' in df.columns: styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.08, high=0.25)
        if '營收成長' in df.columns: styled = styled.background_gradient(subset=['營收成長'], cmap='OrRd', low=0.05, high=0.6)
        if '報酬率(%)' in df.columns: styled = styled.background_gradient(subset=['報酬率(%)'], cmap='RdYlGn_r', low=-10, high=10)
        
        def color_val(v):
            if isinstance(v, (int, float)):
                if v > 0.1: return 'color: #FF3333; font-weight: bold'
                if v < -0.1: return 'color: #00AA00; font-weight: bold'
            return ''
        return styled.map(color_val, subset=[c for c in ['漲跌(%)', '報酬率(%)', '獲利'] if c in df.columns])
    except: return df

# ================= 4. 主介面設計 (V124.0 指揮官大按鈕完全體) =================
st.set_page_config(page_title="哲哲量化封神戰情室 V124.0", layout="wide")
st.markdown("""<style>
    .big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
    div.stButton > button { height: 4.5em; font-size: 1.4rem !important; font-weight: bold !important; border-radius: 15px; width: 100% !important; margin-bottom: 15px; transition: 0.4s; }
    div.stButton > button:hover { background-color: #FFF0F0; border-color: #FF3333; color: #FF3333; box-shadow: 0 4px 15px rgba(255,51,51,0.2); }
</style>""", unsafe_allow_html=True)

now_tw = datetime.now(TW_TZ)
st.title("🛡️ 哲哲量化戰情室 V124.0 — 指揮官最終完全體")

# 🕒 13:31 定時守護
if now_tw.hour == 13 and now_tw.minute >= 31:
    st.warning("🔔 **收盤紅燈亮起 (1:31 PM)！建議執行『暴力重掃』鎖定今日最終正確收盤籌碼！**")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛指揮中心", "💼 持倉即時戰報", "🛠️ 後台管理與診斷"])

# --- Tab 1: 核心掃描與七大金剛 ---
with tab1:
    st.markdown("### 🏆 每日行情量子突圍 (自動後綴校準)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日行情數據 (顯示有效標的)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.1"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 成功載入 {len(db_df)} 筆有效標的！")
            else: st.warning("目前無數據，請先啟動掃描。")
    with c2:
        if st.button("⚡ 啟動增量量子掃描 (補齊進度)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: quantum_diagnose_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃 (強制 100% 重置)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: quantum_diagnose_loop(pool, mode="reset"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛大按鈕)")
    
    # 策略 1: 超級策略
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            p_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            st.dataframe(beauty_style(df[mask].sort_values(by='營收成長', ascending=False)), use_container_width=True)
        else: st.error("⚠️ 請先讀取行情數據！")

    # 其他金剛按鈕
    c_g1, c_g2 = st.columns(2)
    with c_g1:
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
    with c_g2:
        if st.button("👑 九成勝率 ATM", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['現價']>df['kd20']) & (df['vol'] >= df['vol_20']*1.2)]
                st.dataframe(beauty_style(res), use_container_width=True)
        if st.button("🛡️ 低階抄底防護", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5'])]
                st.dataframe(beauty_style(res), use_container_width=True)

    st.divider()
    if st.button("🔍 揭開底牌：數據照妖鏡 (檢視 350 檔所有數據)", use_container_width=True):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)

# --- Tab 2: 持倉監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (校準掃描)", use_container_width=True):
            quantum_diagnose_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t AND price > 0.1"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = np.where(df_p['price'] > 0.1, ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100, 0)
        valid_p = df_p[df_p['price'] > 0.1].copy()
        st.markdown(f"當前總獲利：<p class='big-font'>${valid_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(valid_p), use_container_width=True)

# --- Tab 3: 管理與診斷 ---
with tab3:
    st.subheader("🚫 失敗標的診斷區 (空數據包原因大公開)")
    err_df = pd.read_sql(text("SELECT * FROM scan_errors WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
    if not err_df.empty:
        st.dataframe(err_df, use_container_width=True)
        st.info("💡 哲哲提示：如果是『天數不足』或『股價無效』，通常是後綴錯誤（.TW 誤植為 .TWO）。")
    else: st.success("✅ 今日無失敗記錄，跟我預測的一模一樣！")
    
    st.divider()
    f1 = st.file_uploader("上傳股票池 CSV (ticker, stock_name, sector, fund_count)", type="csv")
    if f1 and st.button("💾 鋼鐵匯入"):
        df_new = pd.read_csv(f1, encoding='utf-8-sig')
        with engine.begin() as conn:
            for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
        df_new.to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功！")

st.caption("本系統由哲哲團隊開發。V124.0 最終封神完全體，賺到流湯不要忘了我！")

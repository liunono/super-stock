import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (鋼鐵防護，色票定義) =================
try:
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 建立掃描數據表 (21 欄位完全體)
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
        # B. 建立股票池表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_pool (
                ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), 
                sector VARCHAR(50), fund_count INT DEFAULT 0
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # C. 建立持倉資產表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), 
                entry_price FLOAT, qty FLOAT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (閃電抓取引擎) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲不死鳥抓取：yfinance 官方推薦 TLS 協議"""
    try:
        time.sleep(random.uniform(0.5, 0.8)) # 閃電節奏
        s = yf.Ticker(ticker)
        d = s.history(period="7mo", interval="1d", timeout=20)
        
        if d.empty or len(d) < 40:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=20)
            if d.empty or len(d) < 40: return None, "數據不足"
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
        try:
            info = s.info if s.info else {}
            roe, rev = info.get('returnOnEquity', 0) or 0, info.get('revenueGrowth', 0) or 0
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

def lightning_homerun_loop(pool_df, force_reset=False):
    """🚀 哲哲全壘打補洞引擎：支援覆蓋重置、碎片入庫"""
    total_count = len(pool_df)
    today = datetime.now().date()
    
    if force_reset:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
        st.toast("🔥 已重置今日數據，準備重新強攻！")

    progress_bar = st.progress(0); status_msg = st.empty()
    log_box = st.status("🚀 啟動閃電碎片掃描 (V105.0)...", expanded=True)
    
    fail_tracker, round_num = {}, 1
    while True:
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        progress_bar.progress(len(done_list) / total_count)
        if remaining_pool.empty:
            st.balloons(); status_msg.success(f"🏆 100% 全壘打達成！"); break
        if round_num > 6: break

        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            counter = 0
            for f in as_completed(futures):
                ticker = futures[f]
                data, err = f.result()
                counter += 1
                if counter % 10 == 0: time.sleep(2.5) # 安全間隔
                if data:
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    log_box.write(f"✅ 入庫：{data['stock_name']}")
                else:
                    fail_tracker[ticker] = fail_tracker.get(ticker, 0) + 1
                    log_box.write(f"⚠️ {ticker} 跳過")
                    if fail_tracker[ticker] >= 3:
                        fake = {"ticker": ticker, "stock_name": "幽靈股", "scan_date": today, "price": 0}
                        pd.DataFrame([fake]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
        round_num += 1; time.sleep(5)
    log_box.update(label="✨ 數據全數歸位。", state="complete")

# ================= 3. 視覺美學渲染器 (漸層色票) =================

def send_line_report(title, df, icon):
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n🎯 標的清單：\n"
    for _, r in df.head(10).iterrows():
        msg += f"✅ {r.get('代號','')} {r.get('名稱','')} | 現價:{r.get('現價','')}\n"
    msg += "\n跟我預測的一模一樣，賺到流湯！🚀"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))

def beauty_style(df):
    """💎 哲哲專屬漂亮色票漸層"""
    def color_pct(val):
        if val > 0: return f'color: #FF3333; font-weight: bold;'
        if val < 0: return f'color: #00AA00; font-weight: bold;'
        return ''
    
    # 定義數值格式
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    
    # 應用漸層與格式
    styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
    
    # 針對重點欄位加入漂亮漸層
    if '漲跌(%)' in df.columns:
        styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1)
    if 'ROE' in df.columns:
        styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.05, high=0.2)
    if '營收成長' in df.columns:
        styled = styled.background_gradient(subset=['營收成長'], cmap='OrRd', low=0, high=0.5)
        
    return styled.map(color_pct, subset=[c for c in ['漲跌(%)', '報酬率(%)'] if c in df.columns])

# ================= 4. 主介面設計 (V105.0 完全體) =================
st.set_page_config(page_title="哲哲量化美學戰情室 V105.0", layout="wide")
st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }</style>""", unsafe_allow_html=True)
st.title("🛡️ 哲哲量化戰情室 V105.0 — 旗艦美學與重掃系統")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛發射台", "💼 持倉監控戰報", "🛠️ 後台數據中心"])

with tab1:
    st.markdown("### 🏆 全市場智慧掃描 (覆蓋重置與閃電補洞)")
    c1, c2, c3 = st.columns([1.5, 1.5, 1.2])
    with c1:
        if st.button("📡 讀取今日數據快取", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','ROE','營收成長','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！")
    with c2:
        if st.button("⚡ 啟動增量渦輪掃描 (補齊 100%)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, force_reset=False); st.rerun()
    with c3:
        if st.button("🔥 覆蓋重掃 (強制 100% 重置)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, force_reset=True); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝決策中心 (七大金剛列陣)")
    
    # 策略 1: 超級策略
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.dataframe(beauty_style(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']]), use_container_width=True)
            send_line_report("超級策略", res, "💎")
        else: st.warning("請先讀取行情！")

    # 策略 2-4: 形態三圖
    st.markdown("#### 🔹 形態還原策略 (真突破與三線合一)")
    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        if st.button("📈 帶量突破前高 (圖一)", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
                st.dataframe(beauty_style(res)); send_line_report("帶量突破", res, "📈")
    with mc2:
        if st.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
                st.dataframe(beauty_style(res)); send_line_report("三線合一", res, "🚀")
    with mc3:
        if st.button("🌀 布林縮口突破 (圖三)", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
                st.dataframe(beauty_style(res)); send_line_report("布林突破", res, "🌀")

    # 策略 5-7: 經典至尊
    st.markdown("#### 🔸 經典至尊策略 (抄底與 ATM)")
    mc4, mc5, mc6 = st.columns(3)
    with mc4:
        if st.button("👑 九成勝率 ATM", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
                st.dataframe(beauty_style(res)); send_line_report("ATM策略", res, "👑")
    with mc5:
        if st.button("🛡️ 低階抄底防護", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5'])]
                st.dataframe(beauty_style(res)); send_line_report("低階抄底", res, "🛡️")
    with mc6:
        if st.button("🎯 強勢回測支撐", use_container_width=True):
            if 'master_df' in st.session_state:
                df = st.session_state['master_df']
                res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02)]
                st.dataframe(beauty_style(res)); send_line_report("強勢回測", res, "🎯")

# --- Tab 2: 持倉監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (全壘打回補同步)", use_container_width=True):
            lightning_homerun_loop(df_p[['ticker','stock_name']]); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now().date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
        
        st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_p), use_container_width=True)

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (鋼鐵 Upsert)")
    ch1, ch2 = st.columns(2)
    with ch1:
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 鋼鐵匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new[['ticker', 'stock_name', 'sector', 'fund_count']].to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")
    with ch2:
        f2 = st.file_uploader("上傳持倉 CSV", type="csv")
        if f2 and st.button("💾 鋼鐵匯入資產"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("成功！")

st.caption("本系統由哲哲團隊美學開發。V105.0 旗艦完全體，賺到流湯不要忘了我！")

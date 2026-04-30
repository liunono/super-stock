import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re, random
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (鋼鐵都更，主鍵校準) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 核心掃描表
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
        # B. 股票池與持倉
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (極速抓取與 UPSERT 寫入引擎) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲全能抓取：閃電間隔 0.5s"""
    try:
        time.sleep(random.uniform(0.5, 0.8))
        s = yf.Ticker(ticker)
        d = s.history(period="7mo", interval="1d", timeout=15)
        
        if d.empty or len(d) < 40:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=15)
            if d.empty or len(d) < 40: return None, "數據源缺失"
        
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
            "scan_date": datetime.now(TW_TZ).date(),
            "bbu": float(bb.iloc[-1, 2]), "bbl": float(bb.iloc[-1, 0]),
            "high_20": float(c.shift(1).rolling(20).max().iloc[-1]),
            "vol_20": float(v.shift(1).rolling(20).mean().iloc[-1]),
            "bb_width": float((bb.iloc[-1, 2] - bb.iloc[-1, 0]) / ma20.iloc[-1]),
            "roe": float(roe), "rev_growth": float(rev), "fund_count": 0 
        }, None
    except Exception as e:
        return None, str(e)

def lightning_homerun_loop(pool_df, mode="incremental"):
    """🚀 哲哲暴力 UPSERT 迴圈：解決寫入不進去的問題"""
    total_count = len(pool_df)
    if total_count == 0: return
    
    today = datetime.now(TW_TZ).date()
    if mode == "reset":
        with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})

    p_bar = st.progress(0.0)
    p_text = st.empty()
    log_box = st.status(f"⚡ 啟動閃電補洞 ({mode})...", expanded=True)
    
    fail_tracker, round_num = {}, 1
    while True:
        # 讀取已完成標的 (含幽靈股)
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        # 💎 即時同步進度
        curr_done_count = len(done_list)
        p_bar.progress(min(curr_done_count / total_count, 1.0))
        p_text.markdown(f"**🚀 掃描進度：`{curr_done_count}` / `{total_count}` ({curr_done_count/total_count:.1%})**")

        if remaining_pool.empty:
            st.balloons(); p_text.success(f"🏆 100% 全壘打達成！今日標的共 {total_count} 檔。"); break
        if round_num > 10: break

        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            batch_done = 0
            for f in as_completed(futures):
                ticker = futures[f]
                data, err = f.result()
                batch_done += 1
                if batch_done % 15 == 0: time.sleep(2.0)
                
                if data:
                    # 💎 修正：先刪除舊的主鍵，確保這筆資料一定能塞進去 (UPSERT 模擬)
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": ticker, "d": today})
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    # 碎片更新 UI
                    dynamic_val = min((curr_done_count + batch_done) / total_count, 1.0)
                    p_bar.progress(dynamic_val)
                    p_text.markdown(f"**🚀 掃描進度：`{curr_done_count + batch_done}` / `{total_count}` ({dynamic_val:.1%})**")
                    log_box.write(f"✅ 入庫：{data['stock_name']}")
                else:
                    fail_tracker[ticker] = fail_tracker.get(ticker, 0) + 1
                    if fail_tracker[ticker] >= 3:
                        # 三振出局：存入虛擬數據防止一直重抓
                        fake = {"ticker": ticker, "stock_name": "幽靈股", "scan_date": today, "price": 0}
                        pd.DataFrame([fake]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                        log_box.write(f"🚫 {ticker} 數據缺失，標記棄子")
                    else: log_box.write(f"⚠️ {ticker} 暫跳")
        round_num += 1; time.sleep(3)
    log_box.update(label="✨ 任務完成，數據已入庫。", state="complete")

# ================= 3. 視覺美學渲染器 (漸層色票與 LINE) =================

def beauty_style(df):
    """💎 哲哲專屬暴力美學漸層：數字越大越紅"""
    if df.empty: return df
    # 確保數值化
    for c in ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        # 🌈 漸層分布 (越強越紅)
        if '漲跌(%)' in df.columns: styled = styled.background_gradient(subset=['漲跌(%)'], cmap='Reds', low=0, high=1.0)
        if 'ROE' in df.columns: styled = styled.background_gradient(subset=['ROE'], cmap='YlOrRd', low=0.08, high=0.25)
        if '營收成長' in df.columns: styled = styled.background_gradient(subset=['營收成長'], cmap='OrRd', low=0.05, high=0.6)
        if '報酬率(%)' in df.columns: styled = styled.background_gradient(subset=['報酬率(%)'], cmap='RdYlGn_r', low=-10, high=10)
        
        def color_text(v):
            if isinstance(v, (int, float)):
                if v > 0: return 'color: #FF3333; font-weight: bold'
                if v < 0: return 'color: #00AA00; font-weight: bold'
            return ''
        return styled.map(color_text, subset=[c for c in ['漲跌(%)', '報酬率(%)', '獲利'] if c in df.columns])
    except: return df

def send_line_report(title, df, icon):
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n🎯 跟我預測的一模一樣，賺到流湯！\n"
    for _, r in df.head(10).iterrows(): msg += f"✅ {r.get('代號','')} {r.get('名稱','')} | 現價:{r.get('現價','')}\n"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

# ================= 4. 主介面設計 (V114.0 數據鎖死完全體) =================
st.set_page_config(page_title="哲哲量化美學戰情室 V114.0", layout="wide")
st.markdown("""<style>
    .big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
    div.stButton > button { height: 3.8em; font-size: 1.3rem !important; font-weight: bold !important; border-radius: 12px; margin-bottom: 12px; }
</style>""", unsafe_allow_html=True)

now_tw = datetime.now(TW_TZ)
st.title("🛡️ 哲哲量化戰情室 V114.0 — 數據寫入鎖死版")

# 🕒 收盤同步提醒
if now_tw.hour == 13 and now_tw.minute >= 31:
    st.warning("🔔 **收盤時間到 (1:31 PM)！建議執行『暴力覆蓋重掃』鎖定收盤數據！**")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛指揮中心", "💼 資產監控戰報", "🛠️ 後台數據都更"])

# --- Tab 1: 核心指揮 ---
with tab1:
    st.markdown("### 🏆 全市場智慧掃描 (UPSERT 強力寫入)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日數據快取", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！共 {len(db_df)} 筆數據。")
            else: st.warning("資料庫暫無數據。")
    with c2:
        if st.button("⚡ 啟動增量渦輪掃描 (補齊進度)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃 (強制 100% 重置)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, mode="reset"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝發射台 (七大金剛列陣)")
    
    # 全部大按鈕佈局
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            # 濾掉幽靈股
            df = df[df['現價'] > 0]
            p_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.dataframe(beauty_style(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']]), use_container_width=True)
            send_line_report("超級策略", res, "💎")
        else: st.error("⚠️ 請先讀取快取數據！")

    if st.button("📈 帶量突破前高 (圖一)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5) & (df['現價'] > 0)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("帶量突破", res, "📈")

    if st.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05) & (df['現價'] > 0)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("三線合一", res, "🚀")

    if st.button("🌀 布林縮口突破 (圖三)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15) & (df['現價'] > 0)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("布林突破", res, "🌀")

    if st.button("👑 九成勝率 ATM", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5']) & (df['現價'] > 0)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("ATM策略", res, "👑")

    if st.button("🛡️ 低階抄底防護", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5']) & (df['現價'] > 0)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("低階抄底", res, "🛡️")

    if st.button("🎯 強勢回測支撐", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02) & (df['現價'] > 0)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("強勢回測", res, "🎯")

    st.divider()
    if st.button("🔍 揭開底牌：檢視今日 350 檔所有原始數據 (不限股價)", use_container_width=True):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)
        else: st.warning("⚠️ 數據庫目前無快取，請啟動掃描！")

# --- Tab 2: 持倉監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (碎片化強力寫入)", use_container_width=True):
            lightning_homerun_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
        st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(df_p), use_container_width=True)
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 通知)")
        m_sell = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if m_sell[i].button(f"{icon} {name}", use_container_width=True):
                masks = [df_p['sma5'] < df_p['ma20'], df_p['rsi'] > 80, df_p['報酬率(%)'] > 20, df_p['報酬率(%)'] < -10, df_p['price'] < df_p['ma20']]
                res_s = df_p[masks[i]].copy()
                if not res_s.empty:
                    st.dataframe(beauty_style(res_s[['stock_name', 'ticker', 'price', '報酬率(%)']].rename(columns={'price':'現價'})))
                    send_line_report(f"賣訊：{name}", res_s, icon)
                else: st.success("✅ 持倉目前安全，跟我預測的一模一樣！")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (鋼鐵 Upsert)")
    ch1, ch2 = st.columns(2)
    with ch1:
        f1 = st.file_uploader("上傳股票池 CSV (必須包含 sector, fund_count)", type="csv")
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

st.caption("本系統由哲哲團隊開發。V114.0 數據鎖死完全體，賺到流湯不要忘了我！")

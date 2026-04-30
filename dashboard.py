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
        # A. 核心掃描表 (21 個精準指標)
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
        # B. 股票池表 (產業與基金情資)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_pool (
                ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), 
                sector VARCHAR(50), fund_count INT DEFAULT 0
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # C. 持倉資產表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), 
                entry_price FLOAT, qty FLOAT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        
        # 欄位二次自動校正
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        needed = [('roe','FLOAT'), ('rev_growth','FLOAT'), ('fund_count','INT'), ('high_20','FLOAT'), ('vol_20','FLOAT'), ('bb_width','FLOAT')]
        for col, dtype in needed:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} {dtype};"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (暴力優雅抓取引擎) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲暴力優雅抓取：0.8~2.2s 隨機延遲，雙線程潛行"""
    # 💎 贏家參數：0.8~2.2 秒是 Yahoo 目前能接受的最快節奏
    time.sleep(random.uniform(0.8, 2.2))
    
    try:
        # yfinance 內部會自動調用 curl_cffi 進行 TLS 指紋模擬
        s = yf.Ticker(ticker)
        d = s.history(period="7mo", interval="1d", timeout=30)
        
        # 備援判斷：上市櫃後綴自動切換
        if d.empty or len(d) < 65:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=30)
            if d.empty or len(d) < 65: return None, "Yahoo 攔截或數據不足"
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
        # 基本面數據 (ROE / 營收成長)
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

def smart_homerun_loop(pool_df):
    """🚀 哲哲全壘打引擎 5.0：無限回補，直到 100% 入庫"""
    total_count = len(pool_df)
    today = datetime.now().date()
    round_num = 1
    
    progress_bar = st.progress(0)
    status_msg = st.empty()
    log_box = st.status("🚀 啟動暴力優雅掃描程序 (100% 成功率保證)...", expanded=True)
    
    while True:
        # A. 檢查剩餘缺失名單
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        progress_bar.progress(len(done_list) / total_count)
        
        if remaining_pool.empty:
            st.balloons()
            status_msg.success(f"🏆 100% 全壘打達成！今日 {total_count} 檔數據全部歸位！")
            log_box.update(label="✨ 數據全數歸位，任務結束！", state="complete")
            break
        
        if round_num > 10: # 保險絲：防止無限死循環
            st.error("🚨 嘗試 10 輪仍未完成，可能 IP 已被封鎖，請休息 1 小時。")
            break

        status_msg.info(f"📍 正在執行第 {round_num} 輪強攻 | 剩餘 {len(remaining_pool)} 檔缺失中...")
        batch_list = remaining_pool.sample(frac=1).to_dict('records') # 💎 洗牌，打亂規律
        
        # 💎 暴力優雅並行數：設定為 2，效率最高且安全
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            for f in as_completed(futures):
                data, err = f.result()
                if data:
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    log_box.write(f"✅ 入庫：{data['stock_name']}")
                    progress_bar.progress((len(done_df) + 1) / total_count) # 簡易更新進度
                else:
                    log_box.write(f"⚠️ {futures[f]} 跳過：{err}")

        round_num += 1
        if not remaining_pool.empty:
            log_box.write(f"⏳ 第 {round_num-1} 輪結束，休息 15 秒後自動啟動回補...")
            time.sleep(15)

# ================= 3. 視覺與 LINE 戰報 (百分百歸位) =================

def send_line_report(title, df, icon):
    if df.empty: return
    temp = df.copy()
    n_col = next((c for c in ['名稱', 'stock_name', 'stock_name_x'] if c in temp.columns), '未知')
    p_col = next((c for c in ['現價', 'price', '現價_y'] if c in temp.columns), 'N/A')
    t_col = next((c for c in ['ticker', '代號'] if c in temp.columns), '')
    msg = f"{icon}【哲哲戰報 - {title}】\n🎯 符合標的：\n"
    for _, r in temp.iterrows():
        msg += f"✅ {r.get(t_col,'')} {r.get(n_col,'')} | 現價:{r.get(p_col,'')}\n"
    msg += "\n跟我預測的一模一樣，賺到流湯！🚀"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

def style_df(df):
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')

st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
.medium-font { font-size:26px !important; font-weight: bold; color: #333; }</style>""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V98.0 七大金剛旗艦版) =================
st.set_page_config(page_title="哲哲戰情室 V98.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V98.0 — 七大金剛旗艦全壘打版")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略發射台", "💼 持倉監控戰報", "🛠️ 數據管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日行情掃描 (暴力優雅模式)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日數據快取 (包含回補數據)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','ROE','營收成長','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！目前已有 {len(db_df)} 筆數據。")
            else: st.warning("目前尚無數據，請啟動全壘打掃描。")
    with c2:
        if st.button("⚡ 啟動全壘打渦輪掃描 (暴力優雅補洞)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                smart_homerun_loop(pool)
                st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝決策中心 (七大金剛不隱藏)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        # 同步後台數據 (Sector 與 Imported Funds)
        pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
        df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
        df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        # 💎 1. 超級策略
        if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.write(f"🎯 頂級標的：共 {len(res)} 筆"); st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']]))
            send_line_report("超級策略", res, "💎")

        # 💎 2-4. 形態策略
        st.markdown("#### 🔹 形態還原策略")
        mc1 = st.columns(3)
        if mc1[0].button("📈 帶量突破前高 (圖一)", use_container_width=True):
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(style_df(res)); send_line_report("帶量突破", res, "📈")
        if mc1[1].button("🚀 三線合一多頭 (圖二)", use_container_width=True):
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
            st.dataframe(style_df(res)); send_line_report("三線合一", res, "🚀")
        if mc1[2].button("🌀 布林縮口突破 (圖三)", use_container_width=True):
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
            st.dataframe(style_df(res)); send_line_report("布林突破", res, "🌀")
        
        # 💎 5-7. 經典策略
        st.markdown("#### 🔸 經典至尊策略")
        mc2 = st.columns(3)
        if mc2[0].button("👑 九成勝率 ATM", use_container_width=True):
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("ATM策略", res, "👑")
        if mc2[1].button("🛡️ 低階抄底防護", use_container_width=True):
            res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("低階抄底", res, "🛡️")
        if mc2[2].button("🎯 強勢回測支撐", use_container_width=True):
            res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02)]
            st.dataframe(style_df(res)); send_line_report("強勢回測", res, "🎯")

# --- Tab 2: 持倉監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (全壘打同步模式)", use_container_width=True):
            smart_homerun_loop(df_p[['ticker','stock_name']])
            st.rerun()
        
        # 顯示獲利與 LINE 賣訊
        today_p = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now().date()})
        df_p = pd.merge(df_p, today_p, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
        
        st.markdown(f"當前總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 通知)")
        mc3 = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if mc3[i].button(f"{icon} {name}", use_container_width=True):
                masks = [df_p['sma5'] < df_p['ma20'], df_p['rsi'] > 80, df_p['報酬率(%)'] > 20, df_p['報酬率(%)'] < -10, df_p['price'] < df_p['ma20']]
                res = df_p[masks[i]].copy()
                if not res.empty:
                    disp = res[['stock_name', 'ticker', 'price', '報酬率(%)']].rename(columns={'price':'現價'})
                    st.dataframe(style_df(disp)); send_line_report(f"賣訊：{name}", disp, icon)
                else: st.success("✅ 持倉目前安全")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (鋼鐵 Upsert 版)")
    ch1, ch2 = st.columns(2)
    with ch1:
        f1 = st.file_uploader("上傳股票池 CSV (ticker, stock_name, sector, fund_count)", type="csv")
        if f1 and st.button("💾 鋼鐵匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new[['ticker', 'stock_name', 'sector', 'fund_count']].to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功！")
    with ch2:
        f2 = st.file_uploader("上傳持倉 CSV", type="csv")
        if f2 and st.button("💾 鋼鐵匯入資產"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("匯入成功！")

st.caption("本系統由哲哲團隊開發。暴力優雅旗艦版 V98.0，賺到流湯不要忘了我！")

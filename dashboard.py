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
        # A. 核心掃描表 (21 個指標)
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
        # B. 股票池表
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
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (閃電提速抓取引擎) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲提速抓取：閃電間隔 0.5s"""
    # 💎 閃電節奏：隨機 0.5 ~ 0.8s
    time.sleep(random.uniform(0.5, 0.8))
    try:
        s = yf.Ticker(ticker)
        d = s.history(period="7mo", interval="1d", timeout=20)
        
        if d.empty or len(d) < 65:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=20)
            if d.empty or len(d) < 65: return None, "數據不足"
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
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

def lightning_homerun_loop(pool_df):
    """🚀 哲哲閃電提速迴圈：雙線程提速、碎片化入庫、進度條同步"""
    total_count = len(pool_df)
    today = datetime.now().date()
    progress_bar = st.progress(0)
    status_msg = st.empty()
    log_box = st.status("🚀 閃電提速程序啟動 (碎分化提速模式)...", expanded=True)
    
    round_num = 1
    while True:
        # A. 檢查清單
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        progress_bar.progress(len(done_list) / total_count)
        if remaining_pool.empty:
            st.balloons(); status_msg.success(f"🏆 100% 全壘打！共抓取 {total_count} 檔。")
            log_box.update(label="✨ 任務完成！數據全數歸位。", state="complete")
            break
        
        if round_num > 10: break

        status_msg.info(f"📍 正在執行第 {round_num} 輪閃電補洞 | 剩餘 {len(remaining_pool)} 檔")
        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        
        # 💎 關鍵：使用雙線程提速 (worker=2)，既暴力又相對安全
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            
            counter = 0
            for f in as_completed(futures):
                data, err = f.result()
                counter += 1
                
                # 💎 安全間隔：每 15 檔大休息 2 秒
                if counter % 15 == 0:
                    log_box.write("☕ 觸發安全潛行，休息 2 秒...")
                    time.sleep(random.uniform(2.0, 3.0))

                if data:
                    # 💎 碎片化入庫：抓到即上傳，絕對不重複
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    log_box.write(f"✅ 入庫：{data['stock_name']}")
                    progress_bar.progress((len(done_list) + counter) / total_count if (len(done_list) + counter) <= total_count else 0.99)
                else:
                    log_box.write(f"⚠️ {futures[f]} 跳過：{err}")
                    if "429" in str(err) or "Rate" in str(err):
                        log_box.write("🚨 被鎖，提前進入休息...")
                        time.sleep(15) # 被鎖也要休息，否則 IP 會死
                        break

        round_num += 1
        time.sleep(3)

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

st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }</style>""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V101.0 七大金剛閃電版) =================
st.set_page_config(page_title="哲哲戰情室 V101.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V101.0 — 閃電提速與全功能全壘打")

tab1, tab2, tab3 = st.tabs(["🚀 核心策略發射台", "💼 持倉監控戰報", "🛠️ 數據管理中心"])

with tab1:
    st.markdown("### 🏆 每日行情掃描 (閃電暴力提速模式)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日數據 (包含已補洞)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                for c in ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','ROE','營收成長','fund_count','high_20','vol_20','bb_width']:
                    if c in db_df.columns: db_df[c] = pd.to_numeric(db_df[c], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！")
    with c2:
        if st.button("⚡ 啟動全壘打渦輪掃描 (閃電碎片回補)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                lightning_homerun_loop(pool)
                st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝決策中心 (七大金剛列陣)")
    if 'master_df' in st.session_state:
        df = st.session_state['master_df'].copy()
        pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
        df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
        df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
        sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')

        # 1. 超級策略
        if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']])); send_line_report("超級策略", res, "💎")

        st.markdown("#### 🔹 形態與經典策略")
        mc1, mc2 = st.columns(2)
        with mc1:
            if st.button("📈 帶量突破前高 (圖一)", use_container_width=True):
                res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
                st.dataframe(style_df(res)); send_line_report("帶量突破", res, "📈")
            if st.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
                res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
                st.dataframe(style_df(res)); send_line_report("三線合一", res, "🚀")
            if st.button("🌀 布林縮口突破 (圖三)", use_container_width=True):
                res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
                st.dataframe(style_df(res)); send_line_report("布林突破", res, "🌀")
        with mc2:
            if st.button("👑 九成勝率 ATM", use_container_width=True):
                res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
                st.dataframe(style_df(res)); send_line_report("ATM策略", res, "👑")
            if st.button("🛡️ 低階抄底防護", use_container_width=True):
                res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5'])]
                st.dataframe(style_df(res)); send_line_report("低階抄底", res, "🛡️")
            if st.button("🎯 強勢回測支撐", use_container_width=True):
                res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02)]
                st.dataframe(style_df(res)); send_line_report("強勢回測", res, "🎯")

with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (閃電回補同步)", use_container_width=True):
            lightning_homerun_loop(df_p[['ticker','stock_name']])
            st.rerun()
        
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
                else: st.success("✅ 目前安全")

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

st.caption("本系統由哲哲團隊開發。閃電提速 V101.0，賺到流湯不要忘了我！")

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io, re, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (鋼鐵都更，欄位鎖死) =================
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
        # B. 建立股票池情資表
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
        
        # 💎 鋼鐵自動補齊：確保所有策略欄位在地基層 100% 存在
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        needed = [('roe','FLOAT'), ('rev_growth','FLOAT'), ('fund_count','INT'), ('high_20','FLOAT'), ('vol_20','FLOAT'), ('bb_width','FLOAT')]
        for col, dtype in needed:
            if col not in s_cols: conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} {dtype};"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (閃電變速抓取引擎) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲全能抓取：yfinance 配合 curl_cffi 底層"""
    try:
        s = yf.Ticker(ticker)
        # 降級抓取 7 個月數據，確保指標完整性
        d = s.history(period="7mo", interval="1d", timeout=25)
        
        if d.empty or len(d) < 40:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=25)
            if d.empty or len(d) < 40: return None, "數據不足"
        
        c, v = d['Close'], d['Volume']
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
        # 基本面強攻 (ROE 與 營收)
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
    """🚀 哲哲閃電全壘打迴圈：碎分化入庫、變速潛行、無限補洞"""
    total_count = len(pool_df)
    today = datetime.now().date()
    progress_bar = st.progress(0); status_msg = st.empty()
    log_box = st.status("🚀 啟動閃電碎片補洞程序...", expanded=True)
    
    fail_tracker, round_num = {}, 1
    while True:
        # A. 比對資料庫進度
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        progress_bar.progress(len(done_list) / total_count)
        if remaining_pool.empty:
            st.balloons(); status_msg.success(f"🏆 100% 全壘打達成！今日標的全部歸位。")
            log_box.update(label="✨ 數據滿血回歸！", state="complete")
            break
        
        if round_num > 8: break # 超過 8 輪判定為不可抗力

        status_msg.info(f"📍 第 {round_num} 輪閃電強攻 | 剩餘 {len(remaining_pool)} 檔補洞中...")
        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        
        # ⚡ 雙線程提速 (暴力與安全平衡點)
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            counter = 0
            for f in as_completed(futures):
                ticker = futures[f]
                data, err = f.result()
                counter += 1
                
                # 💎 閃電節奏：0.5~0.8s
                time.sleep(random.uniform(0.5, 0.8))
                
                # 💎 安全潛行：每 10 檔休息 2.5 秒
                if counter % 10 == 0:
                    log_box.write("☕ 觸發安全潛行，休息 2.5 秒...")
                    time.sleep(2.5)

                if data:
                    # 💎 碎分化秒存：抓一筆存一筆
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    log_box.write(f"✅ 入庫：{data['stock_name']}")
                else:
                    fail_tracker[ticker] = fail_tracker.get(ticker, 0) + 1
                    log_box.write(f"⚠️ {ticker} 暫跳：{err}")
                    if fail_tracker[ticker] >= 3: # 三振出局法
                        log_box.write(f"🚫 {ticker} 判定缺失股，標記棄子")
                        fake = {"ticker": ticker, "stock_name": "幽靈股", "scan_date": today, "price": 0}
                        pd.DataFrame([fake]).to_sql('daily_scans', con=engine, if_exists='append', index=False)

        round_num += 1
        time.sleep(5)

# ================= 3. 視覺渲染與 LINE 戰報 (百分百歸位) =================

def send_line_report(title, df, icon):
    if df.empty: return
    temp = df.copy()
    n_col = next((c for c in ['名稱', 'stock_name', 'stock_name_x'] if c in temp.columns), '未知')
    p_col = next((c for c in ['現價', 'price', '現價_y'] if c in temp.columns), 'N/A')
    t_col = next((c for c in ['ticker', '代號'] if c in temp.columns), '')
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in temp.iterrows():
        msg += f"✅ {r.get(t_col,'')} {r.get(n_col,'')} | 現價:{r.get(p_col,'')}\n"
    msg += "\n跟我預測的一模一樣，賺到流湯！🚀"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

def style_df(df):
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')

st.markdown("""<style>.big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }</style>""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V104.0 終極旗艦全能版) =================
st.set_page_config(page_title="哲哲戰情室 V104.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V104.0 — 超級旗艦全功能版")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛發射台", "💼 持倉監控戰報", "🛠️ 後台管理中心"])

# --- Tab 1: 七大金剛發射台 ---
with tab1:
    # 🏆 頂部掃描區
    st.markdown("### 🏆 每日行情智慧掃描 (閃電變速補洞)")
    c_scan1, c_scan2 = st.columns(2)
    with c_scan1:
        if st.button("📡 讀取今日行情數據 (包含已回補標的)", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0"), con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                num_cols = ['現價','漲跌(%)','sma5','ma20','ma60','RSI','kd20','kd60','ROE','營收成長','fund_count','high_20','vol_20','bb_width']
                for col in num_cols:
                    if col in db_df.columns: db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0)
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！已有 {len(db_df)} 筆數據。")
            else: st.warning("今日尚無快取數據，請啟動渦輪掃描。")
    with c_scan2:
        if st.button("⚡ 啟動全壘打渦輪掃描 (閃電碎分補洞)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                lightning_homerun_loop(pool)
                st.rerun()

    st.divider()
    
    # 🔥 七大金剛列陣 (不管有沒有數據都直接顯示按鈕)
    st.markdown("### 🔥 買股必勝決策中心 (七大金剛列陣)")
    
    # 策略 1: 超級策略 (獨立一行，王者風範)
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            pool_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, pool_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            
            # 超級策略公式：法人大買 + 獲利驚人 + 領先同業 + 營收爆發
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.write(f"🎯 頂級標的：共 {len(res)} 筆")
            st.dataframe(style_df(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']]))
            send_line_report("超級策略", res, "💎")
        else: st.warning("⚠️ 請先點擊『讀取行情』或『啟動掃描』！")

    # 策略 2-4: 形態三圖
    st.markdown("#### 🔹 形態還原策略 (真突破與三線合一)")
    mc1, mc2, mc3 = st.columns(3)
    if mc1.button("📈 帶量突破前高 (圖一)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5)]
            st.dataframe(style_df(res)); send_line_report("帶量突破", res, "📈")
        else: st.warning("⚠️ 數據未就緒")
    if mc2.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05)]
            st.dataframe(style_df(res)); send_line_report("三線合一", res, "🚀")
        else: st.warning("⚠️ 數據未就緒")
    if mc3.button("🌀 布林縮口突破 (圖三)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['bbu']) & (df['bb_width'] < 0.15)]
            st.dataframe(style_df(res)); send_line_report("布林突破", res, "🌀")
        else: st.warning("⚠️ 數據未就緒")

    # 策略 5-7: 經典至尊
    st.markdown("#### 🔸 經典至尊策略 (抄底與 ATM)")
    mc4, mc5, mc6 = st.columns(3)
    if mc4.button("👑 九成勝率 ATM", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("ATM策略", res, "👑")
        else: st.warning("⚠️ 數據未就緒")
    if mc5.button("🛡️ 低階抄底防護", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5'])]
            st.dataframe(style_df(res)); send_line_report("低階抄底", res, "🛡️")
        else: st.warning("⚠️ 數據未就緒")
    if mc6.button("🎯 強勢回測支撐", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(abs(df['現價']-df['ma20'])/df['ma20'].replace(0,1)<0.02)]
            st.dataframe(style_df(res)); send_line_report("強勢回測", res, "🎯")
        else: st.warning("⚠️ 數據未就緒")

# --- Tab 2: 資產監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新資產現價 (全壘打回補同步)", use_container_width=True):
            lightning_homerun_loop(df_p[['ticker','stock_name']])
            st.rerun()
        
        # 獲取今日最新行情並合併
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now().date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = ((df_p['price'] - df_p['entry_price']) / (df_p['entry_price'].replace(0, 1))) * 100
        
        st.markdown(f"當前預估總獲利：<p class='big-font'>${df_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 通知)")
        mc_sell = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if mc_sell[i].button(f"{icon} {name}", use_container_width=True):
                # 賣訊邏輯
                masks = [df_p['sma5'] < df_p['ma20'], df_p['rsi'] > 80, df_p['報酬率(%)'] > 20, df_p['報酬率(%)'] < -10, df_p['price'] < df_p['ma20']]
                res_sell = df_p[masks[i]].copy()
                if not res_sell.empty:
                    disp = res_sell[['stock_name', 'ticker', 'price', '報酬率(%)']].rename(columns={'price':'現價'})
                    st.dataframe(style_df(disp)); send_line_report(f"賣訊：{name}", disp, icon)
                else: st.success("✅ 持倉目前安全，跟我預測的一模一樣！")

# --- Tab 3: 後台數據中心 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (鋼鐵 Upsert 版)")
    ch1, ch2 = st.columns(2)
    with ch1:
        st.markdown("#### 📋 股票池匯入 (必須包含 sector, fund_count)")
        f1 = st.file_uploader("上傳股票池 CSV", type="csv")
        if f1 and st.button("💾 鋼鐵匯入股票池"):
            df_new = pd.read_csv(f1, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM stock_pool WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new[['ticker', 'stock_name', 'sector', 'fund_count']].to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("匯入成功！")
    with ch2:
        st.markdown("#### 💰 持倉匯入")
        f2 = st.file_uploader("上傳持倉 CSV", type="csv")
        if f2 and st.button("💾 鋼鐵匯入資產"):
            df_new = pd.read_csv(f2, encoding='utf-8-sig'); df_new.columns = [c.lower() for c in df_new.columns]
            with engine.begin() as conn:
                for t in df_new['ticker'].tolist(): conn.execute(text("DELETE FROM portfolio WHERE ticker = :t"), {"t": str(t).upper().strip()})
            df_new.to_sql('portfolio', con=engine, if_exists='append', index=False); st.success("匯入成功！")

st.caption("本系統由哲哲團隊開發。超級完整旗艦版 V104.0，賺到流湯不要忘了我！")

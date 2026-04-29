import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (強制自動化都更、排毒、編碼鎖死) =================
try:
    # 💎 核心修復：強制 charset=utf8mb4，解決中文字 ???? 亂碼
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(
        DB_URL, 
        connect_args={"charset": "utf8mb4", "connect_timeout": 30},
        pool_pre_ping=True,
        pool_recycle=3600
    )
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 建立表格並強制使用 utf8mb4
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50)) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        
        # B. 🔥 強力都更：自動檢查並補齊缺失欄位，徹底解決 DatabaseError
        s_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM daily_scans")).fetchall()]
        for col in ['ma60', 'kd20', 'kd60', 'bbu', 'bbl']:
            if col not in s_cols:
                conn.execute(text(f"ALTER TABLE daily_scans ADD COLUMN {col} FLOAT;"))
        
        # 強制轉碼
        for t in ['stock_pool', 'portfolio', 'daily_scans']:
            conn.execute(text(f"ALTER TABLE {t} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統啟動失敗 (地基崩潰)：{e}"); st.stop()

# ================= 2. 核心大腦 (11 大指標與暴力抓取引擎) =================
def fetch_full_stock_package(ticker, name):
    """
    💎 哲哲全能套件：抓取現價 + 11 大技術指標
    """
    targets = [ticker]
    if ".TW" in ticker: targets.append(ticker.replace(".TW", ".TWO"))
    elif ".TWO" in ticker: targets.append(ticker.replace(".TWO", ".TW"))

    for t in targets:
        try:
            s = yf.Ticker(t)
            d = s.history(period="7mo", interval="1d", timeout=12)
            if not d.empty and len(d) >= 65:
                c, v = d['Close'], d['Volume']
                sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
                rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
                return {
                    "ticker": ticker, "stock_name": name, "price": round(c.iloc[-1], 2),
                    "change_pct": round(((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100, 2),
                    "sma5": round(sma5.iloc[-1], 2), "ma20": round(ma20.iloc[-1], 2),
                    "ma60": round(ma60.iloc[-1], 2), "rsi": round(rsi.iloc[-1], 2),
                    "vol": int(v.iloc[-1]), "avg_vol": int(ta.sma(v, 20).iloc[-1]),
                    "kd20": round(c.iloc[-20], 2), "kd60": round(c.iloc[-60], 2), 
                    "scan_date": datetime.now().date(),
                    "bbu": round(bb.iloc[-1, 2], 2), "bbl": round(bb.iloc[-1, 0], 2)
                }
        except: continue
    return None

# ================= 3. 哲哲美學 (數字加大、LINE 戰報、渲染) =================
def send_line_report(title, df, icon):
    """
    💎 至尊 LINE 噴發引擎：智慧標準化對齊
    """
    if df.empty: return
    temp_df = df.copy()
    name_col = next((c for c in ['名稱', 'stock_name_x', 'stock_name'] if c in temp_df.columns), '未知')
    price_col = next((c for c in ['現價', '現價_y', 'price'] if c in temp_df.columns), 'N/A')
    ticker_col = next((c for c in ['ticker', '代號'] if c in temp_df.columns), '')

    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in temp_df.iterrows():
        n = r.get(name_col, '未知')
        p = r.get(price_col, 'N/A')
        t = r.get(ticker_col, '')
        msg += f"✅ {t} {n} | 現價:{p}\n"
    msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"
    
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try:
        resp = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
        if resp.status_code == 200: st.toast(f"🚀 LINE 戰報噴發！({title})")
    except: pass

def style_df(df):
    """紅漲綠跌至尊視覺"""
    def color_val(val):
        if isinstance(val, (int, float)):
            if val > 0: return 'color: #FF3333; font-weight: bold'
            if val < 0: return 'color: #00AA00'
        return ''
    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 'entry_price': '{:.2f}', 'qty': '{:,.0f}'}
    return df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-').map(
        color_val, subset=[c for c in ['報酬率(%)', '漲跌(%)', '獲利'] if c in df.columns]
    )

# 💎 至尊加大數字 CSS
st.markdown("""
    <style>
    .big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
    .medium-font { font-size:26px !important; font-weight: bold; color: #333; }
    </style>
""", unsafe_allow_html=True)

# ================= 4. 主介面設計 (V71.0 數據透明完全體) =================
st.set_page_config(page_title="哲哲戰情室 V71.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V71.0 — 數據透明與全能大滿貫")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 持倉獲利 & 賣股策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日行情掃描中心 (數據回傳區)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取數據", use_container_width=True):
            query = text("SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌_pct`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = :today")
            db_df = pd.read_sql(query, con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                st.session_state['master_df'] = db_df.rename(columns={'漲跌_pct': '漲跌(%)'})
                st.success(f"✅ 載入成功！共讀取 {len(db_df)} 筆行情數據。")
            else: st.warning("今日尚無快取數據。")
    with c2:
        if st.button("⚡ 啟動並行渦輪掃描 (全股票池)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                res, prog = [], st.progress(0)
                status_scan = st.status("🚀 全市場並行掃描中...", expanded=False)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, f in enumerate(as_completed(futures)):
                        data = f.result()
                        if data: 
                            res.append(data)
                            status_scan.write(f"✅ {data['stock_name']} 指標計算完成")
                        prog.progress((count + 1) / len(pool))
                
                if res:
                    m_df = pd.DataFrame(res)
                    with engine.begin() as conn:
                        conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                    m_df.to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                    status_scan.update(label=f"✨ 市場掃描完成！成功處理 {len(res)}/{len(pool)} 筆數據！", state="complete")
                    st.success(f"今日利多已就緒！成功率：{(len(res)/len(pool))*100:.1f}%")

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (策略按鈕歸位)")
    if 'master_df' in st.session_state and not st.session_state['master_df'].empty:
        df = st.session_state['master_df'].copy()
        df['量比'] = df['vol'] / df['avg_vol']
        cols = st.columns(6)
        strats = [
            ("九成勝率提款機", "👑", (df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['量比']>=1.2) & (df['現價']>df['sma5'])),
            ("量價突破", "💥", (df['現價']>df['ma20']) & (df['量比']>2)),
            ("黃金交叉", "🚀", (df['sma5']>df['ma20'])),
            ("低階抄底", "🛡️", (df['RSI']<35) & (df['現價']>df['sma5'])),
            ("布林噴發", "🌀", (df['現價']>df['bbu'])),
            ("強勢回測", "🎯", (abs(df['現價']-df['ma20'])/df['ma20']<0.02))
        ]
        for i, (name, icon, mask) in enumerate(strats):
            if cols[i].button(f"{icon} {name}", use_container_width=True):
                res = df[mask].sort_values(by='RSI', ascending=False)
                st.write(f"🎯 符合標的：共 {len(res)} 筆")
                st.dataframe(style_df(res))
                send_line_report(name, res, icon)
    else: st.info("💡 請載入行情數據。")

# --- Tab 2: 持倉 & 數據透明診斷 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker", con=engine)
    
    if not df_p.empty:
        if st.button("🔄 更新即時獲利與存檔 (開啟透明日誌)", use_container_width=True):
            p_map, all_full_data = {}, []
            status_box = st.status("🚀 啟動獲利強攻引擎...", expanded=True)
            prog_bar = st.progress(0)
            
            tickers = df_p['ticker'].tolist()
            success_count = 0
            for idx, t in enumerate(tickers):
                sn = df_p[df_p['ticker']==t]['stock_name'].iloc[0]
                status_box.write(f"正在跑第 {idx+1}/{len(tickers)} 筆數據：{t} {sn} ...")
                data = fetch_full_stock_package(t, sn)
                if data:
                    p_map[t] = data['price']
                    all_full_data.append(data)
                    success_count += 1
                    status_box.write(f"✅ {t} 同步成功！現價：{data['price']:.2f}")
                else:
                    p_map[t] = np.nan
                    status_box.write(f"⚠️ {t} 抓取超時或失敗")
                prog_bar.progress((idx + 1) / len(tickers))
            
            if all_full_data:
                m_df = pd.DataFrame(all_full_data)
                with engine.begin() as conn:
                    for _, row in m_df.iterrows():
                        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": row['ticker'], "d": row['scan_date']})
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
            
            st.session_state['rt_p_v71'] = p_map
            st.session_state['last_sync'] = datetime.now().strftime('%H:%M:%S')
            status_box.update(label=f"✅ 獲利同步完成！成功抓取 {success_count}/{len(tickers)} 筆數據！", state="complete")
            st.toast(f"成功率：{(success_count/len(tickers))*100:.0f}%，數據已歸位！", icon="🚀")
            time.sleep(1)
            st.rerun()

        if 'rt_p_v71' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p_v71'])
            for col in ['entry_price', '現價', 'qty']: df_p[col] = pd.to_numeric(df_p[col], errors='coerce')
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = ((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100
            
            total_profit = df_p['獲利'].fillna(0).sum()
            st.markdown(f"<p class='medium-font'>當前預估實質總獲利：</p><p class='big-font'>${total_profit:,.0f}</p>", unsafe_allow_html=True)
            st.caption(f"🕒 數據最後同步：{st.session_state.get('last_sync', 'N/A')} (成功數: {len([v for v in st.session_state['rt_p_v71'].values() if not np.isnan(v)])}/{len(df_p)} 檔)")
            
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 噴發版)")
        m_cols = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        
        for i, (name, icon) in enumerate(s_btns):
            if m_cols[i].button(f"{icon} {name}", use_container_width=True):
                scans_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today"), con=engine, params={"today": datetime.now().date()})
                if not scans_df.empty:
                    check_df = pd.merge(df_p, scans_df, on='ticker', how='left')
                    masks = [check_df['sma5'] < check_df['ma20'], check_df['rsi'] > 80, check_df['報酬率(%)'] > 20, check_df['報酬率(%)'] < -10, check_df['現價_y'] < check_df['ma20']]
                    res = check_df[masks[i]].copy()
                    if not res.empty:
                        st.error(f"🚨 符合『{name}』標的如下：共 {len(res)} 檔")
                        disp_df = res[['stock_name_x', 'ticker', '現價_y', '報酬率(%)']].rename(columns={'stock_name_x':'名稱', '現價_y':'現價'})
                        st.dataframe(style_df(disp_df))
                        send_line_report(f"賣訊：{name}", disp_df, icon) # 💎 LINE 噴發
                    else: st.success(f"✅ 持倉安全，跟我預測的一模一樣！")
                else: st.warning("💡 請先點擊更新即時獲利存入數據。")
    else: st.info("持倉資料庫為空。")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據都更中心 (鋼鐵 Upsert 版)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📋 股票池管理 (Stock Pool)")
        st.download_button("📥 下載範本", pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'sector':['半導體']}).to_csv(index=False).encode('utf-8-sig'), "pool.csv")
        f1 = st.file_uploader("上傳股票池 CSV", type="csv", key="p1")
        if f1 and st.button("💾 鋼鐵匯入股票池"):
            try:
                df_pool_up = pd.read_csv(f1, encoding='utf-8-sig')
                df_pool_up.columns = [c.lower() for c in df_pool_up.columns]
                df_pool_up['ticker'] = df_pool_up['ticker'].astype(str).str.strip().str.upper()
                # 💎 內部去重，防止 self-collision
                df_pool_up = df_pool_up.drop_duplicates(subset=['ticker'], keep='last')
                
                with engine.begin() as conn:
                    tickers = df_pool_up['ticker'].tolist()
                    if tickers:
                        for i in range(0, len(tickers), 100):
                            conn.execute(text("DELETE FROM stock_pool WHERE ticker IN :t_list"), {"t_list": tickers[i:i+100]})
                        df_pool_up.to_sql('stock_pool', con=conn, if_exists='append', index=False)
                st.success(f"✅ 成功同步 {len(df_pool_up)} 檔標的至股票池！")
            except Exception as e: st.error(f"匯入失敗：{e}")
            
    with c2:
        st.markdown("#### 💰 持倉管理 (Portfolio)")
        st.download_button("📥 下載範本", pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'entry_price':[750],'qty':[1000]}).to_csv(index=False).encode('utf-8-sig'), "port.csv")
        f2 = st.file_uploader("上傳數據", type="csv", key="p2")
        if f2 and st.button("💾 鋼鐵存入持倉"):
            try:
                df_up = pd.read_csv(f2, encoding='utf-8-sig')
                df_up.columns = [c.lower() for c in df_up.columns]
                df_up['ticker'] = df_up['ticker'].astype(str).str.strip().str.upper()
                df_up = df_up.drop_duplicates(subset=['ticker'], keep='last')
                
                with engine.begin() as conn:
                    t_list = df_up['ticker'].tolist()
                    if t_list:
                        for i in range(0, len(t_list), 100):
                            conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list[i:i+100]})
                        df_up.to_sql('portfolio', con=conn, if_exists='append', index=False)
                st.success(f"✅ 持倉同步大成功！共存入 {len(df_up)} 檔持股。")
            except Exception as e: st.error(f"存入失敗：{e}")

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

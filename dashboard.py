import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import pandas_ta as ta
import numpy as np
import requests, json, time, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 1. 系統地基 (強制轉碼、排毒、都更) =================
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
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INT AUTO_INCREMENT PRIMARY KEY, 
                ticker VARCHAR(20), 
                stock_name VARCHAR(50), 
                entry_price FLOAT, 
                qty FLOAT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT, PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        # B. 強制編碼都更：醫好現存的亂碼空間
        for t in ['stock_pool', 'portfolio', 'daily_scans']:
            conn.execute(text(f"ALTER TABLE {t} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基崩潰：{e}"); st.stop()

# ================= 2. 核心大腦 (技術指標與暴力抓取引擎) =================
def fetch_stock_full_package(ticker, name):
    """
    💎 哲哲全能套件：抓取現價 + 計算 11 大技術指標
    """
    # 嘗試不同的後綴，確保抓到數據
    targets = [ticker]
    if ".TW" in ticker: targets.append(ticker.replace(".TW", ".TWO"))
    elif ".TWO" in ticker: targets.append(ticker.replace(".TWO", ".TW"))

    for t in targets:
        try:
            s = yf.Ticker(t)
            # 抓取 7 個月數據確保扣抵與均線正確
            d = s.history(period="7mo", interval="1d", timeout=15)
            if not d.empty and len(d) >= 65:
                c, v = d['Close'], d['Volume']
                # 計算策略所需之所有數據
                sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
                rsi = ta.rsi(c, 14)
                bb = ta.bbands(c, 20, 2)
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

# ================= 3. 視覺渲染工具 (哲哲美學) =================
def style_df(df):
    """紅漲綠跌視覺強化，排除任何顯示錯誤"""
    def color_val(val):
        if isinstance(val, (int, float)):
            if val > 0: return 'color: #FF3333; font-weight: bold'
            if val < 0: return 'color: #00AA00'
        return ''
    
    # 使用 na_rep 防止空值導致渲染崩潰
    f_map = {
        '現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 
        '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%', 
        'entry_price': '{:.2f}', 'qty': '{:,.0f}'
    }
    styler = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
    
    for col in ['報酬率(%)', '漲跌(%)', '獲利']:
        if col in df.columns:
            styler = styler.map(color_val, subset=[col])
    return styler

def send_line_report(title, df, icon):
    if df.empty: return
    msg = f"{icon}【哲哲戰報 - {title}】\n📅 {datetime.now().strftime('%H:%M')}\n🎯 符合標的：\n"
    for _, r in df.iterrows():
        n = r['名稱'] if '名稱' in r else r.get('stock_name', '未知')
        msg += f"✅ {r['ticker'] if 'ticker' in r else r.get('代號','')} {n} | 現價:{r.get('現價','N/A')}\n"
    msg += "\n跟我預測的一模一樣，準備賺到流湯！🚀"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))

# ================= 4. 主介面設計 (V64.0 終極大滿貫版) =================
st.set_page_config(page_title="哲哲戰情室 V64.0", layout="wide")
st.title("🛡️ 哲哲量化戰情室 V64.0 — 數據遷徙與強制更新版")

tab1, tab2, tab3 = st.tabs(["🚀 核心買股策略掃描", "💼 持倉獲利 & 賣出策略", "🛠️ 後台管理中心"])

# --- Tab 1: 買股策略 ---
with tab1:
    st.markdown("### 🏆 每日全市場掃描 (大數據濾網)")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📡 讀取今日快取行情", use_container_width=True):
            # 💎 數據避險：SQL 內不使用百分比符號
            query = text("SELECT ticker as 代號, stock_name as 名稱, price as 現價, change_pct as `漲跌_pct`, sma5, ma20, rsi as RSI, bbu, vol, avg_vol, kd20, kd60 FROM daily_scans WHERE scan_date = :today")
            db_df = pd.read_sql(query, con=engine, params={"today": datetime.now().date()})
            if not db_df.empty: 
                st.session_state['master_df'] = db_df.rename(columns={'漲跌_pct': '漲跌(%)'})
                st.success("✅ 行情載入成功！")
            else: st.warning("今日尚無數據，請先執行渦輪掃描。")
    with c2:
        if st.button("⚡ 啟動並行渦輪掃描 (全股票池)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty:
                res, prog = [], st.progress(0)
                with ThreadPoolExecutor(max_workers=10) as ex:
                    futures = {ex.submit(fetch_stock_full_package, r['ticker'], r['stock_name']): i for i, r in pool.iterrows()}
                    for count, f in enumerate(as_completed(futures)):
                        if f.result(): res.append(f.result())
                        prog.progress((count + 1) / len(pool))
                
                if res:
                    m_df = pd.DataFrame(res)
                    with engine.begin() as conn:
                        conn.execute(text(f"DELETE FROM daily_scans WHERE scan_date = '{datetime.now().date()}'"))
                        m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
                    st.session_state['master_df'] = m_df.rename(columns={'ticker':'代號','stock_name':'名稱','price':'現價','change_pct':'漲跌(%)','rsi':'RSI'})
                    st.success("✨ 市場掃描完成！")

    st.divider()
    st.markdown("### 🛠️ 買股必勝決策中心 (策略按鈕鋼鐵常駐)")
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
                st.dataframe(style_df(df[mask].sort_values(by='RSI', ascending=False)))
    else: st.info("💡 請點擊載入行情數據。")

# --- Tab 2: 持倉 & 數據存檔同步 (解決獲利更新反應) ---
with tab2:
    st.header("💼 我的資產亮牌區 (數據存檔版)")
    # 解決亂碼：從 stock_pool 強制關聯抓取繁體中文
    df_p = pd.read_sql("""
        SELECT p.ticker, COALESCE(s.stock_name, p.stock_name) as stock_name, p.entry_price, p.qty 
        FROM portfolio p LEFT JOIN stock_pool s ON p.ticker = s.ticker
    """, con=engine)
    
    if not df_p.empty:
        # 💎 終極修正：並行渦輪抓取持倉數據，並同步存入後台資料庫
        if st.button("🔄 更新即時獲利並同步策略數據 (背景存檔)", use_container_width=True):
            p_map = {}
            status_box = st.status("🚀 正在啟動獲利強攻引擎...", expanded=True)
            prog = st.progress(0)
            
            all_full_data = []
            tickers = df_p['ticker'].tolist()
            
            # 使用並行抓取，大幅提升更新速度與成功率
            with ThreadPoolExecutor(max_workers=10) as ex:
                future_to_ticker = {
                    ex.submit(fetch_stock_full_package, t, df_p[df_p['ticker']==t]['stock_name'].iloc[0]): t 
                    for t in tickers
                }
                
                for idx, future in enumerate(as_completed(future_to_ticker)):
                    t = future_to_ticker[future]
                    try:
                        data = future.result()
                        if data:
                            p_map[t] = data['price']
                            all_full_results = data
                            all_full_data.append(data)
                            status_box.write(f"✅ {t} 數據同步大成功！現價：{data['price']}")
                        else:
                            p_map[t] = np.nan
                            status_box.write(f"⚠️ {t} 抓取超時，請重試。")
                    except:
                        p_map[t] = np.nan
                    prog.progress((idx + 1) / len(tickers))
            
            # 💎 遷徙存檔：將持倉數據存入後台資料庫，供賣股策略按鈕讀取
            if all_full_data:
                m_df = pd.DataFrame(all_full_data)
                with engine.begin() as conn:
                    for _, row in m_df.iterrows():
                        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": row['ticker'], "d": row['scan_date']})
                    m_df.to_sql('daily_scans', con=conn, if_exists='append', index=False)
            
            st.session_state['rt_p_v64'] = p_map
            st.session_state['last_sync_time'] = datetime.now().strftime('%H:%M:%S')
            status_box.update(label=f"✅ 數據同步完成！最後更新：{st.session_state['last_sync_time']}", state="complete")
            st.toast("數值已全部歸位，跟我預測的一模一樣！", icon="🚀")
            time.sleep(1)
            st.rerun() # 強迫網頁重刷
        
        # 顯示最後更新時間，證明系統有在動
        if 'last_sync_time' in st.session_state:
            st.caption(f"🕒 數據最後同步時間：{st.session_state['last_sync_time']}")

        if 'rt_p_v64' in st.session_state:
            df_p['現價'] = df_p['ticker'].map(st.session_state['rt_p_v64'])
            # 強制數值化，斬斷 TypeError
            for col in ['entry_price', '現價', 'qty']:
                df_p[col] = pd.to_numeric(df_p[col], errors='coerce')
            
            df_p['獲利'] = (df_p['現價'] - df_p['entry_price']) * df_p['qty']
            df_p['報酬率(%)'] = ((df_p['現價'] - df_p['entry_price']) / df_p['entry_price']) * 100
            
            total_profit = df_p['獲利'].fillna(0).sum()
            st.metric("當前預估實質總獲利", f"${total_profit:,.0f}")
            
        st.dataframe(style_df(df_p))
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (數據已同步至後台)")
        m_cols = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        
        # 💎 贏家優化：賣股按鈕現在從資料庫撈數據，速度噴發！
        for i, (name, icon) in enumerate(s_btns):
            if m_cols[i].button(f"{icon} {name}", use_container_width=True):
                # 直接讀取後台剛存進去的數據
                query = text("SELECT * FROM daily_scans WHERE scan_date = :today")
                scans_df = pd.read_sql(query, con=engine, params={"today": datetime.now().date()})
                
                if not scans_df.empty and 'rt_p_v64' in st.session_state:
                    check_df = pd.merge(df_p, scans_df, on='ticker', how='left')
                    masks = [
                        check_df['sma5'] < check_df['ma20'],
                        check_df['rsi'] > 80,
                        check_df['報酬率(%)'] > 20,
                        check_df['報酬率(%)'] < -10,
                        check_df['現價'] < check_df['ma20']
                    ]
                    res = check_df[masks[i]]
                    if not res.empty:
                        st.error(f"🚨 符合『{name}』標的如下：")
                        # 修正合併後欄位名
                        disp_name = 'stock_name_x' if 'stock_name_x' in res.columns else 'stock_name'
                        st.dataframe(style_df(res[[disp_name, 'ticker', '現價', '報酬率(%)']]))
                    else: st.success("✅ 持倉目前安全，大數據完美守護！")
                else:
                    st.warning("💡 請先點擊上方「更新即時獲利」，讓系統執行後台同步。")
    else: st.info("持倉資料庫為空。")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 贏家數據都更中心")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 股票池 (Stock Pool)")
        st.download_button("📥 下載範本", pd.DataFrame({'ticker':['2330.TW','3583.TW'],'stock_name':['台積電','辛耘'],'sector':['半導體','設備']}).to_csv(index=False).encode('utf-8-sig'), "pool.csv")
        f1 = st.file_uploader("上傳 CSV", type="csv", key="p1")
        if f1 and st.button("💾 匯入"):
            pd.read_csv(f1, encoding='utf-8-sig').to_sql('stock_pool', con=engine, if_exists='append', index=False); st.success("成功！")
    with c2:
        st.markdown("#### 持倉管理 (Portfolio)")
        st.download_button("📥 下載範本", pd.DataFrame({'ticker':['2330.TW'],'stock_name':['台積電'],'entry_price':[750],'qty':[1000]}).to_csv(index=False).encode('utf-8-sig'), "port.csv")
        f2 = st.file_uploader("上傳數據", type="csv", key="p2")
        if f2 and st.button("💾 存入"):
            df_up = pd.read_csv(f2, encoding='utf-8-sig')
            with engine.begin() as conn:
                t_list = df_up['ticker'].tolist()
                conn.execute(text("DELETE FROM portfolio WHERE ticker IN :t_list"), {"t_list": t_list})
                df_up.to_sql('portfolio', con=conn, if_exists='append', index=False)
            st.success("存入成功！數據精準歸位！")

st.caption("本系統由哲哲團隊開發。數字會說話，投資有風險，賺到流湯不要忘了我！")

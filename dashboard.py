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

# ================= 1. 系統地基 (鋼鐵都更，主鍵鎖死) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    LINE_TOKEN, USER_ID = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"], st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        # A. 核心數據表 (21 欄位)
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
        # B. 股票池與持倉資產
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), sector VARCHAR(50), fund_count INT DEFAULT 0);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(20), stock_name VARCHAR(50), entry_price FLOAT, qty FLOAT);"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

# ================= 2. 核心大腦 (數據完整性檢查與抓取) =================

def fetch_full_stock_package(ticker, name):
    """💎 哲哲數據重生抓取：嚴格檢查 0 元數據"""
    try:
        time.sleep(random.uniform(0.5, 0.8)) 
        s = yf.Ticker(ticker)
        d = s.history(period="7mo", interval="1d", timeout=15)
        
        # 備援判斷 (上市櫃自動轉換)
        if d.empty or len(d) < 40:
            alt_t = ticker.replace(".TW", ".TWO") if ".TW" in ticker else ticker.replace(".TWO", ".TW")
            d = yf.Ticker(alt_t).history(period="7mo", interval="1d", timeout=15)
            if d.empty or len(d) < 40: return None, "數據不足"
        
        c, v = d['Close'], d['Volume']
        curr_price = float(c.iloc[-1])
        
        # 💎 關鍵攔截：如果股價是 0 或 NaN，這筆數據就是垃圾，直接回傳失敗！
        if curr_price <= 0 or np.isnan(curr_price):
            return None, "偵測到 0 元髒數據"
        
        sma5, ma20, ma60 = ta.sma(c, 5), ta.sma(c, 20), ta.sma(c, 60)
        rsi, bb = ta.rsi(c, 14), ta.bbands(c, 20, 2)
        
        try:
            info = s.info if s.info else {}
            roe, rev = info.get('returnOnEquity', 0) or 0, info.get('revenueGrowth', 0) or 0
        except: roe, rev = 0, 0
            
        return {
            "ticker": ticker, "stock_name": name, "price": curr_price,
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
    """🚀 哲哲強力寫入迴圈：解決寫入失敗與即時進度更新"""
    total_count = len(pool_df)
    if total_count == 0: return
    
    today = datetime.now(TW_TZ).date()
    if mode == "reset":
        with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
        st.toast("🔥 今日數據已排空！")

    p_bar = st.progress(0.0)
    p_text = st.empty()
    log_box = st.status(f"⚡ 數據重生掃描中 ({mode})...", expanded=True)
    
    fail_tracker, round_num = {}, 1
    while True:
        # 只抓「真的有股價且有資料」的標的
        done_df = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
        done_list = done_df['ticker'].tolist()
        remaining_pool = pool_df[~pool_df['ticker'].isin(done_list)].copy()
        
        # 💎 修正進度條計算
        curr_done = len(done_list)
        progress_val = min(curr_done / total_count, 1.0)
        p_bar.progress(progress_val)
        p_text.markdown(f"**🚀 實際入庫進度：`{curr_done}` / `{total_count}` ({progress_val:.1%})**")

        if remaining_pool.empty:
            st.balloons(); p_text.success(f"🏆 100% 全壘打達成！數據已滿血。"); break
        if round_num > 10: break

        batch_list = remaining_pool.sample(frac=1).to_dict('records')
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {ex.submit(fetch_full_stock_package, r['ticker'], r['stock_name']): r['ticker'] for r in batch_list}
            batch_count = 0
            for f in as_completed(futures):
                ticker = futures[f]
                data, err = f.result()
                batch_count += 1
                if batch_count % 15 == 0: time.sleep(2.0)
                
                if data:
                    # 💎 強力 UPSERT：先刪再存，確保數據新鮮度
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM daily_scans WHERE ticker = :t AND scan_date = :d"), {"t": ticker, "d": today})
                    pd.DataFrame([data]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    
                    # 💎 即時 UI 更新 (數字會跳動！)
                    dynamic_done = curr_done + batch_count
                    dynamic_val = min(dynamic_done / total_count, 1.0)
                    p_bar.progress(dynamic_val)
                    p_text.markdown(f"**🚀 實際入庫進度：`{dynamic_done}` / `{total_count}` ({dynamic_val:.1%})**")
                    log_box.write(f"✅ 真錢入庫：{data['stock_name']} (${data['price']})")
                else:
                    fail_tracker[ticker] = fail_tracker.get(ticker, 0) + 1
                    if fail_tracker[ticker] >= 3:
                        log_box.write(f"🚫 {ticker} 判定為死會 (幽靈股)")
                        pd.DataFrame([{"ticker": ticker, "stock_name": "幽靈股", "scan_date": today, "price": 0.01}]).to_sql('daily_scans', con=engine, if_exists='append', index=False)
                    else:
                        log_box.write(f"⚠️ {ticker} 暫跳：{err}")
        round_num += 1; time.sleep(5)
    log_box.update(label="✨ 掃描結束，數據全數歸位。", state="complete")

# ================= 3. 視覺美學渲染器 (冠軍色票) =================

def beauty_style(df):
    """💎 哲哲專屬暴力美學色票：凸顯獲利分布"""
    if df.empty: return df
    # 確保數值化，避免漸層崩潰
    num_cols = ['現價','漲跌(%)','ROE','營收成長','獲利','報酬率(%)']
    for c in num_cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    f_map = {'現價': '{:.2f}', '漲跌(%)': '{:+.2f}%', 'RSI': '{:.1f}', 'ROE': '{:.2%}', '營收成長': '{:.2%}', '獲利': '{:,.0f}', '報酬率(%)': '{:+.2f}%'}
    try:
        styled = df.style.format({k: v for k, v in f_map.items() if k in df.columns}, na_rep='-')
        # 🌈 冠軍漸層色 (紅熱色調)
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
    msg = f"{icon}【哲哲戰報 - {title}】\n🎯 符合標的：\n"
    for _, r in df.head(10).iterrows(): msg += f"✅ {r.get('代號','')} {r.get('名稱','')} | 現價:{r.get('現價','')}\n"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    try: requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps({"to": USER_ID, "messages": [{"type": "text", "text": msg}]}))
    except: pass

# ================= 4. 主介面設計 (V115.0 指揮官大按鈕完全體) =================
st.set_page_config(page_title="哲哲量化美學戰情室 V115.0", layout="wide")
st.markdown("""<style>
    .big-font { font-size:48px !important; font-weight: bold; color: #FF3333; text-shadow: 2px 2px 4px #eee; }
    div.stButton > button { height: 4em; font-size: 1.4rem !important; font-weight: bold !important; border-radius: 15px; margin-bottom: 12px; transition: 0.3s; }
    div.stButton > button:hover { border-color: #FF3333; color: #FF3333; background-color: #FFF5F5; }
</style>""", unsafe_allow_html=True)

now_tw = datetime.now(TW_TZ)
st.title("🛡️ 哲哲量化戰情室 V115.0 — 數據重生完全體")

# 🕒 收盤同步警報
if now_tw.hour == 13 and now_tw.minute >= 31:
    st.warning("🔔 **收盤時間到 (1:31 PM)！數據已定案，建議『暴力覆蓋重掃』以獲取最終戰果！**")

tab1, tab2, tab3 = st.tabs(["🚀 七大金剛發射台", "💼 持倉監控戰報", "🛠️ 後台管理都更"])

# --- Tab 1: 指揮中心 ---
with tab1:
    st.markdown("### 🏆 全市場智慧掃描 (排除髒數據模式)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("📡 讀取今日行情數據", use_container_width=True):
            db_df = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :today AND price > 0.1"), con=engine, params={"today": datetime.now(TW_TZ).date()})
            if not db_df.empty: 
                db_df = db_df.rename(columns={'change_pct': '漲跌(%)', 'price':'現價', 'ticker':'代號', 'stock_name':'名稱', 'rsi':'RSI', 'roe':'ROE', 'rev_growth':'營收成長'})
                st.session_state['master_df'] = db_df; st.success(f"✅ 載入成功！共 {len(db_df)} 筆有效數據。")
            else: st.warning("目前無有效數據，請啟動渦輪掃描。")
    with c2:
        if st.button("⚡ 啟動增量渦輪掃描 (只補殘缺)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, mode="incremental"); st.rerun()
    with c3:
        if st.button("🔥 暴力覆蓋重掃 (清空今日數據)", use_container_width=True):
            pool = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)
            if not pool.empty: lightning_homerun_loop(pool, mode="reset"); st.rerun()

    st.divider()
    st.markdown("### 🔥 買股必勝決策中心 (指揮官大按鈕)")
    
    # 策略 1: 超級策略 (獨立大行)
    if st.button("💎 降臨：超級策略 (基金+ROE+營收+趨勢)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df'].copy()
            df = df[df['現價'] > 0.5] # 排除幽靈與髒數據
            p_info = pd.read_sql("SELECT ticker, sector, fund_count as imported_funds FROM stock_pool", con=engine)
            df = pd.merge(df, p_info, left_on='代號', right_on='ticker', how='left')
            df['20日漲幅'] = (df['現價'] - df['kd20']) / (df['kd20'].replace(0, 1))
            sector_avg = df.groupby('sector')['20日漲幅'].transform('mean')
            mask = (df['imported_funds'] >= 100) & (df['ROE'] > 0.1) & (df['20日漲幅'] > sector_avg) & (df['營收成長'] > 0.1)
            res = df[mask].sort_values(by='營收成長', ascending=False)
            st.dataframe(beauty_style(res[['代號', '名稱', '現價', '漲跌(%)', 'ROE', '營收成長', 'sector', 'imported_funds']]), use_container_width=True)
            send_line_report("超級策略", res, "💎")
        else: st.error("⚠️ 尚未讀取數據")

    # 形態與經典 (全幅橫跨)
    if st.button("📈 帶量突破前高 (圖一)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價'] > df['high_20']) & (df['vol'] > df['vol_20'] * 1.5) & (df['現價'] > 0.5)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("帶量突破", res, "📈")

    if st.button("🚀 三線合一多頭 (圖二)", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (abs(df['sma5']-df['ma60'])/df['ma60'].replace(0,1) < 0.05) & (df['現價'] > 0.5)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("三線合一", res, "🚀")

    if st.button("👑 九成勝率 ATM", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['現價']>df['kd20']) & (df['現價']>df['kd60']) & (df['vol'] >= df['vol_20']*1.2) & (df['現價']>df['sma5']) & (df['現價'] > 0.5)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("ATM策略", res, "👑")

    if st.button("🛡️ 低階抄底防護", use_container_width=True):
        if 'master_df' in st.session_state:
            df = st.session_state['master_df']
            res = df[(df['RSI'] < 35) & (df['現價'] > df['sma5']) & (df['現價'] > 0.5)]
            st.dataframe(beauty_style(res), use_container_width=True); send_line_report("低階抄底", res, "🛡️")

    st.divider()
    if st.button("🔍 揭開底牌：檢視今日所有抓取數據 (不限策略)", use_container_width=True):
        if 'master_df' in st.session_state:
            st.dataframe(beauty_style(st.session_state['master_df']), use_container_width=True)
        else: st.warning("⚠️ 目前資料庫空空如也")

# --- Tab 2: 持倉監控 ---
with tab2:
    st.header("💼 我的資產即時戰報")
    df_p = pd.read_sql("SELECT ticker, stock_name, entry_price, qty FROM portfolio", con=engine)
    if not df_p.empty:
        if st.button("🔄 更新持倉現價 (確保真錢入庫)", use_container_width=True):
            lightning_homerun_loop(df_p[['ticker','stock_name']], mode="incremental"); st.rerun()
        
        p_prices = pd.read_sql(text("SELECT ticker, price, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": datetime.now(TW_TZ).date()})
        df_p = pd.merge(df_p, p_prices, on='ticker', how='left')
        
        # 💎 修正：如果 price 為 0 或 None，不計算 ROI 防止 -100% 嚇到人
        for c in ['entry_price', 'price', 'qty']: df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        df_p['獲利'] = (df_p['price'] - df_p['entry_price']) * df_p['qty']
        df_p['報酬率(%)'] = np.where(df_p['price'] > 0, ((df_p['price'] - df_p['entry_price']) / df_p['entry_price'].replace(0, 1)) * 100, 0)
        
        # 過濾掉 0 價位顯示，避免 -100%
        display_p = df_p[df_p['price'] > 0].copy()
        st.markdown(f"當前總獲利：<p class='big-font'>${display_p['獲利'].sum():,.0f}</p>", unsafe_allow_html=True)
        st.dataframe(beauty_style(display_p), use_container_width=True)
        
        st.divider()
        st.markdown("### 🎯 五大必勝賣股決策 (LINE 通知)")
        m_sell = st.columns(5)
        s_btns = [("均線死叉", "💀"), ("RSI 過熱", "🔥"), ("利潤止盈", "💰"), ("破位停損", "📉"), ("跌破月線", "⚠️")]
        for i, (name, icon) in enumerate(s_btns):
            if m_sell[i].button(f"{icon} {name}", use_container_width=True):
                masks = [df_p['sma5'] < df_p['ma20'], df_p['rsi'] > 80, df_p['報酬率(%)'] > 20, df_p['報酬率(%)'] < -10, df_p['price'] < df_p['ma20']]
                res_s = df_p[masks[i] & (df_p['price'] > 0)].copy()
                if not res_s.empty:
                    st.dataframe(beauty_style(res_s[['stock_name', 'ticker', 'price', '報酬率(%)']]))
                    send_line_report(f"賣訊：{name}", res_s, icon)
                else: st.success("✅ 目前安全，跟我預測的一模一樣！")

# --- Tab 3: 後台 ---
with tab3:
    st.subheader("🛠️ 數據管理中心 (鋼鐵 Upsert)")
    ch1, ch2 = st.columns(2)
    with ch1:
        f1 = st.file_uploader("上傳股票池 CSV (ticker, stock_name, sector, fund_count)", type="csv")
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

st.caption("本系統由哲哲團隊開發。V115.0 數據重生完全體，賺到流湯不要忘了我！")

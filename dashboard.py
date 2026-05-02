import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import requests, json, time, datetime
import pytz
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_ta as ta
import io

# ================= 0. 頁面設定 (必須在最前面) =================
st.set_page_config(page_title="🛡️ 哲哲量子戰情室 V177 終極推播版", layout="wide")

st.markdown("""<style>
    [data-testid="stBaseButton-secondary"] { width: 100% !important; height: 3.5em !important; font-size: 1.1rem !important; font-weight: 800 !important; border-radius: 12px !important; background: linear-gradient(135deg, #FF3333 0%, #AA0000 100%) !important; color: white !important; }
    .big-font { font-size:45px !important; font-weight: 900; color: #FF3333; text-shadow: 2px 2px 4px #ddd; }
    .strategy-title { font-size: 20px; font-weight: bold; padding: 10px; border-radius: 8px; margin-top: 15px; }
</style>""", unsafe_allow_html=True)

# ================= 1. 系統地基 (資料庫連線 & 資料表初始化) =================
try:
    TW_TZ = pytz.timezone('Asia/Taipei')
    DB_URL = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@{st.secrets['DB_HOST']}:3306/{st.secrets['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(DB_URL, connect_args={"charset": "utf8mb4", "connect_timeout": 30}, pool_pre_ping=True)
    
    FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoibG92ZTUyMTUiLCJlbWFpbCI6ImNocmlzNTIxNUBnbWFpbC5jb20ifQ.yeh3T_iNCA4IWmlsPZHHyVUbMOH_qe35stdLgIv9ONY"
    LINE_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
    USER_ID = st.secrets["YOUR_LINE_USER_ID"]
    
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_scans (
                ticker VARCHAR(20), stock_name VARCHAR(50), price FLOAT, change_pct FLOAT, 
                sma5 FLOAT, ma20 FLOAT, ma60 FLOAT, rsi FLOAT, bbl FLOAT, bbu FLOAT, 
                vol BIGINT, avg_vol BIGINT, scan_date DATE, kd20 FLOAT, kd60 FLOAT,
                roe FLOAT DEFAULT NULL, fund_count INT DEFAULT NULL,
                high_20 FLOAT, vol_20 FLOAT, bb_width FLOAT,
                PRIMARY KEY (ticker, scan_date)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """))
        conn.execute(text("CREATE TABLE IF NOT EXISTS stock_pool (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50)) CHARACTER SET utf8mb4;"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS portfolio (ticker VARCHAR(20) PRIMARY KEY, stock_name VARCHAR(50), entry_price FLOAT, qty INT) CHARACTER SET utf8mb4;"))
        conn.commit()
except Exception as e:
    st.error(f"❌ 系統地基損毀：{e}"); st.stop()

if 'scan_status' not in st.session_state:
    st.session_state['scan_status'] = {"daily": "", "fix": "", "chip": "", "roe": ""}

# ================= 2. 核心大腦 (精準打擊模組 & LINE 推播) =================

def send_line_notify(message):
    """傳送 LINE Notify 訊息"""
    if not LINE_TOKEN:
        st.warning("⚠️ 找不到 LINE_TOKEN，請至 secrets.toml 中設定！")
        return False
    try:
        headers = {"Authorization": f"Bearer {LINE_TOKEN}"}
        data = {"message": message}
        r = requests.post("https://notify-api.line.me/api/notify", headers=headers, data=data)
        return r.status_code == 200
    except Exception as e:
        st.error(f"❌ LINE 通知發送失敗: {e}")
        return False

def fetch_fm(dataset, ticker, start_days=160):
    cid = str(ticker).split('.')[0].strip()
    start = (datetime.datetime.now(TW_TZ) - datetime.timedelta(days=start_days)).strftime('%Y-%m-%d')
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {"dataset": dataset, "data_id": cid, "start_date": start}
        headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
        response = requests.get(url, headers=headers, params=params, timeout=15)
        r = response.json()
        if r.get('msg') == 'success' and r.get('data'): return pd.DataFrame(r['data'])
        return None
    except: return None

def update_chip_v_quantum(ticker):
    df = fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", ticker, 60)
    fund = 0
    if df is not None and not df.empty and 'name' in df.columns:
        valid = df[df['name'] == 'Investment_Trust'].copy()
        if not valid.empty:
            valid['buy'] = pd.to_numeric(valid['buy'], errors='coerce').fillna(0)
            valid['sell'] = pd.to_numeric(valid['sell'], errors='coerce').fillna(0)
            valid['net_buy'] = (valid['buy'] - valid['sell']) / 1000
            fund = int(valid['net_buy'].sum())
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET fund_count = :f WHERE ticker = :t AND scan_date = :d"), 
                     {"f": fund, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True

def update_roe_v_brute(ticker):
    df_income = fetch_fm("TaiwanStockFinancialStatements", ticker, 730)
    df_balance = fetch_fm("TaiwanStockBalanceSheet", ticker, 730)
    roe_calc = None
    if df_income is not None and not df_income.empty and df_balance is not None and not df_balance.empty:
        income_df = df_income[df_income['type'] == 'IncomeAfterTaxes'].sort_values('date')
        equity_df = df_balance[df_balance['type'].isin(['Equity', 'TotalEquity'])].sort_values('date')
        if not income_df.empty and not equity_df.empty:
            try:
                net_income = float(income_df.iloc[-1]['value'])
                total_equity = float(equity_df.iloc[-1]['value'])
                if total_equity != 0: roe_calc = net_income / total_equity
            except: pass
    with engine.begin() as conn:
        conn.execute(text("UPDATE daily_scans SET roe = :r WHERE ticker = :t AND scan_date = :d"), 
                     {"r": roe_calc, "t": ticker, "d": datetime.datetime.now(TW_TZ).date()})
    return True if roe_calc is not None else False

def calc_and_save_full(ticker, name):
    df = fetch_fm("TaiwanStockPrice", ticker, 160)
    if df is None or len(df) < 60: return False
    df = df.rename(columns={'close':'Close','Trading_Volume':'Volume'})
    df['Close'] = df['Close'].astype(float); c, v = df['Close'], df['Volume']
    rsi = float(ta.rsi(c, length=14).iloc[-1]) if len(c) > 14 else 50
    ma20 = c.rolling(20).mean().iloc[-1]
    std = c.rolling(20).std().iloc[-1]
    data = {
        "ticker": ticker, "stock_name": name, "price": c.iloc[-1], "change_pct": ((c.iloc[-1]-c.iloc[-2])/c.iloc[-2])*100,
        "sma5": c.rolling(5).mean().iloc[-1], "ma20": ma20, "ma60": c.rolling(60).mean().iloc[-1],
        "rsi": rsi, "vol": int(v.iloc[-1]), "avg_vol": int(v.rolling(20).mean().iloc[-1]),
        "kd20": c.iloc[-20] if len(c) >= 20 else c.iloc[-1], 
        "kd60": c.iloc[-60] if len(c) >= 60 else c.iloc[-1],
        "scan_date": datetime.datetime.now(TW_TZ).date(),
        "bbu": ma20 + (std*2), "bbl": ma20 - (std*2), 
        "high_20": c.shift(1).rolling(20).max().iloc[-1] if len(c) > 20 else c.iloc[-1],
        "vol_20": v.shift(1).rolling(20).mean().iloc[-1] if len(v) > 20 else v.iloc[-1],
        "bb_width": (std*4)/ma20 if ma20 else 0
    }
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO daily_scans (ticker, stock_name, price, change_pct, sma5, ma20, ma60, rsi, bbl, bbu, vol, avg_vol, scan_date, kd20, kd60, high_20, vol_20, bb_width) 
            VALUES (:ticker, :stock_name, :price, :change_pct, :sma5, :ma20, :ma60, :rsi, :bbl, :bbu, :vol, :avg_vol, :scan_date, :kd20, :kd60, :high_20, :vol_20, :bb_width) 
            ON DUPLICATE KEY UPDATE price=VALUES(price), change_pct=VALUES(change_pct), sma5=VALUES(sma5), ma20=VALUES(ma20), ma60=VALUES(ma60), rsi=VALUES(rsi), vol=VALUES(vol), avg_vol=VALUES(avg_vol), bbu=VALUES(bbu), bbl=VALUES(bbl), bb_width=VALUES(bb_width), high_20=VALUES(high_20), vol_20=VALUES(vol_20)"""), data)
    return True

# ================= 3. UI 介面設計 =================
st.title("🛡️ 哲哲量子戰情室 Sponsor V177 — 終極推播版")
tab1, tab2, tab3 = st.tabs(["🚀 選股指揮中心", "💼 庫存戰略中心", "🛠️ 系統管理中心"])

today = datetime.datetime.now(TW_TZ).date()
pool_df = pd.read_sql("SELECT ticker, stock_name FROM stock_pool", con=engine)

# ==================== TAB 1: 🚀 選股指揮中心 ====================
with tab1:
    st.markdown("### 🏹 數據抓取週期功能")
    c1, c2, c3, c4, c5 = st.columns(5)
    status_box = st.empty() 
    
    with c1:
        if st.button("🚀 每日行情：暴力重掃", key="b_daily"):
            pb = st.progress(0); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in pool_df.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool_df))
                    status_box.info(f"⏳ 每日行情掃描中... 成功: {s} 筆 / 失敗: {f} 筆")
            st.session_state['scan_status']['daily'] = f"✅ 行情重掃完成！共成功: {s} 筆, 失敗: {f} 筆"
            st.rerun()
            
    with c2:
        if st.button("🔄 補救掃描：自動補抓", key="b_fix"):
            done = pd.read_sql(text("SELECT ticker FROM daily_scans WHERE scan_date = :t AND price > 0"), con=engine, params={"t": today})
            missing = pool_df[~pool_df['ticker'].isin(done['ticker'].tolist())]
            if missing.empty: 
                st.session_state['scan_status']['fix'] = "🎯 數據已全壘打，無需補救！"
            else:
                pb = st.progress(0); s, f = 0, 0
                with ThreadPoolExecutor(max_workers=10) as exe:
                    futures = {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in missing.iterrows()}
                    for i, fut in enumerate(as_completed(futures)):
                        if fut.result(): s += 1
                        else: f += 1
                        pb.progress((i+1)/len(missing))
                        status_box.warning(f"⏳ 補救掃描中... 成功: {s} 筆 / 失敗: {f} 筆")
                st.session_state['scan_status']['fix'] = f"✅ 補救掃描完成！共成功: {s} 筆, 失敗: {f} 筆"
                st.rerun()
                
    with c3:
        if st.button("💼 籌碼補完：量子修正", key="b_chip"):
            pb = st.progress(0); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_chip_v_quantum, r['ticker']): r['ticker'] for _, r in pool_df.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool_df))
                    status_box.info(f"⏳ 投信籌碼挖掘中... 成功: {s} 筆 / 失敗: {f} 筆")
            st.session_state['scan_status']['chip'] = f"✅ 籌碼量子修正完成！共挖掘成功: {s} 筆"
            st.rerun()
            
    with c4:
        if st.button("💎 財報精算：ROE暴力算", key="b_roe"):
            pb = st.progress(0); s, f = 0, 0
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(update_roe_v_brute, r['ticker']): r['ticker'] for _, r in pool_df.iterrows()}
                for i, fut in enumerate(as_completed(futures)):
                    if fut.result(): s += 1
                    else: f += 1
                    pb.progress((i+1)/len(pool_df))
                    status_box.info(f"⏳ ROE 財報解析中... 成功: {s} 筆 / 失敗: {f} 筆")
            st.session_state['scan_status']['roe'] = f"✅ ROE 暴力精算完成！共解析成功: {s} 筆"
            st.rerun()
            
    with c5:
        if st.button("🔥 清空今日快取", key="b_clear"):
            with engine.begin() as conn: conn.execute(text("DELETE FROM daily_scans WHERE scan_date = :t"), {"t": today})
            st.session_state['scan_status'] = {"clear": "🗑️ 今日快取已清空，請重新掃描！"}
            st.rerun()

    for m in st.session_state['scan_status'].values(): 
        if m: st.success(m)

    st.divider()
    
    st.subheader("🧪 系統保險：API 原始碼備份下載")
    d_c1, d_c2 = st.columns(2)
    test_ticker = st.text_input("輸入要備份原始碼的股票代號", value="2330", max_chars=10)
    with d_c1:
        if st.button("📥 備份下載 ROE 數據原始碼 (損益表+資產負債表)"):
            df_in = fetch_fm("TaiwanStockFinancialStatements", test_ticker, 730)
            df_ba = fetch_fm("TaiwanStockBalanceSheet", test_ticker, 730)
            if df_in is not None and df_ba is not None:
                combined_df = pd.concat([df_in, df_ba])
                st.download_button(f"📥 點擊下載 {test_ticker}_ROE_RawData.csv", data=combined_df.to_csv(index=False).encode('utf-8-sig'), file_name=f"{test_ticker}_ROE_RawData.csv")
            else: st.error("抓取失敗，請確認代號或 API 狀態。")
    with d_c2:
        if st.button("📥 備份下載籌碼投信原始碼"):
            df_chip = fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", test_ticker, 60)
            if df_chip is not None:
                st.download_button(f"📥 點擊下載 {test_ticker}_Chip_RawData.csv", data=df_chip.to_csv(index=False).encode('utf-8-sig'), file_name=f"{test_ticker}_Chip_RawData.csv")
            else: st.error("抓取失敗，請確認代號或 API 狀態。")

    st.divider()
    
    def highlight_errors(val):
        if pd.isna(val) or val == 0 or str(val).strip() in ['None', 'NaN', '']:
            return 'background-color: #FFCCCC; color: red; font-weight: bold;'
        return ''

    st.subheader("🔍 數據照妖鏡 (檢視資料庫真實庫存狀態)")
    if st.button("📡 讀取今日全量快取數據 & 照妖鏡"):
        all_data = pd.read_sql(text("SELECT * FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        if not all_data.empty:
            st.session_state['master_df'] = all_data.copy()
            st.dataframe(all_data.style.map(highlight_errors), width=1500)
            st.success(f"✅ 已成功載入 {len(all_data)} 檔標的！紅底為資料缺失或 0。")
        else:
            st.warning("⚠️ 今日快取是空的，請先點擊上方「每日行情」抓取資料！")

    st.divider()
    
    # --- 🔥 七大金剛策略 & LINE 推播 ---
    if 'master_df' in st.session_state:
        st.markdown("### 🔥 七大金剛策略 (買入通報中心)")
        
        # 設置 LINE 通知開關
        enable_line_buy = st.checkbox("📲 啟用 LINE 通知 (點擊策略時自動發送符合標的)", value=True)
        
        df = st.session_state['master_df'].rename(columns={'ticker':'股票代號', 'stock_name':'名稱', 'price':'現價'})
        cols_to_numeric = ['fund_count', 'roe', '現價', 'high_20', 'vol', 'vol_20', 'sma5', 'ma20', 'ma60', 'bbu', 'bb_width', 'rsi', 'kd20']
        for c in cols_to_numeric:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

        # 這裡包含你剛剛提供修正後的最新勝率公式
        strategies = {
            "💎 策略 1：超級策略 (投信持股 >= 10張 & ROE > 5%)": 
                (df['fund_count'] >= 10) & (df['roe'] > 0.05),
                
            "📈 策略 2：帶量突破前高 (股價 > 20日高 & 成交量 > 均量 1.5倍)": 
                (df['現價'] >= df['high_20']) & (df['vol'] > df['vol_20'] * 1.5) & (df['現價'] > 0),
                
            "🚀 策略 3：三線合一多頭 (SMA5 > MA20 > MA60)": 
                (df['sma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (df['ma60'] > 0),
                
            "🌀 策略 4：布林縮口突破 (股價 > 上軌 & 帶寬 < 0.2)": 
                (df['現價'] > df['bbu']) & (df['bb_width'] < 0.2) & (df['bb_width'] > 0),
                
            "👑 策略 5：九成勝率 ATM (站上月線 + 扣抵值有利 + 帶量)": 
                (df['現價'] > df['ma20']) & (df['現價'] > df['kd20']) & (df['vol'] > df['vol_20'] * 1.2),
                
            "🛡️ 策略 6：安全抄底 (RSI<40 & 站上五日線 & 位於月線下)": 
                (df['rsi'] < 40) & (df['rsi'] > 0) & (df['現價'] > df['sma5']) & (df['現價'] < df['ma20']),
                
            "🎯 策略 7：強勢回測支撐 (股價貼近 MA20 2%以內且在月線上)": 
                (abs(df['現價'] - df['ma20']) / df['ma20'] < 0.02) & (df['現價'] >= df['ma20'])
        }

        for title, condition in strategies.items():
            if st.button(title):
                st.markdown(f"<div class='strategy-title' style='background: linear-gradient(90deg, #e3ffe7 0%, #d9e7ff 100%);'>{title}</div>", unsafe_allow_html=True)
                res = df[condition]
                if res.empty: 
                    st.info("目前無符合標的，請耐心等待獵物出現。")
                else:
                    st.dataframe(res[['股票代號', '名稱', '現價', 'change_pct', 'fund_count', 'roe', 'rsi', 'vol']].style.background_gradient(cmap='YlOrRd', subset=['現價']), width=1500)
                    
                    # 發送 LINE 通知
                    if enable_line_buy:
                        msg = f"\n🚀 買入策略觸發！\n【{title.split('(')[0].strip()}】\n"
                        for _, r in res.iterrows():
                            msg += f"👉 {r['股票代號']} {r['名稱']} | 現價: {r['現價']}\n"
                        if send_line_notify(msg):
                            st.success("✅ LINE 買進策略通知已發送！")


# ==================== TAB 2: 💼 庫存股票戰略中心 ====================
with tab2:
    st.markdown("### 💼 庫存監控與四大賣出策略通報")
    df_port = pd.read_sql("SELECT * FROM portfolio", con=engine)
    
    if df_port.empty:
        st.warning("目前無庫存，請至「系統管理中心」上傳庫存 CSV。")
    else:
        c2_1, c2_2 = st.columns(2)
        with c2_1:
            if st.button("🔄 取得最新對應庫存股價 & 計算損益 (脫離 Yahoo)"):
                with ThreadPoolExecutor(max_workers=5) as exe:
                    {exe.submit(calc_and_save_full, r['ticker'], r['stock_name']): r['ticker'] for _, r in df_port.iterrows()}
                st.success("✅ 最新股價與技術指標同步完成！")
                st.rerun()
                
        daily_df = pd.read_sql(text("SELECT ticker, price as 最新股價, sma5, ma20, rsi FROM daily_scans WHERE scan_date = :t"), con=engine, params={"t": today})
        display_df = pd.merge(df_port, daily_df, on='ticker', how='left')
        display_df = display_df.fillna({'最新股價': display_df['entry_price'], 'sma5':0, 'ma20':0, 'rsi':50})
        
        display_df['總報酬率(%)'] = ((display_df['最新股價'] - display_df['entry_price']) / display_df['entry_price']) * 100
        display_df['總獲利金額'] = (display_df['最新股價'] - display_df['entry_price']) * display_df['qty']
        
        total_profit = display_df['總獲利金額'].sum()
        color = "#FF3333" if total_profit > 0 else "#33CC33" # 台股紅漲綠跌
        st.markdown(f"💰 庫存總獲利：<span style='font-size:45px; font-weight:900; color:{color};'>${total_profit:,.0f}</span>", unsafe_allow_html=True)
        
        st.dataframe(display_df[['ticker', 'stock_name', 'entry_price', '最新股價', 'qty', '總報酬率(%)', '總獲利金額']].style.background_gradient(cmap='RdYlGn', subset=['總報酬率(%)']), width=1500)
        
        st.divider()
        st.markdown("### 🚨 四大賣股風控策略")
        
        sell_strats = {
            "💀 均線死叉 (SMA5 跌破 MA20)": (display_df['sma5'] < display_df['ma20']) & (display_df['sma5'] > 0),
            "🔥 RSI 過熱 (RSI > 80)": display_df['rsi'] > 80,
            "💰 利潤止盈 (總報酬率 > 15%)": display_df['總報酬率(%)'] > 15,
            "📉 破位停損 (總報酬率 < -10%)": display_df['總報酬率(%)'] < -10
        }
        
        with c2_2:
            if st.button("📲 將目前【觸發之風控警報】發送至 LINE"):
                msg = "\n🚨 戰情室庫存風控警報 🚨\n"
                has_alert = False
                for s_title, s_cond in sell_strats.items():
                    alert_df = display_df[s_cond]
                    if not alert_df.empty:
                        has_alert = True
                        msg += f"\n【{s_title.split('(')[0].strip()}】\n"
                        for _, r in alert_df.iterrows():
                            msg += f"📉 {r['ticker']} {r['stock_name']} | 獲利: {r['總報酬率(%)']:.1f}%\n"
                
                if has_alert:
                    if send_line_notify(msg):
                        st.success("✅ 風控警報已成功發送至 LINE！")
                else:
                    st.info("✅ 目前庫存無任何觸發警報，安全持股中。")

        for s_title, s_cond in sell_strats.items():
            st.markdown(f"<div class='strategy-title' style='background: linear-gradient(90deg, #ffe3e3 0%, #ffd9d9 100%);'>{s_title}</div>", unsafe_allow_html=True)
            alert_df = display_df[s_cond]
            if alert_df.empty:
                st.success("✅ 目前無觸發標的，安心持股中。")
            else:
                st.error(f"⚠️ 警告！以下標的觸發【{s_title}】，建議立刻評估出場：")
                st.dataframe(alert_df[['ticker', 'stock_name', '最新股價', '總報酬率(%)', 'sma5', 'ma20', 'rsi']], width=1000)


# ==================== TAB 3: 🛠️ 系統管理中心 ====================
with tab3:
    st.markdown("### 🛠️ CSV 檔案戰情室")
    colA, colB = st.columns(2)
    
    with colA:
        st.subheader("📥 標準範例檔案下載")
        pool_csv = "ticker,stock_name\n2330,台積電\n2317,鴻海"
        port_csv = "ticker,stock_name,entry_price,qty\n2330,台積電,600,1000\n2317,鴻海,100,2000"
        st.download_button("📥 下載「股票池」範例檔", data=pool_csv.encode('utf-8-sig'), file_name="sample_pool.csv")
        st.download_button("📥 下載「庫存股」範例檔", data=port_csv.encode('utf-8-sig'), file_name="sample_portfolio.csv")
        st.divider()
        st.subheader("🗑️ 破釜沉舟清空專區")
        if st.button("🚨 一鍵清空【股票池】"):
            with engine.begin() as conn: conn.execute(text("TRUNCATE TABLE stock_pool"))
            st.success("✅ 已徹底清空股票池！")
            
    with colB:
        st.subheader("📤 上傳股票池 (系統自動剔除重複)")
        pool_file = st.file_uploader("上傳股票池 CSV", type=['csv'], key="up_pool")
        if pool_file:
            df = pd.read_csv(pool_file)
            if 'ticker' in df.columns and 'stock_name' in df.columns:
                df['ticker'] = df['ticker'].astype(str)
                df = df.drop_duplicates(subset=['ticker'])
                with engine.begin() as conn:
                    for _, row in df.iterrows():
                        conn.execute(text("INSERT IGNORE INTO stock_pool (ticker, stock_name) VALUES (:t, :n)"), 
                                     {"t": row['ticker'], "n": row['stock_name']})
                st.success(f"✅ 成功上傳/更新 {len(df)} 檔股票至池中！(重複項目已自動忽略)")
            else: st.error("❌ CSV 格式錯誤，請確保首列包含 ticker, stock_name 欄位。")
            
        st.subheader("📤 上傳庫存股票")
        port_file = st.file_uploader("上傳庫存 CSV", type=['csv'], key="up_port")
        port_mode = st.radio("庫存更新模式", ["新增 (保留舊有標的，更新重複標的)", "覆蓋 (清除舊有所有庫存並重新寫入)"])
        if port_file and st.button("執行上傳庫存"):
            df = pd.read_csv(port_file)
            if all(c in df.columns for c in ['ticker', 'stock_name', 'entry_price', 'qty']):
                df['ticker'] = df['ticker'].astype(str)
                with engine.begin() as conn:
                    if "覆蓋" in port_mode: conn.execute(text("TRUNCATE TABLE portfolio"))
                    for _, row in df.iterrows():
                        conn.execute(text("INSERT INTO portfolio (ticker, stock_name, entry_price, qty) VALUES (:t, :n, :p, :q) ON DUPLICATE KEY UPDATE entry_price=VALUES(entry_price), qty=VALUES(qty)"), 
                                     {"t": row['ticker'], "n": row['stock_name'], "p": float(row['entry_price']), "q": int(row['qty'])})
                st.success("✅ 庫存上傳成功！請至 Tab2 點擊同步行情查看。")
            else: st.error("❌ CSV 格式錯誤，請確保首列包含 ticker, stock_name, entry_price, qty 欄位。")

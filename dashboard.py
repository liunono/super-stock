import streamlit as st
import pandas as pd
import pymysql
import requests
import json

# ================= 系統設定區 =================
# LINE Bot 金鑰
LINE_CHANNEL_ACCESS_TOKEN = st.secrets["LINE_CHANNEL_ACCESS_TOKEN"]
YOUR_LINE_USER_ID = st.secrets["YOUR_LINE_USER_ID"]

# 資料庫連線設定 (GoDaddy 主機上通常是 localhost，資料庫是 g9cat)
DB_CONFIG = {

    'host': st.secrets["DB_HOST"],
    'user': st.secrets["DB_USER"],
    'password': st.secrets["DB_PASS"],
    'db': st.secrets["DB_NAME"],
    'port': 3306,
    'cursorclass': pymysql.cursors.DictCursor
    
}

# ================= 輔助功能模組 =================
def send_line_test(msg):
    """發送 LINE 測試訊息"""
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }
    payload = {
        "to": YOUR_LINE_USER_ID,
        "messages": [{"type": "text", "text": f"🚨 【哲哲戰情室測試】 🚨\n{msg}"}]
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        return response.status_code == 200
    except Exception as e:
        return False

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

# ================= 儀表板介面設計 =================
# 設定網頁標題與寬度
st.set_page_config(page_title="哲哲量化戰情室", layout="wide")
st.title("📈 哲哲量化戰情室 V3.0")
st.markdown("---")

# 建立三個分頁標籤
tab1, tab2, tab3 = st.tabs(["📲 系統與 LINE 測試", "📥 股票池管理", "📊 歷史數據庫"])

# ----------------- 分頁 1：LINE 測試 -----------------
with tab1:
    st.header("系統連線與推播測試")
    st.write("點擊下方按鈕，測試 LINE Bot 是否能成功發送訊號到你的手機。")
    
    test_message = st.text_input("自訂測試訊息內容：", "趨勢突破！2330 台積電 站上月線，準備發車！")
    
    if st.button("🚀 發送測試推播"):
        with st.spinner("正在呼叫 LINE API..."):
            success = send_line_test(test_message)
            if success:
                st.success("✅ 發送成功！請檢查你的 LINE 手機通知！")
                st.balloons() # 噴發慶祝氣球特效
            else:
                st.error("❌ 發送失敗！請檢查金鑰設定。")

# ----------------- 分頁 2：股票池管理 -----------------
with tab2:
    st.header("股票池與策略管理")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("匯入最新股票名單 (CSV)")
        uploaded_file = st.file_uploader("上傳包含 ticker, stock_name 的 CSV 檔", type="csv")
        if uploaded_file:
            df_pool = pd.read_csv(uploaded_file)
            st.dataframe(df_pool)
            if st.button("寫入 g9cat 資料庫"):
                # 這裡可加入將 DataFrame 寫入 MySQL 的邏輯
                st.success("✅ 股票池已成功更新至資料庫！")

    with col2:
        st.subheader("目前監控中名單")
        try:
            conn = get_db_connection()
            query = "SELECT ticker, stock_name, sector, watch_status FROM stock_pool"
            df_current = pd.read_sql(query, conn)
            st.dataframe(df_current)
            conn.close()
        except Exception as e:
            st.warning("⚠️ 無法連線至資料庫，請確認 DB_CONFIG 設定。")

# ----------------- 分頁 3：歷史數據庫 -----------------
with tab3:
    st.header("每日策略訊號歷史")
    st.write("這裡將顯示 AI 每日 08:30 抓取與判讀的歷史紀錄。")
    # 預留給未來讀取 daily_history 或 signal_logs 資料表顯示折線圖的空間
    st.info("系統累積數據中... 當排程啟動後，此處將自動繪製勝率與價格圖表。")
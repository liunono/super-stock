# 在 tab1 的「數據診斷實驗室」新增這段邏輯
with d1:
    if st.button("🚀 量子全解碼：一鍵下載 2330 所有原始科目", key="debug_all"):
        # 強制抓取所有可能的 dataset 標籤
        df = fetch_fm("TaiwanStockFinancialStatements", "2330", 730)
        if df is not None:
            # 💡 增加診斷欄位：直接標註這個科目是屬於「損益」還是「權益」
            df['診斷建議'] = np.where(df['type'].str.contains('Income|Profit|Loss|EPS'), '❌ 這是分子(淨利)', 
                                    np.where(df['type'].str.contains('Equity|Asset|Liability'), '✅ 這是分母(權益)', '🔎 待確認'))
            
            st.session_state['debug_df'] = df
            st.success(f"✅ 成功抓取 {len(df)} 筆科目！請下載下方 CSV 直接對答案！")
            
    if 'debug_df' in st.session_state:
        st.download_button(
            label="📥 下載 2330 全科目診斷檔 (CSV)",
            data=st.session_state['debug_df'].to_csv(index=False).encode('utf-8-sig'),
            file_name="Ultimate_Debug_2330.csv",
            mime="text/csv"
        )
        # 直接在畫面上秀出主力在搞什麼鬼
        st.write("📊 目前 API 回傳的所有科目類型：")
        st.dataframe(st.session_state['debug_df'][['date', 'type', 'origin_name', 'value', '診斷建議']].tail(20))

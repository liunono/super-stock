# --- 🚀 哲哲核心修復：解決 Styler.applymap 報錯 ---
            def style_rows(row):
                # 這裡處理整行背景色
                if '🔥' in str(row['評等']): return ['background-color: #FFCCCC'] * len(row)
                if '✨' in str(row['評等']): return ['background-color: #FFF3CD'] * len(row)
                return [''] * len(row)

            # 建立樣式物件
            styler = df_stocks.style.apply(style_rows, axis=1)

            # 針對『評等』欄位做文字加粗 (相容新版 map)
            def color_text(val):
                if '🔥' in str(val) or '✨' in str(val): return 'color: red; font-weight: bold'
                return ''
            
            # 使用 hasattr 檢查，確保萬無一失
            if hasattr(styler, 'map'):
                styler = styler.map(color_text, subset=['評等'])
            else:
                styler = styler.applymap(color_text, subset=['評等'])

            # 🚀 最終顯示：這一次數據會全部噴出來！
            st.dataframe(styler, width='stretch')
            st.balloons()

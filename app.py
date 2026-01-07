import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定
SUPER_PASSWORD = "ccycs"
AGENT_TRANSLATION = {
    # 這裡可以放入您原本的代理 ID 翻譯
}

# --- 1. 資料抓取與計算邏輯 ---
def run_crawler_logic(st_dt, ed_dt, admin_acc):
    CONFIG = {
        "banknote": {
            "url": "https://wpapi.ldjzmr.top/master/banknote_log",
            "token": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL3dwYXBpLmxkanptci50b3AvbWFzdGVyL2xvZ2luIiwiaWF0IjoxNzY3NTgwMTU3LCJleHAiOjE3OTkxMTYxNTcsIm5iZiI6MTc2NzU4MDE1NywianRpIjoiRWo3SUlEYklvTWE2aHgzYyIsInN1YiI6IjEyIiwicHJ2IjoiMTg4ODk5NDM5MDUwZTVmMzc0MDliMThjYzZhNDk1NjkyMmE3YWIxYiJ9.hdrOsQYgdGMNl5R6n17Z6ls_eI8uZ0_TRDGZnFWXe0A"
        },
        "brand": {
            "url": "https://wpapi.ldjzmr.top/master/brand",
            "token": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL3dwYXBpLmxkanptci50b3AvbWFzdGVyL2xvZ2luIiwiaWF0IjoxNzY3NjcxMjM2LCJleHAiOjE3OTkyMDcyMzYsIm5iZiI6MTc2NzY3MTIzNiwianRpIjoiTjZoeUo4Z2VPM2pHdk95ZiIsInN1YiI6IjEyIiwicHJ2IjoiMTg4ODk5NDM5MDUwZTVmMzc0MDliMThjYzZhNDk1NjkyMmE3YWIxYiJ9._oUGuey_kRBVKCeo8xZZWiAtulRZ666G498rHb0KqjQ"
        }
    }

    # 計算前日結束時間 (用於計算今日變化)
    dt_end = datetime.strptime(ed_dt, "%Y-%m-%d %H:%M:%S")
    dt_offset_end = (dt_end - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    is_new_month_start = True if dt_end.day == 1 and dt_end.hour >= 8 else False

    # 抓取店家
    brand_headers = {"Authorization": CONFIG["brand"]["token"]}
    brand_res = requests.get(CONFIG["brand"]["url"], headers=brand_headers, params={"page_size": 1000})
    brand_raw_list = brand_res.json().get('data', {}).get('data', [])
    
    brand_mapping = []
    brand_agent_map = {}
    for b in brand_raw_list:
        b_name = b.get('name')
        if not b_name: continue
        member_info = b.get('member')
        a_id = b.get('agent_id', 0)
        a_name = (member_info.get('nickname') if member_info else None) or AGENT_TRANSLATION.get(a_id, f"ID:{a_id}")
        brand_agent_map[b_name] = a_name
        brand_mapping.append({
            'name': b_name,
            '管理員帳號': str(member_info.get('phone', '無')) if member_info else '無',
            '台數': int(b.get('terminal_count', 0)),
            '代理名稱': a_name
        })

    # 抓取流水 (全量抓取)
    banknote_headers = {"Authorization": CONFIG["banknote"]["token"]}
    init_res = requests.get(CONFIG["banknote"]["url"], headers=banknote_headers, params={"pagesize": 100})
    total_pages = init_res.json()['data']['list']['last_page']
    
    all_raw_banknote = []
    def fetch_worker(page):
        try:
            r = requests.get(CONFIG["banknote"]["url"], headers=banknote_headers, params={"pagenum": page, "pagesize": 500}, timeout=15)
            return r.json().get('data', {}).get('list', {}).get('data', [])
        except: return []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_worker, p) for p in range(1, total_pages + 1)]
        for f in as_completed(futures):
            all_raw_banknote.extend(f.result())

    full_df = pd.DataFrame(all_raw_banknote).drop_duplicates(subset=['id'])
    full_df['amount'] = pd.to_numeric(full_df['amount'], errors='coerce').fillna(0)
    full_df['店家'] = full_df['brand'].apply(lambda x: x.get('name', "未知"))

    # 計算邏輯
    df_range_a = full_df[(full_df['created_at'].astype(str) >= st_dt) & (full_df['created_at'].astype(str) <= ed_dt)]
    
    report_rows = []
    for brand, group in df_range_a.groupby('店家'):
        v_in = group[group['currency_type'] == 1]['amount'].sum()
        v_open = group[group['currency_type'] == 2]['amount'].sum()
        v_wash = group[group['currency_type'] == 3]['amount'].sum()
        accumulated = int(v_open - v_wash + v_in)
        
        prev_accum = 0
        if not is_new_month_start:
            df_range_b = full_df[(full_df['created_at'].astype(str) >= st_dt) & (full_df['created_at'].astype(str) <= dt_offset_end)]
            g_b = df_range_b[df_range_b['店家'] == brand]
            prev_accum = int(g_b[g_b['currency_type'] == 2]['amount'].sum() - g_b[g_b['currency_type'] == 3]['amount'].sum() + g_b[g_b['currency_type'] == 1]['amount'].sum())
            
        report_rows.append({
            '店家': brand, '開分': int(v_open), '投鈔': int(v_in), '洗分': int(v_wash),
            '月初至今日累計營業額': accumulated, '前日累計額': prev_accum,
            '今日變化': accumulated - prev_accum, '代理名稱': brand_agent_map.get(brand, "未知")
        })
    
    df_report = pd.DataFrame(report_rows)
    df_brand_map = pd.DataFrame(brand_mapping)
    df_report = pd.merge(df_report, df_brand_map[['name', '管理員帳號', '台數']], left_on='店家', right_on='name', how='left').drop(columns=['name'])

    if admin_acc.strip() != SUPER_PASSWORD:
        df_report = df_report[df_report['管理員帳號'] == admin_acc.strip()]

    # 總計
    if not df_report.empty:
        summary = {
            '店家': '總計', '開分': df_report['開分'].sum(), '投鈔': df_report['投鈔'].sum(),
            '洗分': df_report['洗分'].sum(), '月初至今日累計營業額': df_report['月初至今日累計營業額'].sum(),
            '前日累計額': df_report['前日累計額'].sum(), '今日變化': df_report['今日變化'].sum(),
            '代理名稱': '', '管理員帳號': '', '台數': 0
        }
        return pd.concat([df_report, pd.DataFrame([summary])], ignore_index=True)
    return df_report

# --- 2. Streamlit 網頁呈現 ---
st.set_page_config(page_title="王牌財務分析系統", layout="wide")

st.title("📱 王牌雲端財務分析工具")

# 側邊欄：輸入區
with st.sidebar:
    st.header("🔍 查詢設定")
    acc = st.text_input("管理員帳號", value="")
    
    today = datetime.now()
    st.subheader("📅 開始時間 (01號 08:00)")
    st_date = st.date_input("開始日期", today.replace(day=1))
    st_hour = st.selectbox("開始小時", range(24), index=8)
    
    st.subheader("📅 結束時間")
    ed_date = st.date_input("結束日期", today)
    ed_hour = st.selectbox("結束小時", range(24), index=7)
    
    st_time = f"{st_date} {st_hour:02d}:00:00"
    ed_time = f"{ed_date} {ed_hour:02d}:59:59"
    
    run_btn = st.button("🚀 生成對帳報表", use_container_width=True)

if run_btn:
    if not acc:
        st.error("❌ 請輸入管理員帳號")
    else:
        with st.spinner("📡 雲端數據計算中，請稍候..."):
            try:
                df_final = run_crawler_logic(st_time, ed_time, acc)
                st.session_state.df = df_final
                st.session_state.time_range = f"{st_time} 至 {ed_time}"
                st.success("✅ 數據更新成功")
            except Exception as e:
                st.error(f"❌ 錯誤: {e}")

# 結果顯示區
if 'df' in st.session_state:
    if 'df' in st.session_state and 'time_range' in st.session_state:
    st.info(f"📋 目前對帳區間：{st.session_state.time_range}")
    df = st.session_state.df
    
    # 期待值計算指標
    total_row = df[df['店家'] == '總計']
    if not total_row.empty:
        profit = total_row['月初至今日累計營業額'].values[0]
        v_in = total_row['開分'].values[0] + total_row['投鈔'].values[0]
        expect = (profit / v_in * 100) if v_in != 0 else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("🎯 總體期待值", f"{expect:.2f}%")
        col2.metric("💰 總累計營業額", f"{profit:,.0f}")
        col3.metric("📈 今日總變化", f"{total_row['今日變化'].values[0]:,.0f}")

    # 分頁標籤 (對應原本的 Notebook)
    tab1, tab2, tab3 = st.tabs(["📝 營業明細", "📊 圖表分析", "🏠 店家管理"])

    with tab1:
        # 排除不顯示的欄位
        disp = df.drop(columns=['管理員帳號', '台數'], errors='ignore')
        # 使用 style 來高亮「總計」行
        def highlight_total(s):
            return ['background-color: #FFFFE0; font-weight: bold' if s.店家 == '總計' else '' for _ in s]
        
        st.dataframe(
            disp.style.apply(highlight_total, axis=1).format(subset=['開分','投鈔','洗分','月初至今日累計營業額','前日累計額','今日變化'], formatter="{:,.0f}"),
            use_container_width=True, 
            height=600
        )
        st.caption("💡 提示：點擊表頭即可排序，支援手機長按數值複製。")

    with tab2:
        st.write("📊 這裡未來可以放置各代理佔比的圓餅圖")
        if not df.empty:
            # 簡單的視覺化範例
            chart_data = df[df['店家'] != '總計'].set_index('店家')['今日變化']
            st.bar_chart(chart_data)

    with tab3:
        st.write("⚙️ 這裡未來可以放置參數設定與 Token 管理")

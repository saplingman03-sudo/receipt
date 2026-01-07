import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定
SUPER_PASSWORD = "ccycs"

# --- 1. 資料抓取與計算邏輯 (保留你原本的精華) ---
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

    dt_end = datetime.strptime(ed_dt, "%Y-%m-%d %H:%M:%S")
    is_new_month_start = True if (dt_end.day == 1 and dt_end.hour >= 8) else False
    dt_offset_end = (dt_end - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

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
        a_name = member_info.get('nickname') if member_info else f"ID:{b.get('agent_id')}"
        brand_agent_map[b_name] = a_name
        brand_mapping.append({
            'name': b_name,
            '管理員帳號': str(member_info.get('phone', '無')) if member_info else '無',
            '台數': int(b.get('terminal_count', 0)),
            '代理名稱': a_name
        })

    # 抓取流水
    banknote_headers = {"Authorization": CONFIG["banknote"]["token"]}
    init_res = requests.get(CONFIG["banknote"]["url"], headers=banknote_headers, params={"pagesize": 100})
    total_pages = init_res.json()['data']['list']['last_page']
    
    all_raw_banknote = []
    def fetch_worker(page):
        r = requests.get(CONFIG["banknote"]["url"], headers=banknote_headers, params={"pagenum": page, "pagesize": 500})
        return r.json().get('data', {}).get('list', {}).get('data', [])

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_worker, p) for p in range(1, min(total_pages + 1, 50))] # 限制頁數加快速度
        for f in as_completed(futures):
            all_raw_banknote.extend(f.result())

    full_df = pd.DataFrame(all_raw_banknote).drop_duplicates(subset=['id'])
    full_df['amount'] = pd.to_numeric(full_df['amount'], errors='coerce').fillna(0)
    full_df['店家'] = full_df['brand'].apply(lambda x: x.get('name', "未知"))

    df_range_a = full_df[(full_df['created_at'].astype(str) >= st_dt) & (full_df['created_at'].astype(str) <= ed_dt)]

    report_rows = []
    for brand, group in df_range_a.groupby('店家'):
        v_in = group[group['currency_type'] == 1]['amount'].sum()
        v_open = group[group['currency_type'] == 2]['amount'].sum()
        v_wash = group[group['currency_type'] == 3]['amount'].sum()
        accumulated = int(v_open - v_wash + v_in)
        report_rows.append({
            '店家': brand, '開分': int(v_open), '投鈔': int(v_in), '洗分': int(v_wash),
            '月初至今日累計營業額': accumulated, '代理名稱': brand_agent_map.get(brand, "未知")
        })
    
    df_report = pd.DataFrame(report_rows)
    df_brand_map = pd.DataFrame(brand_mapping)
    df_report = pd.merge(df_report, df_brand_map[['name', '管理員帳號', '台數']], left_on='店家', right_on='name', how='left').drop(columns=['name'])

    # 權限過濾
    if admin_acc.strip() != SUPER_PASSWORD:
        df_report = df_report[df_report['管理員帳號'] == admin_acc.strip()]

    # 總計
    summary = {
        '店家': '總計', '開分': df_report['開分'].sum(), '投鈔': df_report['投鈔'].sum(),
        '洗分': df_report['洗分'].sum(), '月初至今日累計營業額': df_report['月初至今日累計營業額'].sum(),
        '代理名稱': '', '管理員帳號': '', '台數': 0
    }
    return pd.concat([df_report, pd.DataFrame([summary])], ignore_index=True)

# --- 2. Streamlit 網頁介面 ---
st.set_page_config(page_title="王牌財務分析系統", layout="wide")

st.title("📱 王牌財務分析工具 V3.0 (iPhone 專用)")

# 側邊欄控制
with st.sidebar:
    st.header("🔍 查詢設定")
    acc = st.text_input("管理員帳號", value="jjk888")
    
    # 手機端建議使用簡易日期選擇
    today = datetime.now()
    st_date = st.date_input("開始日期 (固定 01 號)", today.replace(day=1))
    ed_date = st.date_input("結束日期", today)
    
    st_time = f"{st_date} 08:00:00"
    ed_time = f"{ed_date} 07:59:59"
    
    run_btn = st.button("🚀 生成對帳報表", use_container_width=True)

# 執行與呈現
if run_btn:
    with st.spinner("📡 正在從 API 抓取數據..."):
        try:
            df_final = run_crawler_logic(st_time, ed_time, acc)
            st.session_state.df = df_final
            st.success("✅ 數據更新成功")
        except Exception as e:
            st.error(f"❌ 錯誤: {e}")

if 'df' in st.session_state:
    df = st.session_state.df
    
    # 期待值計算與顯示
    total_row = df[df['店家'] == '總計']
    if not total_row.empty:
        v_profit = total_row['月初至今日累計營業額'].values[0]
        v_in = total_row['開分'].values[0] + total_row['投鈔'].values[0]
        expect_val = (v_profit / v_in * 100) if v_in != 0 else 0
        st.metric("🎯 當前總體期待值", f"{expect_val:.2f}%", delta=f"{v_profit:,.0f} (累計)")

    # 分頁顯示
    tab1, tab2, tab3, tab4 = st.tabs(["📝 營業明細", "📊 數據分析", "📈 趨勢圖", "⚙️ 設定"])

    with tab1:
        # 排除顯示欄位
        display_df = df.drop(columns=['管理員帳號', '台數'], errors='ignore')
        # 老闆手機可以排序、縮放、長按複製
        st.dataframe(display_df.style.format(thousands=","), use_container_width=True, height=500)

    with tab2:
        st.info("💡 未來這裡會放圓餅圖看各店佔比")

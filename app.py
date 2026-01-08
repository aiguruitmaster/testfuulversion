import streamlit as st
from supabase import create_client
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
import time
import requests
from urllib.parse import urlparse, urlunparse
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# -----------------------
# Конфигурация страницы
# -----------------------
st.set_page_config(page_title="Link Checker", layout="wide")

# ==========================================
# 🌍 СИСТЕМА ПЕРЕВОДОВ (LOCALIZATION)
# ==========================================

if "lang" not in st.session_state:
    st.session_state.lang = "en"  # Default language

TRANSLATIONS = {
    "en": {
        "nav_title": "Navigation",
        "home_btn": "🏠 HOME (All Projects)",
        "projects_list": "Your Projects:",
        "view_proj": "📊 View Project",
        "general_folder": "📄 General (No Folder)",
        "create_proj_exp": "➕ Create Project",
        "proj_name_placeholder": "Project Name",
        "create_btn": "Create",
        "del_proj_exp": "🗑 Delete Current Project",
        "del_proj_confirm": "Yes, Delete Project",
        "dash_title": "📊 All Projects",
        "no_projs": "No projects found. Create one in the sidebar.",
        "total_projs": "Total Projects",
        "total_queue": "TOTAL IN QUEUE",
        "ready_global": "Ready to check: **{}** links across all projects.",
        "run_global": "🚀 RUN CHECK FOR ALL PROJECTS",
        "queue_empty": "Queue is empty.",
        "reset_global": "🔄 Reset statuses in ALL PROJECTS and re-check",
        "folder_struct": "Folder Structure:",
        "flat_mode": "No folders in this project. Working in flat mode.",
        "create_first_folder": "➕ Create first folder",
        "root_folder": "Root Folder",
        "back_to_proj": "⬅ Back to Project",
        "back_to_folders": "⬅ Back to Folders",
        "empty_folder": "This folder is empty.",
        "total": "Total",
        "indexed": "Indexed",
        "queue": "Queue",
        "run_queue": "🚀 Check Queue",
        "rerun_all": "🔄 Re-check All",
        "del_selected": "🗑 Delete {} links",
        "add_links_title": "📥 Add links to '{}'",
        "paste_links": "Paste links list:",
        "save_btn": "💾 Save",
        "success_added": "✅ Added {} links!",
        "open_btn": "Open ➡",
        "del_btn": "🗑",
        "del_folder_btn": "🗑 Delete",
        "folder_name": "Folder Name",
        "create_folder_btn": "Create Folder",
        "add_new_folder": "➕ Add New Folder",
        "processing": "📤 Processing {}-{} of {}...",
        "analyzing": "⏳ Analyzing...",
        "sending_report": "📊 Sending report...",
        "done": "✅ Done!",
        "slack_success": "✅ Report sent to Slack!",
        "slack_error": "❌ Slack Error: {}",
        "report_msg": "✅ *Check Completed ({})!*\n🔗 Total: {}",
        "col_url": "URL",
        "col_index": "Indexed?",
        "col_status": "Status",
        "col_date": "Last Check",
        "warn_del_proj": "Warning! This will delete the project and ALL links inside.",
        "confirm_del": "Yes, delete",
        "project": "Project",
        "links_count": "Links count",
        "in_index": "In Index",
        "in_queue": "In Queue",
        "db_error_retry": "⚠️ DB Connection failed. Retrying..."
    },
    "uk": {
        "nav_title": "Навігація",
        "home_btn": "🏠 ГОЛОВНА (Всі проекти)",
        "projects_list": "Ваші проекти:",
        "view_proj": "📊 Огляд проекту",
        "general_folder": "📄 Загальна (Без папки)",
        "create_proj_exp": "➕ Створити Проект",
        "proj_name_placeholder": "Назва проекту",
        "create_btn": "Створити",
        "del_proj_exp": "🗑 Видалити поточний проект",
        "del_proj_confirm": "Так, видалити проект",
        "dash_title": "📊 Всі проекти",
        "no_projs": "Немає проектів. Створіть перший у меню зліва.",
        "total_projs": "Всього проектів",
        "total_queue": "ВСЬОГО В ЧЕРЗІ",
        "ready_global": "Готово до перевірки: **{}** посилань у всіх проектах.",
        "run_global": "🚀 ЗАПУСТИТИ ПЕРЕВІРКУ ВСІХ ПРОЕКТІВ",
        "queue_empty": "Черга пуста.",
        "reset_global": "🔄 Скинути статуси у ВСІХ ПРОЕКТАХ та перевірити",
        "folder_struct": "Структура папок:",
        "flat_mode": "У цьому проекті немає папок. Працюємо у простому режимі.",
        "create_first_folder": "➕ Створити першу папку",
        "root_folder": "Коренева папка",
        "back_to_proj": "⬅ До проекту",
        "back_to_folders": "⬅ До папок",
        "empty_folder": "У цій папці поки порожньо.",
        "total": "Всього",
        "indexed": "В індексі",
        "queue": "Черга",
        "run_queue": "🚀 Перевірити чергу",
        "rerun_all": "🔄 Переперевірити все",
        "del_selected": "🗑 Видалити {} посилань",
        "add_links_title": "📥 Додати посилання в '{}'",
        "paste_links": "Вставте список посилань:",
        "save_btn": "💾 Зберегти",
        "success_added": "✅ Додано {} посилань!",
        "open_btn": "Відкрити ➡",
        "del_btn": "🗑",
        "del_folder_btn": "🗑 Видалити",
        "folder_name": "Назва папки",
        "create_folder_btn": "Створити папку",
        "add_new_folder": "➕ Додати нову папку",
        "processing": "📤 Обробка {}-{} з {}...",
        "analyzing": "⏳ Аналіз...",
        "sending_report": "📊 Відправка звіту...",
        "done": "✅ Готово!",
        "slack_success": "✅ Звіт відправлено в Slack!",
        "slack_error": "❌ Помилка Slack: {}",
        "report_msg": "✅ *Перевірка завершена ({})!*\n🔗 Всього: {}",
        "col_url": "URL",
        "col_index": "Індекс?",
        "col_status": "Статус",
        "col_date": "Дата перевірки",
        "warn_del_proj": "Увага! Це видалить проект і ВСІ посилання в ньому.",
        "confirm_del": "Так, видалити",
        "project": "Проект",
        "links_count": "Кількість",
        "in_index": "В індексі",
        "in_queue": "В черзі",
        "db_error_retry": "⚠️ З'єднання з БД втрачено. Повторна спроба..."
    }
}

def t(key):
    """Helper to get translation"""
    lang = st.session_state.lang
    return TRANSLATIONS[lang].get(key, key)

# ==========================================
# ИНИЦИАЛИЗАЦИЯ
# ==========================================

TASK_POST = "/v3/serp/google/organic/task_post"
TASK_GET_ADV = "/v3/serp/google/organic/task_get/advanced/{task_id}"

# Инициализация состояния
if "selected_project_id" not in st.session_state:
    st.session_state.selected_project_id = None
if "selected_folder_id" not in st.session_state:
    st.session_state.selected_folder_id = None 

@st.cache_resource
def init_supabase():
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

def init_requests():
    s = requests.Session()
    s.auth = (st.secrets["dataforseo"]["login"], st.secrets["dataforseo"]["password"])
    s.headers.update({"Content-Type": "application/json"})
    return s

try:
    supabase = init_supabase()
except Exception as e:
    st.error(f"DB Connection Error: {e}")
    st.stop()

# -----------------------
# ХЕЛПЕРЫ
# -----------------------
def send_slack_file(file_bytes, filename, message):
    try:
        if "slack" in st.secrets:
            token = st.secrets["slack"].get("bot_token")
            channel = st.secrets["slack"].get("channel_id")
            if token and channel:
                client = WebClient(token=token)
                client.files_upload_v2(
                    channel=channel, file=file_bytes, filename=filename, title=filename, initial_comment=message
                )
                st.success(t("slack_success"))
    except Exception as e:
        st.error(t("slack_error").format(e))

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    return output.getvalue()

def norm_url(u: str) -> str:
    p = urlparse(u.strip())
    netloc = (p.netloc or "").lower()
    if netloc.startswith("www."): netloc = netloc[4:]
    path = (p.path or "").rstrip("/")
    return urlunparse(("", netloc, path, "", "", "")).lower()

def build_site_query(url: str) -> str:
    p = urlparse(url.strip())
    host = (p.netloc or "").lower()
    if host.startswith("www."): host = host[4:]
    path = (p.path or "").strip().lstrip("/").rstrip("/")
    return f"site:{host}" if path in ("", "/") else f"site:{host}/{path}"

def match_indexed(original_url: str, items):
    orig = norm_url(original_url)
    for it in items:
        if it.get("type") == "organic":
            u = it.get("url")
            if u and norm_url(u) == orig: return True
    return False

def parse_text_urls(text_input):
    urls = []
    if not text_input: return urls
    lines = text_input.split('\n')
    for line in lines:
        line = line.strip()
        if line and (line.startswith("http://") or line.startswith("https://")):
            urls.append(line)
    return urls

# Функция с защитой от сбоев сети (Retry)
def safe_fetch(table, select="*", order_col=None):
    try:
        query = supabase.table(table).select(select)
        if order_col:
            query = query.order(order_col, desc=(order_col == "created_at"))
        return query.execute().data
    except Exception as e:
        # Если ошибка, ждем и пробуем еще раз
        time.sleep(1)
        try:
            query = supabase.table(table).select(select)
            if order_col:
                query = query.order(order_col, desc=(order_col == "created_at"))
            return query.execute().data
        except Exception as e2:
            st.error(f"Failed to fetch data: {e2}")
            return []

# -----------------------
# ЛОГИКА ПРОВЕРКИ
# -----------------------
def run_check(links_data, report_name_prefix="Report"):
    """
    Main function for checking links via DataForSEO.
    Handles:
    - 20000: Success (Check items for index)
    - 40102: No Search Results (Not Indexed)
    - 40601/40602: Polling (Wait and retry)
    """
    if not links_data: return
    session = init_requests()
    host = st.secrets["dataforseo"].get("host", "api.dataforseo.com").replace("https://", "")
    base_url = f"https://{host}"
    
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    payload = []
    tasks_map = {} 
    
    # 1. Prepare payload
    for item in links_data:
        payload.append({
            "location_code": 2840, 
            "language_code": "en", 
            "depth": 10,
            "keyword": build_site_query(item['url'])
        })

    BATCH_SIZE = 50
    total = len(links_data)
    processed = 0
    
    # 2. Batch processing
    for i in range(0, total, BATCH_SIZE):
        batch_links = links_data[i : i + BATCH_SIZE]
        batch_payload = payload[i : i + BATCH_SIZE]
        
        msg_proc = t("processing").format(i+1, min(i+BATCH_SIZE, total), total)
        status_text.write(msg_proc)
        
        try:
            # --- STEP 1: POST TASKS ---
            r = session.post(base_url + TASK_POST, json=batch_payload, timeout=60)
            res = r.json()
            
            if res.get('status_code') == 20000:
                batch_ids = []
                for idx, task in enumerate(res.get('tasks', [])):
                    if task.get('id'):
                        tid = task['id']
                        tasks_map[tid] = batch_links[idx]['id']
                        batch_ids.append(tid)
                
                if not batch_ids: 
                    processed += len(batch_links)
                    continue

                # --- STEP 2: POLLING LOOP ---
                for tid in batch_ids:
                    link_id = tasks_map[tid]
                    max_retries = 10 
                    retry_delay = 3   
                    
                    for attempt in range(max_retries):
                        try:
                            # Get Task Result
                            r_get = session.get(base_url + TASK_GET_ADV.format(task_id=tid), timeout=30)
                            d_get = r_get.json()
                            
                            task_res = (d_get.get('tasks') or [{}])[0]
                            status_code = task_res.get('status_code')

                            # CASE A: Success (20000) -> Check if URL is in results
                            if status_code == 20000:
                                items = (task_res.get('result') or [{}])[0].get('items', [])
                                url_obj = next(l for l in batch_links if l['id'] == link_id)
                                is_ind = match_indexed(url_obj['url'], items)
                                
                                supabase.table("links").update({
                                    "status": "done", 
                                    "is_indexed": is_ind, 
                                    "last_check": datetime.utcnow().isoformat(), 
                                    "task_id": tid
                                }).eq("id", link_id).execute()
                                break 

                            # CASE B: No Search Results (40102) -> Definitely Not Indexed
                            elif status_code == 40102:
                                supabase.table("links").update({
                                    "status": "done", 
                                    "is_indexed": False,  # Explicitly False
                                    "last_check": datetime.utcnow().isoformat(), 
                                    "task_id": tid
                                }).eq("id", link_id).execute()
                                break 

                            # CASE C: Wait (40602 Queue / 40601 Handed)
                            elif status_code == 40602 or status_code == 40601:
                                status_text.write(f"⏳ Task {tid} processing... Status: {status_code} (Attempt {attempt+1}/{max_retries})")
                                time.sleep(retry_delay)
                                continue 

                            # CASE D: Actual Error
                            else:
                                error_msg = task_res.get('status_message', 'Unknown API Error')
                                print(f"API Error for {tid}: {error_msg}")
                                supabase.table("links").update({"status": "error"}).eq("id", link_id).execute()
                                break 

                        except Exception as e:
                            print(f"Network error polling task {tid}: {e}")
                            time.sleep(1)
                    else:
                        # Timeout
                        supabase.table("links").update({"status": "timeout"}).eq("id", link_id).execute()

            else:
                st.error(f"API Error: {res.get('status_message')}")
            
            processed += len(batch_links)
            progress_bar.progress(processed / total)
            
        except Exception as e:
            st.error(f"Global Net Error: {e}")
            time.sleep(1.5)

    # 3. Report Generation
    status_text.write(t("sending_report"))
    try:
        checked_ids = [item['id'] for item in links_data]
        res = supabase.table("links").select("url, status, is_indexed, last_check").in_("id", checked_ids).execute()
        df_report = pd.DataFrame(res.data)
        
        if not df_report.empty:
            excel_bytes = to_excel(df_report)
            date_str = datetime.now().strftime('%Y-%m-%d')
            fname = f"{report_name_prefix}_{date_str}.xlsx"
            
            msg = t("report_msg").format(report_name_prefix, total)
            send_slack_file(excel_bytes, fname, msg)
    except Exception as e:
        st.error(f"Report Generation Error: {e}")

    status_text.success(t("done"))
    time.sleep(1)
    st.rerun()
# -----------------------
# ФУНКЦИЯ ОТРИСОВКИ ИНТЕРФЕЙСА ПАПКИ/ПРОЕКТА
# -----------------------
def render_link_interface(project_id, folder_id=None, folder_name=""):
    """
    Рисует таблицу ссылок и интерфейс добавления.
    - Поддерживает .xlsx, .xls, .csv
    - Сохраняет СТРОГИЙ порядок строк (как в файле)
    - Умно ищет колонку с ссылкой (приоритет на Referring Page)
    """
    
    # ---------------------------------------------------------
    # 1. ЗАГРУЗКА И ОТОБРАЖЕНИЕ (СОРТИРОВКА ПО ВОЗРАСТАНИЮ ID)
    # ---------------------------------------------------------
    query = supabase.table("links").select("*").eq("project_id", project_id)
    
    if folder_id is None:
        query = query.is_("folder_id", "null")
    else:
        query = query.eq("folder_id", folder_id)
    
    # !!! ГЛАВНОЕ ИСПРАВЛЕНИЕ ПОРЯДКА !!!
    # desc=False означает "от старых к новым". 
    # Так первая строка из Excel останется первой в таблице.
    links = query.order("id", desc=False).execute().data
    
    df = pd.DataFrame(links)

    if df.empty:
        st.info(t("empty_folder"))
    else:
        # Метрики
        total = len(df)
        indexed = len(df[df['is_indexed'] == True])
        pending = len(df[df['status'] == 'pending'])
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(t("total"), total)
        m2.metric(t("indexed"), f"{indexed} ({(indexed/total*100):.1f}%)")
        m3.metric(t("queue"), pending)
        
        with m4:
            if pending > 0:
                if st.button(t("run_queue"), type="primary", key=f"run_{folder_id}", width="stretch"):
                    to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                    run_check(to_check, report_name_prefix=f"Check_{folder_name}")
            else:
                if st.button(t("rerun_all"), key=f"rerun_{folder_id}", width="stretch"):
                    ids = df['id'].tolist()
                    supabase.table("links").update({"status": "pending", "is_indexed": None}).in_("id", ids).execute()
                    st.rerun()

        st.write("")
        # Таблица
        selection = st.dataframe(
            df[['url', 'status', 'is_indexed', 'last_check']],
            width=None, 
            use_container_width=True,
            on_select="rerun",
            selection_mode="multi-row",
            column_config={
                "is_indexed": st.column_config.CheckboxColumn(t("col_index"), disabled=True),
                "url": st.column_config.LinkColumn(t("col_url"), display_text=None)
            }
        )
        
        # Удаление
        if len(selection.selection.rows) > 0:
            sel_idx = selection.selection.rows
            sel_ids = df.iloc[sel_idx]['id'].tolist()
            if st.button(t("del_selected").format(len(sel_ids)), key=f"del_sel_{folder_id}"):
                supabase.table("links").delete().in_("id", sel_ids).execute()
                st.rerun()

    st.divider()
    
    # ---------------------------------------------------------
    # 2. ИНТЕРФЕЙС ЗАГРУЗКИ (XLSX / CSV)
    # ---------------------------------------------------------
    st.subheader(f"📥 Add links to '{folder_name}'")
    
    tab_text, tab_file = st.tabs(["📝 Paste List", "ep Upload Excel/CSV"])
    
    # --- Вкладка 1: Текст ---
    with tab_text:
        text_input = st.text_area(t("paste_links"), height=150, key=f"input_{folder_id}")
        if st.button(t("save_btn"), key=f"save_txt_{folder_id}"):
            urls = parse_text_urls(text_input)
            if urls:
                data = [{"project_id": project_id, "url": u, "folder_id": folder_id, "status": "pending"} for u in urls]
                batch_size = 1000
                for i in range(0, len(data), batch_size):
                    supabase.table("links").insert(data[i:i+batch_size]).execute()
                st.success(t("success_added").format(len(urls)))
                time.sleep(1)
                st.rerun()

    # --- Вкладка 2: Файл (XLSX Support) ---
    with tab_file:
        uploaded_file = st.file_uploader("Excel (.xlsx, .xls) or CSV", type=['xlsx', 'xls', 'csv'], key=f"file_{folder_id}")
        
        if uploaded_file is not None and st.button("📤 Process File", key=f"proc_{folder_id}"):
            try:
                df_upload = None
                file_ext = uploaded_file.name.split('.')[-1].lower()
                
                # --- ПОПЫТКА 1: Стандартный Excel (xlsx) ---
                try:
                    df_upload = pd.read_excel(uploaded_file, engine='openpyxl')
                except Exception:
                    uploaded_file.seek(0) # Перемотка файла в начало
                    
                    # --- ПОПЫТКА 2: Старый Excel (xls) ---
                    try:
                        df_upload = pd.read_excel(uploaded_file, engine='xlrd')
                    except Exception:
                        uploaded_file.seek(0)
                        
                        # --- ПОПЫТКА 3: "Фейковый" Excel (HTML/XML внутри) ---
                        # Это решит вашу ошибку "found b'<html xm'"
                        try:
                            # Пытаемся прочитать как HTML таблицу
                            dfs = pd.read_html(uploaded_file)
                            if dfs:
                                df_upload = dfs[0] # Берем первую таблицу со страницы
                        except Exception:
                            uploaded_file.seek(0)
                            
                            # --- ПОПЫТКА 4: Обычный CSV ---
                            try:
                                df_upload = pd.read_csv(uploaded_file)
                            except Exception:
                                # Последний шанс: CSV с разделителем точка-с запятой
                                uploaded_file.seek(0)
                                try:
                                    df_upload = pd.read_csv(uploaded_file, sep=';')
                                except:
                                    pass

                if df_upload is None:
                    st.error("❌ Failed to read file. It might be corrupted or in an unsupported format.")
                    st.stop()

                # --- ДАЛЕЕ ВАША ЛОГИКА ПОИСКА ССЫЛОК (Без изменений) ---
                target_col = None
                clean_cols = {c: str(c).lower().strip() for c in df_upload.columns}
                
                priority_keywords = [
                    'referring page', 'source url', 
                    'target url', 'donor', 
                    'url', 'link', 'website'
                ]
                
                for kw in priority_keywords:
                    for original_col, clean_col in clean_cols.items():
                        if kw in clean_col:
                            target_col = original_col
                            break
                    if target_col: break
                
                if not target_col:
                    target_col = df_upload.columns[0]
                    st.toast(f"⚠️ Column name not recognized. Using first column: '{target_col}'", icon="ℹ️")

                urls_from_file = df_upload[target_col].dropna().astype(str).tolist()
                valid_urls = [u.strip() for u in urls_from_file if len(u.strip()) > 5]

                if valid_urls:
                    data = [{
                        "project_id": project_id, 
                        "url": u, 
                        "folder_id": folder_id, 
                        "status": "pending"
                    } for u in valid_urls]
                    
                    batch_size = 1000
                    for i in range(0, len(data), batch_size):
                        supabase.table("links").insert(data[i:i+batch_size]).execute()
                        
                    st.success(f"✅ Success! Added {len(data)} links. Order preserved.")
                    time.sleep(1.5)
                    st.rerun()
                else:
                    st.error("❌ No valid URLs found in the file.")
                    
            except Exception as e:
                st.error(f"Global Error: {e}")

# ==========================================
# САЙДБАР (ИЕРАРХИЯ)
# ==========================================
with st.sidebar:
    # --- LANGUAGE SWITCHER ---
    lang_choice = st.radio("Language / Мова:", ["🇬🇧 English", "🇺🇦 Українська"], horizontal=True)
    if lang_choice == "🇬🇧 English":
        st.session_state.lang = "en"
    else:
        st.session_state.lang = "uk"
    
    st.divider()
    
    st.title(t("nav_title"))
    
    if st.button(t("home_btn"), width="stretch"):
        st.session_state.selected_project_id = None
        st.session_state.selected_folder_id = None
        st.rerun()
    
    st.divider()
    
    # === SAFE FETCHING FOR SIDEBAR (FIX FOR httpx.ReadError) ===
    projs = safe_fetch("projects", order_col="created_at")
    all_folders = safe_fetch("folders", order_col="name")
    
    if projs:
        st.caption(t("projects_list"))
        for p in projs:
            is_expanded = (st.session_state.selected_project_id == p['id'])
            
            with st.expander(f"📂 {p['name']}", expanded=is_expanded):
                
                # Кнопка самого проекта
                if st.button(t("view_proj"), key=f"dash_{p['id']}", width="stretch"):
                    st.session_state.selected_project_id = p['id']
                    st.session_state.selected_folder_id = None
                    st.rerun()

                # Подпапки
                p_folders = [f for f in all_folders if f['project_id'] == p['id']]
                if p_folders:
                    for f in p_folders:
                        if st.button(f"└ 📁 {f['name']}", key=f"sb_f_{f['id']}", width="stretch"):
                            st.session_state.selected_project_id = p['id']
                            st.session_state.selected_folder_id = f['id']
                            st.rerun()

    st.divider()
    with st.expander(t("create_proj_exp")):
        new_p = st.text_input(t("proj_name_placeholder"))
        if st.button(t("create_btn")):
            supabase.table("projects").insert({"name": new_p}).execute()
            st.rerun()

    if st.session_state.selected_project_id:
        st.write("")
        st.write("")
        with st.expander(t("del_proj_exp")):
            st.warning(t("warn_del_proj"))
            if st.button(t("confirm_del"), type="primary"):
                supabase.table("projects").delete().eq("id", st.session_state.selected_project_id).execute()
                st.session_state.selected_project_id = None
                st.session_state.selected_folder_id = None
                st.rerun()

# ==========================================
# ОСНОВНОЙ ЭКРАН
# ==========================================

# 1. ГЛАВНАЯ (ДАШБОРД)
if not st.session_state.selected_project_id:
    st.title(t("dash_title"))
    
    if not projs:
        st.info(t("no_projs"))
    else:
        # Статистика
        # Используем безопасную загрузку
        all_links = safe_fetch("links", select="id, project_id, status, is_indexed")
        df_all = pd.DataFrame(all_links)
        
        stats_data = []
        global_pending_count = 0
        
        for p in projs:
            if not df_all.empty:
                p_links = df_all[df_all['project_id'] == p['id']]
                cnt = len(p_links)
                pend = len(p_links[p_links['status'] == 'pending'])
                idx = len(p_links[p_links['is_indexed'] == True])
            else:
                cnt, pend, idx = 0, 0, 0
            
            global_pending_count += pend
            stats_data.append({
                t("project"): p['name'],
                t("links_count"): cnt,
                t("in_index"): idx,
                t("in_queue"): pend
            })
        
        m1, m2 = st.columns(2)
        m1.metric(t("total_projs"), len(projs))
        m2.metric(t("total_queue"), global_pending_count)

        st.dataframe(pd.DataFrame(stats_data), width="stretch", hide_index=True)
        st.divider()
        
        if global_pending_count > 0:
            st.warning(t("ready_global").format(global_pending_count))
            if st.button(t("run_global"), type="primary", width="stretch"):
                 pending_full = supabase.table("links").select("id, url").eq("status", "pending").execute().data
                 run_check(pending_full, report_name_prefix="Global_Check")
        else:
            st.success(t("queue_empty"))
            st.write("")
            if st.button(t("reset_global")):
                supabase.table("links").update({"status": "pending", "is_indexed": None}).neq("id", 0).execute()
                st.rerun()

# 2. ВНУТРИ ПРОЕКТА
elif st.session_state.selected_project_id:
    curr_proj = next(p for p in projs if p['id'] == st.session_state.selected_project_id)
    
    # Список папок этого проекта
    p_folders = [f for f in all_folders if f['project_id'] == curr_proj['id']]

    # 2.1 ЕСЛИ МЫ ВЫБРАЛИ КОНКРЕТНУЮ ПАПКУ
    if st.session_state.selected_folder_id is not None:
        f_obj = next((f for f in p_folders if f['id'] == st.session_state.selected_folder_id), None)
        if not f_obj:
            st.error("Folder not found")
            st.session_state.selected_folder_id = None
            st.rerun()
        
        col_back, col_title = st.columns([1, 5])
        with col_back:
            if st.button(t("back_to_proj")):
                st.session_state.selected_folder_id = None
                st.rerun()
        with col_title:
            st.title(f"{curr_proj['name']} / 📂 {f_obj['name']}")
        
        # Рендер таблицы и кнопок для ЭТОЙ папки
        render_link_interface(curr_proj['id'], f_obj['id'], f_obj['name'])

    # 2.2 ЕСЛИ МЫ В КОРНЕ ПРОЕКТА
    else:
        st.title(f"📂 {curr_proj['name']}")
        
        # Если ЕСТЬ папки -> Показываем структуру папок
        if p_folders:
            st.caption(t("folder_struct"))
            
            links_res = supabase.table("links").select("folder_id, status, is_indexed").eq("project_id", curr_proj['id']).execute()
            df_links = pd.DataFrame(links_res.data)
            
            for f in p_folders:
                if not df_links.empty:
                    f_links = df_links[df_links['folder_id'] == f['id']]
                    total = len(f_links)
                    indexed = len(f_links[f_links['is_indexed'] == True])
                else:
                    total, indexed = 0, 0
                
                with st.container(border=True):
                    c1, c2, c3 = st.columns([3, 1, 0.5]) 
                    with c1:
                        st.subheader(f"📁 {f['name']}")
                        st.caption(f"{t('total')}: {total} | {t('indexed')}: {indexed}")
                    with c2:
                        st.write("")
                        if st.button(t("open_btn"), key=f"open_card_{f['id']}", width="stretch"):
                            st.session_state.selected_folder_id = f['id']
                            st.rerun()
                    with c3:
                        st.write("")
                        if st.button(t("del_btn"), key=f"del_f_{f['id']}"):
                            supabase.table("folders").delete().eq("id", f['id']).execute()
                            st.rerun()
            
            st.divider()
            with st.popover(t("add_new_folder")):
                new_f_name = st.text_input(t("folder_name"))
                if st.button(t("create_folder_btn")):
                    supabase.table("folders").insert({"name": new_f_name, "project_id": curr_proj['id']}).execute()
                    st.rerun()

        # Если ПАПОК НЕТ -> Показываем плоский список
        else:
            st.info(t("flat_mode"))
            
            with st.popover(t("create_first_folder")):
                new_f_name = st.text_input(t("folder_name"))
                if st.button(t("create_folder_btn")):
                    supabase.table("folders").insert({"name": new_f_name, "project_id": curr_proj['id']}).execute()
                    st.rerun()
            
            st.divider()
            render_link_interface(curr_proj['id'], None, t("root_folder"))

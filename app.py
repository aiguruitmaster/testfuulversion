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
st.set_page_config(page_title="SEO Index Manager", layout="wide")

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
    st.error(f"Ошибка подключения к БД: {e}")
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
                st.success("✅ Отчет отправлен в Slack!")
    except Exception as e:
        st.error(f"Ошибка Slack: {e}")

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

# -----------------------
# ЛОГИКА ПРОВЕРКИ
# -----------------------
def run_check(links_data):
    if not links_data: return
    session = init_requests()
    host = st.secrets["dataforseo"].get("host", "api.dataforseo.com").replace("https://", "")
    base_url = f"https://{host}"
    
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    payload = []
    tasks_map = {} 
    
    for item in links_data:
        payload.append({
            "location_code": 2840, "language_code": "en", "depth": 10,
            "keyword": build_site_query(item['url'])
        })

    BATCH_SIZE = 50
    total = len(links_data)
    processed = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch_links = links_data[i : i + BATCH_SIZE]
        batch_payload = payload[i : i + BATCH_SIZE]
        status_text.write(f"📤 Обработка {i+1}-{min(i+BATCH_SIZE, total)} из {total}...")
        
        try:
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
                time.sleep(2)
                status_text.write("⏳ Анализ...")
                for tid in batch_ids:
                    try:
                        r_get = session.get(base_url + TASK_GET_ADV.format(task_id=tid), timeout=30)
                        d_get = r_get.json()
                        link_id = tasks_map[tid]
                        url_obj = next(l for l in batch_links if l['id'] == link_id)
                        task_res = (d_get.get('tasks') or [{}])[0]
                        if task_res.get('status_code') == 20000:
                            items = (task_res.get('result') or [{}])[0].get('items', [])
                            is_ind = match_indexed(url_obj['url'], items)
                            supabase.table("links").update({
                                "status": "done", "is_indexed": is_ind, 
                                "last_check": datetime.utcnow().isoformat(), "task_id": tid
                            }).eq("id", link_id).execute()
                        else:
                            supabase.table("links").update({"status": "error"}).eq("id", link_id).execute()
                    except: pass
            else:
                st.error(f"API Error: {res.get('status_message')}")
            
            processed += len(batch_links)
            progress_bar.progress(processed / total)
        except Exception as e:
            st.error(f"Net Error: {e}")
        time.sleep(1.5)

    status_text.success("✅ Готово!")
    time.sleep(1)
    st.rerun()

# ==========================================
# ИНТЕРФЕЙС
# ==========================================

# --- САЙДБАР ---
with st.sidebar:
    st.title("🗂 Навигация")
    
    if st.button("🏠 Все проекты", use_container_width=True):
        st.session_state.selected_project_id = None
        st.session_state.selected_folder_id = None
        st.rerun()
    
    st.divider()
    
    projs = supabase.table("projects").select("*").order("created_at", desc=True).execute().data
    
    st.caption("Проекты:")
    if projs:
        for p in projs:
            is_active = (st.session_state.selected_project_id == p['id'])
            btn_type = "primary" if is_active else "secondary"
            if st.button(f"📂 {p['name']}", key=f"p_{p['id']}", use_container_width=True, type=btn_type):
                st.session_state.selected_project_id = p['id']
                st.session_state.selected_folder_id = None
                st.rerun()
                
    st.divider()
    with st.expander("➕ Создать Проект"):
        new_p = st.text_input("Имя проекта")
        if st.button("Создать"):
            supabase.table("projects").insert({"name": new_p}).execute()
            st.rerun()

    # === УДАЛЕНИЕ ПРОЕКТА (Вернули на место) ===
    if st.session_state.selected_project_id:
        st.write("")
        st.divider()
        with st.expander("🗑 Удалить текущий проект"):
            st.warning("Внимание! Это удалит проект и ВСЕ ссылки внутри него.")
            if st.button("Да, удалить проект", type="primary"):
                supabase.table("projects").delete().eq("id", st.session_state.selected_project_id).execute()
                st.session_state.selected_project_id = None
                st.session_state.selected_folder_id = None
                st.success("Проект удален!")
                time.sleep(1)
                st.rerun()

# --- ЛОГИКА ОТОБРАЖЕНИЯ ---

# 1. ГЛАВНАЯ
if not st.session_state.selected_project_id:
    st.title("📊 Все проекты")
    if not projs:
        st.info("Нет проектов. Создайте первый в меню слева.")
    else:
        all_links = supabase.table("links").select("id").execute().data
        st.metric("Всего ссылок в системе", len(all_links))
        st.write("Выберите проект слева, чтобы начать работу.")

# 2. ПРОСМОТР ПРОЕКТА (СПИСОК ПАПОК)
elif st.session_state.selected_project_id and st.session_state.selected_folder_id is None:
    curr_proj = next(p for p in projs if p['id'] == st.session_state.selected_project_id)
    st.title(f"📂 {curr_proj['name']}")
    st.caption("Структура папок")
    
    folders = supabase.table("folders").select("*").eq("project_id", curr_proj['id']).order("created_at", desc=False).execute().data
    links_res = supabase.table("links").select("folder_id, status, is_indexed").eq("project_id", curr_proj['id']).execute()
    df_links = pd.DataFrame(links_res.data)
    
    # --- КАРТОЧКИ ПАПОК ---
    if folders:
        for f in folders:
            if not df_links.empty:
                f_links = df_links[df_links['folder_id'] == f['id']]
                total = len(f_links)
                indexed = len(f_links[f_links['is_indexed'] == True])
            else:
                total, indexed = 0, 0
            
            with st.container(border=True):
                # Добавили колонку для кнопки удаления
                c1, c2, c3 = st.columns([3, 1, 0.5]) 
                with c1:
                    st.subheader(f"📁 {f['name']}")
                    st.caption(f"Ссылок: {total} | В индексе: {indexed}")
                with c2:
                    st.write("")
                    if st.button("Открыть ➡", key=f"open_{f['id']}", use_container_width=True):
                        st.session_state.selected_folder_id = f['id']
                        st.rerun()
                # КНОПКА УДАЛЕНИЯ ПОДПАПКИ
                with c3:
                    st.write("")
                    if st.button("🗑", key=f"del_f_{f['id']}", help="Удалить папку"):
                        # Удаляем папку (ссылки станут General из-за настройки БД on delete set null, или удалятся если cascade)
                        # Лучше явно удалить папку, ссылки обычно остаются но становятся "без папки"
                        supabase.table("folders").delete().eq("id", f['id']).execute()
                        st.rerun()
    
    # General папка
    gen_links = df_links[df_links['folder_id'].isnull()] if not df_links.empty else pd.DataFrame()
    if not gen_links.empty:
        with st.container(border=True):
            c1, c2, c3 = st.columns([3, 1, 0.5])
            with c1:
                st.subheader("📄 Общая (Без папки)")
                st.caption(f"Ссылок: {len(gen_links)}")
            with c2:
                st.write("")
                if st.button("Открыть ➡", key="open_general", use_container_width=True):
                    st.session_state.selected_folder_id = -1
                    st.rerun()
            with c3:
                st.write("") 
                # General удалить нельзя

    st.divider()
    with st.popover("➕ Добавить новую папку"):
        new_f_name = st.text_input("Название папки")
        if st.button("Создать папку"):
            supabase.table("folders").insert({"name": new_f_name, "project_id": curr_proj['id']}).execute()
            st.rerun()
            
    st.write("---")
    if not df_links.empty:
        pending = len(df_links[df_links['status'] == 'pending'])
        if pending > 0:
            if st.button(f"🚀 Проверить весь проект ({pending} в очереди)", type="primary"):
                 to_check = supabase.table("links").select("id, url").eq("project_id", curr_proj['id']).eq("status", "pending").execute().data
                 run_check(to_check)

# 3. ВНУТРИ ПАПКИ
elif st.session_state.selected_folder_id is not None:
    curr_proj = next(p for p in projs if p['id'] == st.session_state.selected_project_id)
    
    if st.session_state.selected_folder_id == -1:
        folder_name = "Общая (Без папки)"
        folder_db_id = None
    else:
        f_res = supabase.table("folders").select("*").eq("id", st.session_state.selected_folder_id).execute().data
        if not f_res:
            st.error("Папка не найдена")
            st.session_state.selected_folder_id = None
            st.rerun()
        folder_name = f_res[0]['name']
        folder_db_id = st.session_state.selected_folder_id

    col_back, col_title = st.columns([1, 5])
    with col_back:
        if st.button("⬅ Назад к папкам"):
            st.session_state.selected_folder_id = None
            st.rerun()
    with col_title:
        st.title(f"{curr_proj['name']} / {folder_name}")

    query = supabase.table("links").select("*").eq("project_id", curr_proj['id'])
    if folder_db_id is None:
        query = query.is_("folder_id", "null")
    else:
        query = query.eq("folder_id", folder_db_id)
    
    links = query.order("id", desc=True).execute().data
    df = pd.DataFrame(links)

    if df.empty:
        st.info("В этой папке пока пусто.")
    else:
        total = len(df)
        indexed = len(df[df['is_indexed'] == True])
        pending = len(df[df['status'] == 'pending'])
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Всего", total)
        m2.metric("В индексе", f"{indexed} ({(indexed/total*100):.1f}%)")
        m3.metric("Очередь", pending)
        
        with m4:
            if pending > 0:
                if st.button("🚀 Проверить эту папку", type="primary"):
                    to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                    run_check(to_check)
            else:
                if st.button("🔄 Перепроверить папку"):
                    ids = df['id'].tolist()
                    supabase.table("links").update({"status": "pending", "is_indexed": None}).in_("id", ids).execute()
                    st.rerun()

        st.write("")
        selection = st.dataframe(
            df[['url', 'status', 'is_indexed', 'last_check']],
            use_container_width=True,
            on_select="rerun",
            selection_mode="multi-row",
            column_config={
                "is_indexed": st.column_config.CheckboxColumn("Index?", disabled=True),
                "url": st.column_config.LinkColumn("URL")
            }
        )
        
        if len(selection.selection.rows) > 0:
            sel_idx = selection.selection.rows
            sel_ids = df.iloc[sel_idx]['id'].tolist()
            if st.button(f"🗑 Удалить {len(sel_ids)} ссылок"):
                supabase.table("links").delete().in_("id", sel_ids).execute()
                st.rerun()

    st.divider()
    st.subheader(f"📥 Добавить ссылки в '{folder_name}'")
    text_input = st.text_area("Вставьте ссылки списком:", height=100)
    if st.button("💾 Сохранить"):
        urls = parse_text_urls(text_input)
        if urls:
            data = [{
                "project_id": curr_proj['id'],
                "url": u,
                "folder_id": folder_db_id,
                "status": "pending"
            } for u in urls]
            
            batch_size = 500
            for i in range(0, len(data), batch_size):
                supabase.table("links").insert(data[i:i+batch_size]).execute()
            
            st.success(f"Добавлено {len(urls)} ссылок!")
            time.sleep(1)
            st.rerun()

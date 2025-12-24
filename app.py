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
def run_check(links_data, report_name_prefix="Report"):
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

    status_text.write("📊 Отправка отчета...")
    try:
        checked_ids = [item['id'] for item in links_data]
        res = supabase.table("links").select("url, status, is_indexed, last_check").in_("id", checked_ids).execute()
        df_report = pd.DataFrame(res.data)
        
        if not df_report.empty:
            excel_bytes = to_excel(df_report)
            date_str = datetime.now().strftime('%Y-%m-%d')
            fname = f"{report_name_prefix}_{date_str}.xlsx"
            msg = f"✅ *Проверка завершена ({report_name_prefix})!*\n🔗 Всего: {total}"
            send_slack_file(excel_bytes, fname, msg)
    except Exception as e:
        st.error(f"Ошибка отчета: {e}")

    status_text.success("✅ Готово!")
    time.sleep(1)
    st.rerun()

# -----------------------
# ФУНКЦИЯ ОТРИСОВКИ ИНТЕРФЕЙСА ПАПКИ/ПРОЕКТА
# -----------------------
def render_link_interface(project_id, folder_id=None, folder_name=""):
    """Рисует таблицу, кнопки и загрузку для конкретного контекста"""
    
    # Запрос ссылок
    query = supabase.table("links").select("*").eq("project_id", project_id)
    if folder_id is None:
        query = query.is_("folder_id", "null")
    else:
        query = query.eq("folder_id", folder_id)
    
    links = query.order("id", desc=True).execute().data
    df = pd.DataFrame(links)

    if df.empty:
        st.info("Здесь пока пусто.")
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
                if st.button("🚀 Проверить очередь", type="primary", key=f"run_{folder_id}"):
                    to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                    run_check(to_check, report_name_prefix=f"Check_{folder_name}")
            else:
                if st.button("🔄 Перепроверить всё", key=f"rerun_{folder_id}"):
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
            if st.button(f"🗑 Удалить {len(sel_ids)} ссылок", key=f"del_sel_{folder_id}"):
                supabase.table("links").delete().in_("id", sel_ids).execute()
                st.rerun()

    st.divider()
    st.subheader(f"📥 Добавить ссылки в '{folder_name}'")
    text_input = st.text_area("Вставьте ссылки списком:", height=100, key=f"input_{folder_id}")
    if st.button("💾 Сохранить", key=f"save_{folder_id}"):
        urls = parse_text_urls(text_input)
        if urls:
            data = [{
                "project_id": project_id,
                "url": u,
                "folder_id": folder_id,
                "status": "pending"
            } for u in urls]
            
            batch_size = 500
            for i in range(0, len(data), batch_size):
                supabase.table("links").insert(data[i:i+batch_size]).execute()
            
            st.success(f"Добавлено {len(urls)} ссылок!")
            time.sleep(1)
            st.rerun()

# ==========================================
# САЙДБАР (ИЕРАРХИЯ)
# ==========================================
with st.sidebar:
    st.title("🗂 Навигация")
    
    if st.button("🏠 ГЛАВНАЯ (Все проекты)", use_container_width=True):
        st.session_state.selected_project_id = None
        st.session_state.selected_folder_id = None
        st.rerun()
    
    st.divider()
    
    projs = supabase.table("projects").select("*").order("created_at", desc=True).execute().data
    all_folders = supabase.table("folders").select("*").order("name", desc=False).execute().data
    
    if projs:
        st.caption("Ваши проекты:")
        for p in projs:
            is_expanded = (st.session_state.selected_project_id == p['id'])
            
            with st.expander(f"📂 {p['name']}", expanded=is_expanded):
                
                # Кнопка самого проекта (открывает корень или список папок)
                if st.button("📊 Обзор проекта", key=f"dash_{p['id']}", use_container_width=True):
                    st.session_state.selected_project_id = p['id']
                    st.session_state.selected_folder_id = None
                    st.rerun()

                # Подпапки
                p_folders = [f for f in all_folders if f['project_id'] == p['id']]
                if p_folders:
                    for f in p_folders:
                        if st.button(f"└ 📁 {f['name']}", key=f"sb_f_{f['id']}", use_container_width=True):
                            st.session_state.selected_project_id = p['id']
                            st.session_state.selected_folder_id = f['id']
                            st.rerun()
                # Кнопку "General" УБРАЛИ по просьбе

    st.divider()
    with st.expander("➕ Создать Проект"):
        new_p = st.text_input("Имя проекта")
        if st.button("Создать"):
            supabase.table("projects").insert({"name": new_p}).execute()
            st.rerun()

    if st.session_state.selected_project_id:
        st.write("")
        st.write("")
        with st.expander("🗑 Удалить текущий проект"):
            if st.button("Да, удалить", type="primary"):
                supabase.table("projects").delete().eq("id", st.session_state.selected_project_id).execute()
                st.session_state.selected_project_id = None
                st.session_state.selected_folder_id = None
                st.rerun()

# ==========================================
# ОСНОВНОЙ ЭКРАН
# ==========================================

# 1. ГЛАВНАЯ (ДАШБОРД)
if not st.session_state.selected_project_id:
    st.title("📊 Все проекты")
    
    if not projs:
        st.info("Нет проектов. Создайте первый в меню слева.")
    else:
        # Статистика
        all_links_res = supabase.table("links").select("id, project_id, status, is_indexed").execute()
        df_all = pd.DataFrame(all_links_res.data)
        
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
                "Проект": p['name'],
                "Всего ссылок": cnt,
                "В индексе": idx,
                "В очереди": pend
            })
        
        m1, m2 = st.columns(2)
        m1.metric("Всего проектов", len(projs))
        m2.metric("ВСЕГО В ОЧЕРЕДИ", global_pending_count)

        st.dataframe(pd.DataFrame(stats_data), use_container_width=True, hide_index=True)
        st.divider()
        
        if global_pending_count > 0:
            st.warning(f"Готово к проверке: **{global_pending_count}** ссылок.")
            if st.button(f"🚀 ЗАПУСТИТЬ ПРОВЕРКУ ВСЕХ ПРОЕКТОВ", type="primary", use_container_width=True):
                 pending_full = supabase.table("links").select("id, url").eq("status", "pending").execute().data
                 run_check(pending_full, report_name_prefix="Global_Check")
        else:
            st.success("Очередь пуста.")
            st.write("")
            if st.button("🔄 Сбросить статусы ВО ВСЕХ ПРОЕКТАХ и проверить заново"):
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
            st.error("Папка не найдена")
            st.session_state.selected_folder_id = None
            st.rerun()
        
        col_back, col_title = st.columns([1, 5])
        with col_back:
            if st.button("⬅ К проекту"):
                st.session_state.selected_folder_id = None
                st.rerun()
        with col_title:
            st.title(f"{curr_proj['name']} / 📂 {f_obj['name']}")
        
        # Рендер таблицы и кнопок для ЭТОЙ папки
        render_link_interface(curr_proj['id'], f_obj['id'], f_obj['name'])

    # 2.2 ЕСЛИ МЫ В КОРНЕ ПРОЕКТА
    else:
        st.title(f"📂 {curr_proj['name']}")
        
        # --- ЛОГИКА ОТОБРАЖЕНИЯ ---
        # Если ЕСТЬ папки -> Показываем структуру папок (без General)
        if p_folders:
            st.caption("Структура папок проекта:")
            
            # Получаем статистику для карточек
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
                        st.caption(f"Ссылок: {total} | В индексе: {indexed}")
                    with c2:
                        st.write("")
                        if st.button("Открыть ➡", key=f"open_card_{f['id']}", use_container_width=True):
                            st.session_state.selected_folder_id = f['id']
                            st.rerun()
                    with c3:
                        st.write("")
                        if st.button("🗑", key=f"del_f_{f['id']}"):
                            supabase.table("folders").delete().eq("id", f['id']).execute()
                            st.rerun()
                            
            st.divider()
            with st.popover("➕ Добавить новую папку"):
                new_f_name = st.text_input("Название")
                if st.button("Создать папку"):
                    supabase.table("folders").insert({"name": new_f_name, "project_id": curr_proj['id']}).execute()
                    st.rerun()

        # Если ПАПОК НЕТ -> Показываем плоский список (как раньше)
        else:
            st.info("В этом проекте нет папок. Работаем в плоском режиме.")
            
            with st.popover("➕ Создать первую папку (переключиться в режим структуры)"):
                new_f_name = st.text_input("Название")
                if st.button("Создать папку"):
                    supabase.table("folders").insert({"name": new_f_name, "project_id": curr_proj['id']}).execute()
                    st.rerun()
            
            st.divider()
            # Рендер таблицы для корня проекта (folder_id=None)
            render_link_interface(curr_proj['id'], None, "Корневая папка")

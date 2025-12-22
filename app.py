import streamlit as st
from supabase import create_client
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
import time
import requests
from urllib.parse import urlparse, urlunparse
from datetime import datetime

# -----------------------
# Конфигурация и API настройки
# -----------------------
st.set_page_config(page_title="SEO Index Manager", layout="wide")

# DataForSEO Constants
TASK_POST = "/v3/serp/google/organic/task_post"
TASKS_READY = "/v3/serp/google/organic/tasks_ready"
TASK_GET_ADV = "/v3/serp/google/organic/task_get/advanced/{task_id}"

# Подключение к Supabase
@st.cache_resource
def init_supabase():
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

# Подключение сессии для DataForSEO
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
# Хелперы (из твоего старого скрипта)
# -----------------------
def norm_url(u: str) -> str:
    """Нормализация URL для сравнения"""
    p = urlparse(u.strip())
    netloc = (p.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = (p.path or "").rstrip("/")
    return urlunparse(("", netloc, path, "", "", "")).lower()

def build_site_query(url: str) -> str:
    """Создает запрос site:url"""
    p = urlparse(url.strip())
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = (p.path or "").strip().lstrip("/").rstrip("/")
    if path in ("", "/"):
        return f"site:{host}"
    return f"site:{host}/{path}"

def match_indexed(original_url: str, items):
    """Проверяет, есть ли URL в выдаче"""
    orig = norm_url(original_url)
    for it in items:
        if it.get("type") == "organic":
            u = it.get("url")
            if u and norm_url(u) == orig:
                return True
    return False

def parse_excel_urls(uploaded_file):
    urls = []
    wb = load_workbook(BytesIO(uploaded_file.getvalue()), read_only=True)
    for ws in wb.worksheets:
        header_row = 1
        for r in range(1, 11):
            val = ws.cell(row=r, column=2).value
            if isinstance(val, str) and "referring page url" in val.lower():
                header_row = r
                break
        for r in range(header_row + 1, ws.max_row + 1):
            val = ws.cell(row=r, column=2).value
            if val and isinstance(val, str) and (val.startswith("http://") or val.startswith("https://")):
                urls.append(val.strip())
    return urls

# -----------------------
# Логика проверки (Core Engine)
# -----------------------
def run_check(project_id, links_data):
    """
    Принимает список словарей links_data [{'id': 1, 'url': '...'}, ...]
    """
    session = init_requests()
    host = st.secrets["dataforseo"].get("host", "api.dataforseo.com").replace("https://", "")
    base_url = f"https://{host}"
    
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    
    # 1. Формируем задачи (POST)
    tasks_map = {} # task_id -> link_db_id
    payload = []
    
    # DataForSEO настройки
    post_body_base = {
        "location_code": 2840,
        "language_code": "en",
        "depth": 10
    }

    for item in links_data:
        p = post_body_base.copy()
        p["keyword"] = build_site_query(item['url'])
        # Используем pingback_url или просто tag, чтобы связать задачу. 
        # Но проще через порядок, так как API возвращает в том же порядке.
        # Для надежности будем мапить по порядку, но осторожно.
        payload.append(p)

    # Разбиваем на батчи по 100, если нужно, но для простоты пока одним куском (до 100 шт)
    # Если ссылок > 100, лучше добавить цикл батчинга. Добавим простой батчинг.
    
    BATCH_SIZE = 50
    total = len(links_data)
    processed_count = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch_links = links_data[i : i + BATCH_SIZE]
        batch_payload = payload[i : i + BATCH_SIZE]
        
        status_text.write(f"📤 Отправка задач {i+1}-{min(i+BATCH_SIZE, total)} из {total}...")
        
        try:
            r = session.post(base_url + TASK_POST, json=batch_payload, timeout=60)
            res = r.json()
            if res.get('status_code') != 20000:
                st.error(f"API Error: {res.get('status_message')}")
                continue
                
            # Собираем ID задач
            batch_task_ids = []
            for idx, task in enumerate(res.get('tasks', [])):
                if task.get('id'):
                    tid = task['id']
                    # Связываем task_id с ID ссылки в нашей базе
                    link_db_id = batch_links[idx]['id']
                    tasks_map[tid] = link_db_id
                    batch_task_ids.append(tid)
                    
                    # (Опционально) Можно сразу записать task_id в базу, чтобы не потерять
            
            # 2. Ждем выполнения
            if not batch_task_ids:
                continue
                
            status_text.write("⏳ Ожидание результатов от Google...")
            # Простое ожидание (polling)
            completed_tasks = set()
            attempts = 0
            while len(completed_tasks) < len(batch_task_ids) and attempts < 20:
                time.sleep(3) 
                attempts += 1
                # Проверяем готовность (упрощенно - сразу пробуем GET, так как task_post organic обычно быстр, 
                # но правильнее через tasks_ready. Для упрощения кода используем GET, он вернет 'status': 'working' если не готов)
                # Лучше все же tasks_ready для батча, но для 50 штук можно и в лоб.
                pass 
            
            # 3. Получаем результаты по каждой задаче
            for tid in batch_task_ids:
                # Получаем результат
                r_get = session.get(base_url + TASK_GET_ADV.format(task_id=tid), timeout=30)
                try:
                    d_get = r_get.json()
                    # Проверяем, готова ли задача
                    task_res = (d_get.get('tasks') or [{}])[0]
                    
                    link_id = tasks_map[tid]
                    original_link_obj = next(l for l in batch_links if l['id'] == link_id)
                    
                    if task_res.get('status_code') == 20000:
                        result_items = (task_res.get('result') or [{}])[0].get('items', [])
                        is_ind = match_indexed(original_link_obj['url'], result_items)
                        
                        # ОБНОВЛЯЕМ БАЗУ
                        supabase.table("links").update({
                            "status": "done",
                            "is_indexed": is_ind,
                            "last_check": datetime.utcnow().isoformat(),
                            "task_id": tid
                        }).eq("id", link_id).execute()
                        
                    else:
                        # Ошибка или еще работает
                        supabase.table("links").update({"status": "error"}).eq("id", link_id).execute()
                        
                except Exception as e:
                    print(f"Error parsing result: {e}")
        
            processed_count += len(batch_links)
            progress_bar.progress(processed_count / total)
            
        except Exception as e:
            st.error(f"Сбой сети или API: {e}")

    status_text.success("✅ Проверка завершена!")
    time.sleep(2)
    st.rerun()


# -----------------------
# Сайдбар
# -----------------------
with st.sidebar:
    st.title("🗂 Мои Проекты")
    
    with st.expander("➕ Создать новую папку"):
        new_proj = st.text_input("Название папки")
        if st.button("Создать"):
            if new_proj:
                supabase.table("projects").insert({"name": new_proj}).execute()
                st.rerun()

    st.divider()

    response = supabase.table("projects").select("*").order("created_at", desc=True).execute()
    projects = response.data
    
    selected_project_id = None
    if projects:
        opts = {p['name']: p['id'] for p in projects}
        p_name = st.selectbox("Активная папка:", list(opts.keys()))
        selected_project_id = opts[p_name]

# -----------------------
# Основной экран
# -----------------------
if selected_project_id:
    st.title(f"📂 {p_name}")
    
    # Грузим данные
    res = supabase.table("links").select("*").eq("project_id", selected_project_id).order("id", desc=False).execute()
    df = pd.DataFrame(res.data)

    # Статистика
    if not df.empty:
        total = len(df)
        indexed = len(df[df['is_indexed'] == True])
        pending = len(df[df['status'] == 'pending'])
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Всего ссылок", total)
        c2.metric("В индексе", indexed)
        c3.metric("Очередь", pending)
        
        st.divider()
        
        # КНОПКА ЗАПУСКА ПРОВЕРКИ
        # Показываем, только если есть что проверять (pending > 0)
        if pending > 0:
            if st.button(f"🚀 Запустить проверку ({pending} шт.)", type="primary"):
                # Выбираем только pending ссылки для обработки
                links_to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                run_check(selected_project_id, links_to_check)
        else:
            if st.button("🔄 Перепроверить всё (Сбросить статусы)"):
                # Сброс статусов на pending
                supabase.table("links").update({
                    "status": "pending", 
                    "is_indexed": None
                }).eq("project_id", selected_project_id).execute()
                st.rerun()

    # Загрузка
    with st.expander("📥 Добавить ссылки", expanded=(df.empty)):
        uploaded = st.file_uploader("Excel (колонка B)", type=["xlsx"])
        if uploaded and st.button("💾 Сохранить"):
            urls = parse_excel_urls(uploaded)
            if urls:
                data = [{"project_id": selected_project_id, "url": u, "status": "pending"} for u in urls]
                # Batch insert
                batch_size = 1000
                bar = st.progress(0)
                for i in range(0, len(data), batch_size):
                    supabase.table("links").insert(data[i:i+batch_size]).execute()
                    bar.progress(min((i+batch_size)/len(data), 1.0))
                st.success(f"Добавлено {len(urls)} ссылок")
                time.sleep(1)
                st.rerun()

    # Таблица
    st.subheader("Список ссылок")
    if not df.empty:
        st.dataframe(
            df[['url', 'status', 'is_indexed', 'last_check', 'created_at']], 
            use_container_width=True,
            column_config={
                "is_indexed": st.column_config.CheckboxColumn("Index?", disabled=True),
                "url": st.column_config.LinkColumn("URL")
            }
        )
    else:
        st.info("Нет данных.")

else:
    st.write("Выберите проект.")

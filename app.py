import streamlit as st
from supabase import create_client
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
import time
import requests
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta

# -----------------------
# Конфигурация и API
# -----------------------
st.set_page_config(page_title="SEO Index Manager", layout="wide")

TASK_POST = "/v3/serp/google/organic/task_post"
TASKS_READY = "/v3/serp/google/organic/tasks_ready"
TASK_GET_ADV = "/v3/serp/google/organic/task_get/advanced/{task_id}"

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
# Хелперы
# -----------------------
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
# Логика массовой проверки
# -----------------------
def run_check(links_data):
    """
    Принимает список словарей [{'id': 1, 'url': '...'}, ...]
    Может принимать ссылки из разных проектов сразу.
    """
    if not links_data: return
    
    session = init_requests()
    host = st.secrets["dataforseo"].get("host", "api.dataforseo.com").replace("https://", "")
    base_url = f"https://{host}"
    
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    
    payload = []
    tasks_map = {} 
    
    # Подготовка Payload
    for item in links_data:
        payload.append({
            "location_code": 2840,
            "language_code": "en",
            "depth": 10,
            "keyword": build_site_query(item['url'])
        })

    BATCH_SIZE = 50
    total = len(links_data)
    processed_count = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch_links = links_data[i : i + BATCH_SIZE]
        batch_payload = payload[i : i + BATCH_SIZE]
        
        status_text.write(f"📤 Обработка {i+1}-{min(i+BATCH_SIZE, total)} из {total}...")
        
        try:
            r = session.post(base_url + TASK_POST, json=batch_payload, timeout=60)
            res = r.json()
            
            if res.get('status_code') == 20000:
                batch_task_ids = []
                for idx, task in enumerate(res.get('tasks', [])):
                    if task.get('id'):
                        tid = task['id']
                        link_db_id = batch_links[idx]['id']
                        tasks_map[tid] = link_db_id
                        batch_task_ids.append(tid)
                
                if not batch_task_ids: continue

                # Ожидание
                time.sleep(2) # Небольшая пауза перед поллингом
                status_text.write("⏳ Анализ результатов...")
                
                # Получение результатов (поштучно для надежности)
                for tid in batch_task_ids:
                    try:
                        r_get = session.get(base_url + TASK_GET_ADV.format(task_id=tid), timeout=30)
                        d_get = r_get.json()
                        
                        link_id = tasks_map[tid]
                        original_link_obj = next(l for l in batch_links if l['id'] == link_id)
                        
                        task_res = (d_get.get('tasks') or [{}])[0]
                        if task_res.get('status_code') == 20000:
                            result_items = (task_res.get('result') or [{}])[0].get('items', [])
                            is_ind = match_indexed(original_link_obj['url'], result_items)
                            
                            supabase.table("links").update({
                                "status": "done",
                                "is_indexed": is_ind,
                                "last_check": datetime.utcnow().isoformat(),
                                "task_id": tid
                            }).eq("id", link_id).execute()
                        else:
                            supabase.table("links").update({"status": "error"}).eq("id", link_id).execute()
                            
                    except Exception as e:
                        print(f"Err task {tid}: {e}")
            else:
                st.error(f"API Error: {res.get('status_message')}")

            processed_count += len(batch_links)
            progress_bar.progress(processed_count / total)
            
        except Exception as e:
            st.error(f"Network error: {e}")

    status_text.success("✅ Готово!")
    time.sleep(1)
    st.rerun()

# -----------------------
# Сайдбар
# -----------------------
with st.sidebar:
    st.title("🗂 Меню")
    
    if st.button("🏠 На главную (Дашборд)"):
        st.session_state.selected_project_id = None
        st.rerun()
    
    st.divider()
    
    st.subheader("Мои Проекты")
    # Создание
    with st.expander("➕ Новый проект"):
        new_proj = st.text_input("Название")
        if st.button("Создать"):
            if new_proj:
                supabase.table("projects").insert({"name": new_proj}).execute()
                st.rerun()

    # Список проектов
    response = supabase.table("projects").select("*").order("created_at", desc=True).execute()
    projects = response.data
    
    # Используем session_state для хранения выбранного проекта
    if "selected_project_id" not in st.session_state:
        st.session_state.selected_project_id = None

    if projects:
        for p in projects:
            # Делаем кнопки вместо selectbox для удобства
            if st.button(f"📂 {p['name']}", key=p['id'], use_container_width=True):
                st.session_state.selected_project_id = p['id']
                st.rerun()

# -----------------------
# ЛОГИКА ЭКРАНОВ
# -----------------------

# 1. ЭКРАН ПРОЕКТА (если выбран)
if st.session_state.selected_project_id:
    # Ищем имя проекта
    current_proj = next((p for p in projects if p['id'] == st.session_state.selected_project_id), None)
    if not current_proj:
        st.session_state.selected_project_id = None
        st.rerun()
        
    st.title(f"📂 Проект: {current_proj['name']}")
    
    # Грузим ссылки
    res = supabase.table("links").select("*").eq("project_id", st.session_state.selected_project_id).order("id", desc=False).execute()
    df = pd.DataFrame(res.data)

    if not df.empty:
        total = len(df)
        indexed = len(df[df['is_indexed'] == True])
        pending = len(df[df['status'] == 'pending'])
        
        # Метрики
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Всего", total)
        c2.metric("В индексе", f"{indexed} ({(indexed/total*100):.1f}%)")
        c3.metric("Очередь", pending)
        
        # Кнопки действий
        with c4:
            if pending > 0:
                if st.button("🚀 Проверить очередь", type="primary"):
                    to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                    run_check(to_check)
            else:
                if st.button("🔄 Сбросить и проверить заново"):
                    supabase.table("links").update({"status": "pending", "is_indexed": None}).eq("project_id", st.session_state.selected_project_id).execute()
                    st.rerun()
                    
        # Таблица
        st.divider()
        st.dataframe(
            df[['url', 'status', 'is_indexed', 'last_check', 'created_at']], 
            use_container_width=True,
            column_config={
                "is_indexed": st.column_config.CheckboxColumn("Index?", disabled=True),
                "url": st.column_config.LinkColumn("URL"),
                "last_check": st.column_config.DatetimeColumn("Дата проверки", format="D MMM YYYY, HH:mm")
            }
        )
    else:
        st.info("В папке пусто.")
    
    # Загрузка
    with st.expander("📥 Добавить Excel файл", expanded=(df.empty)):
        uploaded = st.file_uploader("Загрузить ссылки (колонка B)", type=["xlsx"])
        if uploaded and st.button("Сохранить в базу"):
            urls = parse_excel_urls(uploaded)
            if urls:
                data = [{"project_id": st.session_state.selected_project_id, "url": u, "status": "pending"} for u in urls]
                batch_size = 1000
                bar = st.progress(0)
                for i in range(0, len(data), batch_size):
                    supabase.table("links").insert(data[i:i+batch_size]).execute()
                    bar.progress(min((i+batch_size)/len(data), 1.0))
                st.success(f"Добавлено {len(urls)}")
                time.sleep(1)
                st.rerun()

# 2. ГЛАВНЫЙ ДАШБОРД (если проект не выбран)
else:
    st.title("📊 Дашборд мониторинга")
    
    # Получаем ВСЕ ссылки сразу, чтобы посчитать статистику
    # В идеале это делать через RPC на стороне базы, но для тысяч строк Python справится
    all_links_res = supabase.table("links").select("id, project_id, status, is_indexed, last_check, url").execute()
    all_links_df = pd.DataFrame(all_links_res.data)
    
    if projects:
        stats_data = []
        global_pending_count = 0
        
        for p in projects:
            pid = p['id']
            if not all_links_df.empty:
                p_links = all_links_df[all_links_df['project_id'] == pid]
                total = len(p_links)
                idx = len(p_links[p_links['is_indexed'] == True])
                pend = len(p_links[p_links['status'] == 'pending'])
                
                # Ищем самую свежую дату проверки
                last_date = None
                if not p_links['last_check'].isna().all():
                    last_date = pd.to_datetime(p_links['last_check']).max()
            else:
                total, idx, pend, last_date = 0, 0, 0, None
                
            global_pending_count += pend
            
            stats_data.append({
                "ID": pid,
                "Проект": p['name'],
                "Всего ссылок": total,
                "В индексе": idx,
                "% Index": f"{(idx/total*100):.1f}%" if total > 0 else "0%",
                "Очередь": pend,
                "Последняя проверка": last_date
            })
            
        stats_df = pd.DataFrame(stats_data)
        
        # Метрики дашборда
        m1, m2 = st.columns([3, 1])
        m1.metric("Всего проектов", len(projects))
        m2.metric("Всего задач в очереди", global_pending_count)
        
        # ГЛОБАЛЬНАЯ КНОПКА ЗАПУСКА
        if global_pending_count > 0:
            st.warning(f"Найдено {global_pending_count} ссылок ожидающих проверки во всех папках.")
            if st.button(f"🚀 ЗАПУСТИТЬ ВСЕ ({global_pending_count} шт.)", type="primary", use_container_width=True):
                # Собираем все pending ссылки со всех проектов
                pending_all = all_links_df[all_links_df['status'] == 'pending'][['id', 'url']].to_dict('records')
                run_check(pending_all)
        else:
            st.success("Все ссылки проверены! Очередь пуста.")
            
        st.subheader("Сводная таблица")
        st.dataframe(
            stats_df, 
            use_container_width=True,
            column_config={
                "Последняя проверка": st.column_config.DatetimeColumn(format="D MMM YYYY, HH:mm"),
            },
            hide_index=True
        )
        
    else:
        st.info("Создайте первый проект в меню слева!")

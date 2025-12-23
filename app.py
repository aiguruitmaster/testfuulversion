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
# Конфигурация страницы
# -----------------------
st.set_page_config(page_title="SEO Index Manager", layout="wide")

# -----------------------
# 🔐 СИСТЕМА АВТОРИЗАЦИИ
# -----------------------
def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    # Если уже вошли - пропускаем
    if st.session_state.authenticated:
        return True

    # Экран входа
    st.title("🔒 Вход в систему")
    password = st.text_input("Введите пароль доступа", type="password")
    
    if st.button("Войти"):
        # Пароль берется из secrets.toml
        if password == st.secrets["auth"]["password"]:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Неверный пароль!")
    return False

# Блокируем выполнение, пока не введен пароль
if not check_password():
    st.stop()

# ==========================================
# ОСНОВНОЕ ПРИЛОЖЕНИЕ
# ==========================================

TASK_POST = "/v3/serp/google/organic/task_post"
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
def to_excel(df):
    """Конвертация DF в Excel байты"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    return output.getvalue()

def norm_url(u: str) -> str:
    """Нормализация URL для сравнения"""
    p = urlparse(u.strip())
    netloc = (p.netloc or "").lower()
    if netloc.startswith("www."): netloc = netloc[4:]
    path = (p.path or "").rstrip("/")
    return urlunparse(("", netloc, path, "", "", "")).lower()

def build_site_query(url: str) -> str:
    """Формирование site: оператора"""
    p = urlparse(url.strip())
    host = (p.netloc or "").lower()
    if host.startswith("www."): host = host[4:]
    path = (p.path or "").strip().lstrip("/").rstrip("/")
    return f"site:{host}" if path in ("", "/") else f"site:{host}/{path}"

def match_indexed(original_url: str, items):
    """Поиск совпадений в выдаче"""
    orig = norm_url(original_url)
    for it in items:
        if it.get("type") == "organic":
            u = it.get("url")
            if u and norm_url(u) == orig: return True
    return False

def parse_excel_urls(uploaded_file):
    """Парсинг Excel (колонка B)"""
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

def parse_text_urls(text_input):
    """Парсинг ссылок из текстового поля"""
    urls = []
    if not text_input:
        return urls
    lines = text_input.split('\n')
    for line in lines:
        line = line.strip()
        if line and (line.startswith("http://") or line.startswith("https://")):
            urls.append(line)
    return urls

# -----------------------
# Логика проверки (Engine)
# -----------------------
def run_check(links_data):
    """Запуск проверки для переданного списка словарей [{'id':..., 'url':...}]"""
    if not links_data: return
    
    session = init_requests()
    host = st.secrets["dataforseo"].get("host", "api.dataforseo.com").replace("https://", "")
    base_url = f"https://{host}"
    
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    
    payload = []
    tasks_map = {} 
    
    # 1. Формируем Payload
    for item in links_data:
        payload.append({
            "location_code": 2840,
            "language_code": "en",
            "depth": 10,
            "keyword": build_site_query(item['url'])
        })

    # 2. Обрабатываем пачками (Batching)
    BATCH_SIZE = 50
    total = len(links_data)
    processed_count = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch_links = links_data[i : i + BATCH_SIZE]
        batch_payload = payload[i : i + BATCH_SIZE]
        
        status_text.write(f"📤 Обработка {i+1}-{min(i+BATCH_SIZE, total)} из {total}...")
        
        try:
            # POST
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

                # Wait
                time.sleep(2)
                status_text.write("⏳ Анализ результатов...")
                
                # GET Results
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
                            
                            # UPDATE DB
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
# САЙДБАР
# -----------------------
with st.sidebar:
    st.title("🗂 Меню")
    
    if st.button("🏠 На главную (Дашборд)", use_container_width=True):
        st.session_state.selected_project_id = None
        st.rerun()
    
    st.divider()
    
    st.subheader("Мои Проекты")
    # Создание проекта
    with st.expander("➕ Новый проект"):
        new_proj = st.text_input("Название")
        if st.button("Создать"):
            if new_proj:
                supabase.table("projects").insert({"name": new_proj}).execute()
                st.rerun()

    # Список проектов
    response = supabase.table("projects").select("*").order("created_at", desc=True).execute()
    projects = response.data
    
    if "selected_project_id" not in st.session_state:
        st.session_state.selected_project_id = None

    if projects:
        for p in projects:
            is_active = (st.session_state.selected_project_id == p['id'])
            # Делаем активную кнопку другого цвета
            type_btn = "primary" if is_active else "secondary"
            label = f"📂 {p['name']}"
            
            if st.button(label, key=f"proj_{p['id']}", use_container_width=True, type=type_btn):
                st.session_state.selected_project_id = p['id']
                st.rerun()
    
    # Удаление проекта
    if st.session_state.selected_project_id:
        st.divider()
        with st.expander("⚙️ Настройки проекта"):
            st.caption("Опасная зона")
            if st.button("🗑 Удалить этот проект", type="primary"):
                try:
                    supabase.table("projects").delete().eq("id", st.session_state.selected_project_id).execute()
                    st.session_state.selected_project_id = None
                    st.success("Проект удален!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Ошибка удаления: {e}")

    # Выход
    st.write("")
    st.write("")
    st.divider()
    if st.button("🚪 Выйти", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()

# -----------------------
# ЛОГИКА ЭКРАНОВ
# -----------------------

# 1. ЭКРАН ПРОЕКТА (Детальный вид)
if st.session_state.selected_project_id:
    current_proj = next((p for p in projects if p['id'] == st.session_state.selected_project_id), None)
    if not current_proj:
        st.session_state.selected_project_id = None
        st.rerun()
        
    st.title(f"📂 {current_proj['name']}")
    
    # Грузим данные
    res = supabase.table("links").select("*").eq("project_id", st.session_state.selected_project_id).order("id", desc=False).execute()
    df = pd.DataFrame(res.data)

    if not df.empty:
        # Считаем метрики
        total = len(df)
        indexed = len(df[df['is_indexed'] == True])
        not_indexed = len(df[df['is_indexed'] == False])
        pending = len(df[df['status'] == 'pending'])
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Всего", total)
        c2.metric("В индексе", f"{indexed} ({(indexed/total*100):.1f}%)")
        c3.metric("Очередь", pending)
        
        with c4:
            if pending > 0:
                if st.button("🚀 Проверить очередь", type="primary"):
                    to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                    run_check(to_check)
            else:
                if st.button("🔄 Перепроверить всё"):
                    supabase.table("links").update({"status": "pending", "is_indexed": None}).eq("project_id", st.session_state.selected_project_id).execute()
                    st.rerun()
        
        st.divider()

        # Фильтры и Экспорт
        col_filter, col_export = st.columns([4, 1])
        with col_filter:
            filter_option = st.radio(
                "Фильтр:",
                [f"Все ({total})", f"✅ В индексе ({indexed})", f"❌ Не в индексе ({not_indexed})", f"⏳ Ожидание/Ошибки ({pending})"],
                horizontal=True,
                label_visibility="collapsed"
            )
            
            # Применяем фильтр
            if "✅" in filter_option: df_view = df[df['is_indexed'] == True]
            elif "❌" in filter_option: df_view = df[df['is_indexed'] == False]
            elif "⏳" in filter_option: df_view = df[df['status'].isin(['pending', 'error'])]
            else: df_view = df

        with col_export:
            excel_data = to_excel(df[['url', 'is_indexed', 'status', 'last_check']])
            st.download_button("📥 Скачать отчет", excel_data, f"report_{current_proj['name']}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        
        # --- ТАБЛИЦА С МУЛЬТИ-ВЫБОРОМ ---
        st.write("") 
        selection = st.dataframe(
            df_view[['url', 'status', 'is_indexed', 'last_check', 'created_at']], 
            use_container_width=True,
            on_select="rerun", 
            selection_mode="multi-row", # Включаем галочки
            column_config={
                "is_indexed": st.column_config.CheckboxColumn("Index?", disabled=True),
                "url": st.column_config.LinkColumn("URL"),
                "last_check": st.column_config.DatetimeColumn("Дата проверки", format="D MMM YYYY, HH:mm")
            }
        )
        
        # === ДЕЙСТВИЯ С ВЫДЕЛЕННЫМ ===
        if len(selection.selection.rows) > 0:
            selected_indices = selection.selection.rows
            # Получаем реальные ID выделенных строк
            selected_ids = df_view.iloc[selected_indices]['id'].tolist()
            count = len(selected_ids)
            
            st.info(f"Выбрано элементов: {count}")
            
            b_col1, b_col2 = st.columns([1, 1])
            with b_col1:
                # 🚀 Кнопка выборочной проверки
                if st.button(f"🚀 Проверить выбранные ({count})", type="primary", use_container_width=True):
                    # Сброс статуса
                    supabase.table("links").update({"status": "pending", "is_indexed": None}).in_("id", selected_ids).execute()
                    # Получаем данные URL для проверки
                    selected_records = df_view.iloc[selected_indices][['id', 'url']].to_dict('records')
                    run_check(selected_records)

            with b_col2:
                # 🗑 Кнопка удаления
                if st.button(f"🗑 Удалить выбранные ({count})", type="secondary", use_container_width=True):
                    supabase.table("links").delete().in_("id", selected_ids).execute()
                    st.success("Удалено!")
                    time.sleep(1)
                    st.rerun()

    else:
        st.info("В папке пусто.")
    
    # === БЛОК ЗАГРУЗКИ ССЫЛОК (Excel + Текст) ===
    st.write("---")
    st.caption("Добавить ссылки")
    
    # Создаем вкладки
    tab1, tab2 = st.tabs(["📁 Загрузить Excel", "📝 Вставить списком (Текст)"])
    
    with tab1:
        uploaded = st.file_uploader("Файл .xlsx (ссылки в колонке B)", type=["xlsx"])
        if uploaded and st.button("💾 Сохранить Excel"):
            urls = parse_excel_urls(uploaded)
            if urls:
                data = [{"project_id": st.session_state.selected_project_id, "url": u, "status": "pending"} for u in urls]
                batch_size = 1000
                bar = st.progress(0)
                for i in range(0, len(data), batch_size):
                    supabase.table("links").insert(data[i:i+batch_size]).execute()
                    bar.progress(min((i+batch_size)/len(data), 1.0))
                st.success(f"Добавлено {len(urls)} ссылок")
                time.sleep(1)
                st.rerun()
    
    with tab2:
        # Текстовое поле для ручного ввода
        text_input = st.text_area("Вставьте ссылки (каждая с новой строки):", height=150, placeholder="https://site.com/page1\nhttps://site.com/page2")
        if st.button("💾 Сохранить список"):
            urls = parse_text_urls(text_input)
            if urls:
                data = [{"project_id": st.session_state.selected_project_id, "url": u, "status": "pending"} for u in urls]
                # Сохраняем пачками
                batch_size = 1000
                for i in range(0, len(data), batch_size):
                    supabase.table("links").insert(data[i:i+batch_size]).execute()
                
                st.success(f"Добавлено {len(urls)} ссылок из текста")
                time.sleep(1)
                st.rerun()
            else:
                if text_input: st.warning("Не найдено корректных ссылок (должны начинаться с http/https)")

# 2. ГЛАВНЫЙ ДАШБОРД (Обзор всех проектов)
else:
    st.title("📊 Дашборд мониторинга")
    
    # Грузим сводные данные
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
                last_date = pd.to_datetime(p_links['last_check']).max() if not p_links['last_check'].isna().all() else None
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
        
        m1, m2 = st.columns([3, 1])
        m1.metric("Всего проектов", len(projects))
        m2.metric("Всего задач в очереди", global_pending_count)
        
        # Кнопка глобального запуска
        if global_pending_count > 0:
            st.warning(f"Найдено {global_pending_count} ссылок ожидающих проверки во всех папках.")
            if st.button(f"🚀 ЗАПУСТИТЬ ВСЕ ({global_pending_count} шт.)", type="primary", use_container_width=True):
                pending_all = all_links_df[all_links_df['status'] == 'pending'][['id', 'url']].to_dict('records')
                run_check(pending_all)
        else:
            st.success("Все ссылки проверены! Очередь пуста.")
            
        st.subheader("Сводная таблица")
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
    else:
        st.info("Создайте первый проект в меню слева!")

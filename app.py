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
st.set_page_config(page_title="SEO Index Manager PRO", layout="wide")

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
# Хелперы Slack
# -----------------------
def send_slack_file(file_bytes, filename, message):
    try:
        if "slack" in st.secrets:
            token = st.secrets["slack"].get("bot_token")
            channel = st.secrets["slack"].get("channel_id")
            
            if not token or not channel:
                st.error("❌ Ошибка настройки Slack")
                return

            client = WebClient(token=token)
            client.files_upload_v2(
                channel=channel,
                file=file_bytes,
                filename=filename,
                title=filename,
                initial_comment=message
            )
            st.success("✅ Отчет успешно отправлен в Slack!")
        else:
            st.warning("⚠️ Секция [slack] не найдена.")
    except SlackApiError as e:
        st.error(f"❌ Ошибка Slack API: {e.response['error']}")
    except Exception as e:
        st.error(f"❌ Общая ошибка отправки: {e}")

# -----------------------
# Хелперы Excel (НОВАЯ ЛОГИКА)
# -----------------------
def generate_project_report(project_id, project_name):
    """
    Генерирует Excel, где каждая ПАПКА = отдельный ЛИСТ.
    Ссылки без папки попадают на лист "General".
    """
    output = BytesIO()
    
    # 1. Получаем все ссылки проекта
    links_res = supabase.table("links").select("*").eq("project_id", project_id).execute()
    df_links = pd.DataFrame(links_res.data)
    
    # 2. Получаем все папки проекта
    folders_res = supabase.table("folders").select("*").eq("project_id", project_id).execute()
    df_folders = pd.DataFrame(folders_res.data)
    
    if df_links.empty:
        return None

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # --- ЛИСТ 1: Сводка (Dashboard) ---
        summary_data = []
        if not df_folders.empty and not df_links.empty:
             for index, folder in df_folders.iterrows():
                 f_links = df_links[df_links['folder_id'] == folder['id']]
                 total = len(f_links)
                 indexed = len(f_links[f_links['is_indexed'] == True])
                 summary_data.append({
                     "Папка": folder['name'],
                     "Всего": total,
                     "В индексе": indexed,
                     "%": f"{(indexed/total*100):.1f}%" if total > 0 else "0%"
                 })
             # Ссылки без папки
             gen_links = df_links[df_links['folder_id'].isnull()]
             if not gen_links.empty:
                 total = len(gen_links)
                 indexed = len(gen_links[gen_links['is_indexed'] == True])
                 summary_data.append({"Папка": "General (Без папки)", "Всего": total, "В индексе": indexed, "%": f"{(indexed/total*100):.1f}%" if total > 0 else "0%"})
             
             pd.DataFrame(summary_data).to_excel(writer, sheet_name="SUMMARY", index=False)

        # --- ЛИСТЫ ПО ПАПКАМ ---
        # 1. Ссылки с папками
        if not df_folders.empty:
            for index, folder in df_folders.iterrows():
                # Фильтруем ссылки этой папки
                sub_df = df_links[df_links['folder_id'] == folder['id']]
                
                # Имя листа (очистка от спецсимволов)
                sheet_name = "".join(c for c in folder['name'] if c.isalnum() or c in (' ', '_', '-'))[:30]
                if not sheet_name: sheet_name = f"Folder_{folder['id']}"
                
                if not sub_df.empty:
                    sub_df[['url', 'status', 'is_indexed', 'last_check']].to_excel(writer, index=False, sheet_name=sheet_name)
                else:
                    # Создаем пустой лист, чтобы структура сохранилась
                    pd.DataFrame({'Info': ['Нет ссылок']}).to_excel(writer, index=False, sheet_name=sheet_name)
        
        # 2. Ссылки БЕЗ папки (General)
        general_df = df_links[df_links['folder_id'].isnull()]
        if not general_df.empty:
            general_df[['url', 'status', 'is_indexed', 'last_check']].to_excel(writer, index=False, sheet_name="General")

    return output.getvalue()

def to_simple_excel(df):
    """Простой экспорт текущей таблицы"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='List View')
    return output.getvalue()

# -----------------------
# Хелперы Парсинга
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

def parse_excel_with_folders(uploaded_file):
    """
    Парсит Excel. 
    Колонка A = URL
    Колонка B = Название папки (Опционально)
    Возвращает список словарей: [{'url': '...', 'folder_name': '...'}]
    """
    data_list = []
    wb = load_workbook(BytesIO(uploaded_file.getvalue()), read_only=True)
    
    # Ищем первый лист
    ws = wb.worksheets[0]
    
    # Ищем заголовок URL (обычно 1 строка, но вдруг смещена)
    # Предполагаем, что данные начинаются со 2 строки, Col A=URL, Col B=Folder
    for r in range(1, ws.max_row + 1):
        url_val = ws.cell(row=r, column=1).value # A
        folder_val = ws.cell(row=r, column=2).value # B
        
        if url_val and isinstance(url_val, str) and (url_val.startswith("http://") or url_val.startswith("https://")):
            folder_name = str(folder_val).strip() if folder_val else None
            data_list.append({
                "url": url_val.strip(),
                "folder_name": folder_name
            })
            
    return data_list

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
# Логика проверки (Engine)
# -----------------------
def run_check(links_data, project_id=None, project_name="Unknown"):
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
            "location_code": 2840,
            "language_code": "en",
            "depth": 10,
            "keyword": build_site_query(item['url'])
        })

    BATCH_SIZE = 50
    total = len(links_data)
    processed_count = 0
    count_indexed = 0
    count_not_indexed = 0
    
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
                if not batch_task_ids: 
                    processed_count += len(batch_links)
                    continue
                
                time.sleep(2)
                status_text.write("⏳ Анализ результатов...")
                
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
                            if is_ind: count_indexed += 1
                            else: count_not_indexed += 1
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
        
        # Задержка для безопасности (Anti-Fraud)
        time.sleep(1.5)

    # === ОТПРАВКА СТРУКТУРИРОВАННОГО ОТЧЕТА ===
    status_text.write("📊 Формирование отчета по папкам...")
    
    # Генерируем отчет с вкладками
    excel_bytes = generate_project_report(project_id, project_name)
    
    if excel_bytes:
        date_str = datetime.now().strftime('%Y-%m-%d')
        # Имя файла безопасное
        safe_proj_name = "".join(c for c in project_name if c.isalnum() or c in (' ', '_', '-'))[:20]
        fname = f"Report_{safe_proj_name}_{date_str}.xlsx"
        
        msg = f"✅ *Проверка завершена!*\n📂 Проект: {project_name}\n🔗 Всего проверено: {total}"
        send_slack_file(excel_bytes, fname, msg)
    else:
        st.error("Не удалось сформировать данные.")
        
    time.sleep(2)
    st.rerun()

# -----------------------
# САЙДБАР
# -----------------------
with st.sidebar:
    st.title("🗂 Меню проектов")
    
    if st.button("🏠 Домой (Все проекты)", use_container_width=True):
        st.session_state.selected_project_id = None
        st.rerun()
    
    st.divider()
    
    st.subheader("Мои Проекты")
    # Создание проекта
    with st.expander("➕ Создать Проект"):
        new_proj = st.text_input("Имя проекта (напр. Zoome AU)")
        if st.button("Создать Проект"):
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
            type_btn = "primary" if is_active else "secondary"
            # Иконка
            if st.button(f"📂 {p['name']}", key=f"proj_{p['id']}", use_container_width=True, type=type_btn):
                st.session_state.selected_project_id = p['id']
                st.rerun()
    
    # Удаление проекта
    if st.session_state.selected_project_id:
        st.divider()
        with st.expander("🗑 Удалить проект"):
            st.warning("Удалятся все папки и ссылки внутри!")
            if st.button("Подтвердить удаление", type="primary"):
                supabase.table("projects").delete().eq("id", st.session_state.selected_project_id).execute()
                st.session_state.selected_project_id = None
                st.rerun()

# -----------------------
# ЛОГИКА ЭКРАНОВ
# -----------------------

# === ЭКРАН ПРОЕКТА (С ПАПКАМИ) ===
if st.session_state.selected_project_id:
    current_proj = next((p for p in projects if p['id'] == st.session_state.selected_project_id), None)
    if not current_proj:
        st.session_state.selected_project_id = None
        st.rerun()
        
    st.title(f"📂 Проект: {current_proj['name']}")
    
    # 1. Получаем ПАПКИ этого проекта
    folders_res = supabase.table("folders").select("*").eq("project_id", current_proj['id']).order("created_at", desc=False).execute()
    folders = folders_res.data
    
    # Создаем словарь {id: name} для удобства
    folder_map = {f['id']: f['name'] for f in folders}
    folder_map[None] = "General (Без папки)" # Для ссылок без папки
    
    # 2. Управление папками
    col_new_folder, col_stats = st.columns([1, 2])
    with col_new_folder:
        with st.popover("➕ Добавить подпапку"):
            new_folder_name = st.text_input("Название подпапки (напр. GP zoome17)")
            if st.button("Сохранить папку"):
                if new_folder_name:
                    supabase.table("folders").insert({"name": new_folder_name, "project_id": current_proj['id']}).execute()
                    st.rerun()
    
    # 3. Получаем ВСЕ ссылки проекта
    links_res = supabase.table("links").select("*").eq("project_id", current_proj['id']).execute()
    df = pd.DataFrame(links_res.data)

    # --- ТАБЫ: СВОДКА | СПИСОК | ЗАГРУЗКА ---
    tab_dashboard, tab_list, tab_upload = st.tabs(["📊 Сводка", "📝 Список ссылок", "📥 Загрузить"])

    # --- TAB 1: DASHBOARD ---
    with tab_dashboard:
        if not df.empty:
            total_all = len(df)
            pending_all = len(df[df['status'] == 'pending'])
            
            # Кнопка глобального запуска
            if pending_all > 0:
                st.info(f"В очереди на проверку: {pending_all} ссылок (во всех папках).")
                if st.button(f"🚀 ЗАПУСТИТЬ ПРОВЕРКУ ({pending_all} шт)", type="primary"):
                     to_check = df[df['status'] == 'pending'][['id', 'url']].to_dict('records')
                     run_check(to_check, project_id=current_proj['id'], project_name=current_proj['name'])
            
            # Кнопка перепроверки всего
            st.write("")
            if st.button(f"🔄 Сбросить и перепроверить ВЕСЬ ПРОЕКТ ({total_all} шт)", type="secondary"):
                supabase.table("links").update({"status": "pending", "is_indexed": None}).eq("project_id", current_proj['id']).execute()
                to_check = df[['id', 'url']].to_dict('records')
                run_check(to_check, project_id=current_proj['id'], project_name=current_proj['name'])

            st.divider()
            
            # Статистика по папкам
            st.subheader("Статистика по папкам")
            stats_data = []
            
            # Считаем для созданных папок
            for f in folders:
                f_links = df[df['folder_id'] == f['id']]
                tot = len(f_links)
                ind = len(f_links[f_links['is_indexed'] == True])
                stats_data.append({
                    "Папка": f['name'],
                    "Ссылок": tot,
                    "В индексе": ind,
                    "%": f"{(ind/tot*100):.0f}%" if tot > 0 else "-"
                })
            
            # Считаем для General
            gen_links = df[df['folder_id'].isnull()]
            if not gen_links.empty:
                tot = len(gen_links)
                ind = len(gen_links[gen_links['is_indexed'] == True])
                stats_data.append({"Папка": "General (Без папки)", "Ссылок": tot, "В индексе": ind, "%": f"{(ind/tot*100):.0f}%" if tot > 0 else "-"})
            
            st.dataframe(pd.DataFrame(stats_data), use_container_width=True, hide_index=True)

    # --- TAB 2: СПИСОК ---
    with tab_list:
        if not df.empty:
            # Фильтр по папке
            folder_options = ["Все"] + [f['name'] for f in folders] + ["General (Без папки)"]
            selected_folder_filter = st.selectbox("Фильтр по папке:", folder_options)
            
            df_view = df.copy()
            # Маппинг ID папки в Имя для красивой таблицы
            df_view['folder_name'] = df_view['folder_id'].map(folder_map)
            
            if selected_folder_filter != "Все":
                if selected_folder_filter == "General (Без папки)":
                    df_view = df_view[df_view['folder_id'].isnull()]
                else:
                    # Ищем ID папки по имени
                    fid = next((f['id'] for f in folders if f['name'] == selected_folder_filter), None)
                    if fid:
                        df_view = df_view[df_view['folder_id'] == fid]

            # Таблица
            selection = st.dataframe(
                df_view[['url', 'folder_name', 'status', 'is_indexed', 'last_check']],
                use_container_width=True,
                on_select="rerun",
                selection_mode="multi-row",
                column_config={
                    "is_indexed": st.column_config.CheckboxColumn("Index?", disabled=True),
                    "url": st.column_config.LinkColumn("URL"),
                    "folder_name": "Папка"
                }
            )
            
            # Действия
            if len(selection.selection.rows) > 0:
                selected_ids = df_view.iloc[selection.selection.rows]['id'].tolist()
                st.info(f"Выбрано: {len(selected_ids)}")
                if st.button("🗑 Удалить выбранные"):
                    supabase.table("links").delete().in_("id", selected_ids).execute()
                    st.rerun()

    # --- TAB 3: ЗАГРУЗКА ---
    with tab_upload:
        st.info("💡 Если вы загружаете Excel, используйте **Колонку A** для ссылок и **Колонку B** для названия папки. Если папки нет, она создастся автоматически.")
        
        # 1. EXCEL
        uploaded = st.file_uploader("Загрузить Excel (Col A: Url, Col B: Folder)", type=["xlsx"])
        if uploaded and st.button("💾 Обработать Excel"):
            parsed_data = parse_excel_with_folders(uploaded) # Возвращает [{'url':.., 'folder_name':..}]
            
            if parsed_data:
                # 1. Сначала найдем или создадим все уникальные папки из файла
                unique_folders = set(d['folder_name'] for d in parsed_data if d['folder_name'])
                folder_id_map = {f['name']: f['id'] for f in folders} # Текущие папки {name: id}
                
                # Создаем новые папки
                for fname in unique_folders:
                    if fname not in folder_id_map:
                        res = supabase.table("folders").insert({"name": fname, "project_id": current_proj['id']}).execute()
                        if res.data:
                            folder_id_map[fname] = res.data[0]['id']
                
                # 2. Готовим данные для вставки ссылок
                insert_rows = []
                for item in parsed_data:
                    fid = folder_id_map.get(item['folder_name']) if item['folder_name'] else None
                    insert_rows.append({
                        "project_id": current_proj['id'],
                        "url": item['url'],
                        "folder_id": fid,
                        "status": "pending"
                    })
                
                # 3. Вставляем пачками
                batch_size = 500
                bar = st.progress(0)
                for i in range(0, len(insert_rows), batch_size):
                    supabase.table("links").insert(insert_rows[i:i+batch_size]).execute()
                    bar.progress(min((i+batch_size)/len(insert_rows), 1.0))
                
                st.success(f"✅ Загружено {len(insert_rows)} ссылок!")
                time.sleep(1)
                st.rerun()

        st.divider()
        
        # 2. ТЕКСТ (С выбором папки)
        st.write("Ручной ввод:")
        target_folder = st.selectbox("В какую папку добавить?", ["General (Без папки)"] + [f['name'] for f in folders])
        text_input = st.text_area("Список ссылок:", height=100)
        
        if st.button("💾 Сохранить список"):
            urls = parse_text_urls(text_input)
            if urls:
                # Определяем ID папки
                target_fid = None
                if target_folder != "General (Без папки)":
                    target_fid = next((f['id'] for f in folders if f['name'] == target_folder), None)
                
                data = [{"project_id": current_proj['id'], "url": u, "folder_id": target_fid, "status": "pending"} for u in urls]
                supabase.table("links").insert(data).execute()
                st.success(f"Добавлено {len(urls)} ссылок в '{target_folder}'")
                time.sleep(1)
                st.rerun()

# === ГЛАВНЫЙ ДАШБОРД (ВСЕ ПРОЕКТЫ) ===
else:
    st.title("📊 Обзор проектов")
    
    # Краткая статистика по всем проектам
    if projects:
        # Получаем данные скопом, чтобы не делать 100 запросов
        all_stats = []
        all_links_res = supabase.table("links").select("project_id, status").execute()
        df_all = pd.DataFrame(all_links_res.data)
        
        total_pending_global = 0
        
        for p in projects:
            if not df_all.empty:
                p_links = df_all[df_all['project_id'] == p['id']]
                cnt = len(p_links)
                pnd = len(p_links[p_links['status'] == 'pending'])
            else:
                cnt = 0
                pnd = 0
            
            total_pending_global += pnd
            all_stats.append({
                "Проект": p['name'],
                "Ссылок": cnt,
                "В очереди": pnd
            })
            
        m1, m2 = st.columns(2)
        m1.metric("Всего проектов", len(projects))
        m2.metric("Очередь (Global)", total_pending_global)
        
        st.dataframe(pd.DataFrame(all_stats), use_container_width=True, hide_index=True)
        
    else:
        st.info("Создайте первый проект в меню слева")

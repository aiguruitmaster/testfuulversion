import streamlit as st
from supabase import create_client
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
import time

# -----------------------
# Конфигурация
# -----------------------
st.set_page_config(page_title="SEO Index Manager", layout="wide")

# Подключение к Supabase
@st.cache_resource
def init_connection():
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

try:
    supabase = init_connection()
except Exception as e:
    st.error(f"Ошибка подключения к БД: {e}")
    st.stop()

# -----------------------
# Вспомогательные функции
# -----------------------
def parse_excel_urls(uploaded_file):
    """
    Читает Excel файл, ищет ссылки в колонке B (как в твоем старом скрипте).
    Возвращает список URL.
    """
    urls = []
    wb = load_workbook(BytesIO(uploaded_file.getvalue()), read_only=True)
    
    for ws in wb.worksheets:
        # Ищем заголовок в первых 10 строках
        header_row = 1
        for r in range(1, 11):
            val = ws.cell(row=r, column=2).value # Колонка B
            if isinstance(val, str) and "referring page url" in val.lower():
                header_row = r
                break
        
        # Читаем данные
        for r in range(header_row + 1, ws.max_row + 1):
            val = ws.cell(row=r, column=2).value
            if val and isinstance(val, str) and (val.startswith("http://") or val.startswith("https://")):
                urls.append(val.strip())
                
    return urls

# -----------------------
# Сайдбар: Проекты
# -----------------------
with st.sidebar:
    st.title("🗂 Мои Проекты")
    
    # Создание проекта
    with st.expander("➕ Создать новую папку"):
        new_proj = st.text_input("Название папки")
        if st.button("Создать"):
            if new_proj:
                supabase.table("projects").insert({"name": new_proj}).execute()
                st.success(f"Создано: {new_proj}")
                time.sleep(1)
                st.rerun()

    st.divider()

    # Выбор проекта
    response = supabase.table("projects").select("*").order("created_at", desc=True).execute()
    projects = response.data
    
    selected_project_id = None
    if projects:
        opts = {p['name']: p['id'] for p in projects}
        p_name = st.selectbox("Активная папка:", list(opts.keys()))
        selected_project_id = opts[p_name]
    else:
        st.warning("Создайте первый проект!")

# -----------------------
# Основной экран
# -----------------------
if selected_project_id:
    # 1. Заголовок и статистика
    st.title(f"📂 {p_name}")
    
    # Получаем ссылки из БД для этого проекта
    # count='exact' позволяет узнать количество без скачивания всех данных сразу, но пока скачаем всё для таблицы
    res = supabase.table("links").select("*").eq("project_id", selected_project_id).execute()
    df = pd.DataFrame(res.data)

    # Метрики
    col1, col2, col3 = st.columns(3)
    total_links = len(df) if not df.empty else 0
    indexed_links = len(df[df['is_indexed'] == True]) if not df.empty else 0
    pending_links = len(df[df['status'] == 'pending']) if not df.empty else 0
    
    col1.metric("Всего ссылок", total_links)
    col2.metric("В индексе", indexed_links)
    col3.metric("Ожидают проверки", pending_links)

    st.divider()

    # 2. Загрузка новых ссылок
    with st.expander("📥 Добавить ссылки из Excel", expanded=(total_links == 0)):
        uploaded = st.file_uploader("Загрузить .xlsx (ссылки в колонке B)", type=["xlsx"])
        
        if uploaded and st.button("💾 Сохранить в базу"):
            with st.spinner("Читаем файл..."):
                urls = parse_excel_urls(uploaded)
            
            if not urls:
                st.error("Ссылки не найдены! Проверьте, что они в колонке B.")
            else:
                # Готовим данные для вставки
                data_to_insert = [
                    {"project_id": selected_project_id, "url": u, "status": "pending"} 
                    for u in urls
                ]
                
                # Вставляем пачками (batch), чтобы не было таймаутов на больших файлах
                batch_size = 1000
                progress_bar = st.progress(0)
                
                with st.spinner(f"Сохраняем {len(urls)} ссылок в облако..."):
                    for i in range(0, len(data_to_insert), batch_size):
                        batch = data_to_insert[i:i+batch_size]
                        supabase.table("links").insert(batch).execute()
                        progress_bar.progress(min((i + batch_size) / len(urls), 1.0))
                
                st.success(f"Успешно добавлено {len(urls)} ссылок!")
                time.sleep(1)
                st.rerun()

    # 3. Таблица данных
    st.subheader("Список ссылок")
    if not df.empty:
        # Показываем красивую таблицу, скрывая технические ID
        display_df = df[['url', 'status', 'is_indexed', 'last_check', 'created_at']].copy()
        st.dataframe(display_df, use_container_width=True, height=500)
    else:
        st.info("В этой папке пока пусто. Загрузите файл выше.")

else:
    st.write("Выберите проект слева.")

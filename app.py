import streamlit as st
from supabase import create_client, Client
import time

# -----------------------
# Конфигурация страницы
# -----------------------
st.set_page_config(page_title="SEO Index Manager", layout="wide")

# -----------------------
# Подключение к Supabase
# -----------------------
# Используем кэширование, чтобы не переподключаться при каждом клике
@st.cache_resource
def init_connection():
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

try:
    supabase = init_connection()
except Exception as e:
    st.error(f"Ошибка подключения к базе данных: {e}")
    st.stop()

# -----------------------
# Сайдбар: Управление проектами
# -----------------------
with st.sidebar:
    st.title("🗂 Мои Проекты")

    # 1. Создание нового проекта
    with st.expander("➕ Создать новую папку"):
        new_project_name = st.text_input("Название папки")
        if st.button("Создать"):
            if new_project_name:
                try:
                    # Вставляем строку в таблицу projects
                    supabase.table("projects").insert({"name": new_project_name}).execute()
                    st.success(f"Папка '{new_project_name}' создана!")
                    time.sleep(1)
                    st.rerun() # Перезагружаем страницу, чтобы обновить список
                except Exception as e:
                    st.error(f"Ошибка: {e}")
            else:
                st.warning("Введите название!")

    st.divider()

    # 2. Получение списка проектов из базы
    # Делаем SELECT * FROM projects ORDER BY created_at DESC
    response = supabase.table("projects").select("*").order("created_at", desc=True).execute()
    projects = response.data

    selected_project = None
    selected_project_id = None

    if projects:
        # Формируем список для выбора: "Название (ID)"
        project_options = {f"{p['name']}": p['id'] for p in projects}
        
        selected_name = st.selectbox(
            "Выберите активную папку:",
            options=list(project_options.keys())
        )
        selected_project_id = project_options[selected_name]
        selected_project_name = selected_name
    else:
        st.info("У вас пока нет папок. Создайте первую!")

# -----------------------
# Основная часть экрана
# -----------------------
if selected_project_id:
    st.title(f"📂 {selected_project_name}")
    st.caption(f"Project ID: {selected_project_id}")
    
    st.write("---")
    st.info("В следующем шаге здесь появится таблица со ссылками и кнопка загрузки Excel.")

else:
    st.title("Добро пожаловать в SEO Index Manager 👋")
    st.markdown("👈 **Выберите папку в меню слева** или создайте новую, чтобы начать работу.")

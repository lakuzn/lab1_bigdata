import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time

# Настройка 
st.set_page_config(page_title="Service Desk Dashboard", layout="wide")
st.title("📊 Real-Time Service Desk Analytics")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, '..', 'data', 'processed_tickets.csv')

# загрузка данных
def load_data():
    if os.path.exists(DATA_PATH):
        return pd.read_csv(DATA_PATH)
    return pd.DataFrame()

st_autorefresh = st.empty()

df = load_data()

if df.empty:
    st.warning("Ожидание данных из Kafka... Запустите producer.py и consumer.py")
else:
    # 1: Отслеживаемые метрики
    st.subheader("Ключевые показатели")
    col1, col2, col3 = st.columns(3)
    
    total_tickets = len(df)
    # Считаем точность
    accuracy = (df['priority'] == df['predicted_priority']).mean() * 100
    avg_priority = df['priority'].mean()
    
    col1.metric("Обработано заявок", total_tickets)
    col2.metric("Точность ML-модели", f"{accuracy:.1f}%")
    col3.metric("Средний приоритет", f"{avg_priority:.2f}")
    
    st.divider()
    
    # 2: Графики 
    st.subheader("Анализ потока данных")
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        # График 1: Распределение по отделам
        dept_counts = df['department'].value_counts().reset_index()
        dept_counts.columns = ['department', 'count']
        fig_dept = px.bar(dept_counts, x='department', y='count', 
                          title="Количество заявок по отделам", color='department')
        st.plotly_chart(fig_dept, width='stretch')
        
    with chart_col2:
        # График 2: Тональность обращений
        sent_counts = df['sentiment'].value_counts().reset_index()
        sent_counts.columns = ['sentiment', 'count']
        fig_sent = px.pie(sent_counts, names='sentiment', values='count', 
                          title="Тональность текстов заявок", hole=0.4)
        st.plotly_chart(fig_sent, width='stretch')

# Кнопка ручного обновления
if st.button("Обновить данные вручную"):
    st.rerun()

time.sleep(2)
st.rerun()
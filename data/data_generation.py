import pandas as pd
import numpy as np
from datetime import datetime

def generate_smart_service_desk_dataset(num_samples=400000):
    np.random.seed(42)
    ticket_ids = np.arange(1, num_samples + 1)
    
    # 1. Генерируем признаки
    departments = ['IT', 'HR', 'Бухгалтерия', 'Продажи']
    dept_col = np.random.choice(departments, num_samples, p=[0.3, 0.2, 0.25, 0.25])
    
    problem_types = ['Доступ и учетные записи', 'Оборудование', 'ПО и 1С', 'Сетевая инфраструктура']
    prob_col = np.random.choice(problem_types, num_samples, p=[0.3, 0.2, 0.3, 0.2])
    
    sentiments = ['Негативная', 'Нейтральная', 'Позитивная']
    sentiment_col = np.random.choice(sentiments, num_samples, p=[0.4, 0.5, 0.1])
    
    # 2. Вычисляем приоритет на основе логики
    priority = np.zeros(num_samples)
    
    # База от типа проблемы
    prob_mapping = {'Сетевая инфраструктура': 4, 'ПО и 1С': 3, 'Оборудование': 2, 'Доступ и учетные записи': 2}
    priority += np.vectorize(prob_mapping.get)(prob_col)
    
    # Модификатор от отдела
    dept_mapping = {'Бухгалтерия': 1, 'Продажи': 1, 'IT': 0, 'HR': 0}
    priority += np.vectorize(dept_mapping.get)(dept_col)
    
    # Модификатор от тональности
    sent_mapping = {'Негативная': 1, 'Нейтральная': 0, 'Позитивная': -1}
    priority += np.vectorize(sent_mapping.get)(sentiment_col)
    
    # Добавляем 20% рандомного шума
    noise = np.random.choice([-1, 0, 1], num_samples, p=[0.1, 0.8, 0.1])
    priority += noise
    
    # Ограничиваем рамками от 1 до 5
    priority = np.clip(priority, 1, 5).astype(int)
    
    # 3. Временные метки
    print("Генерация временных меток...")
    start_date = datetime(2025, 1, 1)
    minutes_offsets = np.random.randint(0, 500000, num_samples)
    timestamps = pd.to_datetime([start_date] * num_samples) + pd.to_timedelta(minutes_offsets, unit='m')
    
    # Собираем датафрейм
    df = pd.DataFrame({
        'ticket_id': ticket_ids,
        'department': dept_col,
        'problem_type': prob_col,
        'priority': priority,
        'sentiment': sentiment_col,
        'timestamp': timestamps
    })
    
    # Сортируем по времени
    df = df.sort_values(by='timestamp').reset_index(drop=True)
    df['ticket_id'] = np.arange(1, num_samples + 1)
    
    file_name = 'service_desk_tickets.csv'
    df.to_csv(file_name, index=False)
    print(f"Успешно! Умный датасет на {num_samples} строк сохранен в '{file_name}'")

if __name__ == "__main__":
    generate_smart_service_desk_dataset()
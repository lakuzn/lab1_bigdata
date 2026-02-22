import os
import json
import time
import joblib
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

class TicketConsumer:
    def __init__(self, bootstrap_servers, topic_name):
        self.topic_name = topic_name
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        notebooks_dir = os.path.join(base_dir, '..', 'notebooks')
        self.output_csv = os.path.join(base_dir, '..', 'data', 'processed_tickets.csv')
        
        print("Загрузка ML-моделей и энкодеров...")
        self.model = joblib.load(os.path.join(notebooks_dir, 'rf_model.pkl'))
        self.le_dept = joblib.load(os.path.join(notebooks_dir, 'le_dept.pkl'))
        self.le_prob = joblib.load(os.path.join(notebooks_dir, 'le_prob.pkl'))
        self.le_sent = joblib.load(os.path.join(notebooks_dir, 'le_sent.pkl'))

        print(f"Подключение к Kafka ({bootstrap_servers})...")
        while True:
            try:
                self.consumer = KafkaConsumer(
                    self.topic_name,
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                print("Консьюмер успешно подключился к Kafka.")
                break
            except NoBrokersAvailable:
                print("Kafka еще не готова. Повторная попытка через 5 секунд.")
                time.sleep(5)

    def start_listening(self):
        print(f"Слушаем топик '{self.topic_name}'. Ожидание сообщений...")
        
        for message in self.consumer:
            ticket = message.value
            
            # 1. Препроцессинг текста в числа
            dept_enc = self.le_dept.transform([ticket['department']])[0]
            prob_enc = self.le_prob.transform([ticket['problem_type']])[0]
            sent_enc = self.le_sent.transform([ticket['sentiment']])[0]
            
            # 2. Предсказание
            features_df = pd.DataFrame(
                [[dept_enc, prob_enc, sent_enc]], 
                columns=['dept_encoded', 'prob_encoded', 'sent_encoded']
            )
            pred_priority = self.model.predict(features_df)[0]
            
            print(f"Заявка #{ticket['ticket_id']} | Истинный приоритет: {ticket['priority']} -> ML предсказал: {pred_priority}")
            
            # 3. Сохранение результата для дэшборда
            result_row = ticket.copy()
            result_row['predicted_priority'] = int(pred_priority)
            
            df = pd.DataFrame([result_row])
            df.to_csv(self.output_csv, mode='a', header=not os.path.exists(self.output_csv), index=False)

if __name__ == "__main__":
    print("Ожидание запуска Kafka (15 секунд)...")
    time.sleep(15)
    
    KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    TOPIC = 'service_desk_tickets'
    
    desk_consumer = TicketConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        topic_name=TOPIC
    )
    desk_consumer.start_listening()
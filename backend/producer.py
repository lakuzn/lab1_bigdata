import os
import time
import json
import pandas as pd
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

class TicketProducer:
    def __init__(self, bootstrap_servers, topic_name, csv_path):
        self.topic_name = topic_name
        self.csv_path = csv_path
        
        print(f"Подключение к Kafka ({bootstrap_servers})...")
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("Продюсер успешно подключился к Kafka.")
                break
            except NoBrokersAvailable:
                print("Kafka еще не готова. Повторная попытка через 5 секунд.")
                time.sleep(5)

    def start_streaming(self):
        print(f"Загрузка данных из {self.csv_path}...")
        df = pd.read_csv(self.csv_path)
        
        print(f"Начинаем потоковую передачу в топик '{self.topic_name}'...")
        
        for index, row in df.iterrows():
            ticket_data = row.to_dict()
            self.producer.send(self.topic_name, ticket_data)
            print(f"Отправлена заявка #{ticket_data['ticket_id']} из отдела {ticket_data['department']}")
            time.sleep(random.uniform(0.1, 1.0))
            
        self.producer.flush()
        print("Передача данных завершена.")

if __name__ == "__main__":
    print("Ожидание запуска Kafka (15 секунд)...")
    time.sleep(15)
    
    KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    TOPIC = 'service_desk_tickets'
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CSV_FILE = os.path.join(BASE_DIR, '..', 'data', 'service_desk_tickets.csv')
    
    service_desk_producer = TicketProducer(
        bootstrap_servers=[KAFKA_BROKER],
        topic_name=TOPIC,
        csv_path=CSV_FILE
    )
    service_desk_producer.start_streaming()
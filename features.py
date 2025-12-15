import time
import random
from datetime import datetime
import pika
import json
import sys
import os
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification

# Создаём данные
X, y = make_classification(n_samples=10000, n_features=20, random_state=42)

# Загрузка модели
try:
    model = joblib.load('model_joblib.pkl')
    print("Модель загружена успешно!")
except Exception as e:
    print(f"Ошибка загрузки модели: {e}")
    # Создаем модель, если файл не найден
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    joblib.dump(model, 'model_joblib.pkl')
    print("Создана и сохранена новая модель!")

def create_connection():
    """Создание подключения к RabbitMQ с повторными попытками"""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('rabbitmq', heartbeat=600))
            channel = connection.channel()
            
            # Создаем очереди (ДОБАВЛЕНО - строки 33-35)
            channel.queue_declare(queue='features')
            channel.queue_declare(queue='y_true')
            channel.queue_declare(queue='y_pred')
            
            print("Подключение к RabbitMQ установлено успешно!")
            return connection, channel
            
        except pika.exceptions.AMQPConnectionError:
            print("Ошибка подключения к RabbitMQ. Повторная попытка через 5 секунд...")
            time.sleep(5)
        except Exception as e:
            print(f"Неожиданная ошибка: {e}. Повторная попытка через 5 секунд...")
            time.sleep(5)

def main():
    """Основная функция объединенного микросервиса"""
    print("Объединенный микросервис features запущен...")
    
    connection, channel = None, None
    
    try:
        while True:
            try:
                if connection is None or not connection.is_open:
                    connection, channel = create_connection()
                
                # Выбираем случайную строку
                random_row = random.randint(0, len(X) - 1)
                features = X[random_row].tolist()
                true_value = int(y[random_row])
                
                # Делаем предсказание
                prediction = model.predict([features])[0]
                
                # Создаем уникальный ID
                message_id = datetime.now().timestamp()
                
                # Формируем сообщения
                message_features = {
                    'id': message_id,
                    'features': features
                }
                
                message_y_true = {
                    'id': message_id,
                    'body': true_value
                }
                
                message_y_pred = {
                    'id': message_id,
                    'body': float(prediction)
                }
                
                # Отправляем сообщения (без persistent mode)
                channel.basic_publish(
                    exchange='',
                    routing_key='features',
                    body=json.dumps(message_features)
                )
                
                channel.basic_publish(
                    exchange='',
                    routing_key='y_true',
                    body=json.dumps(message_y_true)
                )
                
                channel.basic_publish(
                    exchange='',
                    routing_key='y_pred',
                    body=json.dumps(message_y_pred)
                )
                
                print(f"Отправлено сообщение ID: {message_id}")
                print(f"Признаки: {features[:5]}...")  # Показываем первые 5 признаков
                print(f"Истинное значение: {true_value}")
                print(f"Предсказание: {prediction}")
                print("-" * 50)
                
                # Задержка 10 секунд
                time.sleep(10)
                
            except pika.exceptions.ConnectionClosed:
                print("Потеряно соединение с RabbitMQ. Переподключение...")
                connection, channel = None, None
                time.sleep(5)
            except Exception as e:
                print(f"Ошибка: {e}")
                time.sleep(5)
                
    except KeyboardInterrupt:
        print("Остановка сервиса...")
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()

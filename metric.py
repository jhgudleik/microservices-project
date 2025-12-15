import pika
import json
import pandas as pd
import os
import csv
from datetime import datetime

# Подключение к RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

# Создаем очереди (ИСПРАВЛЕНО имена)
channel.queue_declare(queue='y_true')  # было y-true
channel.queue_declare(queue='y_pred')  # было y-pred

# Создаем директорию для метрик (ИСПРАВЛЕНО путь)
os.makedirs('/app/metrics', exist_ok=True)

# Файл для метрик (ИСПРАВЛЕНО путь и имя)
metrics_file = 'logs/metric_log.csv'

# Инициализируем файл с заголовками
if not os.path.exists(metrics_file):
    with open(metrics_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'y_true', 'y_pred', 'absolute_error'])

# Словари для хранения данных
true_values = {}
pred_values = {}
processed_ids = set()

print("Микросервис metric запущен и ожидает данных...")

def process_metric(message_id):
    """Обработка метрики когда есть оба значения"""
    if message_id in true_values and message_id in pred_values:
        y_true = true_values[message_id]
        y_pred = pred_values[message_id]
        absolute_error = abs(y_true - y_pred)
        
        # Записываем в файл с принудительной синхронизацией
        with open(metrics_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([message_id, y_true, y_pred, absolute_error])
            f.flush()  # Принудительно сбрасываем буфер
            os.fsync(f.fileno())  # Принудительно синхронизируем с диском
        
        print(f"Обработана метрика ID: {message_id}")
        print(f"y_true: {y_true}, y_pred: {y_pred}, AE: {absolute_error}")
        print("-" * 50)
        
        # Удаляем обработанные данные
        del true_values[message_id]
        del pred_values[message_id]
        processed_ids.add(message_id)

def callback_true(ch, method, properties, body):
    try:
        if not body:
            return
            
        data = json.loads(body)
        message_id = data['id']
        y_true = data['body']
        
        true_values[message_id] = y_true
        process_metric(message_id)
        
    except Exception as e:
        print(f"Ошибка обработки y_true: {e}")

def callback_pred(ch, method, properties, body):
    try:
        if not body:
            return
            
        data = json.loads(body)
        message_id = data['id']
        y_pred = data['body']
        
        pred_values[message_id] = y_pred
        process_metric(message_id)
        
    except Exception as e:
        print(f"Ошибка обработки y_pred: {e}")

# Подписываемся на очереди (ИСПРАВЛЕНО имена очередей)
channel.basic_consume(
    queue='y_true',  # ИСПРАВЛЕНО: y_true вместо y-true
    on_message_callback=callback_true,
    auto_ack=True
)

channel.basic_consume(
    queue='y_pred',  # ИСПРАВЛЕНО: y_pred вместо y-pred
    on_message_callback=callback_pred,
    auto_ack=True
)

print("Ожидание сообщений...")
channel.start_consuming()

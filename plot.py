import pandas as pd
import matplotlib.pyplot as plt
import time
import os

# Создаем директорию для логов
os.makedirs('logs', exist_ok=True)

log_file = 'logs/metric_log.csv'
output_file = 'logs/error_distribution.png'

print("Микросервис plot запущен и мониторит файл метрик...")

while True:
    try:
        if os.path.exists(log_file):
            # Читаем CSV файл
            df = pd.read_csv(log_file)
            
            if len(df) > 0:
                # Строим гистограмму абсолютных ошибок
                plt.figure(figsize=(10, 6))
                plt.hist(df['absolute_error'], bins=20, alpha=0.7, color='blue', edgecolor='black')
                plt.title('Распределение абсолютных ошибок модели')
                plt.xlabel('Абсолютная ошибка')
                plt.ylabel('Частота')
                plt.grid(True, alpha=0.3)
                
                # Сохраняем график
                plt.savefig(output_file)
                plt.close()
                
                print(f"Гистограмма обновлена. Обработано записей: {len(df)}")
                
        # Задержка перед следующей проверкой
        time.sleep(5)
        
    except Exception as e:
        print(f"Ошибка построения графика: {e}")
        time.sleep(10)

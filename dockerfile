FROM python:3.11-slim

WORKDIR /app

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ВСЕ необходимые скрипты И МОДЕЛЬ
COPY features.py .
COPY metric.py .
COPY plot.py .
COPY model_joblib.pkl .  

# Создаем директорию для логов
RUN mkdir -p logs

# Запускаем сервис (по умолчанию для plot)
CMD ["python", "plot.py"]

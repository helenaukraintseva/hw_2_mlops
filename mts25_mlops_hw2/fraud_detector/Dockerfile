FROM python:3.9-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода и модели
COPY . .

# Создание директории для логов
RUN mkdir -p /app/logs

# Запуск сервиса
CMD ["python", "app.py"]
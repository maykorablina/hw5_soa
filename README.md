# HW5 SOA

Сервис-ориентированная архитектура (Домашнее задание 5).

## Запуск проекта

1. Создайте виртуальное окружение:
   ```bash
   python -m venv .venv
   ```

2. Активируйте виртуальное окружение:
   - На Windows:
     ```bash
     .venv\Scripts\activate
     ```
   - На Linux/macOS:
     ```bash
     source .venv/bin/activate
     ```

3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

4. Запустите приложение:
   ```bash
   uvicorn main:app --reload
   ```

## Структура

- `main.py` - Точка входа в приложение (FastAPI)
- `requirements.txt` - Зависимости проекта

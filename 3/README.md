# Test Task 3

## Описание

Асинхронный API-клиент для работы с GitHub REST API и сохранением данных в ClickHouse.

## Запуск

1. **Клонируйте репозиторий и перейдите в папку проекта:**

    ```bash
    git clone <repo>
    cd test-task-tomilov/3
    ```

2. **Запустите ClickHouse и подготовьте таблицы: вариант через docker**
    
    #### Предварительно подготовьте .env файл в директории dockers/clickhouse на основе `example.env`

    ```bash
    cd dockers/clickhouse
    docker compose up --build -d
    ```

3. **Создайте файл `.env` на основе примера `example.env` в рут-директории и заполните необходимые переменные окружения:**

    ```bash
    cp example.env .env
    # Отредактируйте .env - заполните его данными подключения к GitHub
    ```

4. **Установите зависимости:**

    ```bash
    uv sync
    ```

    #### Или через Python
    ```bash
    python3 -m venv .venv
    python3 -m pip install .
    ```

5. **Запустите тесты:**

    ```bash
    uv run pytest .
    ```

    #### Или через обычный Python
    ```bash
    source .venv/bin/activate
    python3 -m pytest .
    ```

6. **Запустите проект:**

    ```bash
    uv run main.py
    ```

    #### Или через обычный Python
    ```bash
    source .venv/bin/activate
    python3 -m main
    ```

## Особенности
- Для работы необходим файл `.env` с токеном GitHub и данными о подключении к ClickHouse
- Используется aiohttp и clickhouse-connect.  

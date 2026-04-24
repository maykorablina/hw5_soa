# HW5 SOA — Online Cinema: Event Streaming + Analytics Pipeline

## Архитектура (1–4 балла)

```
Movie Service (Producer) → Kafka (movie-events) → ClickHouse (Kafka Engine) → movie_events table
                    ↕ Schema Registry (Avro)
```

## Запуск

```bash
docker-compose up --build
```

Все компоненты поднимаются одной командой. Топик создаётся автоматически через сервис `kafka-setup`.

## Сервисы

| Сервис          | URL                        |
|-----------------|----------------------------|
| Movie Service   | http://localhost:8000       |
| Swagger UI      | http://localhost:8000/docs  |
| Schema Registry | http://localhost:8081       |
| ClickHouse HTTP | http://localhost:8123       |

## Kafka Topic: `movie-events`

- **Партиций:** 3
- **Ключ партиционирования:** `user_id`
  - Гарантирует порядок событий для одного пользователя в пределах партиции
  - Позволяет строить user journey аналитику без cross-partition join
- **Формат схемы:** Avro (зарегистрирована в Schema Registry)

## Схема события (Avro)

Файл: `movie_service/schemas/movie_event.avsc`

| Поле              | Тип                  | Описание                          |
|-------------------|----------------------|-----------------------------------|
| event_id          | string (UUID)        | Уникальный идентификатор события  |
| user_id           | string               | Идентификатор пользователя        |
| movie_id          | string               | Идентификатор фильма              |
| event_type        | enum                 | VIEW_STARTED, VIEW_FINISHED, ...  |
| timestamp         | string (ISO 8601)    | Время события (UTC)               |
| device_type       | enum                 | MOBILE, DESKTOP, TV, TABLET       |
| session_id        | string               | Идентификатор сессии              |
| progress_seconds  | int (nullable)       | Прогресс просмотра в секундах     |

## API Movie Service

### POST /events
Публикует одно событие в Kafka.

```json
{
  "user_id": "user_123",
  "movie_id": "movie_456",
  "event_type": "VIEW_STARTED",
  "device_type": "DESKTOP",
  "session_id": "sess_789",
  "progress_seconds": 0
}
```

### POST /events/batch
Публикует список событий.

### GET /health
Health check.

## ClickHouse

Таблицы создаются автоматически при старте через `docker-entrypoint-initdb.d/init.sql`:

- `movie_events_queue` — Kafka Engine (читает из топика)
- `movie_events` — MergeTree (основное хранилище)
- `movie_events_mv` — Materialized View (перекладывает данные)

Проверить данные:
```sql
SELECT * FROM movie_events LIMIT 10;
```

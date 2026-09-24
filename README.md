
## Настройки Debezium-коннектора (`connector-config/postgres-connector.json`)

| Параметр | Значение | Назначение |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | используемый коннектор |
| `database.hostname/port/user/password/dbname` | `postgres`/`5432`/`postgres`/`postgres`/`inventory` | подключение к источнику |
| `topic.prefix` | `dbserver1` | префикс имён топиков: `dbserver1.public.users`, `dbserver1.public.orders` |
| `table.include.list` | `public.users,public.orders` | **отслеживаются только эти две таблицы** (требование задания) |
| `plugin.name` | `pgoutput` | встроенный в PostgreSQL 10+ плагин логического декодирования — не требует установки дополнительных расширений |
| `slot.name` | `debezium_slot` | имя слота логической репликации в PostgreSQL |
| `publication.autocreate.mode` | `filtered` | PUBLICATION создаётся автоматически только для таблиц из `table.include.list` |
| `schema.history.internal.kafka.*` | топик `schema-changes.inventory` | Debezium хранит историю схемы БД во внутреннем топике Kafka |
| `key/value.converter` | `JsonConverter`, `schemas.enable=false` | компактный JSON без встроенной schema-обвязки — удобнее читать глазами и консьюмером |
| `snapshot.mode` | `initial` | при первом запуске сделать снимок уже существующих строк (в т.ч. тестовых данных из `init.sql`), затем перейти на потоковое чтение WAL |

## Инструкция по запуску

1. Поднять весь стек:

   ```bash
   docker compose up -d --build
   ```

   Это поднимет Kafka → PostgreSQL (с автосозданием таблиц и тестовых
   данных) → Kafka Connect → автоматическую регистрацию коннектора
   (`connect-init`) → консьюмер → Prometheus → Grafana.

2. Проверить, что коннектор зарегистрировался:

   ```bash
   docker compose logs connect-init
   ```

   В конце лога должно быть `Коннектор успешно создан (HTTP 201)` (или
   `409`, если контейнер перезапускался повторно) и JSON со статусом
   коннектора.

## Проверка работоспособности решения (Задание 1)

### Шаг 1. Статус коннектора

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

Ожидается `"state": "RUNNING"` и у коннектора, и у его единственной
задачи (`tasks[0].state`).

### Шаг 2. Список созданных Kafka-топиков

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Среди прочих должны быть `dbserver1.public.users` и `dbserver1.public.orders`.

### Шаг 3. Данные в терминале 

```bash
docker compose logs -f cdc-consumer
```

Сразу после старта коннектора (`snapshot.mode=initial`) в логе должны
появиться события со `SNAPSHOT (первичное чтение)` для всех строк,
заранее вставленных в `init.sql` — 4 пользователя и 5 заказов.

### Шаг 4. Проверка потоковой передачи изменений (CDC "живьём")

Подключитесь к PostgreSQL и внесите изменения:

```bash
docker compose exec postgres psql -U postgres -d inventory
```

```sql
-- новый пользователь
INSERT INTO users (name, email) VALUES ('Carol White', 'carol@example.com');

-- новый заказ
INSERT INTO orders (user_id, product_name, quantity) VALUES (5, 'Product F', 7);

-- изменение существующей записи
UPDATE users SET name = 'Bob B. Brown' WHERE id = 4;

-- удаление записи
DELETE FROM orders WHERE id = 5;
```

В логе `docker compose logs -f cdc-consumer` должны практически сразу
появиться соответствующие события `INSERT`, `UPDATE`, `DELETE` с
полями `before`/`after`.

### Тестовые данные (уже загружаются автоматически через `init.sql`)

```sql
-- Пользователи
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');

-- Заказы
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);
```

Их не нужно вставлять вручную — они уже применяются автоматически при
первом старте контейнера `postgres` через `docker-entrypoint-initdb.d`.
Если нужно применить их заново с нуля (например, после `docker compose
down -v`), они просто выполнятся при следующем старте автоматически.

## Мониторинг и метрики (Задание 2)

### Проверка, что метрики вообще экспортируются

```bash
curl -s http://localhost:8081/metrics | head -50
```


Панели дашборда опираются на официально документированные JMX-атрибуты
Kafka Connect (`kafka.connect:type=connector-metrics`,
`kafka.connect:type=source-task-metrics`):
- `connector-running-task-count` / `connector-failed-task-count` —
  сколько тасков коннектора сейчас работает/упало.
- `source-record-write-total` — сколько записей коннектор записал в
  Kafka (используется `rate(...)` для скорости в записях/сек).
- `poll-batch-avg-time-ms` — среднее время одного цикла опроса задачи.

### Проверка Prometheus

```
http://localhost:9090/targets
```

Таргет `kafka-connect` должен быть в состоянии `UP`.

### Проверка Grafana

```
http://localhost:3000
```

Логин/пароль: `admin` / `admin` (задаётся переменными окружения
`GF_SECURITY_ADMIN_USER` / `GF_SECURITY_ADMIN_PASSWORD` в
docker-compose.yaml, при первом входе Grafana предложит сменить пароль —
для учебного стенда это можно пропустить).

Datasource "Prometheus" и дашборд "Kafka Connect / Debezium — метрики"
создаются автоматически через provisioning — искать и добавлять их
вручную не требуется.

## Остановка

```bash
docker compose down -v
```

`-v` дополнительно удаляет volume'ы (данные Postgres, Kafka, Prometheus,
Grafana) — полезно для чистого перезапуска эксперимента с нуля.


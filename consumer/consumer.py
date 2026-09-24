"""
Консьюмер для чтения CDC-событий Debezium из топиков Kafka и вывода их в
терминал (пункт 5 Задания 1).

Debezium публикует по одному топику на таблицу с именем
`<topic.prefix>.<schema>.<table>`. При table.include.list=public.users,
public.orders и topic.prefix=dbserver1 (см. connector-config/postgres-connector.json)
это будут топики:
    dbserver1.public.users
    dbserver1.public.orders

Формат value Debezium-события (при value.converter=JsonConverter,
schemas.enable=false) — плоский JSON с полями:
    op      — тип операции: "c" (create/insert), "u" (update),
              "d" (delete), "r" (read, снимок при первом старте)
    before  — состояние строки до изменения (для u/d), либо null
    after   — состояние строки после изменения (для c/u/r), либо null
    source  — метаданные источника (таблица, LSN, время события и т.д.)
    ts_ms   — время создания события коннектором
"""

import json
import os

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = os.environ.get("KAFKA_TOPICS", "dbserver1.public.users,dbserver1.public.orders").split(",")

OP_NAMES = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT (первичное чтение)",
}

CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "cdc-terminal-printer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}


def describe_event(topic: str, raw_value: bytes) -> str:
    """Форматирует одно CDC-событие Debezium для вывода в терминал."""
    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return f"[{topic}] Не удалось разобрать событие: {exc}. Сырые данные: {raw_value!r}"

    # Сообщения об изменении схемы истории (schema-changes) и "tombstone"
    # записи (value=None при удалении) сюда не попадают, т.к. мы читаем
    # только топики таблиц users/orders.
    op = event.get("op")
    op_name = OP_NAMES.get(op, f"неизвестная операция ({op})")
    before = event.get("before")
    after = event.get("after")

    lines = [f"[{topic}] Операция: {op_name}"]
    if before is not None:
        lines.append(f"  before: {before}")
    if after is not None:
        lines.append(f"  after:  {after}")
    return "\n".join(lines)


def run() -> None:
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe(TOPICS)

    print(f"Подписка на топики: {TOPICS}")
    print("Ожидаю CDC-события... (Ctrl+C для остановки)\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ОШИБКА] {msg.error()}")
                continue

            if msg.value() is None:
                # tombstone-запись Kafka Connect (используется при удалении
                # ключа для compacted-топиков) — пропускаем молча.
                continue

            print(describe_event(msg.topic(), msg.value()))
            print("-" * 60)

    except KeyboardInterrupt:
        print("\nОстановка по запросу пользователя...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()

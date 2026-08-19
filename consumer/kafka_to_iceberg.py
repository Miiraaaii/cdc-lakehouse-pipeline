import json

import pyarrow as pa
from confluent_kafka import Consumer
from pyiceberg.catalog import load_catalog

TOPICS = ["ecommerce.public.customers", "ecommerce.public.orders"]

TABLE_SCHEMAS = {
    "customers": ["customer_id", "full_name", "email", "created_at", "updated_at"],
    "orders": ["order_id", "customer_id", "status", "total_amount", "created_at", "updated_at"],
}

PRIMARY_KEYS = {"customers": "customer_id", "orders": "order_id"}


def get_catalog():
    return load_catalog(
        "rest",
        uri="http://localhost:8181",
        **{
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password123",
            "s3.path-style-access": "true",
        },
    )


def ensure_table(catalog, table_name):
    identifier = f"ecommerce.{table_name}"
    try:
        return catalog.load_table(identifier)
    except Exception:
        try:
            catalog.create_namespace("ecommerce")
        except Exception:
            pass
        fields = [pa.field(col, pa.string()) for col in TABLE_SCHEMAS[table_name]]
        return catalog.create_table(identifier, schema=pa.schema(fields))


def apply_change(catalog, table_name, op, key, after):
    table = ensure_table(catalog, table_name)
    pk = PRIMARY_KEYS[table_name]

    if op == "d":
        table.delete(f"{pk} == '{key}'")
        print(f"[DELETE] {table_name} pk={key}")
        return

    table.delete(f"{pk} == '{key}'")
    cols = TABLE_SCHEMAS[table_name]
    row = {c: str(after.get(c, "")) for c in cols}
    table.append(pa.table({c: [row[c]] for c in cols}))
    print(f"[{'INSERT' if op in ('c', 'r') else 'UPDATE'}] {table_name} pk={key}")


def main():
    consumer = Consumer(
        {"bootstrap.servers": "localhost:29092", "group.id": "iceberg-sink", "auto.offset.reset": "earliest"}
    )
    consumer.subscribe(TOPICS)
    catalog = get_catalog()

    print("Consumer iniciado. Aguardando eventos...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            table_name = msg.topic().split(".")[-1]
            payload = json.loads(msg.value())["payload"]
            op = payload.get("op")
            after = payload.get("after") or {}
            before = payload.get("before") or {}
            pk = PRIMARY_KEYS[table_name]
            key = str(after.get(pk) or before.get(pk))
            if op and key != "None":
                apply_change(catalog, table_name, op, key, after)
    except KeyboardInterrupt:
        print("\nEncerrado.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
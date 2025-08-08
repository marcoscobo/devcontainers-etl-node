import os, json, time, uuid, random
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# -------- Config --------
PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/erp_db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.events")
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "10"))
MAGNITUDE_ORDER = int(os.getenv("MAGNITUDE_ORDER", "10"))
REFRESH_CACHE_CYCLES = int(os.getenv("REFRESH_EVERY_CYCLES", "2"))

# -------- Helpers --------
def _json_serializer(v: dict) -> bytes:
    """Serializes a dictionary to a JSON-encoded UTF-8 byte string."""
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def _key_serializer(k: str) -> bytes:
    """Serializes a string key to UTF-8 bytes."""
    return k.encode("utf-8")

def ensure_topic(bootstrap: str, topic: str, partitions: int = 1, rf: int = 1):
    """Ensures that a Kafka topic exists, creating it if necessary."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="sales-producer-admin")
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
        print(f"✅ Kafka: topic '{topic}' created")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Kafka: topic '{topic}' already exists")
    except Exception as e:
        print(f"⚠️ Kafka: Could not create/verify topic '{topic}': {e}")

def load_catalog(engine: Engine):
    """Loads products (id, price) and customers (id) from Postgres."""
    with engine.connect() as conn:
        products = conn.execute(text("SELECT id, price FROM products")).all()
        customers = conn.execute(text("SELECT id FROM customers")).all()
    product_price = {int(p.id): float(p.price) for p in products}
    customer_ids = [int(c.id) for c in customers]
    return product_price, customer_ids

def price_with_noise(base: float) -> float:
    """Applies a small random variation to simulate discounts/taxes."""
    factor = random.uniform(0.9, 1.1)
    return round(base * factor, 2)

def random_qty() -> int:
    """Generates a random quantity between 1 and 4."""
    return random.randint(1, 4)

def build_event(product_id: int, unit_price: float, customer_id: int) -> dict:
    """Builds a sales event dictionary."""
    return {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "product_id": product_id,
        "qty": random_qty(),
        "unit_price": unit_price
    }

# -------- Main --------
def main():
    engine = create_engine(PG_URL, echo=False, future=True)
    # Wait for Postgres to be accessible
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    # Initial cache
    product_price, customer_ids = load_catalog(engine)
    if not product_price or not customer_ids:
        raise RuntimeError("Postgres is empty: products and customers are required to generate sales.")
    # Create kafka topic and producer
    ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=_key_serializer,
        value_serializer=_json_serializer,
        linger_ms=50,
        acks="all",
    )
    print(f"✅ Kafka: Producer started. Publishing to '{KAFKA_TOPIC}'...")
    # Insert events periodically
    try:
        i = 0
        while True:
            # Periodic refresh of catalog/customers
            if i % REFRESH_CACHE_CYCLES == 0:
                pp_new, cust_new = load_catalog(engine)
                if pp_new:
                    product_price = pp_new
                if cust_new:
                    customer_ids = cust_new
                print(f"ℹ️ Kafka: Cache refreshed")
            # Create and publish random events
            n_events = random.randint(1, MAGNITUDE_ORDER)
            product_ids = list(product_price.keys())
            for _ in range(n_events):
                pid = random.choice(product_ids)
                base_price = product_price[pid]
                price = price_with_noise(base_price)
                cid = random.choice(customer_ids)
                evt = build_event(pid, price, cid)
                producer.send(KAFKA_TOPIC, key=evt["event_id"], value=evt)
            producer.flush()
            print(f"➕ New events: {n_events}")
            i += 1
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Kafka: Interrupted by user. Exiting...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
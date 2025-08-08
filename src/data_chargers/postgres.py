# erp_loader.py
import os, random, string, time
from sqlalchemy import create_engine, String, Integer, Numeric, DateTime, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from datetime import datetime
from typing import Optional

# ---------- Config ----------
PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/erp_db")
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "10"))
MAGNITUDE_ORDER = int(os.getenv("MAGNITUDE_ORDER", "10"))

# ---------- ORM ----------
class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sku: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    category: Mapped[str] = mapped_column(String(128))
    price: Mapped[Numeric] = mapped_column(Numeric(12, 2))
    currency: Mapped[str] = mapped_column(String(8), default="EUR")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class Customer(Base):
    __tablename__ = "customers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    segment: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

# ---------- Helpers ----------
def _rand_sku() -> str:
    """Generates a random SKU string for a product."""
    return "SKU-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

def _rand_external_id() -> str:
    """Generates a random external ID for a customer."""
    return "CUST-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

def _rand_product():
    """Generates a random product dictionary with category, name, price, etc."""
    categories = ["Electronics", "Home", "Sports", "Books", "Fashion"]
    names = {
        "Electronics": ["Smart Speaker", "Wireless Earbuds", "Action Camera", "Smartwatch"],
        "Home": ["Air Purifier", "Blender", "Robot Vacuum", "Espresso Maker"],
        "Sports": ["Yoga Mat", "Dumbbell Set", "Bike Helmet", "Running Shoes"],
        "Books": ["Data Engineering 101", "Modern APIs", "SecOps Guide", "ML in Production"],
        "Fashion": ["Hoodie", "Sneakers", "Jacket", "Backpack"],
    }
    cat = random.choice(categories)
    name = random.choice(names[cat])
    price = round(random.uniform(9.99, 299.99), 2)
    return {
        "sku": _rand_sku(),
        "name": name,
        "category": cat,
        "price": price,
        "currency": "EUR",
    }

def _rand_customer():
    """Generates a random customer dictionary with name, email, segment, etc."""
    segments = ["B2C", "B2B", "Enterprise", "Startups", "Education"]
    first = random.choice(["Alex", "Sam", "Taylor", "Jordan", "Charlie", "Drew", "Casey"])
    last = random.choice(["Lopez", "Garcia", "Martin", "Santos", "Prieto", "Ruiz"])
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}+{random.randint(1000,9999)}@devcontainers.com"
    return {
        "external_id": _rand_external_id(),
        "name": name,
        "email": email,
        "segment": random.choice(segments),
    }

def ensure_min_products(sess, min_count: int = 10):
    """Initial seeding: ensures a minimum catalog so the ERP makes sense."""
    count = sess.query(Product).count()
    to_create = max(0, min_count - count)
    for _ in range(to_create):
        sess.add(Product(**_rand_product()))
    if to_create:
        sess.commit()

# ---------- Main ----------
def main():
    engine = create_engine(PG_URL, echo=False, future=True)
    # Optional: wait for Postgres to be accessible
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    print("✅ Tables created/updated. Starting periodic inserts…")
    try:
        with Session() as sess:
            ensure_min_products(sess, min_count=25)

        i = 0
        while True:
            with Session() as sess:
                # 1) Insert 1..N random products (every 5 cycles)
                if i % 5 == 0:
                    n_new = random.randint(1, MAGNITUDE_ORDER // 2)
                    for _ in range(n_new):
                        sess.add(Product(**_rand_product()))
                    print(f"➕ New products: {n_new}")

                # 2) Insert customers continuously
                n_cust = random.randint(1, MAGNITUDE_ORDER)
                for _ in range(n_cust):
                    sess.add(Customer(**_rand_customer()))
                print(f"➕ New customers: {n_cust}")

                sess.commit()
            i += 1
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Exiting…")

if __name__ == "__main__":
    main()
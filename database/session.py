import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv(override=True)

# Build Connection String
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("DB_NAME", "crypto_arb")
DB_PORT = os.getenv("DB_PORT", "3306")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create Engine
engine = create_engine(DATABASE_URL, pool_recycle=3600)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency for getting DB session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def run_migrations(engine):
    """Add new columns to existing tables. Safe to call on every startup."""
    from sqlalchemy import text

    migrations = [
        "ALTER TABLE positions ADD COLUMN calc_entry_premium_ask FLOAT",
        "ALTER TABLE strategy_collector ADD COLUMN entry_premium_pct_ask FLOAT",
        "ALTER TABLE positions ADD COLUMN exit_reason VARCHAR(50)",
        # Capital event tracking — safe to run multiple times (IF NOT EXISTS)
        """CREATE TABLE IF NOT EXISTS capital_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            amount_krw FLOAT NOT NULL,
            amount_usdt FLOAT NOT NULL,
            fx_rate FLOAT NOT NULL,
            notes VARCHAR(200)
        )""",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  # Column already exists — ignore


def init_db():
    """Creates all tables defined in models.py"""
    # Import Base here to avoid circular imports
    from database.models import Base

    Base.metadata.create_all(bind=engine)
    run_migrations(engine)
    print("✅ Database tables initialized.")

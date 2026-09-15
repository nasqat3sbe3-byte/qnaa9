from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import settings


def normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


database_url = normalize_database_url(settings.database_url)
kwargs = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
engine = create_engine(database_url, pool_pre_ping=True, connect_args=kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def _migrate_existing_schema():
    """Small idempotent migrations for the existing Render database."""
    inspector = inspect(engine)
    if "stocks" not in inspector.get_table_names():
        return
    columns = {c["name"] for c in inspector.get_columns("stocks")}
    if "exchange" not in columns:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE stocks ADD COLUMN exchange VARCHAR(32)"))


def init_db():
    from . import models  # noqa: F401
    Base.metadata.create_all(engine)
    _migrate_existing_schema()

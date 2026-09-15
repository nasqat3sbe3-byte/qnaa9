from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import settings


def normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


database_url = normalize_database_url(settings.database_url)
kwargs = {"check_same_thread": False} if database_url.startswith("sqlite") else {
    "connect_timeout": 8,
}
engine = create_engine(database_url, pool_pre_ping=True, pool_timeout=10, connect_args=kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def init_db():
    from . import models  # noqa: F401
    # Keep startup bounded. create_all handles fresh databases; the ALTER is
    # idempotent on PostgreSQL and avoids slow schema introspection on Render.
    Base.metadata.create_all(engine)
    if not database_url.startswith("sqlite"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE stocks ADD COLUMN IF NOT EXISTS exchange VARCHAR(32)"))
    else:
        try:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE stocks ADD COLUMN exchange VARCHAR(32)"))
        except Exception:
            pass

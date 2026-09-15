from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.pool import NullPool
from .config import settings


def normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


database_url = normalize_database_url(settings.database_url)

# Render's free web service can have several long-running/background requests
# (borrow, prices, launch monitor) at the same time as dashboard requests.
# SQLAlchemy's default QueuePool is only 5 + 10 overflow connections, so a
# burst can exhaust the local pool and make the dashboard time out even though
# PostgreSQL itself is healthy.  PostgreSQL connections here are short-lived
# and every SessionLocal user closes its session, so NullPool is a safer fit:
# each request/task gets its own connection and closing the session releases it
# immediately instead of occupying a process-local pool slot.
if database_url.startswith("sqlite"):
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False},
    )
else:
    engine = create_engine(
        database_url,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args={},
    )

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def init_db():
    from . import models  # noqa: F401
    Base.metadata.create_all(engine)

"""
Database connection and session management for PostgreSQL.

Uses SQLAlchemy sync engine. DATABASE_URL must be set (e.g. in Docker or .env).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from app.db_models import Base

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/immunostream",
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    """Create all tables. Safe to call on existing DB."""
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Yield a DB session; commits on success, rolls back on exception."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db_session() -> Session:
    """Return a session that caller must close/commit. Prefer get_db() in new code."""
    return SessionLocal()


def get_db_dependency() -> Generator[Session, None, None]:
    """FastAPI dependency: yield a session, commit on success, rollback on exception."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

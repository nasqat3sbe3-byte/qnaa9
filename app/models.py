from datetime import date, datetime
from sqlalchemy import String, Date, DateTime, Float, Integer, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base

class Stock(Base):
    __tablename__ = "stocks"
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Split(Base):
    __tablename__ = "splits"
    __table_args__ = (UniqueConstraint("stock_id", "effective_date", name="uq_split_stock_date"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"), index=True)
    effective_date: Mapped[date] = mapped_column(Date, index=True)
    split_from: Mapped[float] = mapped_column(Float, default=1)
    split_to: Mapped[float] = mapped_column(Float)
    split_type: Mapped[str] = mapped_column(String(16), default="reverse")
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    post_split_open: Mapped[float | None] = mapped_column(Float)
    post_split_high: Mapped[float | None] = mapped_column(Float)
    post_split_low: Mapped[float | None] = mapped_column(Float)
    current_price: Mapped[float | None] = mapped_column(Float)
    distance_from_low_pct: Mapped[float | None] = mapped_column(Float)
    half_level: Mapped[float | None] = mapped_column(Float)
    half_level_reached: Mapped[bool] = mapped_column(Boolean, default=False)
    stability_sessions: Mapped[int] = mapped_column(Integer, default=0)
    last_low_date: Mapped[date | None] = mapped_column(Date)
    ready_score: Mapped[float | None] = mapped_column(Float)
    stock: Mapped[Stock] = relationship()

class DailyBar(Base):
    __tablename__ = "daily_bars"
    __table_args__ = (UniqueConstraint("stock_id", "trade_date", name="uq_bar_stock_date"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"), index=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float | None] = mapped_column(Float)

class FourHourBar(Base):
    __tablename__ = "four_hour_bars"
    __table_args__ = (UniqueConstraint("stock_id", "bar_time", name="uq_4h_bar_stock_time"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"), index=True)
    bar_time: Mapped[datetime] = mapped_column(DateTime, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(32), default="yahoo_1h")

class BorrowSnapshot(Base):
    __tablename__ = "borrow_snapshots"
    id: Mapped[int] = mapped_column(primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    available_shares: Mapped[float | None] = mapped_column(Float)
    fee_rate: Mapped[float | None] = mapped_column(Float)
    rebate_rate: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(64))

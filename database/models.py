# database/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime, timedelta, timezone

Base = declarative_base()


def kst_now():
    return datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None)


class Position(Base):
    __tablename__ = "positions"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    exchange = Column(String(20), nullable=False)  # Renamed from 'spot_exchange'
    status = Column(String(20), default="OPEN")

    # State Management (Live Tracking)
    current_spot_qty = Column(Float)
    current_hedge_qty = Column(Float)

    # Entry Snapshot
    entry_time = Column(DateTime)
    entry_spot_qty = Column(Float)
    entry_spot_price = Column(Float)
    entry_spot_amount_krw = Column(Float)
    entry_hedge_qty = Column(Float)
    entry_hedge_price = Column(Float)
    entry_hedge_amount_usdt = Column(Float)

    # Exit Snapshot
    exit_time = Column(DateTime, nullable=True)
    exit_spot_qty = Column(Float, nullable=True)
    exit_spot_price = Column(Float, nullable=True)
    exit_spot_amount_krw = Column(Float, nullable=True)
    exit_hedge_qty = Column(Float, nullable=True)
    exit_hedge_price = Column(Float, nullable=True)
    exit_hedge_amount_usdt = Column(Float, nullable=True)

    # Performance / Audit
    gross_spot_pnl_krw = Column(Float, nullable=True)
    gross_hedge_pnl_usdt = Column(Float, nullable=True)
    total_fees_usdt = Column(Float, nullable=True)
    net_pnl_usdt = Column(Float, nullable=True)

    # Rates & Config
    entry_usdt_rate = Column(Float)
    exit_usdt_rate = Column(Float, nullable=True)
    config_entry_threshold = Column(Float)
    config_exit_threshold = Column(Float, nullable=True)
    calc_entry_premium = Column(Float)
    calc_exit_premium = Column(Float, nullable=True)

    # Integrity (Order IDs)
    entry_spot_order_id = Column(String(100))
    entry_hedge_order_id = Column(String(100))
    exit_spot_order_id = Column(String(100), nullable=True)
    exit_hedge_order_id = Column(String(100), nullable=True)


class StrategyCollector(Base):
    __tablename__ = "strategy_collector"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

    symbol = Column(String(20), index=True)
    spot_exchange = Column(String(20))

    # Spot Prices
    spot_bid_price = Column(Float)
    spot_ask_price = Column(Float)
    spot_bid_size = Column(Float)

    # Binance Prices
    binance_bid_price = Column(Float)
    binance_ask_price = Column(Float)  # Required for Active Maker Exit
    binance_mark_price = Column(Float)

    # Funding
    funding_rate = Column(Float)
    next_funding_time = Column(DateTime)
    annualized_funding_pct = Column(Float)

    # Analysis
    implied_fx = Column(Float)
    kimchi_premium_pct = Column(Float)
    entry_premium_pct = Column(Float)
    exit_premium_pct = Column(Float)

    ref_usdt_ask = Column(Float)


class ExchangeRules(Base):
    __tablename__ = "exchange_rules"
    symbol = Column(String(50), primary_key=True)
    spot_exchange = Column(String(20), primary_key=True, default="UPBIT")

    binance_step_size = Column(Float)
    binance_tick_size = Column(Float)
    binance_min_qty = Column(Float)
    binance_min_notional = Column(Float)

    spot_step_size = Column(Float)
    spot_min_notional = Column(Float)

    updated_at = Column(DateTime, default=kst_now)


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=kst_now)
    total_usdt_value = Column(Float)
    total_krw_value = Column(Float)
    binance_usdt_free = Column(Float)
    binance_unrealized_pnl = Column(Float)
    spot_krw_free = Column(Float)
    fx_rate = Column(Float)
    details = Column(Text)
    inventory_val_usdt = Column(Float)


class MarketMetric(Base):
    __tablename__ = "market_metrics"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    exchange = Column(String(20))
    median_entry_premium = Column(Float)
    median_exit_premium = Column(Float)
    opportunity_count = Column(Integer)

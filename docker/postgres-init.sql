-- ============================================================
-- TradePulse — PostgreSQL Init Script
-- Event Store + Order Write DB
-- ============================================================

-- ─────────────────────────────────────────
-- EVENT STORE (Event Sourcing için)
-- Her domain event append-only olarak buraya yazılır
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_store (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    aggregate_id    UUID        NOT NULL,          -- hangi entity'e ait (order_id, portfolio_id)
    aggregate_type  VARCHAR(100) NOT NULL,          -- 'Order', 'Portfolio', 'Position'
    event_type      VARCHAR(200) NOT NULL,          -- 'OrderPlaced', 'OrderExecuted', 'PriceUpdated'
    event_version   INT         NOT NULL DEFAULT 1,
    payload         JSONB       NOT NULL,           -- event verisi
    metadata        JSONB,                          -- correlation_id, causation_id, user_id
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kafka_offset    BIGINT,                         -- Kafka offset ile eşleme
    kafka_partition INT,
    kafka_topic     VARCHAR(200)
);

-- Sorgu performansı için index'ler
CREATE INDEX idx_event_store_aggregate    ON event_store (aggregate_id, event_version);
CREATE INDEX idx_event_store_type         ON event_store (event_type);
CREATE INDEX idx_event_store_occurred_at  ON event_store (occurred_at DESC);

-- ─────────────────────────────────────────
-- ORDERS — CQRS Write Side
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID        NOT NULL,
    symbol          VARCHAR(20) NOT NULL,    -- 'AAPL', 'MSFT', 'GOOGL'
    order_type      VARCHAR(10) NOT NULL,    -- 'BUY' | 'SELL'
    quantity        DECIMAL(18,8) NOT NULL,
    price           DECIMAL(18,4),           -- NULL ise market order
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                                             -- PENDING | FILLED | PARTIAL | CANCELLED | REJECTED
    filled_quantity DECIMAL(18,8) DEFAULT 0,
    filled_price    DECIMAL(18,4),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version         INT         NOT NULL DEFAULT 1  -- optimistic concurrency
);

CREATE INDEX idx_orders_user_id   ON orders (user_id);
CREATE INDEX idx_orders_symbol    ON orders (symbol);
CREATE INDEX idx_orders_status    ON orders (status);
CREATE INDEX idx_orders_created   ON orders (created_at DESC);

-- ─────────────────────────────────────────
-- TRADE EXECUTIONS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_executions (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID        NOT NULL REFERENCES orders(id),
    symbol          VARCHAR(20) NOT NULL,
    quantity        DECIMAL(18,8) NOT NULL,
    price           DECIMAL(18,4) NOT NULL,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kafka_offset    BIGINT
);

CREATE INDEX idx_executions_order_id ON trade_executions (order_id);
CREATE INDEX idx_executions_symbol   ON trade_executions (symbol);

-- ─────────────────────────────────────────
-- OUTBOX PATTERN — Kafka publish garantisi
-- Servis önce DB'ye yazar, sonra Kafka'ya gönderir
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox_messages (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           VARCHAR(200) NOT NULL,
    key             VARCHAR(200),
    payload         JSONB       NOT NULL,
    headers         JSONB,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING | SENT | FAILED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ,
    retry_count     INT         NOT NULL DEFAULT 0,
    error_message   TEXT
);

CREATE INDEX idx_outbox_status     ON outbox_messages (status, created_at);

-- ─────────────────────────────────────────
-- SEED DATA — Test sembolleri
-- ─────────────────────────────────────────
INSERT INTO orders (id, user_id, symbol, order_type, quantity, price, status)
VALUES
    (gen_random_uuid(), gen_random_uuid(), 'AAPL',  'BUY',  10, 185.50, 'PENDING'),
    (gen_random_uuid(), gen_random_uuid(), 'MSFT',  'BUY',   5, 420.00, 'FILLED'),
    (gen_random_uuid(), gen_random_uuid(), 'GOOGL', 'SELL',  2, 175.00, 'PENDING');

RAISE NOTICE 'TradePulse DB init tamamlandı ✓';

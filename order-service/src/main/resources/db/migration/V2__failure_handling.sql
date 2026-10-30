-- M4 failure handling.
-- FAILED replaces PAYMENT_FAILED: a payment failure and a saga timeout both end FAILED, and
-- failure_reason says which (provider reason or TIMEOUT). RESERVED is accepted as an active state
-- next to PENDING; the reaper expires both.
UPDATE orders.orders SET status = 'FAILED' WHERE status = 'PAYMENT_FAILED';
ALTER TABLE orders.orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders.orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('PENDING', 'RESERVED', 'CONFIRMED', 'FAILED', 'REJECTED'));

-- The reaper scans active orders by age.
CREATE INDEX orders_active_by_created_at ON orders.orders (created_at)
    WHERE status IN ('PENDING', 'RESERVED');

-- Transactional outbox: written in the same transaction as the order status change, published
-- by OutboxRelay, which sets published_at only after Kafka acknowledged the record.
CREATE TABLE orders.outbox (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        text NOT NULL,
    msg_key      text NOT NULL,
    type         text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished_by_created_at ON orders.outbox (created_at)
    WHERE published_at IS NULL;

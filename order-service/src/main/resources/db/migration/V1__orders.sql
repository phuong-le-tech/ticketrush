-- Etat de la saga. Les colonnes de la requete sont nullables: une ligne peut naitre d'un
-- PaymentResult ou d'un ReservationRejected arrive avant OrderRequested (topics differents).
CREATE TABLE orders.orders (
    order_id        uuid PRIMARY KEY,
    idempotency_key text,
    event_id        text,
    customer_id     text,
    quantity        int CHECK (quantity BETWEEN 1 AND 4),
    status          text NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'PAYMENT_FAILED', 'REJECTED')),
    amount_cents    bigint,
    currency        char(3),
    failure_reason  text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

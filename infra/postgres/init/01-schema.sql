-- StreamHouse — OLTP source schema.
--
-- Runs automatically on the first boot of an empty postgres volume
-- (mounted at /docker-entrypoint-initdb.d), and re-applied on demand by `make db-init`.
-- Every statement is idempotent, so re-running is safe.
--
-- wal_level=logical, max_replication_slots and max_wal_senders are set on the postgres
-- command line in docker-compose.yml rather than here. They are live from first boot and
-- need no restart. See docs/decisions/0001-why-debezium.md.
--
-- Conventions: money is NUMERIC(10,2) suffixed _inr; timestamps are TIMESTAMPTZ suffixed _ts.
-- `updated_at` is written by the generator, not by a trigger — Phase 1 chaos scenarios
-- deliberately emit out-of-order timestamps, which a trigger would silently overwrite.

BEGIN;

-- ---------------------------------------------------------------------- customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id     BIGSERIAL   PRIMARY KEY,
    name            TEXT        NOT NULL,
    phone           TEXT        NOT NULL,
    city            TEXT        NOT NULL,
    signup_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
    tier            TEXT        NOT NULL DEFAULT 'STANDARD'
                                CHECK (tier IN ('STANDARD', 'PLUS', 'PRO')),
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- restaurants
-- SCD2 target: is_open, rating and commission_pct change over time.
CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id   BIGSERIAL   PRIMARY KEY,
    name            TEXT        NOT NULL,
    city            TEXT        NOT NULL,
    lat             NUMERIC(9,6) NOT NULL,
    lon             NUMERIC(9,6) NOT NULL,
    cuisine         TEXT        NOT NULL,
    rating          NUMERIC(2,1) NOT NULL DEFAULT 4.0
                                CHECK (rating >= 0 AND rating <= 5),
    commission_pct  NUMERIC(5,2) NOT NULL DEFAULT 20.00
                                CHECK (commission_pct >= 0 AND commission_pct <= 100),
    is_open         BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- menu_items
-- SCD2 target: price_inr changes are the canonical price-history example.
CREATE TABLE IF NOT EXISTS menu_items (
    menu_item_id    BIGSERIAL   PRIMARY KEY,
    restaurant_id   BIGINT      NOT NULL REFERENCES restaurants (restaurant_id),
    name            TEXT        NOT NULL,
    price_inr       NUMERIC(10,2) NOT NULL CHECK (price_inr >= 0),
    is_available    BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- riders
-- SCD2 target: tier and vehicle_type change over time.
CREATE TABLE IF NOT EXISTS riders (
    rider_id        BIGSERIAL   PRIMARY KEY,
    name            TEXT        NOT NULL,
    city            TEXT        NOT NULL,
    vehicle_type    TEXT        NOT NULL DEFAULT 'BIKE'
                                CHECK (vehicle_type IN ('BIKE', 'SCOOTER', 'BICYCLE', 'CAR')),
    tier            TEXT        NOT NULL DEFAULT 'BRONZE'
                                CHECK (tier IN ('BRONZE', 'SILVER', 'GOLD')),
    shift_start     TIMESTAMPTZ,
    is_online       BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- orders
-- The heavily mutated table. Every row walks the state machine:
--   PLACED -> ACCEPTED -> PICKED_UP -> DELIVERED | CANCELLED
-- Timestamp columns fill in as each transition occurs, so a row's history is
-- reconstructable from the CDC stream alone.
CREATE TABLE IF NOT EXISTS orders (
    order_id          BIGSERIAL   PRIMARY KEY,
    customer_id       BIGINT      NOT NULL REFERENCES customers (customer_id),
    restaurant_id     BIGINT      NOT NULL REFERENCES restaurants (restaurant_id),
    rider_id          BIGINT      REFERENCES riders (rider_id),   -- NULL until ACCEPTED
    status            TEXT        NOT NULL DEFAULT 'PLACED'
                                  CHECK (status IN ('PLACED', 'ACCEPTED', 'PICKED_UP',
                                                    'DELIVERED', 'CANCELLED')),
    placed_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_ts       TIMESTAMPTZ,
    picked_up_ts      TIMESTAMPTZ,
    delivered_ts      TIMESTAMPTZ,
    promised_ts       TIMESTAMPTZ NOT NULL,
    cancel_reason     TEXT,
    subtotal_inr      NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (subtotal_inr >= 0),
    delivery_fee_inr  NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (delivery_fee_inr >= 0),
    discount_inr      NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (discount_inr >= 0),
    total_inr         NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (total_inr >= 0),
    -- Deliberately NOT a foreign key: payments.order_id already references orders,
    -- and a second FK the other way would make the pair circular and un-insertable.
    payment_id        BIGINT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- order_items
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   BIGSERIAL   PRIMARY KEY,
    order_id        BIGINT      NOT NULL REFERENCES orders (order_id),
    menu_item_id    BIGINT      NOT NULL REFERENCES menu_items (menu_item_id),
    qty             INTEGER     NOT NULL CHECK (qty > 0),
    unit_price_inr  NUMERIC(10,2) NOT NULL CHECK (unit_price_inr >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- payments
CREATE TABLE IF NOT EXISTS payments (
    payment_id      BIGSERIAL   PRIMARY KEY,
    order_id        BIGINT      NOT NULL REFERENCES orders (order_id),
    method          TEXT        NOT NULL
                                CHECK (method IN ('UPI', 'CARD', 'NETBANKING', 'COD', 'WALLET')),
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                                CHECK (status IN ('PENDING', 'AUTHORISED', 'CAPTURED',
                                                  'FAILED', 'REFUNDED')),
    amount_inr      NUMERIC(10,2) NOT NULL CHECK (amount_inr >= 0),
    gateway_ref     TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------- indexes
-- Foreign keys are not indexed automatically in Postgres. The generator reads by these.
CREATE INDEX IF NOT EXISTS idx_menu_items_restaurant  ON menu_items  (restaurant_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer        ON orders      (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_restaurant      ON orders      (restaurant_id);
CREATE INDEX IF NOT EXISTS idx_orders_rider           ON orders      (rider_id);
CREATE INDEX IF NOT EXISTS idx_orders_status          ON orders      (status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at      ON orders      (updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order      ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_menu_item  ON order_items (menu_item_id);
CREATE INDEX IF NOT EXISTS idx_payments_order         ON payments    (order_id);

-- ---------------------------------------------------------------------- replica identity
-- Without this, Debezium emits only the primary key in the `before` block on UPDATE,
-- which makes before/after deltas impossible to compute. Costs extra WAL volume on every
-- orders update — a deliberate trade, recorded in ADR-0001.
ALTER TABLE orders REPLICA IDENTITY FULL;

COMMIT;

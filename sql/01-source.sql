-- Synthetic instructor example; students design their own source model.
CREATE TABLE orders (
 order_id text PRIMARY KEY,
 ordered_at timestamptz NOT NULL,
 status text NOT NULL
);
CREATE TABLE payments (
 payment_id text PRIMARY KEY,
 order_id text NOT NULL REFERENCES orders(order_id),
 amount bigint NOT NULL CHECK(amount >= 0),
 status text NOT NULL
);
CREATE TABLE refunds (
 refund_id text PRIMARY KEY,
 payment_id text NOT NULL REFERENCES payments(payment_id),
 amount bigint NOT NULL CHECK(amount >= 0),
 status text NOT NULL
);
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
ALTER TABLE refunds REPLICA IDENTITY FULL;
CREATE PUBLICATION cdc_lab FOR TABLE orders, payments, refunds;
INSERT INTO orders VALUES ('O-001','2026-10-01T10:00:00+09:00','confirmed');
INSERT INTO payments VALUES ('P-001','O-001',100000,'success');

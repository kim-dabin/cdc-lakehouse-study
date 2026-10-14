-- Instructor: psql -v namespace=s01-instructor-20261014a
-- Use a fresh namespace for each rehearsal. No shared-row resets.
BEGIN;
INSERT INTO orders (order_id, ordered_at, status)
VALUES (:'namespace' || '-o1', '2026-10-19T10:00:00+09:00', 'confirmed');
INSERT INTO payments (payment_id, order_id, amount, status)
VALUES (:'namespace' || '-p1', :'namespace' || '-o1', 100000, 'success');
COMMIT;

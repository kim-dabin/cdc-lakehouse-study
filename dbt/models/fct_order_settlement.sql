-- Gold serving table: one row per order, built from the Silver core.
-- Aggregate payments and refunds per order first to avoid join fan-out.
with payments as (
  select order_id, sum(amount) paid
  from {{ ref('fct_payment') }}
  where status='success' group by order_id
), refunds as (
  select order_id, sum(amount) refunded
  from {{ ref('fct_refund') }}
  where status='success' and payment_status='success'
  group by order_id
)
select o.order_id, o.order_date,
       coalesce(p.paid,0) paid, coalesce(r.refunded,0) refunded,
       coalesce(p.paid,0)-coalesce(r.refunded,0) net_paid,
       current_timestamp() mart_built_at
from {{ ref('dim_order') }} o
left join payments p on o.order_id=p.order_id
left join refunds r on o.order_id=r.order_id

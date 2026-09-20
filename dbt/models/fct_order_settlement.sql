-- Aggregate payments and refunds independently to avoid join fan-out.
with payments as (
  select order_id, sum(amount) paid
  from {{ source('silver','payments') }}
  where not is_deleted and status='success' group by order_id
), refunds as (
  select p.order_id, sum(r.amount) refunded
  from {{ source('silver','refunds') }} r
  join {{ source('silver','payments') }} p on r.payment_id=p.payment_id
  where not r.is_deleted and not p.is_deleted and r.status='success' and p.status='success'
  group by p.order_id
)
select o.order_id, to_date(from_utc_timestamp(o.ordered_at,'Asia/Seoul')) order_date,
       coalesce(p.paid,0) paid, coalesce(r.refunded,0) refunded,
       coalesce(p.paid,0)-coalesce(r.refunded,0) net_paid,
       current_timestamp() mart_built_at
from {{ source('silver','orders') }} o
left join payments p on o.order_id=p.order_id
left join refunds r on o.order_id=r.order_id
where not o.is_deleted
